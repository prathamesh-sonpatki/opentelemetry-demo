#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
import time
from concurrent import futures

# Pip
import grpc
from opentelemetry import trace, metrics
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter,
)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider

from openfeature.contrib.hook.opentelemetry import TracingHook

# Local
import logging
import demo_pb2
import demo_pb2_grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from metrics import (
    init_metrics
)

cached_ids = []
first_run = True

# Circuit breaker state management
circuit_breaker_state = {
    'failures': 0,
    'last_failure_time': 0,
    'open': False,
    'max_failures': 5,
    'timeout': 60  # seconds to wait before trying again
}


def reset_circuit_breaker():
    """Reset circuit breaker after successful connection"""
    circuit_breaker_state['failures'] = 0
    circuit_breaker_state['open'] = False
    circuit_breaker_state['last_failure_time'] = 0


def open_circuit_breaker():
    """Open circuit breaker after too many failures"""
    circuit_breaker_state['open'] = True
    circuit_breaker_state['last_failure_time'] = time.time()
    logger.warning("Circuit breaker opened - too many connection failures to product catalog service")


def should_attempt_connection():
    """Check if we should attempt connection based on circuit breaker state"""
    if not circuit_breaker_state['open']:
        return True
    
    # Check if timeout has elapsed to try again
    elapsed = time.time() - circuit_breaker_state['last_failure_time']
    if elapsed > circuit_breaker_state['timeout']:
        logger.info("Circuit breaker timeout elapsed, attempting to reconnect")
        return True
    
    return False


def call_product_catalog_with_retry(max_retries=3, base_delay=1):
    """
    Call product catalog service with exponential backoff retry logic
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
    
    Returns:
        List of product IDs or None if all retries failed
    """
    if not should_attempt_connection():
        logger.warning("Circuit breaker is open, skipping product catalog call")
        return None
    
    for attempt in range(max_retries):
        try:
            # Add timeout to prevent hanging connections
            cat_response = product_catalog_stub.ListProducts(
                demo_pb2.Empty(),
                timeout=5.0  # 5 second timeout
            )
            response_ids = [x.id for x in cat_response.products]
            
            # Connection successful - reset circuit breaker
            if circuit_breaker_state['failures'] > 0:
                logger.info("Successfully connected to product catalog service after previous failures")
                reset_circuit_breaker()
            
            return response_ids
            
        except grpc.RpcError as e:
            circuit_breaker_state['failures'] += 1
            status_code = e.code() if hasattr(e, 'code') else 'UNKNOWN'
            
            logger.warning(
                f"Product catalog connection attempt {attempt + 1}/{max_retries} failed: "
                f"status={status_code}, details={e.details() if hasattr(e, 'details') else str(e)}"
            )
            
            # Check if we should open circuit breaker
            if circuit_breaker_state['failures'] >= circuit_breaker_state['max_failures']:
                open_circuit_breaker()
                return None
            
            # Don't retry on final attempt
            if attempt < max_retries - 1:
                # Exponential backoff: 1s, 2s, 4s
                delay = base_delay * (2 ** attempt)
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(
                    f"All {max_retries} attempts to connect to product catalog failed. "
                    "Falling back to cached recommendations."
                )
                
        except Exception as e:
            logger.error(f"Unexpected error calling product catalog: {type(e).__name__}: {str(e)}")
            circuit_breaker_state['failures'] += 1
            return None
    
    return None


class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        prod_list = get_product_list(request.product_ids)
        span = trace.get_current_span()
        span.set_attribute("app.products_recommended.count", len(prod_list))
        logger.info(f"Receive ListRecommendations for product ids:{prod_list}")

        # build and return response
        response = demo_pb2.ListRecommendationsResponse()
        response.product_ids.extend(prod_list)

        # Collect metrics for this service
        rec_svc_metrics["app_recommendations_counter"].add(len(prod_list), {'recommendation.type': 'catalog'})

        return response

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


def get_product_list(request_product_ids):
    global first_run
    global cached_ids
    with tracer.start_as_current_span("get_product_list") as span:
        max_responses = 5

        # Formulate the list of characters to list of strings
        request_product_ids_str = ''.join(request_product_ids)
        request_product_ids = request_product_ids_str.split(',')

        # Feature flag scenario - Cache Leak
        if check_feature_flag("recommendationCacheFailure"):
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss")
                
                # Use retry logic for product catalog call
                response_ids = call_product_catalog_with_retry()
                
                if response_ids:
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    # Fall back to cached IDs if available
                    if cached_ids:
                        logger.info("Using cached product IDs as fallback")
                        span.set_attribute("app.fallback_to_cache", True)
                        product_ids = cached_ids
                    else:
                        # Return empty list if no cache available
                        logger.warning("No cached products available, returning empty recommendations")
                        span.set_attribute("app.no_products_available", True)
                        return []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            # Use retry logic for product catalog call
            response_ids = call_product_catalog_with_retry()
            
            if response_ids:
                product_ids = response_ids
                # Update cache for future fallback use
                if not cached_ids:
                    cached_ids = response_ids
            else:
                # Fall back to cached IDs if available
                if cached_ids:
                    logger.info("Product catalog unavailable, using cached product IDs")
                    span.set_attribute("app.fallback_to_cache", True)
                    product_ids = cached_ids
                else:
                    # Return empty list if no cache available
                    logger.warning("Product catalog unavailable and no cache, returning empty recommendations")
                    span.set_attribute("app.no_products_available", True)
                    return []

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Sample list of indicies to return
        if num_products > 0:
            indices = random.sample(range(num_products), num_return)
            # Fetch product ids from indices
            prod_list = [filtered_products[i] for i in indices]
        else:
            prod_list = []

        span.set_attribute("app.filtered_products.list", prod_list)

        return prod_list


def must_map_env(key: str):
    value = os.environ.get(key)
    if value is None:
        raise Exception(f'{key} environment variable must be set')
    return value


def check_feature_flag(flag_name: str):
    # Initialize OpenFeature
    client = api.get_client()
    return client.get_boolean_value("recommendationCacheFailure", False)


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    api.set_provider(FlagdProvider(host=os.environ.get('FLAGD_HOST', 'flagd'), port=os.environ.get('FLAGD_PORT', 8013)))
    api.add_hooks([TracingHook()])

    # Initialize Traces and Metrics
    tracer = trace.get_tracer_provider().get_tracer(service_name)
    meter = metrics.get_meter_provider().get_meter(service_name)
    rec_svc_metrics = init_metrics(meter)

    # Initialize Logs
    logger_provider = LoggerProvider(
        resource=Resource.create(
            {
                'service.name': service_name,
            }
        ),
    )
    set_logger_provider(logger_provider)
    log_exporter = OTLPLogExporter(insecure=True)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

    # Attach OTLP handler to logger
    logger = logging.getLogger('main')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    catalog_addr = must_map_env('PRODUCT_CATALOG_ADDR')
    logger.info(f'Connecting to product catalog at: {catalog_addr}')
    
    # Configure gRPC channel with better defaults for resilience
    channel_options = [
        ('grpc.keepalive_time_ms', 10000),  # Send keepalive ping every 10 seconds
        ('grpc.keepalive_timeout_ms', 5000),  # Wait 5 seconds for keepalive response
        ('grpc.keepalive_permit_without_calls', True),  # Allow keepalive pings when no calls
        ('grpc.http2.max_pings_without_data', 0),  # Allow unlimited pings without data
        ('grpc.http2.min_time_between_pings_ms', 10000),  # Minimum time between pings
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),  # Minimum ping interval
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=channel_options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)

    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add class to gRPC server
    service = RecommendationService()
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_pb2_grpc.add_HealthServicer_to_server(service, server)

    # Start server
    port = must_map_env('RECOMMENDATION_PORT')
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f'Recommendation service started, listening on port {port}')
    server.wait_for_termination()
