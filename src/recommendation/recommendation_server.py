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

# Circuit breaker state for graceful degradation
circuit_breaker = {
    'failures': 0,
    'last_failure_time': 0,
    'state': 'closed',  # closed, open, half_open
    'failure_threshold': 5,
    'timeout': 60  # seconds to wait before retrying in open state
}

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


def call_product_catalog_with_retry(max_retries=3, initial_delay=0.1):
    """
    Call ProductCatalog service with exponential backoff retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        
    Returns:
        List of product IDs or empty list if all retries fail
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            span = trace.get_current_span()
            span.set_attribute("app.catalog_call.attempt", attempt + 1)
            span.set_attribute("app.catalog_call.max_retries", max_retries)
            
            # Make the gRPC call
            cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
            product_ids = [x.id for x in cat_response.products]
            
            # Success - reset circuit breaker
            reset_circuit_breaker()
            span.set_attribute("app.catalog_call.success", True)
            logger.info(f"Successfully retrieved {len(product_ids)} products from catalog")
            
            return product_ids
            
        except grpc.RpcError as e:
            last_exception = e
            status_code = e.code()
            
            span = trace.get_current_span()
            span.set_attribute("app.catalog_call.error", True)
            span.set_attribute("app.catalog_call.error_code", str(status_code))
            span.set_attribute("app.catalog_call.error_details", e.details())
            
            # Record the failure in circuit breaker
            record_circuit_breaker_failure()
            
            if attempt < max_retries - 1:
                logger.warning(
                    f"ProductCatalog call failed (attempt {attempt + 1}/{max_retries}): "
                    f"Status={status_code}, Details={e.details()}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(
                    f"ProductCatalog call failed after {max_retries} attempts: "
                    f"Status={status_code}, Details={e.details()}. "
                    f"Falling back to empty recommendations."
                )
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error calling ProductCatalog: {type(e).__name__}: {str(e)}")
            record_circuit_breaker_failure()
            
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
    
    # All retries failed - return empty list for graceful degradation
    span = trace.get_current_span()
    span.set_attribute("app.catalog_call.all_retries_failed", True)
    span.set_attribute("app.fallback.used", True)
    
    return []


def check_circuit_breaker():
    """
    Check if circuit breaker allows the request to proceed.
    
    Returns:
        bool: True if request should be allowed, False otherwise
    """
    current_time = time.time()
    
    if circuit_breaker['state'] == 'open':
        # Check if timeout has passed
        if current_time - circuit_breaker['last_failure_time'] > circuit_breaker['timeout']:
            circuit_breaker['state'] = 'half_open'
            logger.info("Circuit breaker transitioning to half-open state")
            return True
        else:
            logger.warning("Circuit breaker is OPEN - skipping ProductCatalog call")
            return False
    
    return True


def record_circuit_breaker_failure():
    """Record a failure in the circuit breaker."""
    circuit_breaker['failures'] += 1
    circuit_breaker['last_failure_time'] = time.time()
    
    if circuit_breaker['failures'] >= circuit_breaker['failure_threshold']:
        if circuit_breaker['state'] != 'open':
            circuit_breaker['state'] = 'open'
            logger.error(
                f"Circuit breaker OPENED after {circuit_breaker['failures']} failures. "
                f"Will retry after {circuit_breaker['timeout']}s"
            )


def reset_circuit_breaker():
    """Reset the circuit breaker after successful call."""
    if circuit_breaker['failures'] > 0:
        logger.info(f"Circuit breaker reset after {circuit_breaker['failures']} previous failures")
    circuit_breaker['failures'] = 0
    circuit_breaker['state'] = 'closed'


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
                
                # Check circuit breaker before making call
                if not check_circuit_breaker():
                    # Circuit breaker is open - use cached fallback if available
                    span.set_attribute("app.circuit_breaker.open", True)
                    if cached_ids:
                        logger.warning("Using cached product IDs due to open circuit breaker")
                        product_ids = cached_ids
                    else:
                        logger.warning("No cached IDs available and circuit breaker is open")
                        return []
                else:
                    # Call with retry logic
                    response_ids = call_product_catalog_with_retry()
                    
                    if response_ids:
                        cached_ids = cached_ids + response_ids
                        cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                        product_ids = cached_ids
                    else:
                        # Fallback to cached IDs if available
                        if cached_ids:
                            logger.warning("ProductCatalog unavailable, using cached product IDs")
                            product_ids = cached_ids
                        else:
                            logger.error("ProductCatalog unavailable and no cached IDs available")
                            return []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            # Check circuit breaker before making call
            if not check_circuit_breaker():
                span.set_attribute("app.circuit_breaker.open", True)
                logger.warning("Circuit breaker is open, returning empty recommendations")
                return []
            
            # Call with retry logic
            product_ids = call_product_catalog_with_retry()
            
            # Graceful degradation - return empty list if catalog is unavailable
            if not product_ids:
                logger.warning("ProductCatalog unavailable, returning empty recommendations")
                span.set_attribute("app.graceful_degradation", True)
                return []

        span.set_attribute("app.products.count", len(product_ids))
        span.set_attribute("app.circuit_breaker.state", circuit_breaker['state'])
        span.set_attribute("app.circuit_breaker.failures", circuit_breaker['failures'])

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Sample list of indicies to return
        indices = random.sample(range(num_products), num_return) if num_products > 0 else []
        # Fetch product ids from indices
        prod_list = [filtered_products[i] for i in indices]

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

    catalog_addr = must_map_env('PRODUCT_CATALOG_ADDR')
    pc_channel = grpc.insecure_channel(catalog_addr)
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
