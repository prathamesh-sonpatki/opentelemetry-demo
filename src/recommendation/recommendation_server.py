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

# Circuit breaker state
circuit_breaker = {
    'state': 'closed',  # closed, open, half_open
    'failure_count': 0,
    'last_failure_time': None,
    'threshold': 5,  # Open circuit after 5 failures
    'timeout': 30,  # Try again after 30 seconds
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


def check_circuit_breaker():
    """Check if circuit breaker allows the request to proceed"""
    if circuit_breaker['state'] == 'open':
        # Check if timeout has passed
        if time.time() - circuit_breaker['last_failure_time'] > circuit_breaker['timeout']:
            circuit_breaker['state'] = 'half_open'
            logger.info("Circuit breaker state changed to half_open")
            return True
        return False
    return True


def record_success():
    """Record successful call"""
    circuit_breaker['failure_count'] = 0
    if circuit_breaker['state'] == 'half_open':
        circuit_breaker['state'] = 'closed'
        logger.info("Circuit breaker state changed to closed")


def record_failure():
    """Record failed call and update circuit breaker state"""
    circuit_breaker['failure_count'] += 1
    circuit_breaker['last_failure_time'] = time.time()
    
    if circuit_breaker['failure_count'] >= circuit_breaker['threshold']:
        circuit_breaker['state'] = 'open'
        logger.warning(f"Circuit breaker opened after {circuit_breaker['failure_count']} failures")


def call_product_catalog_with_retry(max_retries=3, initial_delay=0.1):
    """
    Call ProductCatalog service with retry logic and exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        
    Returns:
        List of product IDs or None if all retries failed
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            # Check circuit breaker before attempting call
            if not check_circuit_breaker():
                logger.warning("Circuit breaker is open, using fallback")
                return None
            
            # Set a reasonable timeout for the gRPC call
            cat_response = product_catalog_stub.ListProducts(
                demo_pb2.Empty(),
                timeout=5.0  # 5 second timeout
            )
            product_ids = [x.id for x in cat_response.products]
            
            # Record success and return
            record_success()
            logger.info(f"Successfully retrieved {len(product_ids)} products from catalog")
            return product_ids
            
        except grpc.RpcError as e:
            last_exception = e
            
            # Log the specific error
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                logger.warning(
                    f"ProductCatalog service unavailable (attempt {attempt + 1}/{max_retries}): {e.details()}"
                )
            else:
                logger.error(
                    f"gRPC error calling ProductCatalog (attempt {attempt + 1}/{max_retries}): "
                    f"code={e.code()}, details={e.details()}"
                )
            
            # Record failure
            record_failure()
            
            # Don't retry on certain error codes
            if e.code() in [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.UNAUTHENTICATED]:
                logger.error(f"Non-retryable error: {e.code()}")
                break
            
            # Wait before retrying (exponential backoff)
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error calling ProductCatalog: {type(e).__name__}: {str(e)}")
            record_failure()
            break
    
    # All retries failed
    logger.error(f"Failed to call ProductCatalog after {max_retries} attempts: {str(last_exception)}")
    return None


def get_fallback_product_ids():
    """
    Provide fallback product IDs when ProductCatalog is unavailable
    Uses cached IDs if available, otherwise returns a default set
    """
    global cached_ids
    
    if cached_ids:
        logger.info(f"Using {len(cached_ids)} cached product IDs as fallback")
        return cached_ids
    else:
        # Default fallback product IDs
        default_products = [
            "OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", 
            "L9ECAV7KIM", "2ZYFJ3GM2N", "0PUK6V6EV0",
            "9SIQT8TOJO", "6E92ZMYYFZ"
        ]
        logger.info(f"Using {len(default_products)} default product IDs as fallback")
        return default_products


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
                
                # Call with retry logic
                product_ids = call_product_catalog_with_retry()
                
                # Use fallback if call failed
                if product_ids is None:
                    span.set_attribute("app.catalog_call_failed", True)
                    span.set_attribute("app.using_fallback", True)
                    product_ids = get_fallback_product_ids()
                else:
                    span.set_attribute("app.catalog_call_failed", False)
                    # Update cache with fresh data
                    cached_ids = cached_ids + product_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids if cached_ids else get_fallback_product_ids()
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            # Call with retry logic
            product_ids = call_product_catalog_with_retry()
            
            # Use fallback if call failed
            if product_ids is None:
                span.set_attribute("app.catalog_call_failed", True)
                span.set_attribute("app.using_fallback", True)
                product_ids = get_fallback_product_ids()
                
                # Cache the fallback for future use
                if not cached_ids:
                    cached_ids = product_ids
            else:
                span.set_attribute("app.catalog_call_failed", False)
                # Update cache with fresh data
                if not cached_ids:
                    cached_ids = product_ids

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Handle edge case where we have no products to recommend
        if num_products == 0:
            logger.warning("No products available for recommendation")
            span.set_attribute("app.filtered_products.list", [])
            return []

        # Sample list of indicies to return
        indices = random.sample(range(num_products), num_return)
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
    
    # Configure gRPC channel with keep-alive and connection options
    channel_options = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', True),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.http2.min_time_between_pings_ms', 10000),
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=channel_options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    logger.info(f"Configured gRPC channel to ProductCatalog at {catalog_addr}")

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
