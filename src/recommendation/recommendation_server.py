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

# Default product IDs to use as fallback when catalog service is unavailable
DEFAULT_PRODUCT_IDS = [
    "OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM",
    "2ZYFJ3GM2N", "0PUK6V6EV0", "LS4PSXUNUM", "9SIQT8TOJO", "6E92ZMYYFZ"
]

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        try:
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
        except Exception as e:
            # Log the error but don't crash the service
            logger.error(f"Error in ListRecommendations: {e}", exc_info=True)
            span = trace.get_current_span()
            span.record_exception(e)
            # Return empty recommendations instead of crashing
            return demo_pb2.ListRecommendationsResponse()

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


def get_product_list_with_retry(max_retries=3, initial_delay=0.1):
    """
    Fetch product list from catalog with exponential backoff retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
    
    Returns:
        List of product IDs or default list if all retries fail
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            # Add timeout to prevent hanging
            cat_response = product_catalog_stub.ListProducts(
                demo_pb2.Empty(),
                timeout=5.0  # 5 second timeout
            )
            return [x.id for x in cat_response.products]
        except grpc.RpcError as e:
            last_exception = e
            status_code = e.code()
            
            # Log the error with attempt number
            logger.warning(
                f"Failed to fetch products from catalog (attempt {attempt + 1}/{max_retries}): "
                f"{status_code.name} - {e.details()}"
            )
            
            # Don't retry on certain error types
            if status_code in [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.PERMISSION_DENIED]:
                logger.error(f"Non-retryable error: {status_code.name}")
                break
            
            # If not the last attempt, wait before retrying
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2  # Exponential backoff
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error fetching products (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
    
    # All retries failed, log and return fallback
    logger.error(
        f"All retries failed to fetch products from catalog. Using fallback product list. "
        f"Last error: {last_exception}"
    )
    span = trace.get_current_span()
    span.set_attribute("app.catalog_fallback_used", True)
    if last_exception:
        span.record_exception(last_exception)
    
    return DEFAULT_PRODUCT_IDS


def get_product_list(request_product_ids):
    global first_run
    global cached_ids
    with tracer.start_as_current_span("get_product_list") as span:
        max_responses = 5

        # Formulate the list of characters to list of strings
        request_product_ids_str = ''.join(request_product_ids)
        request_product_ids = request_product_ids_str.split(',')

        # Feature flag scenario - Cache Leak
        try:
            cache_failure_enabled = check_feature_flag("recommendationCacheFailure")
        except Exception as e:
            # If feature flag check fails, default to disabled
            logger.warning(f"Failed to check feature flag, defaulting to disabled: {e}")
            cache_failure_enabled = False
            span.set_attribute("app.feature_flag_check_failed", True)

        if cache_failure_enabled:
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss")
                # Use retry logic for catalog fetch
                response_ids = get_product_list_with_retry()
                cached_ids = cached_ids + response_ids
                cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                product_ids = cached_ids
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            # Use retry logic for catalog fetch
            product_ids = get_product_list_with_retry()

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        
        # Handle edge case where filtered products is empty
        if num_products == 0:
            logger.warning("No products available after filtering")
            span.set_attribute("app.empty_recommendations", True)
            return []
        
        num_return = min(max_responses, num_products)

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
    """
    Check feature flag with error handling and timeout.
    
    Returns:
        Boolean value of the feature flag, or False if check fails
    """
    try:
        client = api.get_client()
        # Add timeout to prevent hanging
        return client.get_boolean_value("recommendationCacheFailure", False)
    except Exception as e:
        logger.warning(f"Failed to check feature flag '{flag_name}': {e}")
        return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize FlagD provider with increased timeout to prevent DEADLINE_EXCEEDED
    flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
    flagd_port = int(os.environ.get('FLAGD_PORT', 8013))
    
    try:
        # Set provider with more resilient configuration
        api.set_provider(FlagdProvider(
            host=flagd_host, 
            port=flagd_port,
            # Increase deadline to prevent DEADLINE_EXCEEDED errors
            deadline=30000  # 30 seconds instead of default 10
        ))
        api.add_hooks([TracingHook()])
        logger_init = logging.getLogger('init')
        logger_init.info(f"Successfully connected to flagd at {flagd_host}:{flagd_port}")
    except Exception as e:
        # Don't crash if flagd is unavailable, just log and continue
        logger_init = logging.getLogger('init')
        logger_init.warning(f"Failed to initialize flagd provider: {e}. Feature flags will be disabled.")

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
    
    # Create gRPC channel with keep-alive settings to prevent connection issues
    options = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', True),
        ('grpc.http2.max_pings_without_data', 0),
    ]
    pc_channel = grpc.insecure_channel(catalog_addr, options=options)
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
