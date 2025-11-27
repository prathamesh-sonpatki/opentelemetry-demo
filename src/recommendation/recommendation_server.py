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
from grpc import StatusCode
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

# Retry configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 0.1  # 100ms
MAX_RETRY_DELAY = 2.0  # 2 seconds

def retry_with_backoff(func, *args, max_retries=MAX_RETRIES, **kwargs):
    """
    Retry a function with exponential backoff.
    Only retries on transient gRPC errors (UNAVAILABLE, DEADLINE_EXCEEDED).
    """
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as e:
            last_exception = e
            status_code = e.code()
            
            # Check if error is retryable
            retryable_codes = [
                StatusCode.UNAVAILABLE,
                StatusCode.DEADLINE_EXCEEDED,
                StatusCode.RESOURCE_EXHAUSTED,
            ]
            
            if status_code not in retryable_codes or attempt == max_retries - 1:
                logger.error(
                    f"gRPC call failed (non-retryable or max retries): "
                    f"code={status_code}, details={e.details()}, attempt={attempt + 1}"
                )
                raise
            
            # Calculate exponential backoff with jitter
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt) + random.uniform(0, 0.1), MAX_RETRY_DELAY)
            logger.warning(
                f"gRPC call failed (retrying): code={status_code}, "
                f"details={e.details()}, attempt={attempt + 1}/{max_retries}, "
                f"retry_delay={delay:.2f}s"
            )
            time.sleep(delay)
    
    raise last_exception

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
        try:
            cache_enabled = check_feature_flag("recommendationCacheFailure")
        except Exception as e:
            # If feature flag check fails, default to false and log warning
            logger.warning(f"Failed to check feature flag 'recommendationCacheFailure': {e}")
            cache_enabled = False
        
        if cache_enabled:
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss")
                
                try:
                    # Use retry logic for product catalog call
                    cat_response = retry_with_backoff(
                        product_catalog_stub.ListProducts,
                        demo_pb2.Empty(),
                        timeout=5.0  # 5 second timeout
                    )
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                except grpc.RpcError as e:
                    logger.error(f"Failed to fetch products from catalog: {e}")
                    # Return empty list on failure to prevent cascade failure
                    span.set_attribute("app.products.fetch_failed", True)
                    return []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            try:
                # Use retry logic for product catalog call
                cat_response = retry_with_backoff(
                    product_catalog_stub.ListProducts,
                    demo_pb2.Empty(),
                    timeout=5.0  # 5 second timeout
                )
                product_ids = [x.id for x in cat_response.products]
            except grpc.RpcError as e:
                logger.error(f"Failed to fetch products from catalog: {e}")
                # Return empty list on failure to prevent cascade failure
                span.set_attribute("app.products.fetch_failed", True)
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
    # Initialize OpenFeature with timeout protection
    try:
        client = api.get_client()
        # Use a short timeout for feature flag checks to prevent blocking
        return client.get_boolean_value("recommendationCacheFailure", False)
    except Exception as e:
        logger.warning(f"Feature flag check failed for '{flag_name}': {e}. Using default value: False")
        return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize feature flags with error handling
    try:
        flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
        flagd_port = int(os.environ.get('FLAGD_PORT', 8013))
        api.set_provider(FlagdProvider(host=flagd_host, port=flagd_port))
        api.add_hooks([TracingHook()])
        logger = logging.getLogger('main')
        logger.info(f"Connected to feature flag service at {flagd_host}:{flagd_port}")
    except Exception as e:
        logger = logging.getLogger('main')
        logger.warning(f"Failed to initialize feature flag provider: {e}. Feature flags will be disabled.")

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
    
    # Create gRPC channel with keepalive and connection timeout settings
    channel_options = [
        ('grpc.keepalive_time_ms', 30000),  # Send keepalive ping every 30 seconds
        ('grpc.keepalive_timeout_ms', 10000),  # Wait 10 seconds for keepalive ack
        ('grpc.keepalive_permit_without_calls', 1),  # Allow keepalive pings when no calls
        ('grpc.http2.max_pings_without_data', 0),  # Allow unlimited pings without data
        ('grpc.http2.min_time_between_pings_ms', 10000),  # Minimum 10 seconds between pings
        ('grpc.http2.min_ping_interval_without_data_ms', 30000),  # 30 seconds without data
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=channel_options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    logger.info(f"Connected to Product Catalog service at {catalog_addr}")

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
