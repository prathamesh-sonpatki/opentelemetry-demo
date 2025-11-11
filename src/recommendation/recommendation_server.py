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

# Default product IDs to use as fallback when ProductCatalog is unavailable
DEFAULT_PRODUCT_IDS = [
    "OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM", 
    "2ZYFJ3GM2N", "0PUK6V6EV0", "LS4PSXUNUM", "9SIQT8TOJO", 
    "6E92ZMYYFZ"
]

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
        List of product IDs from catalog, or None if all retries failed
    """
    delay = initial_delay
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to fetch products from catalog (attempt {attempt + 1}/{max_retries})")
            
            # Call the ProductCatalog service
            cat_response = product_catalog_stub.ListProducts(
                demo_pb2.Empty(),
                timeout=5.0  # 5 second timeout per request
            )
            
            product_ids = [x.id for x in cat_response.products]
            logger.info(f"Successfully fetched {len(product_ids)} products from catalog")
            return product_ids
            
        except grpc.RpcError as e:
            status_code = e.code()
            logger.warning(
                f"ProductCatalog gRPC call failed (attempt {attempt + 1}/{max_retries}): "
                f"status={status_code}, details={e.details()}"
            )
            
            # Don't retry on certain errors
            if status_code in [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.NOT_FOUND]:
                logger.error(f"Non-retryable error from ProductCatalog: {status_code}")
                return None
            
            # If this was the last attempt, return None
            if attempt == max_retries - 1:
                logger.error(f"All {max_retries} retry attempts failed for ProductCatalog")
                return None
            
            # Wait before retrying (exponential backoff)
            time.sleep(delay)
            delay *= 2  # Double the delay for next retry
            
        except Exception as e:
            logger.error(f"Unexpected error calling ProductCatalog: {type(e).__name__}: {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(delay)
            delay *= 2
    
    return None


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
                
                # Use retry logic for ProductCatalog call
                response_ids = call_product_catalog_with_retry()
                
                if response_ids is None:
                    # Fallback to default products if catalog is unavailable
                    logger.warning("ProductCatalog unavailable, using default product list")
                    span.set_attribute("app.product_catalog.fallback", True)
                    response_ids = DEFAULT_PRODUCT_IDS
                else:
                    span.set_attribute("app.product_catalog.fallback", False)
                
                cached_ids = cached_ids + response_ids
                cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                product_ids = cached_ids
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            # Use retry logic for ProductCatalog call
            product_ids = call_product_catalog_with_retry()
            
            if product_ids is None:
                # Fallback to cached products or defaults
                if cached_ids:
                    logger.warning("ProductCatalog unavailable, using cached product list")
                    span.set_attribute("app.product_catalog.fallback", True)
                    span.set_attribute("app.fallback_source", "cache")
                    product_ids = cached_ids
                else:
                    logger.warning("ProductCatalog unavailable and no cache, using default product list")
                    span.set_attribute("app.product_catalog.fallback", True)
                    span.set_attribute("app.fallback_source", "defaults")
                    product_ids = DEFAULT_PRODUCT_IDS
            else:
                span.set_attribute("app.product_catalog.fallback", False)

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
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
    
    # Configure gRPC channel with robust options
    channel_options = [
        ('grpc.keepalive_time_ms', 30000),  # Send keepalive ping every 30 seconds
        ('grpc.keepalive_timeout_ms', 10000),  # Wait 10 seconds for keepalive ack
        ('grpc.keepalive_permit_without_calls', 1),  # Allow keepalive pings without calls
        ('grpc.http2.max_pings_without_data', 0),  # No limit on pings without data
        ('grpc.http2.min_time_between_pings_ms', 10000),  # Min 10 seconds between pings
        ('grpc.http2.min_ping_interval_without_data_ms', 30000),  # Min 30 seconds without data
        ('grpc.enable_retries', 1),  # Enable gRPC retry support
        ('grpc.max_connection_age_ms', 300000),  # Max connection age 5 minutes
        ('grpc.max_connection_idle_ms', 60000),  # Close idle connections after 1 minute
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=channel_options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    logger.info(f"Configured ProductCatalog gRPC channel to {catalog_addr} with robust connection options")

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
