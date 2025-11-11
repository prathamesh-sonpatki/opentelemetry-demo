#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
import time
from concurrent import futures
from functools import wraps

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

# Configuration for retry logic
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.1  # 100ms
MAX_BACKOFF = 2.0  # 2 seconds
BACKOFF_MULTIPLIER = 2


def retry_on_grpc_error(max_retries=MAX_RETRIES):
    """
    Decorator to retry gRPC calls with exponential backoff on connection failures.
    
    This handles transient network issues and temporary service unavailability
    by retrying with increasing delays between attempts.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            backoff = INITIAL_BACKOFF
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except grpc.RpcError as e:
                    last_exception = e
                    status_code = e.code()
                    
                    # Only retry on specific error codes
                    if status_code in [
                        grpc.StatusCode.UNAVAILABLE,
                        grpc.StatusCode.DEADLINE_EXCEEDED,
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        grpc.StatusCode.UNKNOWN
                    ]:
                        if attempt < max_retries - 1:
                            logger.warning(
                                f"gRPC call failed with {status_code.name}, "
                                f"attempt {attempt + 1}/{max_retries}. "
                                f"Retrying in {backoff}s... Error: {e.details()}"
                            )
                            time.sleep(backoff)
                            backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
                        else:
                            logger.error(
                                f"gRPC call failed after {max_retries} attempts. "
                                f"Error: {e.details()}"
                            )
                    else:
                        # Don't retry on other error codes (e.g., INVALID_ARGUMENT)
                        logger.error(f"Non-retryable gRPC error: {status_code.name} - {e.details()}")
                        raise
                except Exception as e:
                    last_exception = e
                    logger.error(f"Unexpected error in gRPC call: {str(e)}")
                    raise
            
            # If all retries failed, raise the last exception
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


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


@retry_on_grpc_error(max_retries=MAX_RETRIES)
def call_product_catalog_list():
    """
    Call ProductCatalogService.ListProducts with retry logic.
    
    This function is wrapped with retry decorator to handle transient failures
    when connecting to the product catalog service.
    """
    return product_catalog_stub.ListProducts(demo_pb2.Empty())


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
                
                try:
                    cat_response = call_product_catalog_list()
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                except grpc.RpcError as e:
                    # Fallback to cached IDs if available, otherwise return empty list
                    logger.error(
                        f"Failed to fetch products from catalog service after retries: {e.details()}. "
                        f"Using fallback strategy."
                    )
                    span.set_attribute("app.catalog_service.error", True)
                    span.set_attribute("app.catalog_service.error_message", str(e.details()))
                    
                    if cached_ids:
                        logger.info("Using cached product IDs as fallback")
                        product_ids = cached_ids
                    else:
                        logger.warning("No cached IDs available, returning empty recommendations")
                        return []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            try:
                cat_response = call_product_catalog_list()
                product_ids = [x.id for x in cat_response.products]
            except grpc.RpcError as e:
                # If catalog service is unavailable, return empty list
                logger.error(
                    f"Failed to fetch products from catalog service: {e.details()}. "
                    f"Returning empty recommendations."
                )
                span.set_attribute("app.catalog_service.error", True)
                span.set_attribute("app.catalog_service.error_message", str(e.details()))
                return []

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Handle case where no products are available
        if num_products == 0:
            logger.warning("No products available for recommendation")
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
    
    # Configure gRPC channel with keepalive and timeout options
    # These options help detect and recover from connection issues
    channel_options = [
        ('grpc.keepalive_time_ms', 10000),  # Send keepalive ping every 10 seconds
        ('grpc.keepalive_timeout_ms', 5000),  # Wait 5 seconds for keepalive ack
        ('grpc.keepalive_permit_without_calls', 1),  # Allow keepalive pings without active calls
        ('grpc.http2.max_pings_without_data', 0),  # Allow unlimited pings
        ('grpc.http2.min_time_between_pings_ms', 10000),  # Min time between pings
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),  # Min ping interval
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=channel_options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    logger.info(f"Configured gRPC connection to product catalog at {catalog_addr}")

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
