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

# gRPC connection configuration
GRPC_TIMEOUT_SECONDS = 5.0  # Default timeout for gRPC calls
GRPC_MAX_RETRIES = 3  # Maximum number of retry attempts
GRPC_RETRY_BACKOFF = 0.5  # Initial backoff in seconds

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


def call_product_catalog_with_retry(stub, max_retries=GRPC_MAX_RETRIES, timeout=GRPC_TIMEOUT_SECONDS):
    """
    Call the product catalog service with retry logic and proper error handling.
    
    Args:
        stub: The gRPC stub for the product catalog service
        max_retries: Maximum number of retry attempts
        timeout: Timeout in seconds for each gRPC call
        
    Returns:
        List of product IDs from the catalog
        
    Raises:
        grpc.RpcError: If all retry attempts fail
    """
    span = trace.get_current_span()
    
    for attempt in range(max_retries):
        try:
            # Set timeout for the gRPC call to prevent indefinite blocking
            cat_response = stub.ListProducts(
                demo_pb2.Empty(),
                timeout=timeout
            )
            response_ids = [x.id for x in cat_response.products]
            
            # Log successful call
            span.set_attribute("app.product_catalog.retry_attempt", attempt + 1)
            span.set_attribute("app.product_catalog.call_success", True)
            logger.info(f"Successfully fetched {len(response_ids)} products from catalog")
            
            return response_ids
            
        except grpc.RpcError as e:
            # Categorize the error
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                error_type = "service_unavailable"
                logger.warning(
                    f"Product catalog service unavailable (attempt {attempt + 1}/{max_retries}): {e.details()}"
                )
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                error_type = "deadline_exceeded"
                logger.warning(
                    f"Product catalog call timeout (attempt {attempt + 1}/{max_retries}): {e.details()}"
                )
            else:
                error_type = "other_grpc_error"
                logger.warning(
                    f"Product catalog gRPC error (attempt {attempt + 1}/{max_retries}): {e.code()} - {e.details()}"
                )
            
            # Record error in span
            span.set_attribute("app.product_catalog.error_type", error_type)
            span.set_attribute("app.product_catalog.error_code", str(e.code()))
            span.set_attribute("app.product_catalog.retry_attempt", attempt + 1)
            
            # If this was the last attempt, raise the exception
            if attempt == max_retries - 1:
                span.set_attribute("app.product_catalog.call_success", False)
                logger.error(
                    f"Failed to connect to product catalog after {max_retries} attempts. "
                    f"Last error: {e.code()} - {e.details()}"
                )
                raise
            
            # Exponential backoff before retry
            backoff_time = GRPC_RETRY_BACKOFF * (2 ** attempt)
            logger.info(f"Retrying in {backoff_time} seconds...")
            time.sleep(backoff_time)
    
    # This should never be reached, but added for safety
    raise Exception("Unexpected state in retry logic")


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
                    # Use retry logic for product catalog calls
                    response_ids = call_product_catalog_with_retry(product_catalog_stub)
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                except grpc.RpcError as e:
                    # If all retries fail, fall back to cached IDs or return empty list
                    logger.error(f"Failed to fetch products from catalog, using fallback strategy: {e}")
                    span.set_attribute("app.fallback_strategy", "cached_ids")
                    
                    if cached_ids:
                        logger.info(f"Using {len(cached_ids)} cached product IDs as fallback")
                        product_ids = cached_ids
                    else:
                        logger.warning("No cached products available, returning empty recommendations")
                        return []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            try:
                # Use retry logic for product catalog calls
                response_ids = call_product_catalog_with_retry(product_catalog_stub)
                product_ids = response_ids
            except grpc.RpcError as e:
                # If all retries fail, return empty list
                logger.error(f"Failed to fetch products from catalog: {e}")
                span.set_attribute("app.fallback_strategy", "empty_list")
                return []

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Handle edge case where there are no products to recommend
        if num_products == 0:
            logger.info("No products available for recommendation")
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
    
    # Configure gRPC channel with keep-alive settings to detect connection issues faster
    # and prevent deadline exceeded errors on long-running connections
    channel_options = [
        ('grpc.keepalive_time_ms', 10000),  # Send keepalive ping every 10 seconds
        ('grpc.keepalive_timeout_ms', 5000),  # Wait 5 seconds for keepalive response
        ('grpc.keepalive_permit_without_calls', True),  # Allow keepalive pings when no calls
        ('grpc.http2.max_pings_without_data', 0),  # Allow unlimited pings
        ('grpc.http2.min_time_between_pings_ms', 10000),  # Minimum 10 seconds between pings
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),  # Minimum 5 seconds between pings without data
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=channel_options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    logger.info(f"Configured gRPC channel to product catalog at {catalog_addr} with keep-alive settings")

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
