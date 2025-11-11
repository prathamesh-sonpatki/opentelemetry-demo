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
    Call ProductCatalog service with retry logic and exponential backoff.
    
    This function implements resilient communication with the ProductCatalog service
    to handle transient failures gracefully.
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 0.1)
        
    Returns:
        List of product IDs from the catalog, or empty list if all retries fail
    """
    span = trace.get_current_span()
    
    for attempt in range(max_retries + 1):
        try:
            # Set a reasonable timeout for the gRPC call to prevent indefinite hangs
            cat_response = product_catalog_stub.ListProducts(
                demo_pb2.Empty(), 
                timeout=5.0  # 5 second timeout
            )
            
            # Successfully retrieved products
            response_ids = [x.id for x in cat_response.products]
            
            if attempt > 0:
                # Log successful retry
                logger.info(f"ProductCatalog call succeeded on attempt {attempt + 1}")
                span.set_attribute("app.product_catalog.retry_success", True)
                span.set_attribute("app.product_catalog.attempts", attempt + 1)
            
            return response_ids
            
        except grpc.RpcError as e:
            # Extract error details
            status_code = e.code()
            error_details = e.details() if hasattr(e, 'details') else str(e)
            
            # Record the error in telemetry
            span.set_attribute("app.product_catalog.error", True)
            span.set_attribute("app.product_catalog.error_code", str(status_code))
            span.set_attribute("app.product_catalog.attempt", attempt + 1)
            
            # Check if this is the last attempt
            if attempt == max_retries:
                # All retries exhausted - log error and return empty list for graceful degradation
                logger.error(
                    f"ProductCatalog service unavailable after {max_retries + 1} attempts. "
                    f"Status: {status_code}, Details: {error_details}. "
                    f"Returning empty recommendations for graceful degradation."
                )
                span.set_attribute("app.product_catalog.all_retries_failed", True)
                span.add_event("ProductCatalog service unavailable - graceful degradation")
                
                # Return empty list instead of raising exception
                return []
            
            # Calculate exponential backoff delay
            delay = initial_delay * (2 ** attempt)
            
            # Log the retry attempt
            logger.warning(
                f"ProductCatalog call failed (attempt {attempt + 1}/{max_retries + 1}). "
                f"Status: {status_code}, Details: {error_details}. "
                f"Retrying in {delay:.2f} seconds..."
            )
            
            # Wait before retrying
            time.sleep(delay)
            
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Unexpected error calling ProductCatalog: {type(e).__name__}: {str(e)}")
            span.set_attribute("app.product_catalog.unexpected_error", True)
            span.add_event(f"Unexpected error: {type(e).__name__}")
            
            if attempt == max_retries:
                # Return empty list for graceful degradation
                return []
            
            # Wait before retrying
            delay = initial_delay * (2 ** attempt)
            time.sleep(delay)
    
    # Should never reach here, but return empty list as safety fallback
    return []


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
                
                # Call ProductCatalog with retry logic and error handling
                response_ids = call_product_catalog_with_retry()
                
                # Only update cache if we successfully retrieved products
                if response_ids:
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    # Use existing cache if ProductCatalog is unavailable
                    logger.warning("ProductCatalog unavailable, using existing cached products")
                    product_ids = cached_ids if cached_ids else []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            # Call ProductCatalog with retry logic and error handling
            response_ids = call_product_catalog_with_retry()
            product_ids = response_ids

        span.set_attribute("app.products.count", len(product_ids))

        # Handle case where no products are available (graceful degradation)
        if not product_ids:
            logger.warning("No products available for recommendations")
            span.set_attribute("app.products_available", False)
            return []

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        
        # Handle edge case where all products are filtered out
        if num_products == 0:
            logger.info("All products filtered out, returning empty recommendations")
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
    
    # Configure gRPC channel with connection timeout and keepalive settings
    # to detect and recover from connection failures faster
    channel_options = [
        ('grpc.keepalive_time_ms', 10000),  # Send keepalive ping every 10 seconds
        ('grpc.keepalive_timeout_ms', 5000),  # Wait 5 seconds for keepalive response
        ('grpc.keepalive_permit_without_calls', 1),  # Allow keepalive pings when no calls
        ('grpc.http2.max_pings_without_data', 0),  # Allow unlimited pings
        ('grpc.http2.min_time_between_pings_ms', 10000),  # Min 10 seconds between pings
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),  # Min 5 seconds without data
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
