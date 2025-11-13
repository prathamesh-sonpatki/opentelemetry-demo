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


def call_product_catalog_with_retry(stub, max_retries=3, timeout=5):
    """
    Call product catalog service with exponential backoff retry logic.
    
    Args:
        stub: gRPC stub for ProductCatalogService
        max_retries: Maximum number of retry attempts (default: 3)
        timeout: Timeout in seconds for each request (default: 5)
    
    Returns:
        ListProductsResponse or None if all retries failed
    """
    for attempt in range(max_retries):
        try:
            # Add timeout to prevent hanging requests
            cat_response = stub.ListProducts(
                demo_pb2.Empty(),
                timeout=timeout
            )
            if attempt > 0:
                logger.info(f"Successfully connected to product catalog on attempt {attempt + 1}")
            return cat_response
            
        except grpc.RpcError as e:
            status_code = e.code()
            error_details = e.details()
            
            # Log the error with trace context
            span = trace.get_current_span()
            trace_id = span.get_span_context().trace_id
            logger.error(
                f"gRPC error calling product catalog (attempt {attempt + 1}/{max_retries}): "
                f"status={status_code}, details={error_details}, trace_id={trace_id:032x}"
            )
            
            # Set error attributes on span
            span.set_attribute("error", True)
            span.set_attribute("error.type", str(status_code))
            span.set_attribute("error.message", error_details)
            span.set_attribute("retry.attempt", attempt + 1)
            
            # Check if we should retry
            if status_code in [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED]:
                if attempt < max_retries - 1:
                    # Exponential backoff: 0.5s, 1s, 2s
                    backoff_time = 0.5 * (2 ** attempt)
                    logger.warning(f"Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                    continue
                else:
                    logger.error(
                        f"Max retries ({max_retries}) reached for product catalog. "
                        "Returning empty product list as fallback."
                    )
            else:
                # Non-retryable error
                logger.error(f"Non-retryable gRPC error: {status_code}. Returning empty product list.")
                break
                
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Unexpected error calling product catalog: {str(e)}", exc_info=True)
            break
    
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
                
                # Use retry logic for product catalog call
                cat_response = call_product_catalog_with_retry(product_catalog_stub)
                
                if cat_response:
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    # Fallback: use cached IDs if available, otherwise return empty
                    logger.warning("Using cached IDs as fallback due to product catalog failure")
                    product_ids = cached_ids if cached_ids else []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            # Use retry logic for product catalog call
            cat_response = call_product_catalog_with_retry(product_catalog_stub)
            
            if cat_response:
                product_ids = [x.id for x in cat_response.products]
            else:
                # Fallback: return empty list gracefully
                logger.warning("Product catalog unavailable, returning empty recommendations")
                span.set_attribute("app.fallback.empty_recommendations", True)
                return []

        span.set_attribute("app.products.count", len(product_ids))

        # Handle case where product_ids is empty
        if not product_ids:
            logger.info("No products available for recommendations")
            return []

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        
        if num_products == 0:
            logger.info("No products available after filtering")
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
    Check feature flag with error handling for flagd connection issues.
    """
    try:
        # Initialize OpenFeature
        client = api.get_client()
        return client.get_boolean_value("recommendationCacheFailure", False)
    except Exception as e:
        # Log but don't fail if feature flag service is unavailable
        logger.warning(f"Feature flag check failed for '{flag_name}': {str(e)}. Using default value False.")
        return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize feature flag provider with error handling
    try:
        api.set_provider(FlagdProvider(host=os.environ.get('FLAGD_HOST', 'flagd'), port=os.environ.get('FLAGD_PORT', 8013)))
        api.add_hooks([TracingHook()])
        logger_temp = logging.getLogger('main')
        logger_temp.info("Feature flag provider initialized successfully")
    except Exception as e:
        logger_temp = logging.getLogger('main')
        logger_temp.warning(f"Failed to initialize feature flag provider: {str(e)}. Feature flags will use default values.")

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
    
    # Create channel with keepalive options for better connection stability
    channel_options = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', True),
        ('grpc.http2.max_pings_without_data', 0),
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
