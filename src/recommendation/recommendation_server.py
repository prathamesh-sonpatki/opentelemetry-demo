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
    Call product catalog service with exponential backoff retry logic.
    
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
            span.set_attribute("app.product_catalog.attempt", attempt + 1)
            
            # Attempt to call the product catalog service
            cat_response = product_catalog_stub.ListProducts(
                demo_pb2.Empty(),
                timeout=5.0  # Add timeout to prevent hanging
            )
            product_ids = [x.id for x in cat_response.products]
            
            span.set_attribute("app.product_catalog.success", True)
            logger.info(f"Successfully retrieved {len(product_ids)} products from catalog (attempt {attempt + 1})")
            return product_ids
            
        except grpc.RpcError as e:
            last_exception = e
            status_code = e.code()
            
            span = trace.get_current_span()
            span.set_attribute("app.product_catalog.error", True)
            span.set_attribute("app.product_catalog.error_code", str(status_code))
            span.set_attribute("app.product_catalog.attempt", attempt + 1)
            
            # Log the error with details
            logger.warning(
                f"Product catalog service unavailable (attempt {attempt + 1}/{max_retries}): "
                f"status={status_code}, details={e.details()}"
            )
            
            # Don't retry on certain error codes
            if status_code in [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.UNAUTHENTICATED]:
                logger.error(f"Non-retryable error from product catalog: {status_code}")
                break
            
            # If not the last attempt, wait before retrying with exponential backoff
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
        
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error calling product catalog (attempt {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
    
    # All retries failed
    span = trace.get_current_span()
    span.set_attribute("app.product_catalog.all_retries_failed", True)
    logger.error(f"All {max_retries} attempts to contact product catalog failed. Using fallback strategy.")
    
    # Return empty list as graceful degradation
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
                
                # Use retry logic for product catalog call
                response_ids = call_product_catalog_with_retry()
                
                # If we got products, update cache
                if response_ids:
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    # Fallback to cached IDs if available
                    if cached_ids:
                        logger.info("Using cached product IDs as fallback")
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
            
            # Use retry logic for product catalog call
            product_ids = call_product_catalog_with_retry()
            
            # If product catalog is unavailable and we have cache, use it as fallback
            if not product_ids and cached_ids:
                logger.info("Product catalog unavailable, using cached products as fallback")
                span.set_attribute("app.fallback_to_cache", True)
                product_ids = cached_ids
            elif not product_ids:
                logger.warning("No products available from catalog or cache, returning empty recommendations")
                return []

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        
        # Handle case where we have no products to recommend
        if num_products == 0:
            logger.info("No products available for recommendation after filtering")
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
