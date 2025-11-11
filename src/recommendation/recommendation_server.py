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
                
                # Fetch products with retry logic and error handling
                product_ids = fetch_products_with_retry()
                
                # Update cache if products were fetched successfully
                if product_ids:
                    cached_ids = cached_ids + product_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    # Fall back to cached data if available, otherwise use empty list
                    logger.warning("Failed to fetch products from catalog, using cached data or empty list")
                    product_ids = cached_ids if cached_ids else []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            # Fetch products with retry logic and error handling
            product_ids = fetch_products_with_retry()
            
            # If fetching failed and cache exists, use cached data
            if not product_ids and cached_ids:
                logger.warning("Failed to fetch products from catalog, falling back to cached data")
                product_ids = cached_ids

        span.set_attribute("app.products.count", len(product_ids))

        # If no products available, return empty list
        if not product_ids:
            logger.error("No products available for recommendations")
            return []

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        
        # If no products to recommend after filtering, return empty list
        if num_products == 0:
            logger.info("No products to recommend after filtering")
            return []
            
        num_return = min(max_responses, num_products)

        # Sample list of indicies to return
        indices = random.sample(range(num_products), num_return)
        # Fetch product ids from indices
        prod_list = [filtered_products[i] for i in indices]

        span.set_attribute("app.filtered_products.list", prod_list)

        return prod_list


def fetch_products_with_retry(max_retries=3, initial_delay=0.1, backoff_factor=2):
    """
    Fetch products from catalog service with exponential backoff retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for delay after each retry
    
    Returns:
        List of product IDs or empty list if all retries failed
    """
    delay = initial_delay
    
    for attempt in range(max_retries):
        try:
            with tracer.start_as_current_span("fetch_products_from_catalog") as span:
                span.set_attribute("retry.attempt", attempt + 1)
                span.set_attribute("retry.max_attempts", max_retries)
                
                # Set a reasonable timeout for the gRPC call (5 seconds)
                cat_response = product_catalog_stub.ListProducts(
                    demo_pb2.Empty(),
                    timeout=5.0
                )
                response_ids = [x.id for x in cat_response.products]
                
                span.set_attribute("products.fetched.count", len(response_ids))
                logger.info(f"Successfully fetched {len(response_ids)} products from catalog")
                return response_ids
                
        except grpc.RpcError as e:
            span = trace.get_current_span()
            span.set_attribute("error.occurred", True)
            span.set_attribute("error.type", type(e).__name__)
            
            # Check if it's UNAVAILABLE status (connection refused, service down, etc.)
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                logger.warning(
                    f"Product catalog service unavailable (attempt {attempt + 1}/{max_retries}): {e.details()}"
                )
                span.set_attribute("error.grpc_status", "UNAVAILABLE")
            # Check if it's DEADLINE_EXCEEDED (timeout)
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                logger.warning(
                    f"Product catalog service timeout (attempt {attempt + 1}/{max_retries}): {e.details()}"
                )
                span.set_attribute("error.grpc_status", "DEADLINE_EXCEEDED")
            else:
                # Other gRPC errors
                logger.error(
                    f"Product catalog service error (attempt {attempt + 1}/{max_retries}): "
                    f"code={e.code()}, details={e.details()}"
                )
                span.set_attribute("error.grpc_status", str(e.code()))
            
            # Don't retry on the last attempt
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logger.error(f"Failed to fetch products after {max_retries} attempts")
                span.record_exception(e)
                
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Unexpected error fetching products: {type(e).__name__}: {str(e)}")
            span = trace.get_current_span()
            span.set_attribute("error.occurred", True)
            span.set_attribute("error.type", type(e).__name__)
            span.record_exception(e)
            break
    
    # Return empty list if all retries failed
    return []


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
