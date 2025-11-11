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

# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.1  # 100ms
MAX_BACKOFF = 2.0  # 2 seconds
BACKOFF_MULTIPLIER = 2


def grpc_retry_with_backoff(max_retries=MAX_RETRIES):
    """
    Decorator to retry gRPC calls with exponential backoff.
    Handles UNAVAILABLE and DEADLINE_EXCEEDED errors gracefully.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            backoff = INITIAL_BACKOFF
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except grpc.RpcError as e:
                    last_exception = e
                    status_code = e.code()
                    
                    # Log the retry attempt
                    logger.warning(
                        f"gRPC call failed (attempt {attempt + 1}/{max_retries}): "
                        f"status={status_code.name}, details={e.details()}"
                    )
                    
                    # Only retry on specific error codes
                    if status_code in (grpc.StatusCode.UNAVAILABLE, 
                                      grpc.StatusCode.DEADLINE_EXCEEDED,
                                      grpc.StatusCode.RESOURCE_EXHAUSTED):
                        if attempt < max_retries - 1:
                            # Add jitter to backoff to prevent thundering herd
                            jitter = random.uniform(0, backoff * 0.1)
                            sleep_time = min(backoff + jitter, MAX_BACKOFF)
                            logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                            time.sleep(sleep_time)
                            backoff *= BACKOFF_MULTIPLIER
                        else:
                            logger.error(f"Max retries reached for gRPC call. Giving up.")
                    else:
                        # Don't retry on other error codes
                        logger.error(f"Non-retryable gRPC error: {status_code.name}")
                        break
                except Exception as e:
                    # Catch any other exceptions and log them
                    last_exception = e
                    logger.error(f"Unexpected error in gRPC call: {type(e).__name__}: {str(e)}")
                    break
            
            # If we get here, all retries failed
            raise last_exception
        return wrapper
    return decorator


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
            # Gracefully handle errors and return empty recommendations
            logger.error(f"Error in ListRecommendations: {type(e).__name__}: {str(e)}")
            span = trace.get_current_span()
            span.set_attribute("app.recommendation.error", True)
            span.set_attribute("app.error.type", type(e).__name__)
            
            # Return empty recommendations rather than failing
            response = demo_pb2.ListRecommendationsResponse()
            rec_svc_metrics["app_recommendations_counter"].add(0, {'recommendation.type': 'error_fallback'})
            return response

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


@grpc_retry_with_backoff(max_retries=MAX_RETRIES)
def fetch_product_catalog():
    """
    Fetch products from catalog service with retry logic.
    Raises exception if all retries fail.
    """
    return product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=5.0)


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
                    cat_response = fetch_product_catalog()
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                except grpc.RpcError as e:
                    # Fallback to cached IDs if available
                    logger.error(f"Failed to fetch from catalog after retries: {e.details()}")
                    span.set_attribute("app.catalog_fetch.failed", True)
                    span.set_attribute("app.fallback_to_cache", True)
                    
                    if cached_ids:
                        logger.info("Falling back to cached product IDs")
                        product_ids = cached_ids
                    else:
                        logger.warning("No cached IDs available, returning empty recommendations")
                        span.set_attribute("app.no_fallback_data", True)
                        return []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            try:
                cat_response = fetch_product_catalog()
                product_ids = [x.id for x in cat_response.products]
            except grpc.RpcError as e:
                # Fallback to cached IDs or return empty
                logger.error(f"Failed to fetch from catalog after retries: {e.details()}")
                span.set_attribute("app.catalog_fetch.failed", True)
                span.set_attribute("app.fallback_to_cache", True)
                
                if cached_ids:
                    logger.info("Falling back to cached product IDs")
                    product_ids = cached_ids
                else:
                    logger.warning("No cached IDs available, returning empty recommendations")
                    span.set_attribute("app.no_fallback_data", True)
                    return []

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Sample list of indicies to return
        indices = random.sample(range(num_products), num_return) if num_products > 0 else []
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
    
    # Configure gRPC channel with keepalive settings for better connection resilience
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
