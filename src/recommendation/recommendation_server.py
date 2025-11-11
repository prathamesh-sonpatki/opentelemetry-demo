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

# Retry configuration constants
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.1  # 100ms
MAX_BACKOFF = 2.0  # 2 seconds
BACKOFF_MULTIPLIER = 2

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


def call_product_catalog_with_retry(max_retries=MAX_RETRIES):
    """
    Call ProductCatalog service with exponential backoff retry logic.
    Returns list of product IDs or empty list if all retries fail.
    """
    backoff = INITIAL_BACKOFF
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to call ProductCatalog service (attempt {attempt + 1}/{max_retries})")
            cat_response = product_catalog_stub.ListProducts(
                demo_pb2.Empty(),
                timeout=5.0,  # 5 second timeout per call
                wait_for_ready=True
            )
            product_ids = [x.id for x in cat_response.products]
            logger.info(f"Successfully retrieved {len(product_ids)} products from catalog")
            return product_ids
        except grpc.RpcError as e:
            error_code = e.code()
            error_details = e.details()
            logger.warning(
                f"ProductCatalog call failed (attempt {attempt + 1}/{max_retries}): "
                f"code={error_code}, details={error_details}"
            )
            
            # If this was the last attempt, log error and return empty list
            if attempt == max_retries - 1:
                logger.error(
                    f"ProductCatalog service unavailable after {max_retries} attempts. "
                    f"Returning empty product list as fallback."
                )
                return []
            
            # Exponential backoff with jitter
            sleep_time = min(backoff * (1 + random.uniform(-0.1, 0.1)), MAX_BACKOFF)
            logger.info(f"Retrying in {sleep_time:.2f}s...")
            time.sleep(sleep_time)
            backoff *= BACKOFF_MULTIPLIER
        except Exception as e:
            logger.error(f"Unexpected error calling ProductCatalog: {type(e).__name__}: {str(e)}")
            return []
    
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
        # Wrap feature flag check in try-except to handle Flagd unavailability
        cache_failure_enabled = False
        try:
            cache_failure_enabled = check_feature_flag("recommendationCacheFailure")
        except Exception as e:
            logger.warning(f"Failed to check feature flag 'recommendationCacheFailure': {type(e).__name__}: {str(e)}")
            span.set_attribute("app.feature_flag.error", True)
            span.set_attribute("app.feature_flag.error_message", str(e))
        
        if cache_failure_enabled:
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss")
                response_ids = call_product_catalog_with_retry()
                
                # If we got products, update cache
                if response_ids:
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    # Fallback to cached_ids if available, otherwise use empty list
                    logger.warning("Using cached products as fallback due to catalog unavailability")
                    product_ids = cached_ids if cached_ids else []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            product_ids = call_product_catalog_with_retry()
            
            # If catalog is unavailable and we have cached IDs, use them as fallback
            if not product_ids and cached_ids:
                logger.info("Using cached products as fallback")
                product_ids = cached_ids
                span.set_attribute("app.using_cache_fallback", True)

        span.set_attribute("app.products.count", len(product_ids))

        # If we have no products at all, return empty list
        if not product_ids:
            logger.warning("No products available to recommend")
            span.set_attribute("app.no_products_available", True)
            return []

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        
        # If no products after filtering, return empty list
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
    # Initialize OpenFeature with error handling
    try:
        client = api.get_client()
        return client.get_boolean_value("recommendationCacheFailure", False)
    except Exception as e:
        # Log the error and return default value
        logger.warning(f"Feature flag check failed for '{flag_name}': {str(e)}. Using default value: False")
        return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize OpenFeature with error handling
    try:
        api.set_provider(FlagdProvider(
            host=os.environ.get('FLAGD_HOST', 'flagd'), 
            port=os.environ.get('FLAGD_PORT', 8013),
            deadline=2000  # 2 second deadline for flagd operations
        ))
        api.add_hooks([TracingHook()])
    except Exception as e:
        # Log warning but continue - feature flags will use defaults
        print(f"Warning: Failed to initialize Flagd provider: {str(e)}. Feature flags will use default values.")

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
    
    # Create gRPC channel with timeout and retry configuration
    pc_channel = grpc.insecure_channel(
        catalog_addr,
        options=[
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', 1),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.enable_retries', 1),
            ('grpc.service_config', '''{
                "methodConfig": [{
                    "name": [{"service": "oteldemo.ProductCatalogService"}],
                    "retryPolicy": {
                        "maxAttempts": 3,
                        "initialBackoff": "0.1s",
                        "maxBackoff": "2s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": ["UNAVAILABLE", "DEADLINE_EXCEEDED"]
                    }
                }]
            }''')
        ]
    )
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
