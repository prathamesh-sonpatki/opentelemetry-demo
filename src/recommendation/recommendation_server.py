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
                cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
                response_ids = [x.id for x in cat_response.products]
                cached_ids = cached_ids + response_ids
                cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                product_ids = cached_ids
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
            product_ids = [x.id for x in cat_response.products]

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
    """
    Check feature flag with proper error handling and graceful degradation.
    Returns False (feature disabled) if flagd service is unavailable.
    """
    try:
        # Initialize OpenFeature client
        client = api.get_client()
        
        # Attempt to get feature flag value with timeout handling
        result = client.get_boolean_value("recommendationCacheFailure", False)
        return result
    except grpc.RpcError as e:
        # Handle gRPC-specific errors (timeouts, connection failures, etc.)
        span = trace.get_current_span()
        if span:
            span.set_attribute("app.feature_flag.error", True)
            span.set_attribute("app.feature_flag.error_type", type(e).__name__)
        
        logger.warning(
            f"Feature flag check failed for '{flag_name}': {str(e)}. "
            f"Defaulting to False (feature disabled)."
        )
        return False
    except Exception as e:
        # Handle any other unexpected errors
        span = trace.get_current_span()
        if span:
            span.set_attribute("app.feature_flag.error", True)
            span.set_attribute("app.feature_flag.error_type", type(e).__name__)
        
        logger.error(
            f"Unexpected error checking feature flag '{flag_name}': {str(e)}. "
            f"Defaulting to False (feature disabled)."
        )
        return False


def initialize_flagd_provider_with_retry(max_retries=3, retry_delay=2):
    """
    Initialize FlagdProvider with retry logic and proper timeout configuration.
    """
    flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
    flagd_port = int(os.environ.get('FLAGD_PORT', 8013))
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to flagd at {flagd_host}:{flagd_port} (attempt {attempt + 1}/{max_retries})")
            
            # Initialize FlagdProvider with explicit timeout configuration
            provider = FlagdProvider(
                host=flagd_host,
                port=flagd_port,
                # Add deadline/timeout configuration to prevent hanging connections
                deadline=5000,  # 5 seconds timeout for flagd operations
                keep_alive=True,
                keep_alive_time=30000,  # 30 seconds
            )
            
            # Set the provider
            api.set_provider(provider)
            
            logger.info(f"Successfully connected to flagd at {flagd_host}:{flagd_port}")
            return True
            
        except Exception as e:
            logger.warning(
                f"Failed to initialize flagd provider (attempt {attempt + 1}/{max_retries}): {str(e)}"
            )
            
            if attempt < max_retries - 1:
                # Exponential backoff
                sleep_time = retry_delay * (2 ** attempt)
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(
                    f"Failed to initialize flagd provider after {max_retries} attempts. "
                    f"Service will continue with feature flags disabled."
                )
                return False
    
    return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize FlagdProvider with retry logic and timeout configuration
    flagd_initialized = initialize_flagd_provider_with_retry(max_retries=3, retry_delay=2)
    
    if flagd_initialized:
        # Add tracing hook only if flagd was successfully initialized
        api.add_hooks([TracingHook()])
    else:
        logger.warning("Starting service without flagd integration. All feature flags will be disabled.")

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
