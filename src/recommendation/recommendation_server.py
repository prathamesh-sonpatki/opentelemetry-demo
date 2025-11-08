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

# Configuration for flagd connection resilience
FLAGD_TIMEOUT = int(os.environ.get('FLAGD_TIMEOUT', 30))  # seconds
FLAGD_MAX_RETRIES = int(os.environ.get('FLAGD_MAX_RETRIES', 3))
FLAGD_RETRY_BACKOFF = float(os.environ.get('FLAGD_RETRY_BACKOFF', 1.5))  # exponential backoff multiplier

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


def check_feature_flag(flag_name: str, default_value: bool = False) -> bool:
    """
    Check feature flag with resilient error handling.
    
    This function wraps the OpenFeature client call with retry logic and
    graceful error handling to prevent DEADLINE_EXCEEDED exceptions from
    propagating when the flagd service is unavailable or slow.
    
    Args:
        flag_name: Name of the feature flag to check
        default_value: Default value to return if flag check fails
        
    Returns:
        bool: Feature flag value or default_value on error
    """
    span = trace.get_current_span()
    client = api.get_client()
    
    for attempt in range(FLAGD_MAX_RETRIES):
        try:
            # Add span attributes for debugging
            span.set_attribute("feature_flag.name", flag_name)
            span.set_attribute("feature_flag.attempt", attempt + 1)
            
            # Get feature flag value with timeout protection
            flag_value = client.get_boolean_value(flag_name, default_value)
            
            span.set_attribute("feature_flag.value", flag_value)
            span.set_attribute("feature_flag.success", True)
            
            if attempt > 0:
                logger.info(f"Feature flag '{flag_name}' retrieved successfully after {attempt + 1} attempts")
            
            return flag_value
            
        except grpc.RpcError as e:
            # Handle gRPC-specific errors (including DEADLINE_EXCEEDED)
            status_code = e.code() if hasattr(e, 'code') else None
            error_details = e.details() if hasattr(e, 'details') else str(e)
            
            span.set_attribute("feature_flag.error.type", "grpc_error")
            span.set_attribute("feature_flag.error.code", str(status_code))
            span.set_attribute("feature_flag.error.details", error_details)
            
            if attempt < FLAGD_MAX_RETRIES - 1:
                # Calculate exponential backoff delay
                delay = (FLAGD_RETRY_BACKOFF ** attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{FLAGD_MAX_RETRIES} failed for feature flag '{flag_name}': "
                    f"{status_code} - {error_details}. Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
            else:
                # Max retries exceeded, log error and use default
                logger.error(
                    f"Failed to retrieve feature flag '{flag_name}' after {FLAGD_MAX_RETRIES} attempts. "
                    f"Using default value: {default_value}. Last error: {status_code} - {error_details}"
                )
                span.set_attribute("feature_flag.success", False)
                span.set_attribute("feature_flag.default_used", True)
                
        except Exception as e:
            # Handle any other unexpected errors
            error_type = type(e).__name__
            error_msg = str(e)
            
            span.set_attribute("feature_flag.error.type", error_type)
            span.set_attribute("feature_flag.error.message", error_msg)
            
            if attempt < FLAGD_MAX_RETRIES - 1:
                delay = (FLAGD_RETRY_BACKOFF ** attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{FLAGD_MAX_RETRIES} failed for feature flag '{flag_name}': "
                    f"{error_type}: {error_msg}. Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"Unexpected error retrieving feature flag '{flag_name}' after {FLAGD_MAX_RETRIES} attempts. "
                    f"Using default value: {default_value}. Error: {error_type}: {error_msg}"
                )
                span.set_attribute("feature_flag.success", False)
                span.set_attribute("feature_flag.default_used", True)
    
    # Return default value if all retries failed
    return default_value


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize FlagdProvider with timeout configuration
    flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
    flagd_port = int(os.environ.get('FLAGD_PORT', 8013))
    
    try:
        logger_init = logging.getLogger('init')
        logger_init.info(
            f"Initializing FlagdProvider with host={flagd_host}, port={flagd_port}, "
            f"timeout={FLAGD_TIMEOUT}s, max_retries={FLAGD_MAX_RETRIES}"
        )
        
        # Set provider with improved configuration
        api.set_provider(FlagdProvider(
            host=flagd_host,
            port=flagd_port,
            deadline=FLAGD_TIMEOUT * 1000  # FlagdProvider expects milliseconds
        ))
        api.add_hooks([TracingHook()])
        
        logger_init.info("FlagdProvider initialized successfully")
        
    except Exception as e:
        logger_init = logging.getLogger('init')
        logger_init.error(f"Failed to initialize FlagdProvider: {type(e).__name__}: {str(e)}")
        logger_init.warning("Service will continue with feature flags disabled (default values will be used)")

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
