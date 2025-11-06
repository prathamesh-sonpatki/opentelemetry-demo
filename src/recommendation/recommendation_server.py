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
flagd_provider_initialized = False

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
    Check feature flag value with graceful error handling.
    
    Returns the feature flag value from flagd if available, 
    otherwise returns False as a safe default to prevent service disruption.
    """
    global flagd_provider_initialized
    
    try:
        # Initialize OpenFeature client
        client = api.get_client()
        
        # Attempt to get the flag value with a timeout
        # This will use the flagd provider's internal timeout configuration
        flag_value = client.get_boolean_value(flag_name, False)
        
        # Log successful flag retrieval
        span = trace.get_current_span()
        span.set_attribute(f"app.feature_flag.{flag_name}", flag_value)
        span.set_attribute("app.feature_flag.source", "flagd")
        
        return flag_value
        
    except grpc.RpcError as e:
        # Handle gRPC-specific errors (deadline exceeded, unavailable, etc.)
        status_code = e.code() if hasattr(e, 'code') else 'UNKNOWN'
        logger.warning(
            f"Feature flag '{flag_name}' check failed with gRPC error: {status_code}. "
            f"Using default value: False. Error: {str(e)}"
        )
        
        # Add error details to span
        span = trace.get_current_span()
        span.set_attribute(f"app.feature_flag.{flag_name}", False)
        span.set_attribute("app.feature_flag.source", "default")
        span.set_attribute("app.feature_flag.error", str(status_code))
        span.record_exception(e)
        
        return False
        
    except Exception as e:
        # Handle any other unexpected errors
        logger.warning(
            f"Unexpected error checking feature flag '{flag_name}': {str(e)}. "
            f"Using default value: False."
        )
        
        # Add error details to span
        span = trace.get_current_span()
        span.set_attribute(f"app.feature_flag.{flag_name}", False)
        span.set_attribute("app.feature_flag.source", "default")
        span.set_attribute("app.feature_flag.error", type(e).__name__)
        span.record_exception(e)
        
        return False


def initialize_flagd_provider():
    """
    Initialize the flagd provider with proper error handling and timeout configuration.
    
    Returns True if initialization succeeds, False otherwise.
    """
    global flagd_provider_initialized
    
    try:
        flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
        flagd_port = int(os.environ.get('FLAGD_PORT', '8013'))
        flagd_deadline = int(os.environ.get('FLAGD_DEADLINE', '10'))  # Default 10 seconds
        
        logger.info(
            f"Initializing flagd provider: host={flagd_host}, "
            f"port={flagd_port}, deadline={flagd_deadline}s"
        )
        
        # Initialize the flagd provider with timeout configuration
        provider = FlagdProvider(
            host=flagd_host,
            port=flagd_port,
            deadline=flagd_deadline
        )
        
        # Set the provider in OpenFeature API
        api.set_provider(provider)
        
        # Add tracing hook for observability
        api.add_hooks([TracingHook()])
        
        flagd_provider_initialized = True
        logger.info("Flagd provider initialized successfully")
        
        return True
        
    except Exception as e:
        logger.error(
            f"Failed to initialize flagd provider: {str(e)}. "
            f"Feature flags will use default values."
        )
        flagd_provider_initialized = False
        return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize flagd provider with error handling
    # Service continues even if flagd is unavailable
    initialize_flagd_provider()

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
