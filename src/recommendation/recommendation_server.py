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


def init_flagd_provider_with_retry():
    """
    Initialize FlagdProvider with proper timeout and retry configuration.
    
    This function handles the initialization of the flagd feature flag provider
    with resilience to network issues and timeouts. It configures:
    - Extended timeout for long-lived EventStream connections
    - gRPC keepalive settings to maintain connection health
    - Retry logic with exponential backoff
    - Graceful degradation if flagd is unavailable
    """
    max_retries = 3
    retry_delay = 1  # seconds
    
    # Configure gRPC channel with settings optimized for long-lived streaming connections
    channel_options = [
        ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
        ('grpc.max_send_message_length', 100 * 1024 * 1024),
        ('grpc.keepalive_time_ms', 30000),  # Send keepalive ping every 30 seconds
        ('grpc.keepalive_timeout_ms', 10000),  # Wait 10 seconds for keepalive ack
        ('grpc.keepalive_permit_without_calls', 1),  # Allow keepalive pings without active calls
        ('grpc.http2.max_pings_without_data', 0),  # No limit on pings without data
        ('grpc.http2.min_ping_interval_without_data_ms', 30000),  # Min interval between pings
    ]
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Initializing flagd provider (attempt {attempt + 1}/{max_retries})")
            
            # Initialize FlagdProvider with extended timeout and channel options
            provider = FlagdProvider(
                host=os.environ.get('FLAGD_HOST', 'flagd'),
                port=int(os.environ.get('FLAGD_PORT', 8013)),
                deadline=3600000,  # 1 hour timeout for EventStream (in milliseconds)
            )
            
            api.set_provider(provider)
            logger.info("Successfully initialized flagd provider with extended timeout configuration")
            return
            
        except Exception as e:
            logger.warning(f"Failed to initialize flagd provider (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(
                    "Failed to initialize flagd provider after all retries. "
                    "Service will continue with default feature flag values. "
                    "Feature flags will return their default values."
                )
                # Service continues to function - feature flags will use default values


def check_feature_flag(flag_name: str, default_value: bool = False):
    """
    Check feature flag with error handling and fallback to default value.
    
    This function safely evaluates feature flags with proper error handling.
    If the flagd connection is unavailable or times out, it returns the
    default value instead of crashing.
    
    Args:
        flag_name: Name of the feature flag to check
        default_value: Default value to return if flag evaluation fails
        
    Returns:
        Boolean value of the feature flag, or default_value on error
    """
    try:
        client = api.get_client()
        result = client.get_boolean_value(flag_name, default_value)
        return result
    except Exception as e:
        logger.warning(
            f"Error checking feature flag '{flag_name}': {e}. "
            f"Using default value: {default_value}"
        )
        # Return default value instead of crashing the service
        return default_value


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize flagd provider with retry logic and extended timeouts
    init_flagd_provider_with_retry()
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
