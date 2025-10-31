#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
from concurrent import futures
import time
import threading

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
    Check feature flag value with graceful fallback.
    Returns False if the feature flag service is unavailable.
    """
    try:
        # Initialize OpenFeature
        client = api.get_client()
        return client.get_boolean_value("recommendationCacheFailure", False)
    except Exception as e:
        # Log the error and return default value
        # This handles cases where flagd connection times out or fails
        logger.warning(f"Failed to check feature flag '{flag_name}': {e}. Using default value: False")
        return False


def initialize_flagd_provider_with_retry(host: str, port: int, max_retries: int = 3, retry_delay: int = 5):
    """
    Initialize FlagdProvider with retry logic to handle connection failures.
    The EventStream connection may fail initially or timeout, so we implement
    graceful error handling to allow the service to continue operating.
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Initializing FlagdProvider (attempt {attempt + 1}/{max_retries})...")
            provider = FlagdProvider(host=host, port=port)
            api.set_provider(provider)
            api.add_hooks([TracingHook()])
            logger.info("FlagdProvider initialized successfully")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize FlagdProvider (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("FlagdProvider initialization failed after all retries. Service will continue without feature flags.")
                # Set a no-op provider to allow the service to continue
                try:
                    from openfeature.provider.in_memory_provider import InMemoryProvider
                    api.set_provider(InMemoryProvider({}))
                    logger.info("Using InMemoryProvider as fallback")
                except:
                    logger.warning("Could not set fallback provider")
                return False
    return False


def monitor_flagd_connection():
    """
    Background thread to monitor and reconnect to flagd if the EventStream fails.
    This handles the DEADLINE_EXCEEDED errors that occur when the stream times out.
    """
    while True:
        try:
            # Sleep for 60 seconds between checks
            time.sleep(60)
            
            # Try to check a feature flag to verify the connection is alive
            try:
                client = api.get_client()
                client.get_boolean_value("healthCheck", False)
            except Exception as e:
                logger.warning(f"FlagD connection check failed: {e}. Attempting to reconnect...")
                flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
                flagd_port = int(os.environ.get('FLAGD_PORT', 8013))
                initialize_flagd_provider_with_retry(flagd_host, flagd_port, max_retries=2, retry_delay=5)
        except Exception as e:
            logger.error(f"Error in flagd connection monitor: {e}")


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize FlagdProvider with retry logic
    flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
    flagd_port = int(os.environ.get('FLAGD_PORT', 8013))
    initialize_flagd_provider_with_retry(flagd_host, flagd_port)

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

    # Start background thread to monitor flagd connection
    monitor_thread = threading.Thread(target=monitor_flagd_connection, daemon=True)
    monitor_thread.start()
    logger.info("Started FlagD connection monitor thread")

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
