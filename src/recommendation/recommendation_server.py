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
from grpc import RpcError, StatusCode
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

# Connection configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 10.0  # seconds
GRPC_TIMEOUT = 5.0  # seconds


def create_product_catalog_stub_with_retry():
    """Create a gRPC stub with connection validation and retry logic."""
    catalog_addr = must_map_env('PRODUCT_CATALOG_ADDR')
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Attempting to connect to product catalog at {catalog_addr} (attempt {attempt + 1}/{MAX_RETRIES})")
            
            # Create channel with connection options
            channel_options = [
                ('grpc.keepalive_time_ms', 30000),
                ('grpc.keepalive_timeout_ms', 10000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
            ]
            
            pc_channel = grpc.insecure_channel(catalog_addr, options=channel_options)
            stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
            
            # Test the connection with a simple health check
            try:
                channel_ready = grpc.channel_ready_future(pc_channel)
                channel_ready.result(timeout=GRPC_TIMEOUT)
                logger.info(f"Successfully connected to product catalog at {catalog_addr}")
                return stub, pc_channel
            except grpc.FutureTimeoutError:
                logger.warning(f"Connection attempt timed out for {catalog_addr}")
                pc_channel.close()
                
        except Exception as e:
            logger.error(f"Failed to create product catalog stub: {e}")
        
        if attempt < MAX_RETRIES - 1:
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    
    logger.error(f"Failed to connect to product catalog after {MAX_RETRIES} attempts")
    raise Exception(f"Unable to connect to product catalog at {catalog_addr}")


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
            logger.error(f"Error in ListRecommendations: {e}", exc_info=True)
            # Return empty list on error instead of failing completely
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to generate recommendations: {str(e)}")
            return demo_pb2.ListRecommendationsResponse()

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


def call_with_retry(stub_func, *args, **kwargs):
    """Execute a gRPC call with retry logic and timeout."""
    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            # Add timeout to the call
            return stub_func(*args, timeout=GRPC_TIMEOUT, **kwargs)
        except RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                logger.warning(f"Product catalog unavailable (attempt {attempt + 1}/{max_attempts}): {e.details()}")
                if attempt < max_attempts - 1:
                    time.sleep(0.5)
                    continue
            elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                logger.warning(f"Product catalog call timed out (attempt {attempt + 1}/{max_attempts})")
                if attempt < max_attempts - 1:
                    time.sleep(0.5)
                    continue
            # For other errors or last attempt, raise
            raise
    
    raise Exception("Failed to call product catalog after retries")


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
                    # Use retry logic for catalog calls
                    cat_response = call_with_retry(product_catalog_stub.ListProducts, demo_pb2.Empty())
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                except Exception as e:
                    logger.error(f"Failed to fetch products from catalog: {e}", exc_info=True)
                    # Fallback to cached IDs if available
                    if cached_ids:
                        logger.info("Using cached product IDs as fallback")
                        product_ids = cached_ids
                    else:
                        # Return empty list if no cache available
                        logger.warning("No cached products available, returning empty recommendations")
                        return []
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            try:
                # Use retry logic for catalog calls
                cat_response = call_with_retry(product_catalog_stub.ListProducts, demo_pb2.Empty())
                product_ids = [x.id for x in cat_response.products]
            except Exception as e:
                logger.error(f"Failed to fetch products from catalog: {e}", exc_info=True)
                # Fallback to cached IDs if available
                if cached_ids:
                    logger.info("Using cached product IDs as fallback")
                    product_ids = cached_ids
                else:
                    # Return default product IDs as last resort
                    logger.warning("No products available, using default recommendations")
                    product_ids = ["OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O"]

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Sample list of indicies to return
        if num_products > 0:
            indices = random.sample(range(num_products), num_return)
            # Fetch product ids from indices
            prod_list = [filtered_products[i] for i in indices]
        else:
            prod_list = []

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

    # Initialize product catalog connection with retry
    try:
        product_catalog_stub, pc_channel = create_product_catalog_stub_with_retry()
    except Exception as e:
        logger.error(f"Fatal: Could not connect to product catalog: {e}")
        # Continue anyway to allow service to start - it will use fallback logic
        catalog_addr = os.environ.get('PRODUCT_CATALOG_ADDR', 'product-catalog:3550')
        pc_channel = grpc.insecure_channel(catalog_addr)
        product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
        logger.warning("Service starting with fallback catalog connection")

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
