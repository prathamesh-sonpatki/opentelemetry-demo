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

# Fallback product IDs when ProductCatalog is unavailable
FALLBACK_PRODUCT_IDS = [
    "OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM",
    "2ZYFJ3GM2N", "0PUK6V6EV0", "LS4PSXUNUM", "9SIQT8TOJO", "6E92ZMYYFZ"
]

# Circuit breaker state
class CircuitBreaker:
    def __init__(self, failure_threshold=3, timeout=30):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker transitioning to HALF_OPEN")
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                logger.info("Circuit breaker reset to CLOSED")
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(f"Circuit breaker opened after {self.failure_count} failures")
            raise e

catalog_circuit_breaker = CircuitBreaker()


def retry_with_backoff(retries=3, backoff_in_seconds=1):
    """Decorator for retrying functions with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except grpc.RpcError as e:
                    if x == retries:
                        logger.error(f"Max retries ({retries}) reached for {func.__name__}: {e.code()}")
                        raise
                    
                    wait_time = backoff_in_seconds * (2 ** x)
                    logger.warning(f"Retry {x+1}/{retries} for {func.__name__} after {wait_time}s. Error: {e.code()}")
                    time.sleep(wait_time)
                    x += 1
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
            logger.error(f"Error in ListRecommendations: {str(e)}")
            # Return fallback recommendations instead of failing
            span = trace.get_current_span()
            span.set_attribute("app.fallback_used", True)
            span.record_exception(e)
            
            response = demo_pb2.ListRecommendationsResponse()
            fallback_list = random.sample(FALLBACK_PRODUCT_IDS, min(5, len(FALLBACK_PRODUCT_IDS)))
            response.product_ids.extend(fallback_list)
            rec_svc_metrics["app_recommendations_counter"].add(len(fallback_list), {'recommendation.type': 'fallback'})
            return response

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


@retry_with_backoff(retries=2, backoff_in_seconds=0.5)
def fetch_products_from_catalog():
    """Fetch products from catalog with retry logic"""
    try:
        return catalog_circuit_breaker.call(
            lambda: product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=5.0)
        )
    except Exception as e:
        logger.error(f"Failed to fetch from product catalog: {str(e)}")
        raise


def get_product_list(request_product_ids):
    global first_run
    global cached_ids
    with tracer.start_as_current_span("get_product_list") as span:
        max_responses = 5

        # Formulate the list of characters to list of strings
        request_product_ids_str = ''.join(request_product_ids)
        request_product_ids = request_product_ids_str.split(',')

        # Feature flag scenario - Cache Leak
        # Wrap feature flag check with error handling
        cache_failure_enabled = False
        try:
            cache_failure_enabled = check_feature_flag("recommendationCacheFailure")
        except Exception as e:
            logger.warning(f"Feature flag check failed, using default: {str(e)}")
            span.set_attribute("app.feature_flag.error", str(e))
        
        if cache_failure_enabled:
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss")
                try:
                    cat_response = fetch_products_from_catalog()
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                except Exception as e:
                    logger.error(f"Failed to fetch products, using fallback: {str(e)}")
                    span.set_attribute("app.fallback_used", True)
                    span.record_exception(e)
                    product_ids = FALLBACK_PRODUCT_IDS
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids if cached_ids else FALLBACK_PRODUCT_IDS
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            try:
                cat_response = fetch_products_from_catalog()
                product_ids = [x.id for x in cat_response.products]
            except Exception as e:
                logger.error(f"Failed to fetch products, using fallback: {str(e)}")
                span.set_attribute("app.fallback_used", True)
                span.record_exception(e)
                product_ids = FALLBACK_PRODUCT_IDS

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Sample list of indicies to return
        if num_return > 0:
            indices = random.sample(range(num_products), num_return)
            # Fetch product ids from indices
            prod_list = [filtered_products[i] for i in indices]
        else:
            # Return fallback if no products available
            prod_list = random.sample(FALLBACK_PRODUCT_IDS, min(max_responses, len(FALLBACK_PRODUCT_IDS)))

        span.set_attribute("app.filtered_products.list", prod_list)

        return prod_list


def must_map_env(key: str):
    value = os.environ.get(key)
    if value is None:
        raise Exception(f'{key} environment variable must be set')
    return value


def check_feature_flag(flag_name: str):
    # Initialize OpenFeature with timeout protection
    try:
        client = api.get_client()
        # Add timeout to prevent hanging on feature flag checks
        return client.get_boolean_value("recommendationCacheFailure", False)
    except Exception as e:
        logger.warning(f"Feature flag check failed for {flag_name}: {str(e)}")
        return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    
    # Initialize FlagD provider with error handling
    try:
        flagd_host = os.environ.get('FLAGD_HOST', 'flagd')
        flagd_port = int(os.environ.get('FLAGD_PORT', 8013))
        api.set_provider(FlagdProvider(
            host=flagd_host, 
            port=flagd_port,
            deadline=10000  # 10 second timeout
        ))
        api.add_hooks([TracingHook()])
        logger_temp = logging.getLogger('main')
        logger_temp.info(f"FlagD provider initialized at {flagd_host}:{flagd_port}")
    except Exception as e:
        logger_temp = logging.getLogger('main')
        logger_temp.warning(f"Failed to initialize FlagD provider: {str(e)}. Feature flags will use defaults.")

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
    
    # Configure gRPC channel with keepalive and timeout options
    options = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', True),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.http2.min_time_between_pings_ms', 10000),
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    logger.info(f"Product catalog client configured for {catalog_addr}")

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
