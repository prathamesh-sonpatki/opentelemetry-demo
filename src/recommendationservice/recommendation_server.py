import os
import random
import time
from concurrent import futures

import grpc
import retrying
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from opentelemetry import trace
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient

from opentelemetry.instrumentation.grpc.grpcext.intercept import ClientInterceptor

import demo_pb2
import demo_pb2_grpc
from logger import getJSONLogger

tracer = trace.get_tracer(__name__)
logger = getJSONLogger(service_name='recommendationservice')

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        try:
            max_responses = 5
            # Artificial sleep to simulate latency
            time.sleep(random.uniform(0.0, 0.3))
            
            with tracer.start_span("recommendation_products") as span:
                span.set_attribute("app.products_searched", len(request.product_ids))
                    
                product_ids = list(request.product_ids)
                filtered = list(filter(lambda x: x not in product_ids, [str(i) for i in range(1, 7)]))
                num_products = min(max_responses, len(filtered))
                products = random.sample(filtered, num_products)
                    
                span.set_attribute("app.products_recommended", len(products))
                    
                logger.info("[Recommendations] Received product_ids={}, responding with {} recommendations".format(
                    product_ids, len(products)))
                    
                response = demo_pb2.ListRecommendationsResponse()
                response.product_ids.extend(products)
                return response
                
        except Exception as e:
            logger.error(f"[Recommendations] Error getting recommendations: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return demo_pb2.ListRecommendationsResponse()

def grpc_client_interceptor():
    class GrpcRetryClientInterceptor(ClientInterceptor):
        def intercept(self, continuation, client_call_details, request):
            # Configure retry parameters
            retry_options = {
                'wait_exponential_multiplier': 1000,  # Wait 1s, 2s, 4s between retries
                'wait_exponential_max': 10000,  # Maximum wait of 10s
                'stop_max_attempt_number': 3    # Maximum 3 retry attempts
            }
            
            @retrying.retry(**retry_options)
            def retry_call():
                return continuation(client_call_details, request)
                
            try:
                return retry_call()
            except Exception as e:
                logger.error(f"[gRPC Client] Error after retries: {e}")
                raise
                
    return GrpcRetryClientInterceptor()

def main():
    # Configure gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=[grpc_client_interceptor()]
    )
    
    # Add service handlers
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(
        RecommendationService(), server)
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)
    
    # Add reflection service
    SERVICE_NAMES = (
        demo_pb2.DESCRIPTOR.services_by_name['RecommendationService'].full_name,
        health_pb2.DESCRIPTOR.services_by_name['Health'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    
    # Start server
    port = os.getenv('PORT', '8080')
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    # Initialize OpenTelemetry instrumentation
    GrpcInstrumentorServer().instrument()
    GrpcInstrumentorClient().instrument()
    
    logger.info(f"[RecommendationService] Listening on port {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    main()