import os
import random
import time
from concurrent import futures

import grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc
from grpc_health.v1 import health
from opentelemetry import trace
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer, GrpcInstrumentorClient

import demo_pb2
import demo_pb2_grpc
from logger import getJSONLogger

from flagd.evaluation.v1 import evaluation_pb2_grpc
from flagd.evaluation.v1 import evaluation_pb2

logger = getJSONLogger('recommendationservice-server')

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        max_responses = 5
        # Artificially sleep to simulate latency
        time.sleep(random.uniform(0, 0.1))

        # build and return response
        response = demo_pb2.ListRecommendationsResponse()
        for i in range(max_responses):
            response.product_ids.append(str(random.randint(1, 1000)))
        return response

def create_flagd_client():
    """Create flagd client with improved timeout and retry logic"""
    max_retries = 3
    initial_timeout = 300  # 5 minutes for long-lived streams
    backoff_multiplier = 1.5

    for attempt in range(max_retries):
        try:
            timeout = initial_timeout * (backoff_multiplier ** attempt)
            channel = grpc.insecure_channel(
                'flagd:8013',
                options=[
                    ('grpc.keepalive_time_ms', 10000),
                    ('grpc.keepalive_timeout_ms', 5000),
                    ('grpc.keepalive_permit_without_calls', True),
                    ('grpc.http2.max_pings_without_data', 0),
                ]
            )
            stub = evaluation_pb2_grpc.ServiceStub(channel)
            request = evaluation_pb2.EventStreamRequest()
            
            # Configure deadline for the streaming call
            return stub.EventStream(
                request,
                timeout=timeout,
                wait_for_ready=True
            )
        except grpc.RpcError as e:
            logger.error(f"Failed to connect to flagd (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                logger.error("Max retries reached, giving up")
                raise
            time.sleep(1 * (attempt + 1))  # Exponential backoff

def serve():
    # Initialize OpenTelemetry tracing
    GrpcInstrumentorServer().instrument()
    GrpcInstrumentorClient().instrument()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add health check service
    service = RecommendationService()
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)

    # Start listening
    port = os.getenv("PORT", "8080")
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"Recommendation service listening on port {port}")

    # Initialize flagd client with retry logic
    try:
        flagd_stream = create_flagd_client()
        logger.info("Successfully connected to flagd service")
    except Exception as e:
        logger.error(f"Failed to initialize flagd client: {str(e)}")
        # Continue without feature flags if flagd is unreachable

    # Keep thread alive
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    logger.info("Initializing recommendation service")
    serve()