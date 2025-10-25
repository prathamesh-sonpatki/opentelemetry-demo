import grpc
from concurrent import futures
import os
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from opentelemetry import trace
from opentelemetry.instrumentation.grpc import GrpcInstrumentationClient, GrpcInstrumentationServer
from . import demo_pb2_grpc, config

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def __init__(self):
        # Initialize flagd client with configurable timeout
        self.flagd_channel = grpc.insecure_channel(
            f'{config.FLAGD_HOST}:{config.FLAGD_PORT}',
            options=[
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
            ]
        )
        # Increase timeout for EventStream calls
        self.flagd_stub = grpc.FlagdStub(
            self.flagd_channel,
            options=[
                ('grpc.enable_retries', 1),
                ('grpc.service_config', '{"methodConfig": [{"name": [{"service": "flagd.evaluation.v1.Service"}],"timeout": "' + str(config.FLAGD_TIMEOUT) + 's"}]}')
            ]
        )

    def ListRecommendations(self, request, context):
        # Implementation details...
        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(
        RecommendationService(), server)

    # Add health service
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    port = config.GRPC_PORT
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
