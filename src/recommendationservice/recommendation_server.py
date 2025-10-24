import os
from concurrent import futures
import random
import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from opentelemetry import trace
from opentelemetry.instrumentation.grpc import GrpcInstrumentator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

import demo_pb2_grpc
from recommendation_service import RecommendationService

# Configure OpenTelemetry
tracer_provider = TracerProvider()
otlp_exporter = OTLPSpanExporter()
span_processor = BatchSpanProcessor(otlp_exporter)
tracer_provider.add_span_processor(span_processor)
trace.set_tracer_provider(tracer_provider)

# Instrument gRPC
GrpcInstrumentator().instrument()

def main():
    # Get port from environment variable
    port = os.environ.get('PORT', '8080')
    catalog_addr = os.environ.get('PRODUCT_CATALOG_SERVICE_ADDR', 'localhost:3550')
    
    # Configure gRPC channel with increased timeout for flagd
    channel_options = [
        ('grpc.keepalive_time_ms', 30000),  # Send keepalive ping every 30 seconds
        ('grpc.keepalive_timeout_ms', 10000),  # Wait 10 seconds for ping ack before marking the connection as dead
        ('grpc.max_receive_message_length', 10 * 1024 * 1024),  # 10MB
        ('grpc.enable_http_proxy', 0),  # Disable HTTP proxy
        ('grpc.initial_reconnect_backoff_ms', 100),  # Fast initial reconnect
        ('grpc.max_reconnect_backoff_ms', 10000),  # Cap maximum reconnect backoff
        ('grpc.client_idle_timeout_ms', 60000),  # Close idle connections after 60s
    ]
    
    # Create gRPC server with optimized options
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=channel_options
    )
    
    # Initialize services
    service = RecommendationService(catalog_addr)
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    # Start server
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    # Mark service as serving
    for service in service.__class__.get_served_services():
        health_servicer.set(service, health_pb2.HealthCheckResponse.SERVING)
    
    server.wait_for_termination()

if __name__ == '__main__':
    main()