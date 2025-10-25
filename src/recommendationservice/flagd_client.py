import grpc
from opentelemetry import trace
from opentelemetry.instrumentation.grpc import GrpcInstrumentator

def create_flagd_client(target="flagd:8013"):
    """
    Create a gRPC client for flagd service with proper timeout configuration.
    """
    # Configure channel options for long-lived streaming
    options = [
        ('grpc.keepalive_time_ms', 10000),  # Send keepalive ping every 10 seconds
        ('grpc.keepalive_timeout_ms', 5000),  # Wait 5 seconds for keepalive ping response
        ('grpc.keepalive_permit_without_calls', True),  # Allow keepalive pings when there are no calls
        ('grpc.http2.max_pings_without_data', 0),  # Allow unlimited pings without data
        ('grpc.max_receive_message_length', 4 * 1024 * 1024),  # 4MB max message size
    ]
    
    # Create secure channel with options
    channel = grpc.insecure_channel(target, options=options)
    
    # Add OpenTelemetry instrumentation
    GrpcInstrumentator().instrument()
    
    return channel

def get_streaming_config(timeout=300):
    """
    Get gRPC call configuration for streaming endpoints.
    
    Args:
        timeout (int): Timeout in seconds for the stream (default: 5 minutes)
    """
    return {
        'timeout': timeout,  # Overall timeout for the stream
        'wait_for_ready': True,  # Wait for server to be ready
        'propagate_mask': None,  # No masks for error propagation
    }