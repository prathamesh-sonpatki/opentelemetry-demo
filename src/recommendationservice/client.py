import grpc
from opentelemetry import trace
from opentelemetry.instrumentation.grpc import GrpcInstrumentator
from config import FLAGD_HOST, FLAGD_PORT, FLAGD_TIMEOUT_MS, GRPC_MAX_RETRIES, GRPC_RETRY_DELAY_MS

def create_flagd_client():
    """Create gRPC client for flagd service with proper timeout and retry configuration"""
    # Initialize retry policy
    retry_policy = {
        'methodConfig': [{
            'name': [{}],  # Apply to all methods
            'retryPolicy': {
                'maxAttempts': GRPC_MAX_RETRIES,
                'initialBackoff': f'{GRPC_RETRY_DELAY_MS}ms',
                'maxBackoff': '5s',
                'backoffMultiplier': 1.5,
                'retryableStatusCodes': ['UNAVAILABLE', 'DEADLINE_EXCEEDED'],
            },
            'timeout': f'{FLAGD_TIMEOUT_MS}ms',
        }]
    }

    # Create channel with retry policy
    channel = grpc.insecure_channel(
        f'{FLAGD_HOST}:{FLAGD_PORT}',
        options=[
            ('grpc.enable_retries', 1),
            ('grpc.service_config', json.dumps(retry_policy))
        ]
    )

    # Instrument gRPC calls
    GrpcInstrumentator().instrument()

    return channel