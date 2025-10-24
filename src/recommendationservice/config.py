import os
from grpc import StatusCode

# gRPC configuration
GRPC_TIMEOUT_SECONDS = int(os.getenv('GRPC_TIMEOUT_SECONDS', '10'))
GRPC_MAX_RETRIES = int(os.getenv('GRPC_MAX_RETRIES', '3'))

# Retry policy for transient errors
RETRY_POLICY = {
    'max_attempts': GRPC_MAX_RETRIES,
    'initial_backoff': 1.0,
    'max_backoff': 5.0,
    'backoff_multiplier': 1.5,
    'retryable_status_codes': [
        StatusCode.DEADLINE_EXCEEDED,
        StatusCode.UNAVAILABLE,
        StatusCode.RESOURCE_EXHAUSTED
    ]
}