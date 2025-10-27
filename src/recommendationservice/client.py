import grpc
from grpc import StatusCode
from opentelemetry import trace
from typing import Optional
import time

DEFAULT_TIMEOUT = 30  # 30 seconds
MAX_RETRIES = 3
BASE_DELAY = 1  # 1 second

tracer = trace.get_tracer(__name__)

def create_channel(target: str) -> grpc.Channel:
    """Create a gRPC channel with appropriate timeout and retry settings."""
    return grpc.insecure_channel(
        target,
        options=[
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
        ]
    )

def call_with_retry(func, *args, **kwargs):
    """Execute a gRPC call with retry logic for timeouts."""
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as e:
            if e.code() == StatusCode.DEADLINE_EXCEEDED:
                delay = BASE_DELAY * (2 ** attempt)  # exponential backoff
                time.sleep(delay)
                last_exception = e
                continue
            raise
    raise last_exception

def get_feature_flag(channel: grpc.Channel, flag_key: str) -> Optional[bool]:
    """Get feature flag value with timeout and retry handling."""
    with tracer.start_as_current_span('get_feature_flag') as span:
        span.set_attribute('flag_key', flag_key)
        try:
            # Create stub with timeout
            stub = flagd_pb2_grpc.ServiceStub(channel)
            request = flagd_pb2.EvaluationRequest(flag_key=flag_key)
            
            response = call_with_retry(
                stub.Evaluate,
                request,
                timeout=DEFAULT_TIMEOUT
            )
            return response.value
        except grpc.RpcError as e:
            span.record_exception(e)
            if e.code() == StatusCode.DEADLINE_EXCEEDED:
                logger.warning(
                    'Timeout getting feature flag',
                    extra={
                        'flag_key': flag_key,
                        'timeout': DEFAULT_TIMEOUT,
                        'error': str(e)
                    }
                )
            return None
