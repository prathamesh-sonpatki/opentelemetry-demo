import grpc
from grpc import RpcError
from typing import Optional
import time
from opentelemetry import trace

# Configure timeouts and retry settings
DEFAULT_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 10  # seconds

class FlagdClient:
    def __init__(self, host: str = "flagd:8013"):
        self._channel = None
        self._host = host
        self._stub = None
        self.tracer = trace.get_tracer(__name__)
    
    @property
    def channel(self):
        if self._channel is None or self._channel.closed():
            self._channel = grpc.insecure_channel(
                self._host,
                options=[
                    ("grpc.enable_retries", 1),
                    ("grpc.keepalive_time_ms", 10000),  # Send keepalive ping every 10 second
                    ("grpc.keepalive_timeout_ms", 5000),  # Wait 5 second for keepalive ping response
                    ("grpc.keepalive_permit_without_calls", 1),  # Allow keepalive pings when there are no calls
                    ("grpc.http2.max_pings_without_data", 0),  # Allow unlimited pings without data
                ]
            )
        return self._channel

    def _retry_unary_call(self, method, request, deadline=None):
        retries = 0
        while retries < MAX_RETRIES:
            try:
                return method(
                    request,
                    timeout=deadline or DEFAULT_TIMEOUT,
                )
            except RpcError as e:
                retries += 1
                if retries == MAX_RETRIES:
                    raise
                
                backoff = min(INITIAL_BACKOFF * (2 ** (retries - 1)), MAX_BACKOFF)
                time.sleep(backoff)
                
                # If channel is in a bad state, reset it
                if e.code() in [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED]:
                    self._channel = None

    def _handle_stream(self, method, request, deadline=None):
        while True:  # Keep trying to maintain the stream
            try:
                for response in method(
                    request,
                    timeout=deadline or DEFAULT_TIMEOUT,
                ):
                    yield response
            except RpcError as e:
                if e.code() == grpc.StatusCode.CANCELLED:
                    return  # Stream was intentionally cancelled
                    
                # For other errors, log and retry with backoff
                with self.tracer.start_as_current_span("flagd_stream_error_handler") as span:
                    span.set_attribute("error.type", str(e.code()))
                    span.set_attribute("error.message", e.details())
                
                time.sleep(INITIAL_BACKOFF)
                self._channel = None  # Force channel recreation
                continue  # Retry the stream