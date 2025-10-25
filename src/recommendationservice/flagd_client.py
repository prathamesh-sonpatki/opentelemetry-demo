import grpc
from grpc import StatusCode
import backoff
from opentelemetry import trace

class FlagdClient:
    def __init__(self, flagd_addr):
        self.channel = grpc.insecure_channel(
            flagd_addr,
            options=[
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.max_reconnect_backoff_ms', 5000)
            ]
        )
        self.stub = None  # Initialize in connect()

    @backoff.on_exception(
        backoff.expo,
        grpc.RpcError,
        max_tries=3,
        giveup=lambda e: e.code() not in [StatusCode.DEADLINE_EXCEEDED, StatusCode.UNAVAILABLE]
    )
    def connect_with_retry(self):
        """Establish connection to flagd service with retry logic"""
        try:
            # Set deadline to 10 seconds for initial connection
            with grpc.insecure_channel(self.flagd_addr) as channel:
                future = grpc.channel_ready_future(channel)
                future.result(timeout=10)
                self.channel = channel
                self.stub = self.channel.stub()
        except grpc.FutureTimeoutError:
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span('flagd_connection_error') as span:
                span.set_attribute('error', 'Connection timeout to flagd service')
            raise

    def get_event_stream(self, timeout=5):
        """Get event stream with timeout and error handling"""
        if not self.stub:
            self.connect_with_retry()

        try:
            # Set shorter timeout for individual requests
            return self.stub.EventStream(timeout=timeout)
        except grpc.RpcError as e:
            if e.code() == StatusCode.DEADLINE_EXCEEDED:
                tracer = trace.get_tracer(__name__)
                with tracer.start_as_current_span('flagd_request_timeout') as span:
                    span.set_attribute('error', 'Request timeout to flagd service')
                # Let retry decorator handle it
                raise
            raise
