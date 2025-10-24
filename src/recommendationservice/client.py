import grpc
from grpc import RpcError
from config import GRPC_TIMEOUT_SECONDS, RETRY_POLICY

class GrpcClientWrapper:
    def __init__(self, target, timeout=None):
        self.target = target
        self.timeout = timeout or GRPC_TIMEOUT_SECONDS
        
        # Create channel with retry policy
        self.channel = grpc.insecure_channel(
            target=self.target,
            options=[
                ('grpc.enable_retries', 1),
                ('grpc.service_config', 
                 '''{
                     "methodConfig": [{
                         "name": [{"service": "flagd.evaluation.v1.Service"}],
                         "retryPolicy": {
                             "maxAttempts": %d,
                             "initialBackoff": "%fs",
                             "maxBackoff": "%fs",
                             "backoffMultiplier": %f,
                             "retryableStatusCodes": ["DEADLINE_EXCEEDED", "UNAVAILABLE"]
                         }
                     }]
                 }''' % (
                     RETRY_POLICY['max_attempts'],
                     RETRY_POLICY['initial_backoff'],
                     RETRY_POLICY['max_backoff'],
                     RETRY_POLICY['backoff_multiplier']
                 ))
            ]
        )

    def __enter__(self):
        return self.channel

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.channel.close()