import os
import random
import time
from concurrent import futures

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from opentelemetry import trace
from python_grpc_interceptor import ServerInterceptor

from logger import getJSONLogger
from recommendations_pb2 import ListProductsResponse
from recommendations_pb2_grpc import RecommendationsServicer, add_RecommendationsServicer_to_server

# Configure gRPC retry policy
retry_policy = {
    'initial_backoff': 0.1,  # Initial retry delay in seconds
    'max_backoff': 5,        # Maximum retry delay in seconds
    'multiplier': 2,         # Backoff multiplier
    'retries': 3,            # Maximum number of retries
}

# Configure gRPC client options
grpc_options = [
    ('grpc.enable_retries', 1),
    ('grpc.service_config', json.dumps({
        'methodConfig': [{
            'name': [{'service': 'flagd.evaluation.v1.Service'}],
            'retryPolicy': {
                'maxAttempts': retry_policy['retries'],
                'initialBackoff': f"{retry_policy['initial_backoff']}s",
                'maxBackoff': f"{retry_policy['max_backoff']}s",
                'backoffMultiplier': retry_policy['multiplier'],
                'retryableStatusCodes': ['DEADLINE_EXCEEDED', 'UNAVAILABLE']
            },
            'timeout': '10s'  # Increased timeout
        }]
    }))
]

def get_feature_flag(flag_key, default_value):
    try:
        # Create channel with retry configuration
        channel = grpc.insecure_channel(
            target=os.getenv('FLAGD_SERVICE_ADDR', 'flagd:8013'),
            options=grpc_options
        )
        
        # Add error handling and fallback
        try:
            # Your existing flag evaluation code here
            response = flagd_client.get_flag(flag_key)
            return response.value
        except grpc.RpcError as e:
            logger.warning(f"Failed to get feature flag {flag_key}: {e}")
            return default_value
        finally:
            channel.close()
    except Exception as e:
        logger.error(f"Error connecting to flagd service: {e}")
        return default_value

# Rest of the existing code...
