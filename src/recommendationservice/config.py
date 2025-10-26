import os

# GRPC Configuration
GRPC_TIMEOUT_MS = int(os.getenv('GRPC_TIMEOUT_MS', '10000'))  # Increased from default
GRPC_RETRY_DELAY_MS = int(os.getenv('GRPC_RETRY_DELAY_MS', '1000'))
GRPC_MAX_RETRIES = int(os.getenv('GRPC_MAX_RETRIES', '5'))

# Flag Service Configuration  
FLAGD_HOST = os.getenv('FLAGD_HOST', 'flagd')
FLAGD_PORT = int(os.getenv('FLAGD_PORT', '8013'))
FLAGD_TIMEOUT_MS = int(os.getenv('FLAGD_TIMEOUT_MS', '30000'))  # Increased for EventStream