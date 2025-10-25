import os

# Service configuration
GRPC_PORT = os.getenv('RECOMMENDATION_SERVICE_PORT', 8080)

# Feature flag service configuration
FLAGD_HOST = os.getenv('FLAGD_HOST', 'flagd')
FLAGD_PORT = os.getenv('FLAGD_PORT', 8013)
FLAGD_TIMEOUT = int(os.getenv('FLAGD_TIMEOUT_SECONDS', '10'))

# Product catalog service configuration
PRODUCT_CATALOG_SERVICE_ADDR = os.getenv('PRODUCT_CATALOG_SERVICE_ADDR', 'productcatalogservice:3550')
