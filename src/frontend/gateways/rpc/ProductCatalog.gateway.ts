// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ClientOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure gRPC client with retry and timeout options
const clientOptions: ClientOptions = {
  'grpc.initial_reconnect_backoff_ms': 1000,
  'grpc.max_reconnect_backoff_ms': 5000,
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.enable_retries': 1,
  'grpc.service_config': JSON.stringify({
    methodConfig: [{
      name: [{ service: 'hipstershop.ProductCatalogService' }],
      retryPolicy: {
        maxAttempts: 3,
        initialBackoff: '0.1s',
        maxBackoff: '1s',
        backoffMultiplier: 2,
        retryableStatusCodes: ['UNAVAILABLE', 'DEADLINE_EXCEEDED'],
      },
    }],
  }),
};

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR, 
  ChannelCredentials.createInsecure(),
  clientOptions
);

// Helper function to handle gRPC errors gracefully
const handleGrpcError = (error: any, operationName: string) => {
  console.error(`[ProductCatalog] ${operationName} failed:`, {
    message: error.message,
    code: error.code,
    details: error.details,
    metadata: error.metadata,
  });
  
  // For observability: add structured logging
  if (error.code === 14) {
    console.warn(`[ProductCatalog] Service unavailable at ${PRODUCT_CATALOG_ADDR}. Check if product-catalog service is running.`);
  }
};

const ProductCatalogGateway = () => ({
  listProducts() {
    return new Promise<ListProductsResponse>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Request timeout: product catalog service not responding'));
      }, 5000);

      client.listProducts({}, (error, response) => {
        clearTimeout(timeout);
        
        if (error) {
          handleGrpcError(error, 'listProducts');
          
          // Graceful degradation: return empty list instead of crashing
          if (error.code === 14 || error.code === 4) {
            console.warn('[ProductCatalog] Returning empty product list due to service unavailability');
            resolve({ products: [] } as ListProductsResponse);
            return;
          }
          
          reject(error);
        } else {
          resolve(response);
        }
      });
    });
  },
  
  getProduct(id: string) {
    return new Promise<Product>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Request timeout: product ${id} not responding`));
      }, 5000);

      client.getProduct({ id }, (error, response) => {
        clearTimeout(timeout);
        
        if (error) {
          handleGrpcError(error, `getProduct(${id})`);
          reject(error);
        } else {
          resolve(response);
        }
      });
    });
  },
});

export default ProductCatalogGateway();
