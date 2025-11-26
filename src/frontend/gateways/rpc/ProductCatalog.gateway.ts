// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Metadata, status } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure gRPC client with connection options
const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR, 
  ChannelCredentials.createInsecure(),
  {
    // Add connection timeout to fail fast instead of hanging
    'grpc.initial_reconnect_backoff_ms': 1000,
    'grpc.max_reconnect_backoff_ms': 5000,
    'grpc.keepalive_time_ms': 10000,
    'grpc.keepalive_timeout_ms': 5000,
  }
);

// Retry configuration
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 500;
const REQUEST_TIMEOUT_MS = 5000; // 5 second timeout per request

// Helper function to determine if an error is retryable
function isRetryableError(error: any): boolean {
  if (!error?.code) return false;
  
  // Retry on transient network errors
  const retryableCodes = [
    status.UNAVAILABLE,        // 14 - Service unavailable (connection refused, network issues)
    status.DEADLINE_EXCEEDED,  // 4  - Request timeout
    status.RESOURCE_EXHAUSTED, // 8  - Server overloaded
    status.ABORTED,           // 10 - Operation aborted (could be transient)
  ];
  
  return retryableCodes.includes(error.code);
}

// Generic retry wrapper for gRPC calls
async function retryableGrpcCall<T>(
  operation: (metadata: Metadata) => Promise<T>,
  operationName: string
): Promise<T> {
  let lastError: any;
  
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const metadata = new Metadata();
      // Add deadline to prevent hanging requests
      metadata.set('grpc-timeout', `${REQUEST_TIMEOUT_MS}m`);
      
      return await operation(metadata);
    } catch (error: any) {
      lastError = error;
      
      // Log the error for observability
      console.error(`[ProductCatalogGateway] ${operationName} attempt ${attempt}/${MAX_RETRIES} failed:`, {
        code: error?.code,
        message: error?.message,
        details: error?.details,
      });
      
      // Check if we should retry
      if (attempt < MAX_RETRIES && isRetryableError(error)) {
        const delayMs = RETRY_DELAY_MS * attempt; // Exponential backoff
        console.info(`[ProductCatalogGateway] Retrying ${operationName} after ${delayMs}ms...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
        continue;
      }
      
      // Not retryable or max retries reached
      break;
    }
  }
  
  // All retries exhausted, throw the last error
  console.error(`[ProductCatalogGateway] ${operationName} failed after ${MAX_RETRIES} attempts`);
  throw lastError;
}

const ProductCatalogGateway = () => ({
  async listProducts(): Promise<ListProductsResponse> {
    return retryableGrpcCall(
      (metadata) => new Promise<ListProductsResponse>((resolve, reject) => {
        // Set deadline for this specific call
        const deadline = new Date();
        deadline.setMilliseconds(deadline.getMilliseconds() + REQUEST_TIMEOUT_MS);
        
        client.listProducts({}, metadata, { deadline }, (error, response) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        });
      }),
      'listProducts'
    );
  },
  
  async getProduct(id: string): Promise<Product> {
    return retryableGrpcCall(
      (metadata) => new Promise<Product>((resolve, reject) => {
        // Set deadline for this specific call
        const deadline = new Date();
        deadline.setMilliseconds(deadline.getMilliseconds() + REQUEST_TIMEOUT_MS);
        
        client.getProduct({ id }, metadata, { deadline }, (error, response) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        });
      }),
      `getProduct(${id})`
    );
  },
});

export default ProductCatalogGateway();
