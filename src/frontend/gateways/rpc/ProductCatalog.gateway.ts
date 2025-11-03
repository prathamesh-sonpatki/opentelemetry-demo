// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ClientOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure gRPC client with timeout and keepalive settings
const clientOptions: ClientOptions = {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.http2.max_pings_without_data': 0,
  'grpc.http2.min_time_between_pings_ms': 10000,
  'grpc.http2.min_ping_interval_without_data_ms': 5000,
};

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR, 
  ChannelCredentials.createInsecure(),
  clientOptions
);

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 2000;
const DEFAULT_TIMEOUT_MS = 5000;

/**
 * Sleep helper for retry delays
 */
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Calculate exponential backoff delay with jitter
 */
const calculateRetryDelay = (attempt: number): number => {
  const delay = Math.min(INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt), MAX_RETRY_DELAY_MS);
  // Add jitter (±20%)
  const jitter = delay * 0.2 * (Math.random() * 2 - 1);
  return Math.floor(delay + jitter);
};

/**
 * Check if error is retryable
 */
const isRetryableError = (error: any): boolean => {
  if (!error || !error.code) return false;
  
  // gRPC error codes that are retryable
  const retryableCodes = [
    14, // UNAVAILABLE
    4,  // DEADLINE_EXCEEDED
    8,  // RESOURCE_EXHAUSTED
    10, // ABORTED
  ];
  
  return retryableCodes.includes(error.code);
};

/**
 * Generic retry wrapper for gRPC calls
 */
async function retryGrpcCall<T>(
  operation: () => Promise<T>,
  operationName: string
): Promise<T> {
  let lastError: any;
  
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Log error for observability
      console.error(`[ProductCatalog] ${operationName} attempt ${attempt + 1}/${MAX_RETRIES} failed:`, {
        code: error?.code,
        message: error?.message,
        details: error?.details,
      });
      
      // Don't retry if error is not retryable or if this is the last attempt
      if (!isRetryableError(error) || attempt === MAX_RETRIES - 1) {
        break;
      }
      
      // Wait before retrying with exponential backoff
      const delay = calculateRetryDelay(attempt);
      console.log(`[ProductCatalog] Retrying ${operationName} in ${delay}ms...`);
      await sleep(delay);
    }
  }
  
  // All retries exhausted, throw the last error
  throw lastError;
}

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryGrpcCall(
      () => new Promise<ListProductsResponse>((resolve, reject) => {
        const deadline = new Date();
        deadline.setMilliseconds(deadline.getMilliseconds() + DEFAULT_TIMEOUT_MS);
        
        client.listProducts(
          {}, 
          { deadline },
          (error, response) => (error ? reject(error) : resolve(response))
        );
      }),
      'listProducts'
    );
  },
  getProduct(id: string) {
    return retryGrpcCall(
      () => new Promise<Product>((resolve, reject) => {
        const deadline = new Date();
        deadline.setMilliseconds(deadline.getMilliseconds() + DEFAULT_TIMEOUT_MS);
        
        client.getProduct(
          { id }, 
          { deadline },
          (error, response) => (error ? reject(error) : resolve(response))
        );
      }),
      `getProduct(${id})`
    );
  },
});

export default ProductCatalogGateway();
