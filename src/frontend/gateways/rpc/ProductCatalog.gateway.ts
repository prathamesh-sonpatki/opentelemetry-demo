// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, status } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 100;
const TIMEOUT_MS = 5000;

// Create client with connection timeout
const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR,
  ChannelCredentials.createInsecure(),
  {
    'grpc.keepalive_time_ms': 10000,
    'grpc.keepalive_timeout_ms': 5000,
    'grpc.initial_reconnect_backoff_ms': 1000,
    'grpc.max_reconnect_backoff_ms': 5000,
  }
);

/**
 * Sleep utility for retry delays
 */
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Check if error is retryable
 */
const isRetryableError = (error: any): boolean => {
  if (!error || !error.code) return false;
  
  // Retry on connection errors and temporary failures
  return [
    status.UNAVAILABLE,       // Service unavailable
    status.DEADLINE_EXCEEDED, // Timeout
    status.UNKNOWN,           // Unknown errors (often connection issues)
    status.INTERNAL,          // Internal errors that might be transient
  ].includes(error.code);
};

/**
 * Execute gRPC call with exponential backoff retry logic
 */
async function withRetry<T>(
  operation: () => Promise<T>,
  operationName: string
): Promise<T> {
  let lastError: any;
  
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Log the error
      console.error(
        `[ProductCatalogGateway] ${operationName} attempt ${attempt + 1}/${MAX_RETRIES} failed:`,
        {
          code: error.code,
          message: error.message,
          details: error.details,
        }
      );
      
      // Check if we should retry
      if (!isRetryableError(error) || attempt === MAX_RETRIES - 1) {
        break;
      }
      
      // Exponential backoff: 100ms, 200ms, 400ms
      const delayMs = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt);
      console.log(`[ProductCatalogGateway] Retrying ${operationName} in ${delayMs}ms...`);
      await sleep(delayMs);
    }
  }
  
  // All retries failed, throw enhanced error
  const enhancedError = new Error(
    `Failed to ${operationName} after ${MAX_RETRIES} attempts: ${lastError.message}`
  );
  (enhancedError as any).originalError = lastError;
  (enhancedError as any).code = lastError.code;
  throw enhancedError;
}

const ProductCatalogGateway = () => ({
  async listProducts(): Promise<ListProductsResponse> {
    return withRetry(
      () =>
        new Promise<ListProductsResponse>((resolve, reject) => {
          const deadline = new Date();
          deadline.setMilliseconds(deadline.getMilliseconds() + TIMEOUT_MS);
          
          client.listProducts(
            {},
            { deadline },
            (error, response) => (error ? reject(error) : resolve(response))
          );
        }),
      'listProducts'
    );
  },
  
  async getProduct(id: string): Promise<Product> {
    return withRetry(
      () =>
        new Promise<Product>((resolve, reject) => {
          const deadline = new Date();
          deadline.setMilliseconds(deadline.getMilliseconds() + TIMEOUT_MS);
          
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
