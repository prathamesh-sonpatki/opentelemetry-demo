// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Metadata, CallOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure gRPC channel with keepalive options for better connection stability
const channelOptions = {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.http2.max_pings_without_data': 0,
  'grpc.initial_reconnect_backoff_ms': 1000,
  'grpc.max_reconnect_backoff_ms': 5000,
};

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR,
  ChannelCredentials.createInsecure(),
  channelOptions
);

/**
 * Retry helper with exponential backoff for gRPC calls
 * @param fn Function that returns a Promise
 * @param maxRetries Maximum number of retry attempts (default: 3)
 * @param operationName Name of the operation for logging
 */
async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  operationName: string = 'operation'
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error: any) {
      lastError = error;
      
      // Log the error with attempt number
      console.error(
        `[ProductCatalogGateway] ${operationName} failed (attempt ${attempt + 1}/${maxRetries}):`,
        {
          code: error.code,
          message: error.message,
          details: error.details,
        }
      );

      // Check if we should retry based on error code
      const shouldRetry = 
        error.code === 14 || // UNAVAILABLE
        error.code === 4 ||  // DEADLINE_EXCEEDED
        error.code === 2;    // UNKNOWN (connection errors)

      if (!shouldRetry) {
        console.error(`[ProductCatalogGateway] Non-retryable error for ${operationName}, throwing immediately`);
        throw error;
      }

      // Don't sleep after the last attempt
      if (attempt < maxRetries - 1) {
        // Exponential backoff: 500ms, 1s, 2s
        const backoffMs = 500 * Math.pow(2, attempt);
        console.log(`[ProductCatalogGateway] Retrying ${operationName} in ${backoffMs}ms...`);
        await new Promise(resolve => setTimeout(resolve, backoffMs));
      }
    }
  }

  // All retries exhausted
  console.error(`[ProductCatalogGateway] Max retries (${maxRetries}) exhausted for ${operationName}`);
  throw lastError;
}

/**
 * Create call options with timeout
 * @param timeoutMs Timeout in milliseconds (default: 5000)
 */
function createCallOptions(timeoutMs: number = 5000): CallOptions {
  const deadline = new Date();
  deadline.setMilliseconds(deadline.getMilliseconds() + timeoutMs);
  
  return {
    deadline,
  };
}

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryWithBackoff(
      () =>
        new Promise<ListProductsResponse>((resolve, reject) => {
          const options = createCallOptions(5000);
          const metadata = new Metadata();
          
          client.listProducts({}, metadata, options, (error, response) => {
            if (error) {
              reject(error);
            } else {
              resolve(response);
            }
          });
        }),
      3, // max 3 retries
      'listProducts'
    );
  },
  
  getProduct(id: string) {
    return retryWithBackoff(
      () =>
        new Promise<Product>((resolve, reject) => {
          const options = createCallOptions(5000);
          const metadata = new Metadata();
          
          client.getProduct({ id }, metadata, options, (error, response) => {
            if (error) {
              reject(error);
            } else {
              resolve(response);
            }
          });
        }),
      3, // max 3 retries
      `getProduct(${id})`
    );
  },
});

export default ProductCatalogGateway();
