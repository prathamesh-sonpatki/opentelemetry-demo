// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Validate that PRODUCT_CATALOG_ADDR is set
if (!PRODUCT_CATALOG_ADDR) {
  console.error('ERROR: PRODUCT_CATALOG_ADDR environment variable is not set');
  console.error('Expected format: hostname:port (e.g., product-catalog:3550)');
  throw new Error('PRODUCT_CATALOG_ADDR environment variable must be set');
}

console.log(`Initializing ProductCatalog gateway with address: ${PRODUCT_CATALOG_ADDR}`);

// Configure gRPC channel options for better connection resilience
const channelOptions: ChannelOptions = {
  'grpc.keepalive_time_ms': 30000,
  'grpc.keepalive_timeout_ms': 10000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.http2.max_pings_without_data': 0,
  'grpc.enable_retries': 1,
  'grpc.initial_reconnect_backoff_ms': 1000,
  'grpc.max_reconnect_backoff_ms': 10000,
};

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR, 
  ChannelCredentials.createInsecure(),
  channelOptions
);

// Helper function to add timeout and retry logic
const callWithRetry = async <T>(
  operation: () => Promise<T>,
  retries = 2,
  delay = 500,
  operationName = 'operation'
): Promise<T> => {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      const isLastAttempt = attempt === retries;
      
      // Log the error with context
      console.error(
        `ProductCatalog ${operationName} failed (attempt ${attempt}/${retries}):`,
        {
          code: error?.code,
          details: error?.details,
          message: error?.message,
          address: PRODUCT_CATALOG_ADDR,
        }
      );

      // Check if error is retryable (UNAVAILABLE or DEADLINE_EXCEEDED)
      const isRetryable = error?.code === 14 || error?.code === 4;
      
      if (isLastAttempt || !isRetryable) {
        console.error(`ProductCatalog ${operationName} failed permanently after ${attempt} attempts`);
        throw error;
      }

      // Exponential backoff for retries
      const backoffDelay = delay * Math.pow(2, attempt - 1);
      console.log(`Retrying ${operationName} in ${backoffDelay}ms...`);
      await new Promise(resolve => setTimeout(resolve, backoffDelay));
    }
  }
  
  throw new Error(`Failed to complete ${operationName} after ${retries} retries`);
};

const ProductCatalogGateway = () => ({
  listProducts() {
    return callWithRetry<ListProductsResponse>(
      () => new Promise<ListProductsResponse>((resolve, reject) => {
        // Add deadline (timeout) to the call
        const deadline = new Date();
        deadline.setSeconds(deadline.getSeconds() + 5); // 5 second timeout
        
        client.listProducts({}, { deadline }, (error, response) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        });
      }),
      2, // retries
      500, // initial delay
      'listProducts'
    );
  },
  getProduct(id: string) {
    return callWithRetry<Product>(
      () => new Promise<Product>((resolve, reject) => {
        // Add deadline (timeout) to the call
        const deadline = new Date();
        deadline.setSeconds(deadline.getSeconds() + 5); // 5 second timeout
        
        client.getProduct({ id }, { deadline }, (error, response) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        });
      }),
      2, // retries
      500, // initial delay
      `getProduct(${id})`
    );
  },
});

export default ProductCatalogGateway();
