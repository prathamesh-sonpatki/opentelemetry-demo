// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure gRPC channel options for resilience
const channelOptions: ChannelOptions = {
  'grpc.keepalive_time_ms': 30000, // Send keepalive ping every 30 seconds
  'grpc.keepalive_timeout_ms': 10000, // Wait 10 seconds for keepalive response
  'grpc.keepalive_permit_without_calls': 1, // Allow keepalive pings when no calls
  'grpc.http2.max_pings_without_data': 0, // Allow unlimited pings without data
  'grpc.http2.min_time_between_pings_ms': 10000, // Minimum 10 seconds between pings
  'grpc.http2.min_ping_interval_without_data_ms': 30000, // 30 seconds between pings without data
  'grpc.max_reconnect_backoff_ms': 10000, // Max reconnect backoff of 10 seconds
  'grpc.initial_reconnect_backoff_ms': 1000, // Start with 1 second reconnect backoff
};

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR,
  ChannelCredentials.createInsecure(),
  channelOptions
);

// Helper function to implement retry logic with exponential backoff
async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  maxRetries = 3,
  initialDelay = 100
): Promise<T> {
  let lastError: Error;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;
      
      // Check if error is retryable (connection issues)
      const errorMessage = error instanceof Error ? error.message : String(error);
      const isRetryable = 
        errorMessage.includes('UNAVAILABLE') ||
        errorMessage.includes('ECONNREFUSED') ||
        errorMessage.includes('ECONNRESET') ||
        errorMessage.includes('DEADLINE_EXCEEDED');
      
      if (!isRetryable || attempt === maxRetries - 1) {
        throw error;
      }
      
      // Exponential backoff delay
      const delay = initialDelay * Math.pow(2, attempt);
      console.warn(
        `ProductCatalog gRPC call failed (attempt ${attempt + 1}/${maxRetries}): ${errorMessage}. Retrying in ${delay}ms...`
      );
      
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw lastError!;
}

const ProductCatalogGateway = () => ({
  async listProducts(): Promise<ListProductsResponse> {
    return retryWithBackoff(
      () =>
        new Promise<ListProductsResponse>((resolve, reject) => {
          // Set deadline to 10 seconds
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 10);
          
          client.listProducts({}, { deadline }, (error, response) => {
            if (error) {
              console.error('ProductCatalog.listProducts error:', error.message);
              reject(error);
            } else {
              resolve(response);
            }
          });
        }),
      3, // Max 3 retry attempts
      100 // Initial delay of 100ms
    );
  },
  async getProduct(id: string): Promise<Product> {
    return retryWithBackoff(
      () =>
        new Promise<Product>((resolve, reject) => {
          // Set deadline to 10 seconds
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 10);
          
          client.getProduct({ id }, { deadline }, (error, response) => {
            if (error) {
              console.error(`ProductCatalog.getProduct error for id ${id}:`, error.message);
              reject(error);
            } else {
              resolve(response);
            }
          });
        }),
      3, // Max 3 retry attempts
      100 // Initial delay of 100ms
    );
  },
});

export default ProductCatalogGateway();
