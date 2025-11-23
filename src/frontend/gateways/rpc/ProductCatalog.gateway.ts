// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure channel options for better resilience
const channelOptions: ChannelOptions = {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.http2.max_pings_without_data': 0,
  'grpc.http2.min_time_between_pings_ms': 10000,
  'grpc.http2.min_ping_interval_without_data_ms': 5000,
  'grpc.enable_retries': 1,
  'grpc.max_receive_message_length': 10 * 1024 * 1024, // 10MB
};

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR,
  ChannelCredentials.createInsecure(),
  channelOptions
);

// Retry helper function with exponential backoff
const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  maxRetries: number = 3,
  baseDelay: number = 1000
): Promise<T> => {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      const isLastAttempt = attempt === maxRetries;
      const isRetriableError = error.code === 14 || error.code === 2; // UNAVAILABLE or UNKNOWN
      
      if (isLastAttempt || !isRetriableError) {
        console.error(`ProductCatalog gRPC call failed after ${attempt + 1} attempts:`, error.message);
        throw error;
      }
      
      const delay = baseDelay * Math.pow(2, attempt);
      console.warn(`ProductCatalog gRPC call failed (attempt ${attempt + 1}/${maxRetries + 1}), retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  throw new Error('Retry logic failed unexpectedly');
};

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryWithBackoff(
      () => new Promise<ListProductsResponse>((resolve, reject) =>
        client.listProducts({}, { deadline: Date.now() + 10000 }, (error, response) => 
          error ? reject(error) : resolve(response)
        )
      )
    );
  },
  getProduct(id: string) {
    return retryWithBackoff(
      () => new Promise<Product>((resolve, reject) =>
        client.getProduct({ id }, { deadline: Date.now() + 10000 }, (error, response) => 
          error ? reject(error) : resolve(response)
        )
      )
    );
  },
});

export default ProductCatalogGateway();
