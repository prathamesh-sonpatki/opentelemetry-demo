// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions, status } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = 'productcatalogservice:3550' } = process.env;

// Channel options for better connection handling
const channelOptions: ChannelOptions = {
  'grpc.enable_retries': 1,
  'grpc.initial_reconnect_backoff_ms': 100,
  'grpc.max_reconnect_backoff_ms': 3000,
  'grpc.min_reconnect_backoff_ms': 100,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_time_ms': 10000
};

// Create client with retry options
const createClient = () => {
  if (!PRODUCT_CATALOG_ADDR) {
    throw new Error('PRODUCT_CATALOG_ADDR environment variable is not set');
  }
  
  return new ProductCatalogServiceClient(
    PRODUCT_CATALOG_ADDR,
    ChannelCredentials.createInsecure(),
    channelOptions
  );
};

const client = createClient();

const retryOperation = async <T>(operation: () => Promise<T>, maxRetries = 3): Promise<T> => {
  let lastError: Error | null = null;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Don't retry if it's not a connection issue
      if (error.code !== status.UNAVAILABLE && error.code !== status.INTERNAL) {
        throw error;
      }
      
      if (attempt === maxRetries) {
        break;
      }
      
      // Wait before retrying (exponential backoff)
      await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 100));
    }
  }
  
  throw lastError;
};

const ProductCatalogGateway = () => ({
  async listProducts(): Promise<ListProductsResponse> {
    return retryOperation(() => 
      new Promise<ListProductsResponse>((resolve, reject) =>
        client.listProducts({}, (error, response) => {
          if (error) {
            console.error('Error listing products:', error);
            reject(error);
            return;
          }
          resolve(response);
        })
      )
    );
  },

  async getProduct(id: string): Promise<Product> {
    return retryOperation(() =>
      new Promise<Product>((resolve, reject) =>
        client.getProduct({ id }, (error, response) => {
          if (error) {
            console.error(`Error getting product ${id}:`, error);
            reject(error);
            return;
          }
          resolve(response);
        })
      )
    );
  },
});

export default ProductCatalogGateway();
