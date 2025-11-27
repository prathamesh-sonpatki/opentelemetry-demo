// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Metadata } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

// Helper function to implement retry logic with exponential backoff
const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  maxRetries: number = 3,
  initialDelay: number = 100
): Promise<T> => {
  let lastError: Error;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Check if error is retryable (connection errors, unavailable, etc.)
      const isRetryable = 
        error.code === 14 || // UNAVAILABLE
        error.code === 4 ||  // DEADLINE_EXCEEDED
        error.message?.includes('ECONNREFUSED') ||
        error.message?.includes('Connection reset');
      
      if (!isRetryable || attempt === maxRetries - 1) {
        throw error;
      }
      
      // Exponential backoff with jitter
      const delay = initialDelay * Math.pow(2, attempt) + Math.random() * 100;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw lastError!;
};

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryWithBackoff(
      () => new Promise<ListProductsResponse>((resolve, reject) => {
        const deadline = new Date();
        deadline.setSeconds(deadline.getSeconds() + 5); // 5 second timeout
        
        const metadata = new Metadata();
        const options = {
          deadline: deadline.getTime()
        };
        
        client.listProducts({}, metadata, options, (error, response) => {
          if (error) {
            // Log error details for observability
            console.error('[ProductCatalogGateway] listProducts failed:', {
              code: error.code,
              message: error.message,
              details: error.details
            });
            reject(error);
          } else {
            resolve(response);
          }
        });
      })
    );
  },
  getProduct(id: string) {
    return retryWithBackoff(
      () => new Promise<Product>((resolve, reject) => {
        const deadline = new Date();
        deadline.setSeconds(deadline.getSeconds() + 5); // 5 second timeout
        
        const metadata = new Metadata();
        const options = {
          deadline: deadline.getTime()
        };
        
        client.getProduct({ id }, metadata, options, (error, response) => {
          if (error) {
            // Log error details for observability
            console.error('[ProductCatalogGateway] getProduct failed:', {
              code: error.code,
              message: error.message,
              details: error.details,
              productId: id
            });
            reject(error);
          } else {
            resolve(response);
          }
        });
      })
    );
  },
});

export default ProductCatalogGateway();
