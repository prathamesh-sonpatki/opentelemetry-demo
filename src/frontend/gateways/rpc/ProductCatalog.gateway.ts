// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, CallOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

// Retry configuration
const MAX_RETRY_ATTEMPTS = 3;
const INITIAL_RETRY_DELAY_MS = 100;
const CALL_TIMEOUT_MS = 10000; // 10 seconds
const CONNECTION_TIMEOUT_MS = 5000; // 5 seconds

/**
 * Utility function to implement exponential backoff retry logic
 */
async function retryWithBackoff<T>(
  operation: (attempt: number) => Promise<T>,
  maxAttempts: number = MAX_RETRY_ATTEMPTS
): Promise<T> {
  let lastError: Error;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation(attempt);
    } catch (error: any) {
      lastError = error;
      
      // Check if error is retryable (connection refused, unavailable, etc.)
      const isRetryable = 
        error.code === 14 || // UNAVAILABLE
        error.code === 2 ||  // UNKNOWN (often connection issues)
        error.message?.includes('ECONNREFUSED') ||
        error.message?.includes('UNAVAILABLE');
      
      if (!isRetryable || attempt === maxAttempts) {
        throw error;
      }
      
      // Exponential backoff with jitter
      const delayMs = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt - 1) + Math.random() * 100;
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  
  throw lastError!;
}

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryWithBackoff<ListProductsResponse>((attempt) => {
      return new Promise<ListProductsResponse>((resolve, reject) => {
        const deadline = new Date();
        deadline.setMilliseconds(deadline.getMilliseconds() + CALL_TIMEOUT_MS);
        
        const options: CallOptions = {
          deadline,
        };
        
        client.listProducts({}, options, (error, response) => {
          if (error) {
            // Enrich error with retry context
            const enrichedError = new Error(
              `Failed to list products (attempt ${attempt}/${MAX_RETRY_ATTEMPTS}): ${error.message}`
            );
            (enrichedError as any).code = error.code;
            (enrichedError as any).originalError = error;
            reject(enrichedError);
          } else {
            resolve(response);
          }
        });
      });
    });
  },
  
  getProduct(id: string) {
    return retryWithBackoff<Product>((attempt) => {
      return new Promise<Product>((resolve, reject) => {
        const deadline = new Date();
        deadline.setMilliseconds(deadline.getMilliseconds() + CALL_TIMEOUT_MS);
        
        const options: CallOptions = {
          deadline,
        };
        
        client.getProduct({ id }, options, (error, response) => {
          if (error) {
            // Enrich error with retry context
            const enrichedError = new Error(
              `Failed to get product ${id} (attempt ${attempt}/${MAX_RETRY_ATTEMPTS}): ${error.message}`
            );
            (enrichedError as any).code = error.code;
            (enrichedError as any).originalError = error;
            reject(enrichedError);
          } else {
            resolve(response);
          }
        });
      });
    });
  },
});

export default ProductCatalogGateway();
