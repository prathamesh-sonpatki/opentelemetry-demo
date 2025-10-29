// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Create client with insecure credentials
const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

/**
 * Retry logic for gRPC calls to handle transient failures
 * @param fn Function to retry
 * @param maxRetries Maximum number of retry attempts
 * @param delayMs Delay between retries in milliseconds
 */
async function retryGrpcCall<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  delayMs: number = 1000
): Promise<T> {
  let lastError: Error | undefined;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      const grpcError = error as ServiceError;
      
      // Only retry on specific error codes that indicate transient failures
      const retryableErrorCodes = [
        14, // UNAVAILABLE - Service is temporarily unavailable
        4,  // DEADLINE_EXCEEDED - Request timeout
        8,  // RESOURCE_EXHAUSTED - Temporary resource exhaustion
      ];
      
      if (attempt < maxRetries && retryableErrorCodes.includes(grpcError.code || 0)) {
        console.warn(
          `gRPC call failed with code ${grpcError.code}: ${grpcError.message}. ` +
          `Retrying (${attempt + 1}/${maxRetries}) after ${delayMs}ms...`
        );
        // Exponential backoff: wait delayMs * 2^attempt
        await new Promise(resolve => setTimeout(resolve, delayMs * Math.pow(2, attempt)));
        continue;
      }
      
      // Don't retry on non-retryable errors or after max retries
      throw error;
    }
  }
  
  throw lastError;
}

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryGrpcCall(
      () => new Promise<ListProductsResponse>((resolve, reject) =>
        client.listProducts({}, (error, response) => {
          if (error) {
            console.error('ProductCatalog.listProducts error:', error.message);
            reject(error);
          } else {
            resolve(response);
          }
        })
      )
    );
  },
  getProduct(id: string) {
    return retryGrpcCall(
      () => new Promise<Product>((resolve, reject) =>
        client.getProduct({ id }, (error, response) => {
          if (error) {
            console.error(`ProductCatalog.getProduct error for id ${id}:`, error.message);
            reject(error);
          } else {
            resolve(response);
          }
        })
      )
    );
  },
});

export default ProductCatalogGateway();
