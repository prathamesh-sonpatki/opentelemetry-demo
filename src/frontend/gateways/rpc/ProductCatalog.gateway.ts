// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Metadata, CallOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 1000;
const TIMEOUT_MS = 10000;

/**
 * Sleep utility for retry delays
 */
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Retry wrapper with exponential backoff for gRPC calls
 */
async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  operationName: string,
  retries = MAX_RETRIES
): Promise<T> {
  let lastError: Error | undefined;
  
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Don't retry on non-retryable errors (e.g., NOT_FOUND, INVALID_ARGUMENT)
      const isRetryable = error.code === 14 || error.code === 4 || error.code === 8; // UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED
      
      if (!isRetryable || attempt === retries - 1) {
        console.error(`${operationName} failed after ${attempt + 1} attempts:`, {
          code: error.code,
          message: error.message,
          details: error.details,
        });
        throw error;
      }
      
      const delayMs = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt);
      console.warn(`${operationName} attempt ${attempt + 1} failed, retrying in ${delayMs}ms...`, {
        code: error.code,
        message: error.message,
      });
      
      await sleep(delayMs);
    }
  }
  
  throw lastError || new Error(`${operationName} failed after ${retries} retries`);
}

/**
 * Create call options with deadline
 */
function createCallOptions(): CallOptions {
  return {
    deadline: Date.now() + TIMEOUT_MS,
  };
}

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryWithBackoff(
      () =>
        new Promise<ListProductsResponse>((resolve, reject) => {
          const metadata = new Metadata();
          client.listProducts({}, metadata, createCallOptions(), (error, response) =>
            error ? reject(error) : resolve(response)
          );
        }),
      'ProductCatalog.listProducts'
    );
  },
  getProduct(id: string) {
    return retryWithBackoff(
      () =>
        new Promise<Product>((resolve, reject) => {
          const metadata = new Metadata();
          client.getProduct({ id }, metadata, createCallOptions(), (error, response) =>
            error ? reject(error) : resolve(response)
          );
        }),
      `ProductCatalog.getProduct(${id})`
    );
  },
});

export default ProductCatalogGateway();
