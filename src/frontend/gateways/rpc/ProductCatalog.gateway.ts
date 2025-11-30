// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Metadata, ServiceError } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configuration for retry logic
const RETRY_CONFIG = {
  maxRetries: 3,
  initialDelayMs: 100,
  maxDelayMs: 2000,
  backoffMultiplier: 2,
  retryableStatusCodes: [14], // UNAVAILABLE
};

// Connection timeout configuration
const CALL_OPTIONS = {
  deadline: Date.now() + 5000, // 5 second timeout
};

let client: ProductCatalogServiceClient | null = null;

/**
 * Get or create the gRPC client with lazy initialization
 * This ensures connection is established when needed, not at module load time
 */
function getClient(): ProductCatalogServiceClient {
  if (!client) {
    client = new ProductCatalogServiceClient(
      PRODUCT_CATALOG_ADDR,
      ChannelCredentials.createInsecure()
    );
  }
  return client;
}

/**
 * Sleep helper for retry delays
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check if error is retryable based on status code
 */
function isRetryableError(error: ServiceError): boolean {
  if (!error.code) return false;
  return RETRY_CONFIG.retryableStatusCodes.includes(error.code);
}

/**
 * Execute a gRPC call with exponential backoff retry logic
 */
async function executeWithRetry<T>(
  operation: () => Promise<T>,
  operationName: string
): Promise<T> {
  let lastError: Error | null = null;
  let delay = RETRY_CONFIG.initialDelayMs;

  for (let attempt = 0; attempt <= RETRY_CONFIG.maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;
      const isServiceError = (error as ServiceError).code !== undefined;
      
      // Log the error for observability
      console.error(`[ProductCatalogGateway] ${operationName} attempt ${attempt + 1} failed:`, {
        message: (error as Error).message,
        code: isServiceError ? (error as ServiceError).code : undefined,
        details: isServiceError ? (error as ServiceError).details : undefined,
      });

      // Check if we should retry
      if (attempt < RETRY_CONFIG.maxRetries && isServiceError && isRetryableError(error as ServiceError)) {
        console.log(`[ProductCatalogGateway] Retrying ${operationName} in ${delay}ms...`);
        await sleep(delay);
        
        // Exponential backoff with max delay cap
        delay = Math.min(delay * RETRY_CONFIG.backoffMultiplier, RETRY_CONFIG.maxDelayMs);
        
        // Recreate client on connection errors to force reconnection
        client = null;
      } else {
        // Don't retry - either max retries reached or non-retryable error
        break;
      }
    }
  }

  // All retries exhausted, throw the last error
  throw lastError;
}

const ProductCatalogGateway = () => ({
  listProducts() {
    return executeWithRetry<ListProductsResponse>(
      () => {
        return new Promise<ListProductsResponse>((resolve, reject) => {
          const clientInstance = getClient();
          const metadata = new Metadata();
          
          clientInstance.listProducts(
            {},
            metadata,
            { deadline: Date.now() + 5000 }, // 5 second deadline per attempt
            (error, response) => {
              if (error) {
                reject(error);
              } else if (response) {
                resolve(response);
              } else {
                reject(new Error('No response received from listProducts'));
              }
            }
          );
        });
      },
      'listProducts'
    );
  },
  getProduct(id: string) {
    return executeWithRetry<Product>(
      () => {
        return new Promise<Product>((resolve, reject) => {
          const clientInstance = getClient();
          const metadata = new Metadata();
          
          clientInstance.getProduct(
            { id },
            metadata,
            { deadline: Date.now() + 5000 }, // 5 second deadline per attempt
            (error, response) => {
              if (error) {
                reject(error);
              } else if (response) {
                resolve(response);
              } else {
                reject(new Error(`No response received from getProduct for id: ${id}`));
              }
            }
          );
        });
      },
      `getProduct(${id})`
    );
  },
});

export default ProductCatalogGateway();
