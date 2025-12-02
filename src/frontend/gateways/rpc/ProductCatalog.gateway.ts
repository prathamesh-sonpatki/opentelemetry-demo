// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure gRPC channel options for better resilience
const channelOptions: ChannelOptions = {
  // Enable keepalive to detect broken connections
  'grpc.keepalive_time_ms': 30000, // Send keepalive pings every 30s
  'grpc.keepalive_timeout_ms': 10000, // Wait 10s for keepalive response
  'grpc.keepalive_permit_without_calls': 1, // Allow keepalive without active calls
  'grpc.http2.min_time_between_pings_ms': 10000, // Minimum time between pings
  'grpc.http2.max_pings_without_data': 0, // No limit on pings without data
  // Connection retry settings
  'grpc.initial_reconnect_backoff_ms': 1000, // Start with 1s backoff
  'grpc.max_reconnect_backoff_ms': 30000, // Max 30s backoff
  'grpc.enable_retries': 1, // Enable retries
};

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR,
  ChannelCredentials.createInsecure(),
  channelOptions
);

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 1000;
const GRPC_DEADLINE_MS = 5000; // 5 second deadline for gRPC calls

/**
 * Sleep utility for retry delays
 */
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Generic retry wrapper with exponential backoff
 */
async function withRetry<T>(
  operation: () => Promise<T>,
  operationName: string,
  retries = MAX_RETRIES
): Promise<T> {
  let lastError: Error | undefined;
  
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Log the error with context
      console.error(`[ProductCatalogGateway] ${operationName} failed (attempt ${attempt + 1}/${retries + 1}):`, {
        error: error.message,
        code: error.code,
        details: error.details,
        metadata: error.metadata?.getMap(),
      });

      // Don't retry on certain error codes (client errors)
      if (error.code === 3 || error.code === 5 || error.code === 7) { // INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED
        console.error(`[ProductCatalogGateway] Non-retryable error for ${operationName}:`, error.code);
        throw error;
      }

      // If we have more retries, wait before retrying
      if (attempt < retries) {
        const delayMs = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt);
        console.log(`[ProductCatalogGateway] Retrying ${operationName} in ${delayMs}ms...`);
        await sleep(delayMs);
      }
    }
  }

  // All retries exhausted
  console.error(`[ProductCatalogGateway] ${operationName} failed after ${retries + 1} attempts`);
  throw lastError;
}

const ProductCatalogGateway = () => ({
  /**
   * List all products from the catalog
   * Returns empty array on failure for graceful degradation
   */
  async listProducts(): Promise<ListProductsResponse> {
    try {
      return await withRetry(
        () =>
          new Promise<ListProductsResponse>((resolve, reject) => {
            const deadline = new Date(Date.now() + GRPC_DEADLINE_MS);
            client.listProducts(
              {},
              { deadline },
              (error, response) => (error ? reject(error) : resolve(response))
            );
          }),
        'listProducts'
      );
    } catch (error: any) {
      // Graceful degradation: return empty product list instead of crashing
      console.error('[ProductCatalogGateway] listProducts - returning empty list due to error:', error.message);
      return { products: [] };
    }
  },

  /**
   * Get a specific product by ID
   * Returns undefined on failure for graceful degradation
   */
  async getProduct(id: string): Promise<Product | undefined> {
    try {
      return await withRetry(
        () =>
          new Promise<Product>((resolve, reject) => {
            const deadline = new Date(Date.now() + GRPC_DEADLINE_MS);
            client.getProduct(
              { id },
              { deadline },
              (error, response) => (error ? reject(error) : resolve(response))
            );
          }),
        `getProduct(${id})`
      );
    } catch (error: any) {
      // Graceful degradation: return undefined instead of crashing
      console.error(`[ProductCatalogGateway] getProduct(${id}) - returning undefined due to error:`, error.message);
      return undefined;
    }
  },
});

export default ProductCatalogGateway();
