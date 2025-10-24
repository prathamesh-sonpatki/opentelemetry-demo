// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const createClient = () => new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

class ProductNotFoundError extends Error {
  constructor(productId: string) {
    super(`Product not found: ${productId}`);
    this.name = 'ProductNotFoundError';
  }
}

const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  retries: number = MAX_RETRIES,
  delay: number = RETRY_DELAY_MS
): Promise<T> => {
  try {
    return await operation();
  } catch (error) {
    if (retries === 0 || !(error instanceof ServiceError)) {
      throw error;
    }

    if (error.code === status.NOT_FOUND) {
      throw error; // Don't retry if product doesn't exist
    }

    // Retry on connection issues
    if (error.code === status.UNAVAILABLE || error.code === status.INTERNAL) {
      await sleep(delay);
      return retryWithBackoff(operation, retries - 1, delay * 2);
    }

    throw error;
  }
};

const ProductCatalogGateway = () => ({
  async listProducts() {
    const client = createClient();
    try {
      return await retryWithBackoff(() =>
        new Promise<ListProductsResponse>((resolve, reject) => {
          client.listProducts({}, (error, response) => {
            if (error) {
              reject(error);
            } else {
              resolve(response);
            }
          });
        })
      );
    } finally {
      client.close();
    }
  },

  async getProduct(id: string) {
    const client = createClient();
    try {
      return await retryWithBackoff(() =>
        new Promise<Product>((resolve, reject) => {
          client.getProduct({ id }, (error, response) => {
            if (error) {
              if (error.code === status.NOT_FOUND) {
                reject(new ProductNotFoundError(id));
              } else {
                reject(error);
              }
            } else {
              resolve(response);
            }
          });
        })
      );
    } finally {
      client.close();
    }
  },
});

export default ProductCatalogGateway();