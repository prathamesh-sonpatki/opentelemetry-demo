// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money } from '../protos/demo';
import { ServiceError } from '@grpc/grpc-js';

const defaultCurrencyCode = 'USD';
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

class ProductNotFoundError extends Error {
  constructor(productId: string) {
    super(`Product not found: ${productId}`);
    this.name = 'ProductNotFoundError';
  }
}

class FeatureFlagError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FeatureFlagError';
  }
}

const ProductCatalogService = () => ({
  async getProductPrice(price: Money, currencyCode: string) {
    return !!currencyCode && currencyCode !== defaultCurrencyCode
      ? await CurrencyGateway.convert(price, currencyCode)
      : price;
  },

  async listProducts(currencyCode = 'USD') {
    try {
      const { products: productList } = await ProductCatalogGateway.listProducts();

      return Promise.all(
        productList.map(async product => {
          const priceUsd = await this.getProductPrice(product.priceUsd!, currencyCode);

          return {
            ...product,
            priceUsd,
          };
        })
      );
    } catch (error) {
      if (error instanceof ServiceError && error.message.includes('Feature Flag Enabled')) {
        throw new FeatureFlagError('Product catalog service is temporarily disabled');
      }
      throw error;
    }
  },

  async getProduct(id: string, currencyCode = 'USD') {
    let lastError: Error | null = null;
    
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const product = await ProductCatalogGateway.getProduct(id);
        return {
          ...product,
          priceUsd: await this.getProductPrice(product.priceUsd!, currencyCode),
        };
      } catch (error) {
        lastError = error;
        
        if (error instanceof ServiceError) {
          if (error.message.includes('Feature Flag Enabled')) {
            // Don't retry if it's a feature flag issue
            throw new FeatureFlagError('Product catalog service is temporarily disabled');
          }
          if (error.message.includes('not found')) {
            throw new ProductNotFoundError(id);
          }
          if (error.code === 14) { // UNAVAILABLE
            // Only retry on connection issues
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * (attempt + 1)));
            continue;
          }
        }
        throw error;
      }
    }
    
    throw lastError || new Error(`Failed to get product ${id} after ${MAX_RETRIES} attempts`);
  },
});

export default ProductCatalogService();
