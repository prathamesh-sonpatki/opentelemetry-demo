// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money } from '../protos/demo';

const defaultCurrencyCode = 'USD';

// Max retries for product catalog operations
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const ProductCatalogService = () => ({
  async getProductPrice(price: Money, currencyCode: string) {
    return !!currencyCode && currencyCode !== defaultCurrencyCode
      ? await CurrencyGateway.convert(price, currencyCode)
      : price;
  },

  async listProducts(currencyCode = 'USD') {
    let attempt = 0;
    let lastError;

    while (attempt < MAX_RETRIES) {
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
        lastError = error;
        // Only retry if it's a feature flag error
        if (error.message?.includes('Feature Flag Enabled')) {
          attempt++;
          if (attempt < MAX_RETRIES) {
            await sleep(RETRY_DELAY);
            continue;
          }
        }
        throw error;
      }
    }

    throw lastError;
  },

  async getProduct(id: string, currencyCode = 'USD') {
    let attempt = 0;
    let lastError;

    while (attempt < MAX_RETRIES) {
      try {
        const product = await ProductCatalogGateway.getProduct(id);
        return {
          ...product,
          priceUsd: await this.getProductPrice(product.priceUsd!, currencyCode),
        };
      } catch (error) {
        lastError = error;
        // Only retry if it's a feature flag error
        if (error.message?.includes('Feature Flag Enabled')) {
          attempt++;
          if (attempt < MAX_RETRIES) {
            await sleep(RETRY_DELAY);
            continue;
          }
        }
        throw error;
      }
    }

    throw lastError;
  },
});

export default ProductCatalogService();