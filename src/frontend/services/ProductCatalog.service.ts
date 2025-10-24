// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money, Product } from '../protos/demo';

const defaultCurrencyCode = 'USD';

interface ProductCatalogError extends Error {
  code?: number;
  details?: string;
}

const isFeatureFlagError = (error: ProductCatalogError) => {
  return error.message?.includes('Product Catalog Fail Feature Flag Enabled');
};

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
      if (isFeatureFlagError(error as ProductCatalogError)) {
        // Return cached or fallback data when feature flag is enabled
        return [];
      }
      throw error;
    }
  },
  async getProduct(id: string, currencyCode = 'USD') {
    try {
      const product = await ProductCatalogGateway.getProduct(id);

      return {
        ...product,
        priceUsd: await this.getProductPrice(product.priceUsd!, currencyCode),
      };
    } catch (error) {
      if (isFeatureFlagError(error as ProductCatalogError)) {
        // Return appropriate error response when feature flag is enabled
        throw new Error(`Product temporarily unavailable. Please try again later.`);
      }
      throw error;
    }
  },
});

export default ProductCatalogService();
