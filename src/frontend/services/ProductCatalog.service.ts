// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money } from '../protos/demo';

const defaultCurrencyCode = 'USD';
const FEATURE_FLAG_ERROR = 'Product Catalog Fail Feature Flag Enabled';

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
    } catch (error: any) {
      // If the error is due to the feature flag, return an empty list
      if (error.message?.includes(FEATURE_FLAG_ERROR)) {
        console.warn('Product catalog feature flag is enabled, returning empty list');
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
    } catch (error: any) {
      // If the error is due to the feature flag, return a fallback product
      if (error.message?.includes(FEATURE_FLAG_ERROR)) {
        console.warn(`Product catalog feature flag is enabled, product ${id} not available`);
        return null;
      }
      throw error;
    }
  },
});

export default ProductCatalogService();