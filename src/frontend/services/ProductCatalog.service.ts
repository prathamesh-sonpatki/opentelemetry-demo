// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money } from '../protos/demo';

const defaultCurrencyCode = 'USD';

const ProductCatalogService = () => ({
  async getProductPrice(price: Money, currencyCode: string) {
    try {
      return !!currencyCode && currencyCode !== defaultCurrencyCode
        ? await CurrencyGateway.convert(price, currencyCode)
        : price;
    } catch (error) {
      console.error('Error converting currency:', error);
      return price; // Fallback to original price
    }
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
      console.error('Error listing products:', error);
      return [];
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
      if (error.message?.includes('Product Catalog Fail Feature Flag Enabled')) {
        console.warn('Product catalog feature flag is enabled, returning fallback data');
        return {
          id,
          name: 'Temporarily Unavailable',
          description: 'This product is currently unavailable',
          picture: '/img/products/placeholder.jpg',
          priceUsd: { units: 0, nanos: 0, currencyCode: defaultCurrencyCode },
          categories: []
        };
      }
      throw error;
    }
  },
});

export default ProductCatalogService();
