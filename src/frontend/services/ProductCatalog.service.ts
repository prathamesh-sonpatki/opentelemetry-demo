// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money, Product } from '../protos/demo';

const defaultCurrencyCode = 'USD';
const DEFAULT_PRODUCT: Product = {
  id: 'unavailable',
  name: 'Product Temporarily Unavailable',
  description: 'This product is currently unavailable. Please try again later.',
  picture: '/img/products/placeholder.jpg',
  categories: [],
  priceUsd: { currencyCode: 'USD', units: 0, nanos: 0 }
};

const ProductCatalogService = () => ({
  async getProductPrice(price: Money, currencyCode: string) {
    try {
      return !!currencyCode && currencyCode !== defaultCurrencyCode
        ? await CurrencyGateway.convert(price, currencyCode)
        : price;
    } catch (error) {
      console.error('Failed to convert currency:', error);
      return price; // Fallback to original price
    }
  },

  async listProducts(currencyCode = 'USD') {
    try {
      const { products: productList } = await ProductCatalogGateway.listProducts();

      return Promise.all(
        productList.map(async product => {
          try {
            const priceUsd = await this.getProductPrice(product.priceUsd!, currencyCode);
            return { ...product, priceUsd };
          } catch (error) {
            console.error(`Failed to process product ${product.id}:`, error);
            return { ...product }; // Return product without price conversion
          }
        })
      );
    } catch (error) {
      console.error('Failed to list products:', error);
      if (error.message?.includes('Product Catalog Fail Feature Flag Enabled')) {
        return [DEFAULT_PRODUCT]; // Return placeholder product
      }
      throw error; // Re-throw unexpected errors
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
      console.error(`Failed to get product ${id}:`, error);
      if (error.message?.includes('Product Catalog Fail Feature Flag Enabled')) {
        return DEFAULT_PRODUCT;
      }
      throw error;
    }
  },
});

export default ProductCatalogService();
