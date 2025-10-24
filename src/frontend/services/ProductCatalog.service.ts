// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money } from '../protos/demo';

const defaultCurrencyCode = 'USD';

const ProductCatalogService = () => ({
  async getProductPrice(price: Money, currencyCode: string) {
    if (!price) {
      throw new Error('Invalid price data');
    }

    try {
      return !!currencyCode && currencyCode !== defaultCurrencyCode
        ? await CurrencyGateway.convert(price, currencyCode)
        : price;
    } catch (error) {
      console.error('Currency conversion error:', error);
      // Fall back to original price if conversion fails
      return price;
    }
  },

  async listProducts(currencyCode = 'USD') {
    try {
      const { products: productList } = await ProductCatalogGateway.listProducts();

      if (!productList?.length) {
        throw new Error('No products available');
      }

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
      console.error('List products error:', error);
      throw new Error(`Failed to fetch product list: ${error.message}`);
    }
  },

  async getProduct(id: string, currencyCode = 'USD') {
    if (!id) {
      throw new Error('Product ID is required');
    }

    try {
      const product = await ProductCatalogGateway.getProduct(id);
      
      if (!product) {
        throw new Error(`Product not found: ${id}`);
      }

      return {
        ...product,
        priceUsd: await this.getProductPrice(product.priceUsd!, currencyCode),
      };
    } catch (error) {
      console.error(`Get product error for ID ${id}:`, error);
      throw new Error(`Failed to fetch product ${id}: ${error.message}`);
    }
  },
});

export default ProductCatalogService();