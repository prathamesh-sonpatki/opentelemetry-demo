// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money } from '../protos/demo';

const defaultCurrencyCode = 'USD';

const ProductCatalogService = () => ({
  async getProductPrice(price: Money, currencyCode: string) {
    try {
      if (!price) {
        throw new Error('Invalid price object');
      }

      if (!currencyCode || currencyCode === defaultCurrencyCode) {
        return price;
      }

      return await CurrencyGateway.convert(price, currencyCode);
    } catch (err) {
      console.error('Error converting currency:', err);
      // Fall back to original price if currency conversion fails
      return price;
    }
  },

  async listProducts(currencyCode = 'USD') {
    try {
      const { products: productList } = await ProductCatalogGateway.listProducts();

      if (!Array.isArray(productList)) {
        throw new Error('Invalid product list response');
      }

      return Promise.all(
        productList.map(async product => {
          try {
            if (!product?.priceUsd) {
              throw new Error(`Invalid price for product ${product?.id}`);
            }

            const priceUsd = await this.getProductPrice(product.priceUsd, currencyCode);

            return {
              ...product,
              priceUsd,
            };
          } catch (err) {
            console.error(`Error processing product ${product?.id}:`, err);
            return null;
          }
        })
      ).then(products => products.filter(Boolean)); // Remove failed products
    } catch (err) {
      console.error('Error fetching product list:', err);
      throw new Error('Failed to fetch product list');
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

      if (!product.priceUsd) {
        throw new Error(`Invalid price for product ${id}`);
      }

      return {
        ...product,
        priceUsd: await this.getProductPrice(product.priceUsd, currencyCode),
      };
    } catch (err) {
      console.error(`Error fetching product ${id}:`, err);
      throw new Error(`Failed to get product #${id}`);
    }
  },
});

export default ProductCatalogService();