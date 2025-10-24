// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import ProductCatalogGateway from '../gateways/rpc/ProductCatalog.gateway';
import CurrencyGateway from '../gateways/rpc/Currency.gateway';
import { Money, Product } from '../protos/demo';

const defaultCurrencyCode = 'USD';

const ProductCatalogService = () => ({
  async getProductPrice(price: Money, currencyCode: string) {
    try {
      return !!currencyCode && currencyCode !== defaultCurrencyCode
        ? await CurrencyGateway.convert(price, currencyCode)
        : price;
    } catch (error) {
      console.error('Error converting price:', error);
      return price; // Fallback to original price on currency conversion error
    }
  },

  async listProducts(currencyCode = 'USD') {
    try {
      const { products: productList = [] } = await ProductCatalogGateway.listProducts();

      return Promise.all(
        productList.map(async product => {
          try {
            const priceUsd = await this.getProductPrice(product.priceUsd!, currencyCode);
            return {
              ...product,
              priceUsd,
            };
          } catch (error) {
            console.error(`Error processing product ${product.id}:`, error);
            return product; // Return product with original price on error
          }
        })
      );
    } catch (error) {
      console.error('Error listing products:', error);
      throw error;
    }
  },

  async getProduct(id: string, currencyCode = 'USD') {
    try {
      if (!id) {
        throw new Error('Product ID is required');
      }

      const product = await ProductCatalogGateway.getProduct(id);
      
      if (!product) {
        throw new Error(`Product not found: ${id}`);
      }

      return {
        ...product,
        priceUsd: await this.getProductPrice(product.priceUsd!, currencyCode),
      };
    } catch (error) {
      console.error(`Error getting product ${id}:`, error);
      throw error;
    }
  },
});

export default ProductCatalogService();
