// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

const ProductCatalogGateway = () => ({
  listProducts() {
    return new Promise<ListProductsResponse>((resolve, reject) =>
      client.listProducts({}, (error, response) => (error ? reject(error) : resolve(response)))
    );
  },
  getProduct(id: string) {
    return new Promise<Product>((resolve, reject) =>
      client.getProduct({ id }, (error, response) => {
        if (error) {
          // Add error handling for feature flag errors
          if (error.message.includes('Feature Flag Enabled')) {
            console.warn(`Feature flag error for product ${id}: ${error.message}`);
            // Return a fallback response
            return resolve({
              id,
              name: 'Product Temporarily Unavailable',
              description: 'This product is currently unavailable due to maintenance',
              priceUsd: { currencyCode: 'USD', units: 0, nanos: 0 },
              // Add other required fields with fallback values
              picture: '/img/products/placeholder.jpg'
            });
          }
          reject(error);
        } else {
          resolve(response);
        }
      })
    );
  },
});

export default ProductCatalogGateway();