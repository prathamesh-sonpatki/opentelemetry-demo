// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, status } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Custom error class for product not found
export class ProductNotFoundError extends Error {
  constructor(productId: string) {
    super(`Product not found: ${productId}`);
    this.name = 'ProductNotFoundError';
  }
}

const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR,
  ChannelCredentials.createInsecure(),
  {
    'grpc.enable_retries': 1,
    'grpc.service_config': JSON.stringify({
      methodConfig: [{
        name: [{ service: 'hipstershop.ProductCatalogService' }],
        retryPolicy: {
          maxAttempts: 3,
          initialBackoff: '0.1s',
          maxBackoff: '1s',
          backoffMultiplier: 1.5,
          retryableStatusCodes: ['UNAVAILABLE']
        }
      }]
    })
  }
);

const ProductCatalogGateway = () => ({
  listProducts() {
    return new Promise<ListProductsResponse>((resolve, reject) =>
      client.listProducts({}, (error, response) => {
        if (error) {
          if (error.code === status.NOT_FOUND) {
            resolve({ products: [] }); // Return empty list instead of error
          } else {
            reject(error);
          }
        } else {
          resolve(response);
        }
      })
    );
  },
  getProduct(id: string) {
    return new Promise<Product>((resolve, reject) =>
      client.getProduct({ id }, (error, response) => {
        if (error) {
          if (error.code === status.NOT_FOUND) {
            reject(new ProductNotFoundError(id));
          } else {
            reject(error);
          }
        } else {
          resolve(response);
        }
      })
    );
  },
});

export default ProductCatalogGateway();