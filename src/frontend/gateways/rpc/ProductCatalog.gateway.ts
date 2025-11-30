// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

const ProductCatalogGateway = () => ({
  listProducts() {
    return new Promise<ListProductsResponse>((resolve, reject) =>
      client.listProducts({}, (error, response) => {
        if (error) {
          // Log the error for debugging
          console.error('[ProductCatalog] listProducts gRPC error:', {
            code: error.code,
            message: error.message,
            details: error.details,
            metadata: error.metadata,
          });
          
          // Instead of rejecting immediately, return empty list for graceful degradation
          // This prevents 500 errors and allows the app to continue functioning
          if (error.code === 14 || error.code === 'UNAVAILABLE') {
            console.warn('[ProductCatalog] Service unavailable, returning empty product list');
            resolve({ products: [] });
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
          // Log the error for debugging
          console.error('[ProductCatalog] getProduct gRPC error:', {
            productId: id,
            code: error.code,
            message: error.message,
            details: error.details,
            metadata: error.metadata,
          });
          
          // For unavailable service, return a placeholder product instead of failing
          if (error.code === 14 || error.code === 'UNAVAILABLE') {
            console.warn(`[ProductCatalog] Service unavailable for product ${id}, returning placeholder`);
            // Return a minimal product object to prevent UI breakage
            resolve({
              id,
              name: 'Product Unavailable',
              description: 'This product is temporarily unavailable',
              picture: '',
              priceUsd: { currencyCode: 'USD', units: 0, nanos: 0 },
              categories: [],
            } as Product);
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
