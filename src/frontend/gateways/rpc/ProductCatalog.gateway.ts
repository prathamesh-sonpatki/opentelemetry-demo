// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, status as GrpcStatus } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';
import GrpcResilienceHelper from '../../utils/grpc/GrpcResilienceHelper';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

if (!PRODUCT_CATALOG_ADDR) {
  console.warn('[ProductCatalogGateway] PRODUCT_CATALOG_ADDR is not set. Product catalog requests will fail.');
}

const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

const ProductCatalogGateway = () => ({
  listProducts() {
    return GrpcResilienceHelper.executeWithRetry(
      () =>
        new Promise<ListProductsResponse>((resolve, reject) =>
          client.listProducts({}, (error, response) => (error ? reject(error) : resolve(response)))
        ),
      'ProductCatalog.listProducts',
      {
        maxRetries: 3,
        initialDelayMs: 100,
        maxDelayMs: 2000,
        retryableStatuses: [
          GrpcStatus.UNAVAILABLE,
          GrpcStatus.DEADLINE_EXCEEDED,
          GrpcStatus.RESOURCE_EXHAUSTED,
        ],
      }
    );
  },
  getProduct(id: string) {
    if (!id) {
      return Promise.reject(new Error('Product ID is required'));
    }

    return GrpcResilienceHelper.executeWithRetry(
      () =>
        new Promise<Product>((resolve, reject) =>
          client.getProduct({ id }, (error, response) => (error ? reject(error) : resolve(response)))
        ),
      'ProductCatalog.getProduct',
      {
        maxRetries: 3,
        initialDelayMs: 100,
        maxDelayMs: 2000,
        retryableStatuses: [
          GrpcStatus.UNAVAILABLE,
          GrpcStatus.DEADLINE_EXCEEDED,
          GrpcStatus.RESOURCE_EXHAUSTED,
        ],
      }
    );
  },
});

export default ProductCatalogGateway();
