// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';
import grpcRetry from '../../utils/grpc/GrpcRetry';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

const client = new ProductCatalogServiceClient(PRODUCT_CATALOG_ADDR, ChannelCredentials.createInsecure());

const ProductCatalogGateway = () => ({
  listProducts() {
    const operation = () =>
      new Promise<ListProductsResponse>((resolve, reject) =>
        client.listProducts({}, (error, response) => (error ? reject(error) : resolve(response)))
      );

    // Wrap with retry logic for resilience against transient connection failures
    return grpcRetry.execute(operation, 'ProductCatalog.listProducts');
  },
  getProduct(id: string) {
    const operation = () =>
      new Promise<Product>((resolve, reject) =>
        client.getProduct({ id }, (error, response) => (error ? reject(error) : resolve(response)))
      );

    // Wrap with retry logic for resilience against transient connection failures
    return grpcRetry.execute(operation, `ProductCatalog.getProduct(${id})`);
  },
});

export default ProductCatalogGateway();
