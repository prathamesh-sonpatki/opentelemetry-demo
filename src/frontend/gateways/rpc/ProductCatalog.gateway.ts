// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';
import { createGrpcClient, callWithRetry } from '../../utils/grpc-client-factory';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

const client = createGrpcClient(ProductCatalogServiceClient, {
  address: PRODUCT_CATALOG_ADDR,
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 10000,
});

const ProductCatalogGateway = () => ({
  listProducts() {
    return callWithRetry<{}, ListProductsResponse>(
      client,
      'listProducts',
      {},
      { maxRetries: 3, retryDelay: 1000 }
    );
  },
  getProduct(id: string) {
    return callWithRetry<{ id: string }, Product>(
      client,
      'getProduct',
      { id },
      { maxRetries: 3, retryDelay: 1000 }
    );
  },
});

export default ProductCatalogGateway();
