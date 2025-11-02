// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, CallOptions } from '@grpc/grpc-js';
import { ListProductsResponse, Product, ProductCatalogServiceClient } from '../../protos/demo';
import { retryGrpcCall } from '../../utils/grpc/retry';

const { PRODUCT_CATALOG_ADDR = '' } = process.env;

// Configure gRPC client with connection options
const client = new ProductCatalogServiceClient(
  PRODUCT_CATALOG_ADDR,
  ChannelCredentials.createInsecure()
);

// Default call options with timeout
const DEFAULT_CALL_OPTIONS: CallOptions = {
  deadline: Date.now() + 10000, // 10 second timeout
};

const ProductCatalogGateway = () => ({
  listProducts() {
    return retryGrpcCall(
      () =>
        new Promise<ListProductsResponse>((resolve, reject) => {
          // Refresh deadline for each retry attempt
          const callOptions = { ...DEFAULT_CALL_OPTIONS, deadline: Date.now() + 10000 };
          client.listProducts({}, callOptions, (error, response) => {
            if (error) {
              console.error(`ProductCatalog.listProducts failed: ${error.message}`);
              reject(error);
            } else {
              resolve(response);
            }
          });
        }),
      {
        maxRetries: 3,
        initialDelayMs: 100,
        maxDelayMs: 2000,
      }
    );
  },
  getProduct(id: string) {
    if (!id) {
      return Promise.reject(new Error('Product ID is required'));
    }
    
    return retryGrpcCall(
      () =>
        new Promise<Product>((resolve, reject) => {
          // Refresh deadline for each retry attempt
          const callOptions = { ...DEFAULT_CALL_OPTIONS, deadline: Date.now() + 10000 };
          client.getProduct({ id }, callOptions, (error, response) => {
            if (error) {
              console.error(`ProductCatalog.getProduct failed for id ${id}: ${error.message}`);
              reject(error);
            } else {
              resolve(response);
            }
          });
        }),
      {
        maxRetries: 3,
        initialDelayMs: 100,
        maxDelayMs: 2000,
      }
    );
  },
});

export default ProductCatalogGateway();
