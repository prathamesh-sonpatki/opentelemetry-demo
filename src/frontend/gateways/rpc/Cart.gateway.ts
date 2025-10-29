// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { Cart, CartItem, CartServiceClient, Empty } from '../../protos/demo';
import { createGrpcClient, callWithRetry } from '../../utils/grpc-client-factory';

const { CART_ADDR = '' } = process.env;

const client = createGrpcClient(CartServiceClient, {
  address: CART_ADDR,
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 10000,
});

const CartGateway = () => ({
  getCart(userId: string) {
    return callWithRetry<{ userId: string }, Cart>(
      client,
      'getCart',
      { userId },
      { maxRetries: 3, retryDelay: 1000 }
    );
  },
  addItem(userId: string, item: CartItem) {
    return callWithRetry<{ userId: string; item: CartItem }, Empty>(
      client,
      'addItem',
      { userId, item },
      { maxRetries: 3, retryDelay: 1000 }
    );
  },
  emptyCart(userId: string) {
    return callWithRetry<{ userId: string }, Empty>(
      client,
      'emptyCart',
      { userId },
      { maxRetries: 3, retryDelay: 1000 }
    );
  },
});

export default CartGateway();
