// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions, status } from '@grpc/grpc-js';
import { Cart, CartItem, CartServiceClient, Empty } from '../../protos/demo';
import { retryWithBackoff } from '../../utils/grpc/retryUtils';

const { CART_ADDR = '' } = process.env;

if (!CART_ADDR) {
  throw new Error('CART_ADDR environment variable must be set');
}

const channelOptions: ChannelOptions = {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.enable_retries': 1,
  'grpc.service_config': JSON.stringify({
    loadBalancingConfig: [{ round_robin: {} }],
    methodConfig: [{
      name: [{ service: 'CartService' }],
      retryPolicy: {
        maxAttempts: 3,
        initialBackoff: '0.1s',
        maxBackoff: '1s',
        backoffMultiplier: 1.5,
        retryableStatusCodes: [status.UNAVAILABLE]
      }
    }]
  })
};

const client = new CartServiceClient(
  CART_ADDR,
  ChannelCredentials.createInsecure(),
  channelOptions
);

const CartGateway = () => ({
  async getCart(userId: string): Promise<Cart> {
    return retryWithBackoff(() => 
      new Promise<Cart>((resolve, reject) =>
        client.getCart({ userId }, (error, response) => {
          if (error) {
            console.error(`Failed to get cart for user ${userId}:`, error);
            reject(error);
          } else {
            resolve(response);
          }
        })
      )
    );
  },

  async addItem(userId: string, item: CartItem): Promise<Empty> {
    return retryWithBackoff(() =>
      new Promise<Empty>((resolve, reject) =>
        client.addItem({ userId, item }, (error, response) => {
          if (error) {
            console.error(`Failed to add item to cart for user ${userId}:`, error);
            reject(error);
          } else {
            resolve(response);
          }
        })
      )
    );
  },

  async emptyCart(userId: string): Promise<Empty> {
    return retryWithBackoff(() =>
      new Promise<Empty>((resolve, reject) =>
        client.emptyCart({ userId }, (error, response) => {
          if (error) {
            console.error(`Failed to empty cart for user ${userId}:`, error);
            reject(error);
          } else {
            resolve(response);
          }
        })
      )
    );
  },
});

export default CartGateway();