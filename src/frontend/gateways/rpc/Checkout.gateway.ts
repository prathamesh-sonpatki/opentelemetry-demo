// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = 'checkout-service:8080' } = process.env;

// Channel options for better connection handling
const channelOptions: ChannelOptions = {
  'grpc.enable_retries': 1,
  'grpc.initial_reconnect_backoff_ms': 100,
  'grpc.max_reconnect_backoff_ms': 3000,
  'grpc.min_reconnect_backoff_ms': 100,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_time_ms': 10000
};

// Create client with retry options
const createClient = () => {
  if (!CHECKOUT_ADDR) {
    throw new Error('CHECKOUT_ADDR environment variable is not set');
  }
  
  return new CheckoutServiceClient(
    CHECKOUT_ADDR,
    ChannelCredentials.createInsecure(),
    channelOptions
  );
};

const client = createClient();

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    try {
      return await new Promise<PlaceOrderResponse>((resolve, reject) =>
        client.placeOrder(order, (error, response) => {
          if (error) {
            // Check if it's a connection issue
            if (error.code === 14 || error.code === 13) { // UNAVAILABLE or INTERNAL
              console.error('Checkout service connection error:', error);
              // Could implement circuit breaker pattern here
            }
            reject(error);
            return;
          }
          resolve(response);
        })
      );
    } catch (error) {
      console.error('Error placing order:', error);
      throw error;
    }
  },
});

export default CheckoutGateway();