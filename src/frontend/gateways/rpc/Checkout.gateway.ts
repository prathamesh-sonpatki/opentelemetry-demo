// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ClientOptions, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 100;

// Configure client options with timeouts and keepalive
const clientOptions: ClientOptions = {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.initial_reconnect_backoff_ms': 100,
  'grpc.max_reconnect_backoff_ms': 5000
};

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  clientOptions
);

// Exponential backoff retry helper
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) =>
          client.placeOrder(order, (error, response) => {
            if (error) {
              // Only retry on specific error codes that are retryable
              if (error.code === status.UNAVAILABLE || 
                  error.code === status.DEADLINE_EXCEEDED ||
                  error.code === status.RESOURCE_EXHAUSTED) {
                reject(error);
              } else {
                // Don't retry on other errors
                reject(new Error(`Checkout failed: ${error.message}`));
              }
            } else {
              resolve(response);
            }
          })
        );
      } catch (error) {
        lastError = error as Error;
        
        if (attempt < MAX_RETRIES) {
          // Exponential backoff with jitter
          const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
          const jitter = Math.random() * 100;
          await wait(backoff + jitter);
          continue;
        }
      }
    }
    
    throw lastError || new Error('Checkout failed after retries');
  },
});

export default CheckoutGateway();
