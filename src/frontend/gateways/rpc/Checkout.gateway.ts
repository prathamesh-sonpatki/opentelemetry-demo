// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '', MAX_RETRIES = '3', RETRY_DELAY_MS = '1000' } = process.env;

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  {
    'grpc.keepalive_timeout_ms': 5000,
    'grpc.keepalive_permit_without_calls': 1
  }
);

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    const maxRetries = parseInt(MAX_RETRIES, 10);
    const retryDelay = parseInt(RETRY_DELAY_MS, 10);
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) =>
          client.placeOrder(order, (error, response) => (error ? reject(error) : resolve(response)))
        );
      } catch (error) {
        lastError = error as Error;
        
        // Don't retry if it's not a network-related error
        if (!error.message.includes('name resolver error') && 
            !error.message.includes('UNAVAILABLE')) {
          throw error;
        }
        
        if (attempt === maxRetries) {
          break;
        }
        
        // Wait before retrying
        await delay(retryDelay * Math.pow(2, attempt)); // Exponential backoff
      }
    }

    throw new Error(`Failed to place order after ${maxRetries} retries: ${lastError?.message}`);
  },
});

export default CheckoutGateway();
