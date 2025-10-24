// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const DEFAULT_CHECKOUT_ADDR = 'checkout-service:7000';
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

interface RetryConfig {
  maxRetries: number;
  delayMs: number;
}

const { CHECKOUT_ADDR = DEFAULT_CHECKOUT_ADDR } = process.env;

if (!CHECKOUT_ADDR) {
  console.warn('CHECKOUT_ADDR not set, using default:', DEFAULT_CHECKOUT_ADDR);
}

const createClient = (addr: string) => {
  try {
    return new CheckoutServiceClient(addr, ChannelCredentials.createInsecure());
  } catch (error) {
    console.error('Failed to create checkout client:', error);
    throw error;
  }
};

const client = createClient(CHECKOUT_ADDR);

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const retryOperation = async <T>(
  operation: () => Promise<T>,
  config: RetryConfig = { maxRetries: MAX_RETRIES, delayMs: RETRY_DELAY_MS }
): Promise<T> => {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= config.maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;
      const isRetryable = error instanceof ServiceError && 
        (error.code === 'UNAVAILABLE' || error.code === 'DEADLINE_EXCEEDED');

      if (!isRetryable || attempt === config.maxRetries) {
        throw error;
      }

      console.warn(`Attempt ${attempt} failed, retrying in ${config.delayMs}ms:`, error.message);
      await delay(config.delayMs);
    }
  }

  throw lastError;
};

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    return retryOperation(() => {
      return new Promise<PlaceOrderResponse>((resolve, reject) =>
        client.placeOrder(order, (error, response) => {
          if (error) {
            console.error('Checkout service error:', error);
            reject(error);
            return;
          }
          resolve(response);
        })
      );
    });
  },
});

export default CheckoutGateway();