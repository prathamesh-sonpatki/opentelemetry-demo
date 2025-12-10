// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

const client = new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure());

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 100;

// Helper function to implement exponential backoff retry logic
const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  retries: number = MAX_RETRIES,
  delay: number = INITIAL_RETRY_DELAY_MS
): Promise<T> => {
  try {
    return await operation();
  } catch (error: any) {
    if (retries === 0) {
      throw error;
    }
    
    // Only retry on connection errors (UNAVAILABLE status)
    const shouldRetry = error.code === 14; // gRPC UNAVAILABLE status code
    if (!shouldRetry) {
      throw error;
    }

    console.warn(`Checkout service call failed, retrying in ${delay}ms. Retries left: ${retries}`);
    await new Promise(resolve => setTimeout(resolve, delay));
    return retryWithBackoff(operation, retries - 1, delay * 2);
  }
};

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    return await retryWithBackoff(
      () =>
        new Promise<PlaceOrderResponse>((resolve, reject) =>
          client.placeOrder(order, (error, response) => (error ? reject(error) : resolve(response)))
        )
    );
  },
});

export default CheckoutGateway();
