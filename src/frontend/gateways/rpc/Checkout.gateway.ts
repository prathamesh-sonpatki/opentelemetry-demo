// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 100;

const createClient = () => {
  const options = {
    'grpc.keepalive_time_ms': 5000,
    'grpc.keepalive_timeout_ms': 3000,
    'grpc.max_reconnect_backoff_ms': 5000,
    'grpc.min_reconnect_backoff_ms': 1000,
  };
  return new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure(), options);
};

let client = createClient();

const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  retries = MAX_RETRIES,
  backoff = INITIAL_BACKOFF_MS
): Promise<T> => {
  try {
    return await operation();
  } catch (error) {
    const isRetryable = (error as ServiceError).code === 14 || // UNAVAILABLE
                       (error as ServiceError).code === 4;  // DEADLINE_EXCEEDED

    if (retries > 0 && isRetryable) {
      await new Promise(resolve => setTimeout(resolve, backoff));
      client = createClient(); // Create new client on retry
      return retryWithBackoff(operation, retries - 1, backoff * 2);
    }
    throw error;
  }
};

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    return retryWithBackoff(() => 
      new Promise<PlaceOrderResponse>((resolve, reject) =>
        client.placeOrder(order, { deadline: Date.now() + 5000 }, (error, response) => 
          error ? reject(error) : resolve(response)
        )
      )
    );
  },
});

export default CheckoutGateway();