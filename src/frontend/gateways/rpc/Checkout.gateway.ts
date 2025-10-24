// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

const RETRY_CODES = [status.UNAVAILABLE, status.DEADLINE_EXCEEDED];
const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 100;

const channelOptions: ChannelOptions = {
  'grpc.enable_retries': 1,
  'grpc.service_config': JSON.stringify({
    'methodConfig': [{
      'name': [{ 'service': 'hipstershop.CheckoutService' }],
      'retryPolicy': {
        'maxAttempts': MAX_RETRIES,
        'initialBackoff': `${INITIAL_BACKOFF_MS}ms`,
        'maxBackoff': '1s',
        'backoffMultiplier': 2,
        'retryableStatusCodes': RETRY_CODES
      }
    }]
  })
};

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  channelOptions
);

interface CheckoutError extends Error {
  code?: number;
  details?: string;
}

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    try {
      return await new Promise<PlaceOrderResponse>((resolve, reject) => {
        client.placeOrder(order, {
          deadline: Date.now() + 5000 // 5 second timeout
        }, (error, response) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        });
      });
    } catch (error) {
      const grpcError = error as CheckoutError;
      if (grpcError.code === status.UNAVAILABLE) {
        throw new Error('Payment service temporarily unavailable. Please try again in a few moments.');
      }
      throw error;
    }
  },
});

export default CheckoutGateway();
