// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ClientOptions, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// Configuration
const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 100;
const MAX_BACKOFF_MS = 1000;
const TIMEOUT_MS = 5000;

const clientOptions: ClientOptions = {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.min_reconnect_backoff_ms': 100,
  'grpc.max_reconnect_backoff_ms': 1000,
  'grpc.service_config': JSON.stringify({
    loadBalancingConfig: [{ round_robin: {} }],
    methodConfig: [{
      name: [{ service: 'hipstershop.CheckoutService' }],
      retryPolicy: {
        maxAttempts: 5,
        initialBackoff: '0.1s',
        maxBackoff: '1s',
        backoffMultiplier: 1.5,
        retryableStatusCodes: [status.UNAVAILABLE]
      }
    }]
  })
};

// Validate checkout address
if (!CHECKOUT_ADDR) {
  console.error('CHECKOUT_ADDR environment variable is not set');
  throw new Error('Checkout service address not configured');
}

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  clientOptions
);

async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  retries = MAX_RETRIES,
  backoff = INITIAL_BACKOFF_MS
): Promise<T> {
  try {
    return await operation();
  } catch (error: any) {
    if (retries === 0 || !isRetryableError(error)) {
      throw error;
    }

    await new Promise(resolve => setTimeout(resolve, backoff));
    
    return retryWithBackoff(
      operation,
      retries - 1,
      Math.min(backoff * 2, MAX_BACKOFF_MS)
    );
  }
}

function isRetryableError(error: any): boolean {
  return error.code === status.UNAVAILABLE || 
         error.code === status.DEADLINE_EXCEEDED ||
         error.message?.includes('name resolver error');
}

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    return retryWithBackoff(() => {
      return new Promise<PlaceOrderResponse>((resolve, reject) => {
        const deadline = new Date();
        deadline.setMilliseconds(deadline.getMilliseconds() + TIMEOUT_MS);

        client.placeOrder(
          order,
          { deadline },
          (error, response) => {
            if (error) {
              console.error('Checkout service error:', {
                code: error.code,
                details: error.details,
                message: error.message
              });
              reject(error);
              return;
            }
            resolve(response);
          }
        );
      });
    });
  },
});

export default CheckoutGateway();