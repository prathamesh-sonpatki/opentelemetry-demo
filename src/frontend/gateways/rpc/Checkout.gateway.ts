// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ClientOptions, ServiceError, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

if (!CHECKOUT_ADDR) {
  throw new Error('CHECKOUT_ADDR environment variable must be set');
}

const RETRY_DELAYS = [100, 200, 500]; // Retry delays in ms

// gRPC client options with retry configuration
const clientOptions: ClientOptions = {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.enable_retries': 1,
  'grpc.service_config': JSON.stringify({
    methodConfig: [{
      name: [{ service: 'hipstershop.CheckoutService' }],
      retryPolicy: {
        maxAttempts: 3,
        initialBackoff: '0.1s',
        maxBackoff: '1s',
        backoffMultiplier: 2,
        retryableStatusCodes: ['UNAVAILABLE', 'DEADLINE_EXCEEDED']
      }
    }]
  })
};

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  clientOptions
);

async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  retryCount = 0
): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    const grpcError = error as ServiceError;
    
    // Only retry on specific error codes
    if (
      grpcError.code === status.UNAVAILABLE ||
      grpcError.code === status.DEADLINE_EXCEEDED
    ) {
      if (retryCount < RETRY_DELAYS.length) {
        const delay = RETRY_DELAYS[retryCount];
        await new Promise(resolve => setTimeout(resolve, delay));
        return retryWithBackoff(operation, retryCount + 1);
      }
    }
    
    throw error;
  }
}

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    return retryWithBackoff(() => {
      return new Promise<PlaceOrderResponse>((resolve, reject) => {
        client.placeOrder(
          order,
          {
            deadline: Date.now() + 5000 // 5 second timeout
          },
          (error, response) => {
            if (error) {
              reject(error);
            } else {
              resolve(response);
            }
          }
        );
      });
    });
  },
});

export default CheckoutGateway();
