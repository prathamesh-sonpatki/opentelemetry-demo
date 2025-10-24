// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, credentials, Metadata } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = 'checkout-service:8080' } = process.env;

// Validate checkout address
if (!CHECKOUT_ADDR) {
  throw new Error('CHECKOUT_ADDR environment variable is required');
}

// Configure client with retry settings
const clientOptions = {
  'grpc.enable_retries': 1,
  'grpc.service_config': JSON.stringify({
    methodConfig: [{
      name: [{ service: 'hipstershop.CheckoutService' }],
      retryPolicy: {
        maxAttempts: 3,
        initialBackoff: '0.1s',
        maxBackoff: '1s',
        backoffMultiplier: 2,
        retryableStatusCodes: ['UNAVAILABLE']
      }
    }]
  })
};

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  clientOptions
);

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    try {
      return await new Promise<PlaceOrderResponse>((resolve, reject) =>
        client.placeOrder(order, new Metadata(), { deadline: Date.now() + 5000 }, // 5s timeout
          (error, response) => (error ? reject(error) : resolve(response))
        )
      );
    } catch (error) {
      // Add more context to the error
      const enhancedError = new Error(
        `Checkout service error: ${error.message} (checkout address: ${CHECKOUT_ADDR})`,
        { cause: error }
      );
      throw enhancedError;
    }
  },
});

export default CheckoutGateway();
