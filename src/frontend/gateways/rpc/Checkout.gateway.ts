// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// Add retry options for the gRPC client
const RETRY_OPTIONS = {
  'grpc.service_config': JSON.stringify({
    methodConfig: [{
      name: [{ service: 'hipstershop.CheckoutService' }],
      retryPolicy: {
        maxAttempts: 3,
        initialBackoff: '0.1s',
        maxBackoff: '1s',
        backoffMultiplier: 1.5,
        retryableStatusCodes: ['UNAVAILABLE']
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
  RETRY_OPTIONS
);

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    try {
      return await new Promise<PlaceOrderResponse>((resolve, reject) =>
        client.placeOrder(order, (error, response) => {
          if (error) {
            console.error('Checkout service error:', error);
            reject(error);
            return;
          }
          resolve(response);
        })
      );
    } catch (error) {
      // Add more context to the error
      throw new Error(`Checkout service error: ${error.message}`);
    }
  },
});

export default CheckoutGateway();
