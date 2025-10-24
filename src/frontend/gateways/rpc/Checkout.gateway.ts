// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

if (!CHECKOUT_ADDR) {
  throw new Error('CHECKOUT_ADDR environment variable must be set');
}

// Configure client with retry options
const client = new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure(), {
  'grpc.enable_retries': 1,
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
});

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let attempts = 0;
    while (attempts < MAX_RETRIES) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) =>
          client.placeOrder(order, (error, response) => {
            if (error) {
              if (error.code === 14) { // UNAVAILABLE
                console.warn(`Checkout service unavailable (attempt ${attempts + 1}/${MAX_RETRIES})`, error);
              }
              reject(error);
            } else {
              resolve(response);
            }
          })
        );
      } catch (error) {
        attempts++;
        if (attempts === MAX_RETRIES || !(error instanceof ServiceError) || error.code !== 14) {
          throw error;
        }
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * attempts));
      }
    }
    throw new Error('Maximum retry attempts reached');
  },
});

export default CheckoutGateway();
