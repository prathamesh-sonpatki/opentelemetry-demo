// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Metadata, credentials } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = 'checkout-service:8080' } = process.env;

// Configure client with retry and timeout options
const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  credentials.createInsecure(),
  {
    'grpc.keepalive_time_ms': 10000,
    'grpc.keepalive_timeout_ms': 5000,
    'grpc.keepalive_permit_without_calls': 1,
    'grpc.enable_retries': 1,
    'grpc.service_config': JSON.stringify({
      'methodConfig': [{
        'name': [{ 'service': 'hipstershop.CheckoutService' }],
        'retryPolicy': {
          'maxAttempts': 3,
          'initialBackoff': '0.1s',
          'maxBackoff': '1s',
          'backoffMultiplier': 1.5,
          'retryableStatusCodes': ['UNAVAILABLE']
        }
      }]
    })
  }
);

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    try {
      return await new Promise<PlaceOrderResponse>((resolve, reject) => {
        const metadata = new Metadata();
        const deadline = new Date();
        deadline.setSeconds(deadline.getSeconds() + 10); // 10 second timeout

        client.placeOrder(order, metadata, { deadline }, (error, response) => {
          if (error) {
            console.error('[CheckoutGateway] Error placing order:', error.message);
            reject(error);
            return;
          }
          resolve(response);
        });
      });
    } catch (error) {
      console.error('[CheckoutGateway] Failed to place order:', error);
      throw error;
    }
  },
});

export default CheckoutGateway();
