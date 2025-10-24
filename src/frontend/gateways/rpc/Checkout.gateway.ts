// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = 'checkout:5050' } = process.env;

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR || 'checkout:5050',
  ChannelCredentials.createInsecure(),
  {
    'grpc.enable_retries': 1,
    'grpc.service_config': JSON.stringify({
      methodConfig: [{
        name: [{ service: 'CheckoutService' }],
        retryPolicy: {
          maxAttempts: 3,
          initialBackoff: '0.1s',
          maxBackoff: '1s',
          backoffMultiplier: 2,
          retryableStatusCodes: ['UNAVAILABLE']
        }
      }]
    })
  }
);

const CheckoutGateway = () => ({
  placeOrder(order: PlaceOrderRequest) {
    return new Promise<PlaceOrderResponse>((resolve, reject) =>
      client.placeOrder(order, (error, response) => (error ? reject(error) : resolve(response)))
    );
  },
});

export default CheckoutGateway();
