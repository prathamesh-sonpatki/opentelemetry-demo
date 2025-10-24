// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions, Status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// gRPC client configuration
const channelOptions: ChannelOptions = {
  'grpc.dns_min_time_between_resolutions_ms': 2000, // Minimum time between DNS queries
  'grpc.initial_reconnect_backoff_ms': 100,
  'grpc.max_reconnect_backoff_ms': 2000,
  'grpc.dns_resolution_error_retry_ms': 500, // Retry DNS resolution after 500ms
  'grpc.keepalive_time_ms': 30000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.service_config': JSON.stringify({
    loadBalancingConfig: [{ round_robin: {} }],
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
  channelOptions
);

const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 100;

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError;
    let attempt = 0;

    while (attempt < MAX_RETRIES) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) => {
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 5); // 5 second timeout

          client.placeOrder(order, { deadline }, (error, response) => {
            if (error) {
              if (error.code === Status.UNAVAILABLE || error.code === Status.DEADLINE_EXCEEDED) {
                reject(error);
              } else {
                // For other errors, reject immediately
                reject(new Error(`Checkout service error: ${error.message}`));
              }
            } else {
              resolve(response);
            }
          });
        });
      } catch (error) {
        lastError = error;
        attempt++;
        
        if (attempt < MAX_RETRIES) {
          const backoffMs = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
          await sleep(backoffMs);
        }
      }
    }

    throw new Error(`Failed to place order after ${MAX_RETRIES} attempts: ${lastError?.message}`);
  },
});

export default CheckoutGateway();