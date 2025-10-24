// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = 'checkout-service:8080' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

const createClient = () => {
  const options = {
    'grpc.keepalive_timeout_ms': 5000,
    'grpc.keepalive_time_ms': 10000,
    'grpc.service_config': JSON.stringify({
      loadBalancingPolicy: 'round_robin',
      methodConfig: [{
        name: [{ service: 'CheckoutService' }],
        retryPolicy: {
          maxAttempts: 5,
          initialBackoff: '0.1s',
          maxBackoff: '1s',
          backoffMultiplier: 1.3,
          retryableStatusCodes: ['UNAVAILABLE']
        }
      }]
    })
  };
  
  return new CheckoutServiceClient(
    CHECKOUT_ADDR,
    ChannelCredentials.createInsecure(),
    options
  );
};

const client = createClient();

const retryWithBackoff = async (fn: () => Promise<any>, retries = MAX_RETRIES): Promise<any> => {
  try {
    return await fn();
  } catch (error) {
    if (retries === 0 || !error.toString().includes('UNAVAILABLE')) {
      throw error;
    }
    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
    return retryWithBackoff(fn, retries - 1);
  }
};

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    return retryWithBackoff(() => {
      return new Promise<PlaceOrderResponse>((resolve, reject) => {
        try {
          client.waitForReady(Date.now() + 5000, (err) => {
            if (err) {
              reject(new Error(`Service connection failed: ${err.message}`));
              return;
            }
            client.placeOrder(order, (error, response) => {
              if (error) {
                reject(error);
                return;
              }
              resolve(response);
            });
          });
        } catch (error) {
          reject(error);
        }
      });
    });
  },
});

export default CheckoutGateway();