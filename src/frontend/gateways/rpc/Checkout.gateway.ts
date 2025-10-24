// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const {
  CHECKOUT_ADDR = 'checkout-service:8080',
  GRPC_MAX_RETRIES = '3',
  GRPC_INITIAL_BACKOFF_MS = '1000',
} = process.env;

const maxRetries = parseInt(GRPC_MAX_RETRIES, 10);
const initialBackoffMs = parseInt(GRPC_INITIAL_BACKOFF_MS, 10);

// Create client with default address if environment variable is not set
const client = new CheckoutServiceClient(
  CHECKOUT_ADDR || 'checkout-service:8080',
  ChannelCredentials.createInsecure(),
  {
    'grpc.enable_retries': 1,
    'grpc.service_config': JSON.stringify({
      methodConfig: [{
        name: [{ service: 'CheckoutService' }],
        retryPolicy: {
          maxAttempts: maxRetries,
          initialBackoff: `${initialBackoffMs}ms`,
          maxBackoff: '5000ms',
          backoffMultiplier: 2,
          retryableStatusCodes: [status.UNAVAILABLE],
        },
      }],
    }),
  }
);

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    let attempt = 0;

    while (attempt < maxRetries) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) => {
          client.placeOrder(order, (error, response) => {
            if (error) {
              reject(error);
            } else {
              resolve(response);
            }
          });
        });
      } catch (error) {
        lastError = error as Error;
        
        // Only retry on UNAVAILABLE errors
        if ((error as ServiceError).code !== status.UNAVAILABLE) {
          throw error;
        }

        attempt++;
        if (attempt < maxRetries) {
          await sleep(Math.min(initialBackoffMs * Math.pow(2, attempt - 1), 5000));
        }
      }
    }

    throw lastError || new Error('Max retries exceeded');
  },
});

export default CheckoutGateway();