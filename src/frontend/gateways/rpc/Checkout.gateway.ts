// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = 'localhost:7000' } = process.env;

// Validate checkout address
if (!CHECKOUT_ADDR) {
  throw new Error('CHECKOUT_ADDR environment variable must be set');
}

const RETRY_OPTIONS = {
  maxRetries: 3,
  initialBackoffMs: 100,
  maxBackoffMs: 1000,
};

const client = new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure());

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const isRetryableError = (error: ServiceError) => {
  return error.code === status.UNAVAILABLE || 
         error.code === status.DEADLINE_EXCEEDED ||
         error.code === status.RESOURCE_EXHAUSTED;
};

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    let attempt = 0;

    while (attempt < RETRY_OPTIONS.maxRetries) {
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
        
        if (!isRetryableError(error as ServiceError)) {
          throw error;
        }

        attempt++;
        if (attempt < RETRY_OPTIONS.maxRetries) {
          const backoffMs = Math.min(
            RETRY_OPTIONS.initialBackoffMs * Math.pow(2, attempt - 1),
            RETRY_OPTIONS.maxBackoffMs
          );
          await sleep(backoffMs);
          continue;
        }
      }
    }

    throw lastError || new Error('Max retries exceeded');
  },
});

export default CheckoutGateway();