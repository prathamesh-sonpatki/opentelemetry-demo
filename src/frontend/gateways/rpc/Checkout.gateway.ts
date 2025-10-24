// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

// Validate checkout address
if (!CHECKOUT_ADDR) {
  throw new Error('CHECKOUT_ADDR environment variable must be set');
}

const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  {
    'grpc.keepalive_time_ms': 10000,
    'grpc.keepalive_timeout_ms': 5000,
    'grpc.max_reconnect_backoff_ms': 1000
  }
);

const isRetryableError = (error: ServiceError) => {
  return error.code === status.UNAVAILABLE || 
         error.code === status.DEADLINE_EXCEEDED ||
         error.code === status.RESOURCE_EXHAUSTED;
};

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) => {
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 5);
          
          client.placeOrder(
            order,
            { deadline },
            (error, response) => {
              if (error) {
                reject(error);
              } else {
                resolve(response);
              }
            }
          );
        });
      } catch (error) {
        lastError = error;
        
        if (!isRetryableError(error as ServiceError) || attempt === MAX_RETRIES) {
          throw new Error(
            `Checkout service error (attempt ${attempt}/${MAX_RETRIES}): ${error.message}`
          );
        }
        
        // Wait before retrying
        await delay(RETRY_DELAY_MS * attempt);
      }
    }
    
    // This should never be reached due to the throw in the catch block
    throw lastError || new Error('Unknown error in checkout service');
  },
});

export default CheckoutGateway();