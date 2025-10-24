// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = 'localhost:7000', CHECKOUT_RETRY_COUNT = '3' } = process.env;

if (!CHECKOUT_ADDR) {
  throw new Error('CHECKOUT_ADDR environment variable must be set');
}

const MAX_RETRIES = parseInt(CHECKOUT_RETRY_COUNT, 10);
const RETRY_DELAY_MS = 1000;

const client = new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure());

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) =>
          client.placeOrder(order, (error, response) => {
            if (error) {
              // Only retry on connection/unavailable errors
              if (error.code === 14 || error.code === 13) {
                reject(error);
              } else {
                // For other errors, fail fast
                reject(new Error(`Checkout service error: ${error.message}`));
              }
            } else {
              resolve(response);
            }
          })
        );
      } catch (error) {
        lastError = error as Error;
        
        // If this is the last attempt, throw the error
        if (attempt === MAX_RETRIES - 1) {
          throw new Error(`Failed to place order after ${MAX_RETRIES} attempts: ${lastError.message}`);
        }
        
        // Wait before retrying
        await delay(RETRY_DELAY_MS * (attempt + 1));
      }
    }

    throw lastError || new Error('Unknown error occurred while placing order');
  },
});

export default CheckoutGateway();