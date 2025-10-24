// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// Maximum number of retries for transient failures
const MAX_RETRIES = 3;
// Base delay for exponential backoff (in ms)
const BASE_DELAY = 100;

const createClient = () => {
  const client = new CheckoutServiceClient(
    CHECKOUT_ADDR,
    ChannelCredentials.createInsecure(),
    {
      'grpc.enable_retries': 1,
      'grpc.service_config': JSON.stringify({
        methodConfig: [{
          name: [{ service: 'hipstershop.CheckoutService' }],
          retryPolicy: {
            maxAttempts: 5,
            initialBackoff: '0.1s',
            maxBackoff: '1s',
            backoffMultiplier: 1.5,
            retryableStatusCodes: ['UNAVAILABLE']
          }
        }]
      })
    }
  );
  return client;
};

const client = createClient();

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) => {
          client.placeOrder(order, (error, response) => {
            if (error) {
              if (error.code === status.UNAVAILABLE) {
                reject(error);
              } else {
                // For non-transient errors, reject immediately
                reject(error);
              }
            } else {
              resolve(response);
            }
          });
        });
      } catch (error: any) {
        lastError = error;
        
        // Only retry on UNAVAILABLE errors
        if (error.code !== status.UNAVAILABLE) {
          throw error;
        }
        
        // Wait with exponential backoff before retrying
        if (attempt < MAX_RETRIES - 1) {
          await sleep(BASE_DELAY * Math.pow(2, attempt));
          continue;
        }
      }
    }
    
    // If we exhausted all retries, throw the last error
    throw lastError || new Error('Failed to place order after maximum retries');
  },
});

export default CheckoutGateway();