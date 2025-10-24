// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 100;

const client = new CheckoutServiceClient(CHECKOUT_ADDR, 
  ChannelCredentials.createInsecure(), 
  {
    'grpc.service_config': JSON.stringify({
      loadBalancingConfig: [{ round_robin: {} }],
      methodConfig: [{
        name: [{ service: 'CheckoutService' }],
        retryPolicy: {
          maxAttempts: 5,
          initialBackoff: '0.1s',
          maxBackoff: '1s',
          backoffMultiplier: 1.5,
          retryableStatusCodes: [status.UNAVAILABLE]
        }
      }]
    })
  }
);

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const isRetryableError = (error: ServiceError) => {
  return error.code === status.UNAVAILABLE || 
         error.code === status.DEADLINE_EXCEEDED || 
         error.message.includes('name resolver error');
};

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        return await new Promise<PlaceOrderResponse>((resolve, reject) =>
          client.placeOrder(order, { deadline: Date.now() + 5000 }, (error, response) => 
            (error ? reject(error) : resolve(response))
          )
        );
      } catch (error) {
        lastError = error as Error;
        
        if (!isRetryableError(error as ServiceError) || attempt === MAX_RETRIES) {
          throw error;
        }
        
        // Exponential backoff
        await sleep(INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt - 1));
      }
    }
    
    throw lastError;
  },
});

export default CheckoutGateway();
