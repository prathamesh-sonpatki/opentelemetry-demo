// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, status, ServiceError } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

// Validate checkout address is provided
if (!CHECKOUT_ADDR) {
  throw new Error('CHECKOUT_ADDR environment variable must be set');
}

// Create client with timeout
const client = new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure(), {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.max_reconnect_backoff_ms': 5000,
});

// Helper for delay between retries
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Check if error is retryable
const isRetryableError = (error: ServiceError): boolean => {
  return [
    status.UNAVAILABLE,
    status.DEADLINE_EXCEEDED,
    status.RESOURCE_EXHAUSTED
  ].includes(error.code);
};

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    let lastError: Error | null = null;
    
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        // Add timeout to each attempt
        const result = await new Promise<PlaceOrderResponse>((resolve, reject) => {
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 10);
          
          client.placeOrder(order, { deadline }, (error, response) => {
            if (error) reject(error);
            else resolve(response);
          });
        });
        
        return result;
      } catch (error) {
        lastError = error;
        
        // Only retry on specific error types
        if (!isRetryableError(error as ServiceError)) {
          throw error;
        }
        
        // Don't delay on last attempt
        if (attempt < MAX_RETRIES - 1) {
          await delay(RETRY_DELAY_MS * Math.pow(2, attempt)); // Exponential backoff
        }
      }
    }
    
    throw new Error(`Failed to place order after ${MAX_RETRIES} attempts. Last error: ${lastError?.message}`);
  },
});

export default CheckoutGateway();