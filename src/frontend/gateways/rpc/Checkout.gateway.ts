// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

// Retryable status codes
const RETRYABLE_STATUS_CODES = [
  status.UNAVAILABLE,
  status.DEADLINE_EXCEEDED,
  status.RESOURCE_EXHAUSTED
];

class CheckoutGatewayImpl {
  private client: CheckoutServiceClient;
  private isInitialized: boolean = false;

  constructor() {
    this.initializeClient();
  }

  private initializeClient() {
    try {
      if (!CHECKOUT_ADDR) {
        throw new Error('CHECKOUT_ADDR environment variable not set');
      }
      
      this.client = new CheckoutServiceClient(
        CHECKOUT_ADDR,
        ChannelCredentials.createInsecure(),
        {
          'grpc.keepalive_time_ms': 10000,
          'grpc.keepalive_timeout_ms': 5000,
          'grpc.keepalive_permit_without_calls': 1,
          'grpc.max_reconnect_backoff_ms': 5000
        }
      );
      this.isInitialized = true;
    } catch (error) {
      console.error('Failed to initialize checkout client:', error);
      this.isInitialized = false;
    }
  }

  private async retryableCall<T>(
    operation: () => Promise<T>,
    retries: number = MAX_RETRIES
  ): Promise<T> {
    try {
      if (!this.isInitialized) {
        this.initializeClient();
      }
      return await operation();
    } catch (error) {
      const grpcError = error as ServiceError;
      
      if (retries > 0 && RETRYABLE_STATUS_CODES.includes(grpcError.code)) {
        console.warn(`Retrying operation due to error: ${grpcError.message}. Retries left: ${retries}`);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
        return this.retryableCall(operation, retries - 1);
      }
      
      throw error;
    }
  }

  async placeOrder(order: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    return this.retryableCall(() => {
      return new Promise<PlaceOrderResponse>((resolve, reject) => {
        this.client.placeOrder(
          order,
          {
            deadline: Date.now() + 10000 // 10 second timeout
          },
          (error, response) => (error ? reject(error) : resolve(response))
        );
      });
    });
  }
}

export default new CheckoutGatewayImpl();