// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// Configuration for connection retry
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;
const BACKOFF_MULTIPLIER = 2;

class CheckoutGateway {
  private client: CheckoutServiceClient;

  constructor() {
    this.client = this.createClient();
  }

  private createClient(): CheckoutServiceClient {
    return new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure());
  }

  /**
   * Check if the error is retryable based on gRPC status codes
   */
  private isRetryableError(error: ServiceError): boolean {
    return [
      status.UNAVAILABLE,       // Service is temporarily unavailable
      status.DEADLINE_EXCEEDED, // Request timeout
      status.RESOURCE_EXHAUSTED, // Too many requests, should back off
      status.INTERNAL,          // Internal server error
    ].includes(error.code);
  }

  /**
   * Execute placeOrder with retry logic and exponential backoff
   */
  async placeOrder(order: PlaceOrderRequest, retryCount = 0): Promise<PlaceOrderResponse> {
    return new Promise<PlaceOrderResponse>(async (resolve, reject) => {
      this.client.placeOrder(order, async (error, response) => {
        if (error) {
          // Check if we should retry
          if (this.isRetryableError(error) && retryCount < MAX_RETRIES) {
            const delay = RETRY_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, retryCount);
            
            console.warn(
              `Checkout service error (attempt ${retryCount + 1}/${MAX_RETRIES + 1}): ${error.message}. Retrying in ${delay}ms...`
            );

            // Wait before retrying
            await new Promise(res => setTimeout(res, delay));
            
            // Recreate client if connection is lost
            if (error.code === status.UNAVAILABLE) {
              this.client = this.createClient();
            }

            try {
              const result = await this.placeOrder(order, retryCount + 1);
              resolve(result);
            } catch (retryError) {
              reject(retryError);
            }
          } else {
            // Max retries reached or non-retryable error
            const enhancedError = new Error(
              `Checkout service failed after ${retryCount} retries: ${error.message}`
            ) as ServiceError;
            enhancedError.code = error.code;
            enhancedError.details = error.details;
            
            console.error('Checkout service error:', {
              message: error.message,
              code: error.code,
              details: error.details,
              retries: retryCount,
            });
            
            reject(enhancedError);
          }
        } else {
          resolve(response);
        }
      });
    });
  }
}

export default new CheckoutGateway();
