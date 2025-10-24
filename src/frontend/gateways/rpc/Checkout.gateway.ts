// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

class CheckoutServiceError extends Error {
  constructor(message: string, public readonly code?: number) {
    super(message);
    this.name = 'CheckoutServiceError';
  }
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function createClient(): CheckoutServiceClient {
  if (!CHECKOUT_ADDR) {
    throw new CheckoutServiceError('CHECKOUT_ADDR environment variable is not set');
  }
  return new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure());
}

async function placeOrderWithRetry(
  client: CheckoutServiceClient,
  order: PlaceOrderRequest,
  retries = MAX_RETRIES
): Promise<PlaceOrderResponse> {
  try {
    return await new Promise<PlaceOrderResponse>((resolve, reject) => {
      client.placeOrder(order, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
  } catch (error) {
    const grpcError = error as ServiceError;
    
    // If we have retries left and it's a retryable error
    if (retries > 0 && (
      grpcError.code === 14 || // UNAVAILABLE
      grpcError.code === 4 ||  // DEADLINE_EXCEEDED
      grpcError.code === 13    // INTERNAL
    )) {
      console.warn(`Checkout service request failed, retrying in ${RETRY_DELAY_MS}ms. Error: ${grpcError.message}`);
      await sleep(RETRY_DELAY_MS);
      return placeOrderWithRetry(client, order, retries - 1);
    }

    // Wrap the error with additional context
    throw new CheckoutServiceError(
      `Failed to place order: ${grpcError.message}`,
      grpcError.code
    );
  }
}

let client: CheckoutServiceClient | null = null;

const CheckoutGateway = () => ({
  async placeOrder(order: PlaceOrderRequest) {
    if (!client) {
      client = createClient();
    }

    try {
      return await placeOrderWithRetry(client, order);
    } catch (error) {
      // If the client connection failed, try to create a new one for next time
      client = null;
      throw error;
    }
  },
});

export default CheckoutGateway();