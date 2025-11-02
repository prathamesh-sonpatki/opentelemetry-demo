// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, CallOptions } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';
import { retryGrpcCall } from '../../utils/grpc/retry';

const { CHECKOUT_ADDR = '' } = process.env;

const client = new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure());

// Default call options with timeout
const DEFAULT_CALL_OPTIONS: CallOptions = {
  deadline: Date.now() + 15000, // 15 second timeout for checkout operations
};

const CheckoutGateway = () => ({
  placeOrder(order: PlaceOrderRequest) {
    if (!order || !order.userId) {
      return Promise.reject(new Error('Invalid order: userId is required'));
    }
    
    return retryGrpcCall(
      () =>
        new Promise<PlaceOrderResponse>((resolve, reject) => {
          // Refresh deadline for each retry attempt
          const callOptions = { ...DEFAULT_CALL_OPTIONS, deadline: Date.now() + 15000 };
          client.placeOrder(order, callOptions, (error, response) => {
            if (error) {
              console.error(`Checkout.placeOrder failed for user ${order.userId}: ${error.message}`);
              reject(error);
            } else {
              resolve(response);
            }
          });
        }),
      {
        maxRetries: 2, // Fewer retries for write operations
        initialDelayMs: 200,
        maxDelayMs: 3000,
      }
    );
  },
});

export default CheckoutGateway();
