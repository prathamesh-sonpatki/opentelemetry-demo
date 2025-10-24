// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

const createClient = () => new CheckoutServiceClient(CHECKOUT_ADDR, ChannelCredentials.createInsecure());

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  retries: number = MAX_RETRIES,
  delay: number = RETRY_DELAY_MS
): Promise<T> => {
  try {
    return await operation();
  } catch (error) {
    if (retries === 0 || !(error instanceof ServiceError)) {
      throw error;
    }
    
    // Only retry on specific error codes
    if (error.code === 14 || error.code === 13) { // UNAVAILABLE or INTERNAL
      await sleep(delay);
      return retryWithBackoff(operation, retries - 1, delay * 2);
    }
    
    throw error;
  }
};

const CheckoutGateway = () => ({
  placeOrder(order: PlaceOrderRequest) {
    const client = createClient();
    
    return retryWithBackoff(() => 
      new Promise<PlaceOrderResponse>((resolve, reject) =>
        client.placeOrder(order, (error, response) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
          client.close();
        })
      )
    );
  },
});

export default CheckoutGateway();