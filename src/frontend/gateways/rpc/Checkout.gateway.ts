// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { CheckoutServiceClient, PlaceOrderRequest, PlaceOrderResponse } from '../../protos/demo';
import { createGrpcClient, callWithRetry } from '../../utils/grpc-client-factory';

const { CHECKOUT_ADDR = '' } = process.env;

const client = createGrpcClient(CheckoutServiceClient, {
  address: CHECKOUT_ADDR,
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 10000,
});

const CheckoutGateway = () => ({
  placeOrder(order: PlaceOrderRequest) {
    return callWithRetry<PlaceOrderRequest, PlaceOrderResponse>(
      client,
      'placeOrder',
      order,
      { maxRetries: 3, retryDelay: 1000 }
    );
  },
});

export default CheckoutGateway();
