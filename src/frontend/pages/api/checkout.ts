// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';
import CheckoutGateway from '../../gateways/rpc/Checkout.gateway';
import { Empty, PlaceOrderRequest } from '../../protos/demo';
import { IProductCheckoutItem, IProductCheckout } from '../../types/Cart';
import ProductCatalogService from '../../services/ProductCatalog.service';

type TResponse = IProductCheckout | Empty;

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  retries: number = MAX_RETRIES,
  delay: number = RETRY_DELAY
): Promise<T> => {
  try {
    return await operation();
  } catch (error) {
    if (retries > 0 && error.message?.includes('name resolver error')) {
      await sleep(delay);
      return retryWithBackoff(operation, retries - 1, delay * 2);
    }
    throw error;
  }
};

const handler = async ({ method, body, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  switch (method) {
    case 'POST': {
      try {
        const { currencyCode = '' } = query;
        const orderData = body as PlaceOrderRequest;

        // Retry the checkout operation with backoff
        const { order: { items = [], ...order } = {} } = await retryWithBackoff(() =>
          CheckoutGateway.placeOrder(orderData)
        );

        const productList: IProductCheckoutItem[] = await Promise.all(
          items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
            try {
              const product = await ProductCatalogService.getProduct(productId, currencyCode as string);
              return {
                cost,
                item: {
                  productId,
                  quantity,
                  product,
                },
              };
            } catch (err) {
              console.error(`Failed to fetch product ${productId} details:`, err);
              // Return basic item info without full product details
              return {
                cost,
                item: {
                  productId,
                  quantity,
                  product: null,
                },
              };
            }
          })
        );

        return res.status(200).json({ ...order, items: productList });
      } catch (err) {
        console.error('Checkout failed:', err);
        return res.status(503).json({
          error: 'Checkout service temporarily unavailable',
          message: 'Please try again in a few moments',
        });
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);