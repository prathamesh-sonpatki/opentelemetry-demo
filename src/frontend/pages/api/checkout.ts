// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';
import CheckoutGateway from '../../gateways/rpc/Checkout.gateway';
import { Empty, PlaceOrderRequest } from '../../protos/demo';
import { IProductCheckoutItem, IProductCheckout } from '../../types/Cart';
import ProductCatalogService from '../../services/ProductCatalog.service';

type TResponse = IProductCheckout | Empty | { error: string };

const handler = async ({ method, body, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  try {
    switch (method) {
      case 'POST': {
        const { currencyCode = '' } = query;
        const orderData = body as PlaceOrderRequest;

        if (!orderData || !currencyCode) {
          return res.status(400).json({ error: 'Missing required order data or currency code' });
        }

        try {
          const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

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
              } catch (error) {
                console.error(`Failed to fetch product ${productId}:`, error);
                return {
                  cost,
                  item: {
                    productId,
                    quantity,
                    product: null, // Return null for failed products but don't fail entire order
                  },
                };
              }
            })
          );

          return res.status(200).json({ ...order, items: productList });
        } catch (error) {
          // Handle specific checkout errors
          if (error.message?.includes('name resolver error')) {
            return res.status(503).json({ 
              error: 'Checkout service temporarily unavailable. Please try again in a few moments.' 
            });
          }
          throw error; // Re-throw other errors to be caught by global handler
        }
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' });
      }
    }
  } catch (error) {
    console.error('Checkout API error:', error);
    return res.status(500).json({ 
      error: 'An unexpected error occurred processing your order. Please try again.' 
    });
  }
};

export default InstrumentationMiddleware(handler);