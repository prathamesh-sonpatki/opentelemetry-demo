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
        
        // Validate input
        if (!orderData || !orderData.order || !Array.isArray(orderData.order.items)) {
          return res.status(400).json({ error: 'Invalid order data' });
        }

        try {
          const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

          const productList: IProductCheckoutItem[] = await Promise.all(
            items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
              try {
                const product = await ProductCatalogService.getProduct(productId, currencyCode as string);
                if (!product) {
                  throw new Error(`Product not found: ${productId}`);
                }

                return {
                  cost,
                  item: {
                    productId,
                    quantity,
                    product,
                  },
                };
              } catch (productError) {
                console.error(`Error fetching product ${productId}:`, productError);
                throw new Error(`Failed to process product ${productId}`);
              }
            })
          );

          return res.status(200).json({ ...order, items: productList });
        } catch (checkoutError: any) {
          console.error('Checkout process failed:', checkoutError);
          
          // Check for specific error types and return appropriate status codes
          if (checkoutError.message?.includes('failed to prepare order')) {
            return res.status(404).json({ error: 'One or more products not found' });
          }
          if (checkoutError.message?.includes('name resolver error')) {
            return res.status(503).json({ error: 'Payment service temporarily unavailable' });
          }
          
          return res.status(500).json({ error: 'Failed to process checkout' });
        }
      }

      default: {
        return res.status(405).send('');
      }
    }
  } catch (error) {
    console.error('Unexpected error in checkout API:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
};

export default InstrumentationMiddleware(handler);