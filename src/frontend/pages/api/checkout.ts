// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';
import CheckoutGateway from '../../gateways/rpc/Checkout.gateway';
import { Empty, PlaceOrderRequest } from '../../protos/demo';
import { IProductCheckoutItem, IProductCheckout } from '../../types/Cart';
import ProductCatalogService from '../../services/ProductCatalog.service';

type TResponse = IProductCheckout | Empty;

const handler = async ({ method, body, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  try {
    switch (method) {
      case 'POST': {
        const { currencyCode = 'USD' } = query;
        const orderData = body as PlaceOrderRequest;
        
        // Validate order items exist
        if (!orderData?.order?.items?.length) {
          return res.status(400).json({ error: 'No items in order' });
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
              } catch (err) {
                // Log specific product error but continue with other products
                console.error(`Error fetching product ${productId}:`, err);
                throw new Error(`Failed to fetch product details for ${productId}`);
              }
            })
          );

          return res.status(200).json({ ...order, items: productList });
        } catch (err) {
          // Check for specific error types and handle accordingly
          if (err.message.includes('name resolver error')) {
            return res.status(503).json({ error: 'Payment service temporarily unavailable' });
          }
          if (err.message.includes('failed to get product')) {
            return res.status(400).json({ error: 'One or more products not found' });
          }
          throw err; // Re-throw unexpected errors
        }
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' });
      }
    }
  } catch (err) {
    console.error('Checkout error:', err);
    return res.status(500).json({ error: 'Internal server error during checkout' });
  }
};

export default InstrumentationMiddleware(handler);