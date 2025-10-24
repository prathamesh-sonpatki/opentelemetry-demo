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
        const { currencyCode = '' } = query;
        const orderData = body as PlaceOrderRequest;
        
        // Validate request data
        if (!orderData || !orderData.order || !Array.isArray(orderData.order.items)) {
          return res.status(400).json({ error: 'Invalid order data' });
        }

        // Get order details with retry mechanism
        const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

        // Process products in parallel with error handling
        const productPromises = items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
          try {
            if (!productId) {
              throw new Error('Product ID is required');
            }

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
          } catch (error) {
            console.error(`Failed to process product ${productId}:`, error);
            // Return a partial result rather than failing the entire order
            return {
              cost,
              item: {
                productId,
                quantity,
                product: null,
                error: `Failed to fetch product details: ${error.message}`,
              },
            };
          }
        });

        const productList = await Promise.all(productPromises);

        return res.status(200).json({ ...order, items: productList });
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' });
      }
    }
  } catch (error) {
    console.error('Checkout error:', error);
    return res.status(500).json({
      error: 'Checkout failed',
      details: error.message,
    });
  }
};

export default InstrumentationMiddleware(handler);
