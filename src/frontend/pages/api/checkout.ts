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

        // Validate input
        if (!orderData || !orderData.order) {
          return res.status(400).json({ error: 'Invalid order data' });
        }

        // Place order with retry handling from gateway
        const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

        // Process products with error handling
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
              console.error(`Error fetching product ${productId}:`, error);
              throw error; // Let the outer try/catch handle it
            }
          })
        );

        return res.status(200).json({ ...order, items: productList });
      }

      default: {
        return res.status(405).send('');
      }
    }
  } catch (error) {
    console.error('Checkout API error:', error);
    
    // Return appropriate error response
    const status = error.code === 'UNAVAILABLE' ? 503 : 500;
    return res.status(status).json({
      error: 'Checkout service temporarily unavailable. Please try again later.'
    });
  }
};

export default InstrumentationMiddleware(handler);
