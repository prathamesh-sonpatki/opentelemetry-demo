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
  switch (method) {
    case 'POST': {
      try {
        const { currencyCode = '' } = query;
        const orderData = body as PlaceOrderRequest;
        
        // Add validation for required fields
        if (!orderData || !orderData.order || !Array.isArray(orderData.order.items)) {
          return res.status(400).json({ error: 'Invalid order data' });
        }

        const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

        const productList: IProductCheckoutItem[] = await Promise.all(
          items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
            if (!productId) {
              throw new Error('Product ID is required');
            }
            
            const product = await ProductCatalogService.getProduct(productId, currencyCode as string);

            return {
              cost,
              item: {
                productId,
                quantity,
                product,
              },
            };
          })
        );

        return res.status(200).json({ ...order, items: productList });
      } catch (error) {
        console.error('Checkout error:', error);
        const statusCode = error.code === 14 ? 503 : 500; // Use 503 for service unavailable
        return res.status(statusCode).json({ 
          error: 'Checkout service temporarily unavailable. Please try again.'
        });
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);
