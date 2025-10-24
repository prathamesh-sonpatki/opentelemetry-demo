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
    if (method !== 'POST') {
      return res.status(405).json({ error: 'Method not allowed' });
    }

    const { currencyCode = '' } = query;
    const orderData = body as PlaceOrderRequest;

    if (!orderData?.userId || !Array.isArray(orderData?.items)) {
      return res.status(400).json({ error: 'Invalid order data' });
    }

    // Place the order
    const orderResult = await CheckoutGateway.placeOrder(orderData);
    const { order } = orderResult || {};
    
    if (!order?.items) {
      throw new Error('Invalid order response from checkout service');
    }

    // Fetch product details in parallel
    const productList: IProductCheckoutItem[] = await Promise.all(
      order.items.map(async ({ item, cost }) => {
        if (!item?.productId) {
          throw new Error(`Invalid product ID in order item`);
        }

        const product = await ProductCatalogService.getProduct(
          item.productId,
          currencyCode as string
        );

        if (!product) {
          throw new Error(`Product not found: ${item.productId}`);
        }

        return {
          cost,
          item: {
            productId: item.productId,
            quantity: item.quantity || 0,
            product,
          },
        };
      })
    );

    return res.status(200).json({ ...order, items: productList });
  } catch (error) {
    console.error('[checkout] Error processing order:', error);
    
    // Return appropriate error response
    const statusCode = error.code === 13 ? 503 : 500; // INTERNAL -> Service Unavailable
    const message = error.code === 13 
      ? 'Checkout service temporarily unavailable'
      : 'Failed to process order';

    return res.status(statusCode).json({ error: message });
  }
};

export default InstrumentationMiddleware(handler);
