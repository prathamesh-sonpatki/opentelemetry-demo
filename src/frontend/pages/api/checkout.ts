// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';
import CheckoutGateway from '../../gateways/rpc/Checkout.gateway';
import { Empty, PlaceOrderRequest } from '../../protos/demo';
import { IProductCheckoutItem, IProductCheckout } from '../../types/Cart';
import ProductCatalogService from '../../services/ProductCatalog.service';

type TResponse = IProductCheckout | Empty | { error: string };

const TIMEOUT_MS = 10000; // 10 second timeout

const validateOrderData = (data: PlaceOrderRequest): string | null => {
  if (!data || typeof data !== 'object') {
    return 'Invalid order data';
  }
  
  if (!data.order || !Array.isArray(data.order.items)) {
    return 'Invalid order structure';
  }
  
  return null;
};

const handler = async ({ method, body, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  try {
    if (method !== 'POST') {
      return res.status(405).json({ error: 'Method not allowed' });
    }

    const { currencyCode = '' } = query;
    
    if (!currencyCode) {
      return res.status(400).json({ error: 'Currency code is required' });
    }

    const orderData = body as PlaceOrderRequest;
    const validationError = validateOrderData(orderData);
    
    if (validationError) {
      return res.status(400).json({ error: validationError });
    }

    // Add timeout to the checkout request
    const checkoutPromise = CheckoutGateway.placeOrder(orderData);
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Checkout request timed out')), TIMEOUT_MS)
    );

    const { order } = await Promise.race([checkoutPromise, timeoutPromise])
      .catch(error => {
        throw new Error(`Checkout failed: ${error.message}`);
      });

    if (!order || !Array.isArray(order.items)) {
      throw new Error('Invalid response from checkout service');
    }

    const productList: IProductCheckoutItem[] = await Promise.all(
      order.items.map(async ({ item, cost }) => {
        if (!item?.productId) {
          throw new Error('Invalid product data in order');
        }

        const product = await ProductCatalogService.getProduct(
          item.productId,
          currencyCode as string
        ).catch(error => {
          throw new Error(`Failed to fetch product ${item.productId}: ${error.message}`);
        });

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
    console.error('Checkout error:', error);
    return res.status(500).json({
      error: 'An error occurred while processing your order. Please try again.',
    });
  }
};

export default InstrumentationMiddleware(handler);