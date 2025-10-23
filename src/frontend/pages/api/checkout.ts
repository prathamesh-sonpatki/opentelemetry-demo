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
    if (method !== 'POST') {
      return res.status(405).json({ error: 'Method not allowed' });
    }

    const { currencyCode = '' } = query;
    if (!currencyCode) {
      return res.status(400).json({ error: 'Currency code is required' });
    }

    const orderData = body as PlaceOrderRequest;
    if (!orderData) {
      return res.status(400).json({ error: 'Order data is required' });
    }

    const orderResponse = await CheckoutGateway.placeOrder(orderData);
    if (!orderResponse?.order) {
      return res.status(500).json({ error: 'Failed to place order' });
    }

    const { order: { items = [], ...order } = {} } = orderResponse;

    try {
      const productList: IProductCheckoutItem[] = await Promise.all(
        items.map(async ({ item, cost }) => {
          if (!item?.productId) {
            throw new Error('Invalid product ID in order item');
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
    } catch (productError) {
      console.error('Error processing products:', productError);
      return res.status(500).json({ error: 'Failed to process product details' });
    }
  } catch (error) {
    console.error('Checkout API error:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
};

export default InstrumentationMiddleware(handler);