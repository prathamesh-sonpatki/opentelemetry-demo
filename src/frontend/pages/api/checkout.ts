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

        // Validate input data
        if (!orderData || !orderData.order) {
          return res.status(400).json({ error: 'Invalid order data' } as any);
        }

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
              // Log the error but continue processing other items
              console.error(`Failed to fetch product ${productId}:`, error);
              return {
                cost,
                item: {
                  productId,
                  quantity,
                  product: null, // Return null for failed products
                },
              };
            }
          })
        );

        return res.status(200).json({ ...order, items: productList });
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' } as any);
      }
    }
  } catch (error: any) {
    console.error('Checkout error:', error);
    return res.status(500).json({
      error: 'Checkout failed',
      details: error.message
    } as any);
  }
};

export default InstrumentationMiddleware(handler);
