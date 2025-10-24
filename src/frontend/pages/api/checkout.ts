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
  switch (method) {
    case 'POST': {
      try {
        const { currencyCode = '' } = query;
        const orderData = body as PlaceOrderRequest;
        
        // Validate order items exist
        if (!orderData?.order?.items?.length) {
          return res.status(400).json({ error: 'Order must contain at least one item' });
        }

        // Pre-validate all products exist before placing order
        try {
          await Promise.all(
            orderData.order.items.map(({ item }) => 
              ProductCatalogService.getProduct(item?.productId || '', currencyCode as string)
            )
          );
        } catch (error) {
          return res.status(400).json({ 
            error: `Invalid product in order: ${error.message || 'Product not found'}` 
          });
        }

        const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

        const productList: IProductCheckoutItem[] = await Promise.all(
          items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
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
        return res.status(500).json({ 
          error: 'Failed to process checkout. Please try again.' 
        });
      }
    }

    default: {
      return res.status(405).json({ error: 'Method not allowed' });
    }
  }
};

export default InstrumentationMiddleware(handler);