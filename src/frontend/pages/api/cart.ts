// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiHandler } from 'next';
import CartGateway from '../../gateways/rpc/Cart.gateway';
import { AddItemRequest, Empty } from '../../protos/demo';
import ProductCatalogService from '../../services/ProductCatalog.service';
import { IProductCart, IProductCartItem } from '../../types/Cart';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';

type TResponse = IProductCart | Empty;

const handler: NextApiHandler<TResponse> = async ({ method, body, query }, res) => {
  switch (method) {
    case 'GET': {
      try {
        const { sessionId = '', currencyCode = '' } = query;
        const { userId, items } = await CartGateway.getCart(sessionId as string);

        const productList: IProductCartItem[] = await Promise.all(
          items.map(async ({ productId, quantity }) => {
            try {
              const product = await ProductCatalogService.getProduct(productId, currencyCode as string);
              return {
                productId,
                quantity,
                product,
              };
            } catch (error) {
              // If product lookup fails, return minimal info
              console.error(`Failed to fetch product ${productId}:`, error);
              return {
                productId,
                quantity,
                product: {
                  id: productId,
                  name: 'Product Unavailable',
                  description: 'Product information temporarily unavailable',
                  picture: '/img/products/placeholder.jpg',
                  priceUsd: { units: 0, nanos: 0, currencyCode: 'USD' },
                },
              };
            }
          })
        );

        return res.status(200).json({ userId, items: productList });
      } catch (error) {
        console.error('Cart operation failed:', error);
        const statusCode = error.code === 14 ? 503 : 500;
        return res.status(statusCode).json({
          error: 'Cart service temporarily unavailable'
        });
      }
    }

    case 'POST': {
      try {
        const { userId, item } = body as AddItemRequest;

        if (!userId || !item) {
          return res.status(400).json({ error: 'Invalid request data' });
        }

        await CartGateway.addItem(userId, item!);
        const cart = await CartGateway.getCart(userId);

        return res.status(200).json(cart);
      } catch (error) {
        console.error('Failed to add item to cart:', error);
        return res.status(500).json({
          error: 'Failed to update cart'
        });
      }
    }

    case 'DELETE': {
      try {
        const { userId } = body as AddItemRequest;
        
        if (!userId) {
          return res.status(400).json({ error: 'User ID is required' });
        }
        
        await CartGateway.emptyCart(userId);
        return res.status(204).send('');
      } catch (error) {
        console.error('Failed to empty cart:', error);
        return res.status(500).json({
          error: 'Failed to empty cart'
        });
      }
    }

    default: {
      return res.status(405).json({ error: 'Method not allowed' });
    }
  }
};

export default InstrumentationMiddleware(handler);
