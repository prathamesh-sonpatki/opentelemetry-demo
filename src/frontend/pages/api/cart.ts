// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiHandler } from 'next';
import CartGateway from '../../gateways/rpc/Cart.gateway';
import { AddItemRequest, Empty } from '../../protos/demo';
import ProductCatalogService from '../../services/ProductCatalog.service';
import { IProductCart, IProductCartItem } from '../../types/Cart';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';

type TResponse = IProductCart | Empty | { error: string; retryAfter?: number };

const handler: NextApiHandler<TResponse> = async ({ method, body, query }, res) => {
  try {
    switch (method) {
      case 'GET': {
        const { sessionId = '', currencyCode = '' } = query;
        
        try {
          const { userId, items } = await CartGateway.getCart(sessionId as string);

          const productList: IProductCartItem[] = await Promise.all(
            items.map(async ({ productId, quantity }) => {
              const product = await ProductCatalogService.getProduct(productId, currencyCode as string);

              return {
                productId,
                quantity,
                product,
              };
            })
          );

          return res.status(200).json({ userId, items: productList });
        } catch (error: any) {
          // Check if circuit breaker is open
          if (error.message?.includes('circuit breaker')) {
            console.error('Cart service circuit breaker open:', error);
            return res.status(503).json({ 
              error: 'Cart service temporarily unavailable. Please try again later.',
              retryAfter: 30 
            });
          }
          throw error;
        }
      }

      case 'POST': {
        const { userId, item } = body as AddItemRequest;

        try {
          await CartGateway.addItem(userId, item!);
          const cart = await CartGateway.getCart(userId);

          return res.status(200).json(cart);
        } catch (error: any) {
          if (error.message?.includes('circuit breaker')) {
            console.error('Cart service circuit breaker open:', error);
            return res.status(503).json({ 
              error: 'Cart service temporarily unavailable. Please try again later.',
              retryAfter: 30 
            });
          }
          throw error;
        }
      }

      case 'DELETE': {
        const { userId } = body as AddItemRequest;
        
        try {
          await CartGateway.emptyCart(userId);
          return res.status(204).send('');
        } catch (error: any) {
          if (error.message?.includes('circuit breaker')) {
            console.error('Cart service circuit breaker open:', error);
            return res.status(503).json({ 
              error: 'Cart service temporarily unavailable. Please try again later.',
              retryAfter: 30 
            });
          }
          throw error;
        }
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' });
      }
    }
  } catch (error: any) {
    console.error('Cart API error:', error);
    
    // Handle gRPC-specific errors
    if (error.code === 14) { // UNAVAILABLE
      return res.status(503).json({ 
        error: 'Cart service is currently unavailable. Please try again shortly.',
        retryAfter: 5 
      });
    } else if (error.code === 4) { // DEADLINE_EXCEEDED
      return res.status(504).json({ 
        error: 'Cart service request timed out. Please try again.' 
      });
    }
    
    // Generic error response
    return res.status(500).json({ 
      error: 'An error occurred processing your cart request.' 
    });
  }
};

export default InstrumentationMiddleware(handler);
