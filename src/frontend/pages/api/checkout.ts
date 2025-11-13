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
      const { currencyCode = '' } = query;
      const orderData = body as PlaceOrderRequest;
      
      try {
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
            } catch (productError: any) {
              // Log but don't fail entire checkout if single product fetch fails
              console.error(`Failed to fetch product ${productId}:`, productError.message);
              throw productError; // Re-throw to be caught by outer catch
            }
          })
        );

        return res.status(200).json({ ...order, items: productList });
      } catch (error: any) {
        console.error('Checkout service error:', error.message);
        
        // Check if it's a gRPC connection error
        if (error.message?.includes('UNAVAILABLE') || 
            error.message?.includes('ECONNREFUSED') ||
            error.message?.includes('failed to prepare order') ||
            error.message?.includes('failed to get product')) {
          return res.status(503).json({
            error: 'Checkout service temporarily unavailable. Please try again later.',
            code: 'SERVICE_UNAVAILABLE'
          } as any);
        }
        
        // Return 500 for unexpected errors
        return res.status(500).json({
          error: 'An error occurred during checkout',
          code: 'CHECKOUT_ERROR'
        } as any);
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);
