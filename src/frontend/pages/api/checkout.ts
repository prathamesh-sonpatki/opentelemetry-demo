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
        
        // Validate order data
        if (!orderData || !orderData.userId || !orderData.email) {
          return res.status(400).json({ error: 'Invalid order data' } as any);
        }

        let placeOrderResult;
        try {
          placeOrderResult = await CheckoutGateway.placeOrder(orderData);
        } catch (error: any) {
          console.error('Checkout service error:', error);
          if (error.message?.includes('name resolver error')) {
            return res.status(503).json({ 
              error: 'Checkout service temporarily unavailable'
            } as any);
          }
          throw error;
        }

        const { order: { items = [], ...order } = {} } = placeOrderResult;

        const productList: IProductCheckoutItem[] = await Promise.all(
          items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
            try {
              const product = await ProductCatalogService.getProduct(productId, currencyCode as string);
              if (!product) {
                throw new Error(`Product not found: ${productId}`);
              }
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
              throw error;
            }
          })
        );

        return res.status(200).json({ ...order, items: productList });
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' } as any);
      }
    }
  } catch (error) {
    console.error('Checkout API error:', error);
    return res.status(500).json({ 
      error: 'An unexpected error occurred processing your order'
    } as any);
  }
};

export default InstrumentationMiddleware(handler);
