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
        
        // Validate order data
        if (!orderData || !orderData.userId) {
          return res.status(400).json({ error: 'Invalid order data: userId is required' });
        }
        
        const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

        // Fetch product details with error handling
        const productList: IProductCheckoutItem[] = await Promise.allSettled(
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
              // Log error but don't fail the entire checkout
              console.error(`Failed to fetch product ${productId}:`, error);
              return {
                cost,
                item: {
                  productId,
                  quantity,
                  product: null, // Graceful degradation
                },
              };
            }
          })
        ).then(results =>
          results
            .filter((result): result is PromiseFulfilledResult<IProductCheckoutItem> => 
              result.status === 'fulfilled'
            )
            .map(result => result.value)
        );

        return res.status(200).json({ ...order, items: productList });
      } catch (error: any) {
        console.error('Checkout failed:', error);
        
        // Return appropriate error status based on error type
        const statusCode = error?.code === '14' || error?.message?.includes('UNAVAILABLE') ? 503 : 500;
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
