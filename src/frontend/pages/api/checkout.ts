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
        const productValidations = await Promise.all(
          orderData.order.items.map(async ({ item }) => {
            if (!item?.productId) {
              return { valid: false, productId: item?.productId, error: 'Product ID is required' };
            }
            try {
              await ProductCatalogService.getProduct(item.productId, currencyCode as string);
              return { valid: true, productId: item.productId };
            } catch (error) {
              return { valid: false, productId: item.productId, error: `Product not found: ${item.productId}` };
            }
          })
        );

        const invalidProducts = productValidations.filter(p => !p.valid);
        if (invalidProducts.length > 0) {
          return res.status(400).json({
            error: `Invalid products in order: ${invalidProducts.map(p => p.error).join(', ')}`
          });
        }

        // Place order now that we know all products exist
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
          error: 'An error occurred during checkout. Please try again.' 
        });
      }
    }

    default: {
      return res.status(405).json({ error: 'Method not allowed' });
    }
  }
};

export default InstrumentationMiddleware(handler);