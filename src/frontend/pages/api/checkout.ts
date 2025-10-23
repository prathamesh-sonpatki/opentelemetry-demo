// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';
import CheckoutGateway from '../../gateways/rpc/Checkout.gateway';
import { Empty, PlaceOrderRequest } from '../../protos/demo';
import { IProductCheckoutItem, IProductCheckout } from '../../types/Cart';
import ProductCatalogService from '../../services/ProductCatalog.service';

type TResponse = IProductCheckout | Empty | { error: string };

const validateOrderItems = (items: any[]): string | null => {
  if (!Array.isArray(items) || items.length === 0) {
    return 'Order must contain at least one item';
  }
  return null;
};

const handler = async (
  { method, body, query }: NextApiRequest,
  res: NextApiResponse<TResponse>
) => {
  try {
    switch (method) {
      case 'POST': {
        const { currencyCode = 'USD' } = query;
        const orderData = body as PlaceOrderRequest;

        // Validate the order data
        if (!orderData || !orderData.order) {
          return res.status(400).json({ error: 'Invalid order data' });
        }

        const validationError = validateOrderItems(orderData.order.items || []);
        if (validationError) {
          return res.status(400).json({ error: validationError });
        }

        // First verify all products exist before placing order
        const productIds = orderData.order.items.map(
          (item) => item.item?.productId
        ).filter(Boolean);

        try {
          await Promise.all(
            productIds.map((id) =>
              ProductCatalogService.getProduct(id!, currencyCode as string)
            )
          );
        } catch (error) {
          console.error('Product validation failed:', error);
          return res.status(400).json({
            error: `One or more products in the order are invalid or unavailable`,
          });
        }

        // Place the order
        const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(
          orderData
        );

        // Get product details for response
        const productList: IProductCheckoutItem[] = await Promise.all(
          items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
            try {
              const product = await ProductCatalogService.getProduct(
                productId,
                currencyCode as string
              );

              return {
                cost,
                item: {
                  productId,
                  quantity,
                  product,
                },
              };
            } catch (error) {
              console.error(
                `Failed to get product details for ${productId}:`,
                error
              );
              return {
                cost,
                item: {
                  productId,
                  quantity,
                  product: null, // Return null for failed product lookups
                },
              };
            }
          })
        );

        return res.status(200).json({ ...order, items: productList });
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' });
      }
    }
  } catch (error) {
    console.error('Checkout error:', error);
    return res.status(500).json({
      error: 'An unexpected error occurred while processing your order',
    });
  }
};

export default InstrumentationMiddleware(handler);