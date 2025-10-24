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
    switch (method) {
      case 'POST': {
        const { currencyCode = '' } = query;
        if (!currencyCode) {
          return res.status(400).json({ error: 'Currency code is required' });
        }

        const orderData = body as PlaceOrderRequest;
        if (!orderData || !orderData.userId || !orderData.userCurrency || !orderData.address) {
          return res.status(400).json({ error: 'Invalid order data' });
        }

        try {
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
          console.error('Checkout service error:', error);
          return res.status(503).json({
            error: 'Unable to process checkout. Please try again later.',
          });
        }
      }

      default: {
        return res.status(405).json({ error: 'Method not allowed' });
      }
    }
  } catch (error) {
    console.error('Unexpected error in checkout endpoint:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
};

export default InstrumentationMiddleware(handler);