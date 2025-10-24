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
      try {
        const { currencyCode = '' } = query;
        const orderData = body as PlaceOrderRequest;
        
        // Validate products exist before placing order
        const productIds = orderData.order?.items?.map(item => item.item?.productId) || [];
        const validProducts = await Promise.all(
          productIds.map(async (id) => {
            try {
              await ProductCatalogService.getProduct(id, currencyCode as string);
              return true;
            } catch {
              return false;
            }
          })
        );

        if (validProducts.some(valid => !valid)) {
          return res.status(400).json({ error: 'One or more products not found' });
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
        return res.status(500).json({ error: 'Failed to process checkout' });
      }
    }

    default: {
      return res.status(405).send('Method not allowed');
    }
  }
};

export default InstrumentationMiddleware(handler);
