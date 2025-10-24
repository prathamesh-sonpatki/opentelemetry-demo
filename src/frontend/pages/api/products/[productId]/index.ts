// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../../../utils/telemetry/InstrumentationMiddleware';
import { Empty, Product } from '../../../../protos/demo';
import ProductCatalogService from '../../../../services/ProductCatalog.service';

type TResponse = Product | Empty;

const handler = async ({ method, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  try {
    switch (method) {
      case 'GET': {
        const { productId = '', currencyCode = '' } = query;
        const product = await ProductCatalogService.getProduct(productId as string, currencyCode as string);
        return res.status(200).json(product);
      }

      default: {
        return res.status(405).send('');
      }
    }
  } catch (error) {
    console.error('Product API error:', error);
    return res.status(500).json({
      id: 'error',
      name: 'Error',
      description: 'An error occurred while fetching the product. Please try again later.',
      picture: '/img/products/error.jpg',
      categories: [],
      priceUsd: { currencyCode: 'USD', units: 0, nanos: 0 }
    });
  }
};

export default InstrumentationMiddleware(handler);
