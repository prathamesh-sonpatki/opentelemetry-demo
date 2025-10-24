// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../../../utils/telemetry/InstrumentationMiddleware';
import { Empty, Product } from '../../../../protos/demo';
import ProductCatalogService from '../../../../services/ProductCatalog.service';

type TResponse = Product | Empty | { error: string };

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
    if (error instanceof Error && error.message.includes('temporarily unavailable')) {
      return res.status(503).json({ error: error.message });
    }
    // Handle other errors
    return res.status(500).json({ error: 'Internal server error' });
  }
};

export default InstrumentationMiddleware(handler);
