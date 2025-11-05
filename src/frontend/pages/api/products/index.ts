// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../../utils/telemetry/InstrumentationMiddleware';
import { Empty, Product } from '../../../protos/demo';
import ProductCatalogService from '../../../services/ProductCatalog.service';

type TResponse = Product[] | Empty | { error: string; message: string };

const handler = async ({ method, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  switch (method) {
    case 'GET': {
      try {
        const { currencyCode = '' } = query;
        const productList = await ProductCatalogService.listProducts(currencyCode as string);

        return res.status(200).json(productList);
      } catch (error: any) {
        console.error('[API /api/products] Error fetching products:', {
          error: error?.message,
          code: error?.code,
          serviceName: error?.serviceName,
        });

        // Return graceful error response
        return res.status(503).json({
          error: 'SERVICE_UNAVAILABLE',
          message: 'Product catalog service is temporarily unavailable. Please try again later.',
        });
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);
