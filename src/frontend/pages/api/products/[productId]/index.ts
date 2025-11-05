// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../../../utils/telemetry/InstrumentationMiddleware';
import { Empty, Product } from '../../../../protos/demo';
import ProductCatalogService from '../../../../services/ProductCatalog.service';

type TResponse = Product | Empty | { error: string; message: string };

const handler = async ({ method, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  switch (method) {
    case 'GET': {
      try {
        const { productId = '', currencyCode = '' } = query;
        
        if (!productId) {
          return res.status(400).json({
            error: 'BAD_REQUEST',
            message: 'Product ID is required',
          });
        }

        const product = await ProductCatalogService.getProduct(productId as string, currencyCode as string);

        return res.status(200).json(product);
      } catch (error: any) {
        console.error('[API /api/products/[productId]] Error fetching product:', {
          error: error?.message,
          code: error?.code,
          serviceName: error?.serviceName,
          productId: query.productId,
        });

        // Return appropriate error response based on error type
        if (error?.code === 'NOT_FOUND' || error?.message?.includes('not found')) {
          return res.status(404).json({
            error: 'NOT_FOUND',
            message: 'Product not found',
          });
        }

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
