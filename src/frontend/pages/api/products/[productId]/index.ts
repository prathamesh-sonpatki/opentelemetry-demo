// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../../../utils/telemetry/InstrumentationMiddleware';
import { Empty, Product } from '../../../../protos/demo';
import ProductCatalogService from '../../../../services/ProductCatalog.service';

type TResponse = Product | Empty;

const handler = async ({ method, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  switch (method) {
    case 'GET': {
      const { productId = '', currencyCode = '' } = query;
      
      try {
        const product = await ProductCatalogService.getProduct(productId as string, currencyCode as string);
        return res.status(200).json(product);
      } catch (error: any) {
        console.error(`Product service error for ${productId}:`, error.message);
        
        // Check if it's a gRPC connection error
        if (error.message?.includes('UNAVAILABLE') || 
            error.message?.includes('ECONNREFUSED') ||
            error.message?.includes('No connection established')) {
          return res.status(503).json({
            error: 'Product catalog service temporarily unavailable',
            code: 'SERVICE_UNAVAILABLE',
            productId
          } as any);
        }
        
        // Check for product not found
        if (error.message?.includes('NOT_FOUND') || error.code === 5) {
          return res.status(404).json({
            error: 'Product not found',
            code: 'NOT_FOUND',
            productId
          } as any);
        }
        
        // Return 500 for unexpected errors
        return res.status(500).json({
          error: 'An error occurred fetching product',
          code: 'PRODUCT_ERROR',
          productId
        } as any);
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);
