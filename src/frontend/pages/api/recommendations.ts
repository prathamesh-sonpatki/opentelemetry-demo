// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';
import RecommendationsGateway from '../../gateways/rpc/Recommendations.gateway';
import { Empty, Product } from '../../protos/demo';
import ProductCatalogService from '../../services/ProductCatalog.service';

type TResponse = Product[] | Empty;

const handler = async ({ method, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  switch (method) {
    case 'GET': {
      const { productIds = [], sessionId = '', currencyCode = '' } = query;
      
      try {
        const { productIds: productList } = await RecommendationsGateway.listRecommendations(
          sessionId as string,
          productIds as string[]
        );
        
        const recommendedProductList = await Promise.all(
          productList.slice(0, 4).map(id => ProductCatalogService.getProduct(id, currencyCode as string))
        );

        return res.status(200).json(recommendedProductList);
      } catch (error: any) {
        // Log error for debugging but don't expose internal details
        console.error('Recommendation service error:', error.message);
        
        // Check if it's a gRPC connection error
        if (error.message?.includes('UNAVAILABLE') || 
            error.message?.includes('ECONNREFUSED') ||
            error.message?.includes('Socket closed')) {
          // Return empty recommendations gracefully instead of 500 error
          // This allows the page to render without recommendations
          return res.status(200).json([]);
        }
        
        // For other errors, return 503 Service Unavailable
        return res.status(503).json([]);
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);
