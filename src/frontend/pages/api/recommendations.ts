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
        
        // Gracefully handle empty recommendations
        if (!productList || productList.length === 0) {
          console.warn('[Recommendations API] No recommendations returned from service');
          return res.status(200).json([]);
        }

        const recommendedProductList = await Promise.all(
          productList.slice(0, 4).map(id => ProductCatalogService.getProduct(id, currencyCode as string))
        );

        return res.status(200).json(recommendedProductList);
      } catch (error) {
        // Log error but return empty array for graceful degradation
        // Recommendations are non-critical and shouldn't break the page
        console.error('[Recommendations API] Failed to fetch recommendations:', error);
        return res.status(200).json([]);
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);
