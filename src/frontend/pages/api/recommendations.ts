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
      try {
        const { productIds = [], sessionId = '', currencyCode = '' } = query;
        const { productIds: productList } = await RecommendationsGateway.listRecommendations(
          sessionId as string,
          productIds as string[]
        );

        // Handle empty product list gracefully
        if (!productList?.length) {
          return res.status(200).json([]);
        }

        try {
          const recommendedProductList = await Promise.all(
            productList.slice(0, 4).map(async (id) => {
              try {
                return await ProductCatalogService.getProduct(id, currencyCode as string);
              } catch (err) {
                console.error(`Failed to fetch product ${id}:`, err);
                return null;
              }
            })
          );

          // Filter out failed product fetches and return successful ones
          const validProducts = recommendedProductList.filter(Boolean);
          return res.status(200).json(validProducts);
        } catch (err) {
          console.error('Failed to process product recommendations:', err);
          return res.status(200).json([]); // Graceful degradation - return empty list
        }
      } catch (err) {
        console.error('Failed to get recommendations:', err);
        return res.status(200).json([]); // Graceful degradation - return empty list
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);