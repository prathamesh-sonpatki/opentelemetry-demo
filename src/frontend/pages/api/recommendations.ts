// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import RecommendationsGateway from '../../gateways/rpc/Recommendations.gateway';
import { Empty, ListRecommendationsResponse } from '../../protos/demo';

type TResponse = ListRecommendationsResponse | Empty;

const handler = async (
  { method, query: { productIds = [], userId = '', sessionId = '', currencyCode = '' } }: NextApiRequest,
  res: NextApiResponse<TResponse>
) => {
  switch (method) {
    case 'GET': {
      try {
        const recommendations = await RecommendationsGateway.listRecommendations(
          sessionId as string,
          typeof productIds === 'string' ? [productIds] : (productIds as string[])
        );
        
        return res.status(200).json(recommendations);
      } catch (error: any) {
        // Log error for monitoring and debugging
        console.error('[RecommendationsAPI] Failed to fetch recommendations:', {
          error: error.message,
          code: error.code,
          userId,
          sessionId,
          productIds,
        });
        
        // Return empty recommendations instead of throwing 500 error
        // This provides graceful degradation for users
        return res.status(200).json({ productIds: [] });
      }
    }

    default: {
      return res.status(405).json({});
    }
  }
};

export default handler;
