// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import InstrumentationMiddleware from '../../utils/telemetry/InstrumentationMiddleware';
import AdGateway from '../../gateways/rpc/Ad.gateway';
import { Ad, Empty } from '../../protos/demo';

type TResponse = Ad[] | Empty;

const handler = async ({ method, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  switch (method) {
    case 'GET': {
      try {
        const { contextKeys = [] } = query;
        const { ads: adList } = await AdGateway.listAds(
          Array.isArray(contextKeys) ? contextKeys : contextKeys.split(',')
        );

        return res.status(200).json(adList);
      } catch (error: any) {
        // Log error for observability
        console.error('Failed to fetch ads:', error.message);
        
        // Return empty array instead of 500 error - graceful degradation
        // The page can still load without ads
        return res.status(200).json([]);
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default InstrumentationMiddleware(handler);
