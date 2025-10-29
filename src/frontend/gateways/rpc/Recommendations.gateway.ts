// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ListRecommendationsResponse, RecommendationServiceClient } from '../../protos/demo';
import { createGrpcClient, callWithRetry } from '../../utils/grpc-client-factory';

const { RECOMMENDATION_ADDR = '' } = process.env;

const client = createGrpcClient(RecommendationServiceClient, {
  address: RECOMMENDATION_ADDR,
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 10000,
});

const RecommendationsGateway = () => ({
  listRecommendations(userId: string, productIds: string[]) {
    return callWithRetry<{ userId: string; productIds: string[] }, ListRecommendationsResponse>(
      client,
      'listRecommendations',
      { userId, productIds },
      { maxRetries: 3, retryDelay: 1000 }
    );
  },
});

export default RecommendationsGateway();
