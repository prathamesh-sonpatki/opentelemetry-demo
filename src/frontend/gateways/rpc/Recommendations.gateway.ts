// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { ListRecommendationsResponse, RecommendationServiceClient } from '../../protos/demo';
import grpcRetry from '../../utils/grpc/GrpcRetry';

const { RECOMMENDATION_ADDR = '' } = process.env;

const client = new RecommendationServiceClient(RECOMMENDATION_ADDR, ChannelCredentials.createInsecure());

const RecommendationsGateway = () => ({
  listRecommendations(userId: string, productIds: string[]) {
    const operation = () =>
      new Promise<ListRecommendationsResponse>((resolve, reject) =>
        client.listRecommendations({ userId, productIds }, (error, response) =>
          error ? reject(error) : resolve(response)
        )
      );

    // Wrap with retry logic for resilience against transient connection failures
    return grpcRetry.execute(operation, 'Recommendations.listRecommendations');
  },
});

export default RecommendationsGateway();
