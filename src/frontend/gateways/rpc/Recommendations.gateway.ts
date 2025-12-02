// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions } from '@grpc/grpc-js';
import { ListRecommendationsResponse, RecommendationServiceClient } from '../../protos/demo';

const { RECOMMENDATION_ADDR = '' } = process.env;

// Configure gRPC channel options for better resilience
const channelOptions: ChannelOptions = {
  'grpc.keepalive_time_ms': 30000,
  'grpc.keepalive_timeout_ms': 10000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.http2.min_time_between_pings_ms': 10000,
  'grpc.http2.max_pings_without_data': 0,
  'grpc.initial_reconnect_backoff_ms': 1000,
  'grpc.max_reconnect_backoff_ms': 30000,
  'grpc.enable_retries': 1,
};

const client = new RecommendationServiceClient(
  RECOMMENDATION_ADDR,
  ChannelCredentials.createInsecure(),
  channelOptions
);

const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 1000;
const GRPC_DEADLINE_MS = 5000;

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function withRetry<T>(
  operation: () => Promise<T>,
  operationName: string,
  retries = MAX_RETRIES
): Promise<T> {
  let lastError: Error | undefined;
  
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      console.error(`[RecommendationsGateway] ${operationName} failed (attempt ${attempt + 1}/${retries + 1}):`, {
        error: error.message,
        code: error.code,
        details: error.details,
      });

      if (error.code === 3 || error.code === 5 || error.code === 7) {
        console.error(`[RecommendationsGateway] Non-retryable error for ${operationName}:`, error.code);
        throw error;
      }

      if (attempt < retries) {
        const delayMs = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt);
        console.log(`[RecommendationsGateway] Retrying ${operationName} in ${delayMs}ms...`);
        await sleep(delayMs);
      }
    }
  }

  console.error(`[RecommendationsGateway] ${operationName} failed after ${retries + 1} attempts`);
  throw lastError;
}

const RecommendationsGateway = () => ({
  async listRecommendations(userId: string, productIds: string[]): Promise<ListRecommendationsResponse> {
    try {
      return await withRetry(
        () =>
          new Promise<ListRecommendationsResponse>((resolve, reject) => {
            const deadline = new Date(Date.now() + GRPC_DEADLINE_MS);
            client.listRecommendations(
              { userId, productIds },
              { deadline },
              (error, response) => (error ? reject(error) : resolve(response))
            );
          }),
        `listRecommendations(userId=${userId})`
      );
    } catch (error: any) {
      // Graceful degradation: return empty recommendations instead of crashing
      console.error('[RecommendationsGateway] listRecommendations - returning empty list due to error:', error.message);
      return { productIds: [] };
    }
  },
});

export default RecommendationsGateway();
