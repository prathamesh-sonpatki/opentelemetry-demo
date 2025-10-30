// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Metadata, status } from '@grpc/grpc-js';
import { ListRecommendationsResponse, RecommendationServiceClient } from '../../protos/demo';

const { RECOMMENDATION_ADDR = '' } = process.env;

const client = new RecommendationServiceClient(RECOMMENDATION_ADDR, ChannelCredentials.createInsecure());

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_BACKOFF_MS = 1000;
const GRPC_DEADLINE_MS = 5000;

/**
 * Sleep utility for retry backoff
 */
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Retry wrapper with exponential backoff for gRPC calls
 */
async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  retries: number = MAX_RETRIES,
  backoff: number = INITIAL_BACKOFF_MS
): Promise<T> {
  try {
    return await operation();
  } catch (error: any) {
    // Only retry on specific transient errors
    const isRetryable = 
      error.code === status.UNAVAILABLE ||
      error.code === status.DEADLINE_EXCEEDED ||
      error.code === status.UNKNOWN;

    if (retries > 0 && isRetryable) {
      console.warn(
        `gRPC call failed with code ${error.code}, retrying in ${backoff}ms. Retries left: ${retries}`,
        { error: error.message }
      );
      await sleep(backoff);
      return retryWithBackoff(operation, retries - 1, backoff * 2);
    }

    // Log non-retryable or exhausted retry errors
    console.error('gRPC call failed after all retries', {
      code: error.code,
      message: error.message,
      details: error.details,
    });
    throw error;
  }
}

const RecommendationsGateway = () => ({
  async listRecommendations(userId: string, productIds: string[]): Promise<ListRecommendationsResponse> {
    return retryWithBackoff(
      () =>
        new Promise<ListRecommendationsResponse>((resolve, reject) => {
          // Set deadline for the gRPC call
          const metadata = new Metadata();
          const deadline = new Date();
          deadline.setMilliseconds(deadline.getMilliseconds() + GRPC_DEADLINE_MS);

          client.listRecommendations(
            { userId, productIds },
            metadata,
            { deadline },
            (error, response) => {
              if (error) {
                // Add context to error for better debugging
                console.error('Recommendation service call failed', {
                  userId,
                  productCount: productIds.length,
                  error: error.message,
                  code: error.code,
                });
                reject(error);
              } else {
                resolve(response);
              }
            }
          );
        })
    );
  },
});

export default RecommendationsGateway();
