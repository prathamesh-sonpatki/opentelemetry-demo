// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, CallOptions } from '@grpc/grpc-js';
import { ListRecommendationsResponse, RecommendationServiceClient } from '../../protos/demo';

const { RECOMMENDATION_ADDR = '' } = process.env;

const client = new RecommendationServiceClient(RECOMMENDATION_ADDR, ChannelCredentials.createInsecure());

// Retry configuration
const MAX_RETRY_ATTEMPTS = 3;
const INITIAL_RETRY_DELAY_MS = 100;
const CALL_TIMEOUT_MS = 10000; // 10 seconds

/**
 * Utility function to implement exponential backoff retry logic
 */
async function retryWithBackoff<T>(
  operation: (attempt: number) => Promise<T>,
  maxAttempts: number = MAX_RETRY_ATTEMPTS,
  gracefulFallback?: T
): Promise<T> {
  let lastError: Error;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation(attempt);
    } catch (error: any) {
      lastError = error;
      
      // Check if error is retryable (connection refused, unavailable, etc.)
      const isRetryable = 
        error.code === 14 || // UNAVAILABLE
        error.code === 2 ||  // UNKNOWN (often connection issues)
        error.message?.includes('ECONNREFUSED') ||
        error.message?.includes('UNAVAILABLE');
      
      if (!isRetryable || attempt === maxAttempts) {
        // If graceful fallback is provided, return it instead of throwing
        if (gracefulFallback !== undefined) {
          console.warn(
            `Recommendations service unavailable after ${attempt} attempts, ` +
            `returning fallback response. Error: ${error.message}`
          );
          return gracefulFallback;
        }
        throw error;
      }
      
      // Exponential backoff with jitter
      const delayMs = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt - 1) + Math.random() * 100;
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  
  throw lastError!;
}

const RecommendationsGateway = () => ({
  listRecommendations(userId: string, productIds: string[]) {
    // Graceful degradation: return empty recommendations on failure
    const fallback: ListRecommendationsResponse = { productIds: [] };
    
    return retryWithBackoff<ListRecommendationsResponse>(
      (attempt) => {
        return new Promise<ListRecommendationsResponse>((resolve, reject) => {
          const deadline = new Date();
          deadline.setMilliseconds(deadline.getMilliseconds() + CALL_TIMEOUT_MS);
          
          const options: CallOptions = {
            deadline,
          };
          
          client.listRecommendations(
            { userId, productIds },
            options,
            (error, response) => {
              if (error) {
                // Enrich error with retry context
                const enrichedError = new Error(
                  `Failed to get recommendations (attempt ${attempt}/${MAX_RETRY_ATTEMPTS}): ${error.message}`
                );
                (enrichedError as any).code = error.code;
                (enrichedError as any).originalError = error;
                reject(enrichedError);
              } else {
                resolve(response);
              }
            }
          );
        });
      },
      MAX_RETRY_ATTEMPTS,
      fallback // Return empty recommendations on failure instead of error
    );
  },
});

export default RecommendationsGateway();
