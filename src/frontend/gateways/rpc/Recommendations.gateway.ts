// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, status as GrpcStatus } from '@grpc/grpc-js';
import { ListRecommendationsResponse, RecommendationServiceClient } from '../../protos/demo';

const { RECOMMENDATION_ADDR = '' } = process.env;

const client = new RecommendationServiceClient(RECOMMENDATION_ADDR, ChannelCredentials.createInsecure());

// Circuit breaker state
let circuitBreakerOpen = false;
let circuitBreakerResetTime = 0;
const CIRCUIT_BREAKER_TIMEOUT = 30000; // 30 seconds
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second
const REQUEST_TIMEOUT = 5000; // 5 seconds

// Helper function to wait
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const RecommendationsGateway = () => ({
  async listRecommendations(userId: string, productIds: string[]): Promise<ListRecommendationsResponse> {
    // Check circuit breaker
    if (circuitBreakerOpen) {
      if (Date.now() < circuitBreakerResetTime) {
        console.warn('[RecommendationsGateway] Circuit breaker is open, returning empty recommendations');
        return { productIds: [] };
      }
      // Try to reset circuit breaker
      circuitBreakerOpen = false;
    }

    let lastError: Error | null = null;
    
    // Retry logic with exponential backoff
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const response = await new Promise<ListRecommendationsResponse>((resolve, reject) => {
          const deadline = new Date(Date.now() + REQUEST_TIMEOUT);
          
          client.listRecommendations(
            { userId, productIds },
            { deadline },
            (error, response) => {
              if (error) {
                reject(error);
              } else {
                resolve(response);
              }
            }
          );
        });
        
        // Success - return the response
        return response;
      } catch (error: any) {
        lastError = error;
        
        // Log the error with context
        console.error(
          `[RecommendationsGateway] Attempt ${attempt}/${MAX_RETRIES} failed:`,
          {
            code: error.code,
            details: error.details,
            metadata: error.metadata,
            userId,
            productCount: productIds.length,
          }
        );

        // Check if error is retryable
        const isRetryable = 
          error.code === GrpcStatus.UNAVAILABLE ||
          error.code === GrpcStatus.DEADLINE_EXCEEDED ||
          error.code === GrpcStatus.RESOURCE_EXHAUSTED ||
          error.code === GrpcStatus.UNKNOWN;

        if (!isRetryable || attempt === MAX_RETRIES) {
          // Open circuit breaker for non-retryable errors or after all retries
          if (error.code === GrpcStatus.UNAVAILABLE) {
            circuitBreakerOpen = true;
            circuitBreakerResetTime = Date.now() + CIRCUIT_BREAKER_TIMEOUT;
            console.warn(
              '[RecommendationsGateway] Opening circuit breaker due to service unavailability'
            );
          }
          break;
        }

        // Wait before retrying with exponential backoff
        if (attempt < MAX_RETRIES) {
          const delay = RETRY_DELAY * Math.pow(2, attempt - 1);
          await wait(delay);
        }
      }
    }

    // All retries failed - return empty recommendations for graceful degradation
    console.warn(
      '[RecommendationsGateway] All retries failed, returning empty recommendations:',
      lastError?.message
    );
    
    return { productIds: [] };
  },
});

export default RecommendationsGateway();
