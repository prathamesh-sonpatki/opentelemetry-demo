// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { AdResponse, AdServiceClient } from '../../protos/demo';

const { AD_ADDR = '' } = process.env;

const client = new AdServiceClient(AD_ADDR, ChannelCredentials.createInsecure());

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 100;

// Helper function to implement exponential backoff retry logic
const retryWithBackoff = async <T>(
  operation: () => Promise<T>,
  retries: number = MAX_RETRIES,
  delay: number = INITIAL_RETRY_DELAY_MS
): Promise<T> => {
  try {
    return await operation();
  } catch (error: any) {
    if (retries === 0) {
      throw error;
    }
    
    // Only retry on connection errors (UNAVAILABLE status)
    const shouldRetry = error.code === 14; // gRPC UNAVAILABLE status code
    if (!shouldRetry) {
      throw error;
    }

    console.warn(`Ad service call failed, retrying in ${delay}ms. Retries left: ${retries}`);
    await new Promise(resolve => setTimeout(resolve, delay));
    return retryWithBackoff(operation, retries - 1, delay * 2);
  }
};

const AdGateway = () => ({
  async listAds(contextKeys: string[]): Promise<AdResponse> {
    try {
      return await retryWithBackoff(
        () =>
          new Promise<AdResponse>((resolve, reject) =>
            client.getAds({ contextKeys: contextKeys }, (error, response) =>
              error ? reject(error) : resolve(response)
            )
          )
      );
    } catch (error: any) {
      // Log error for monitoring but return empty ads array as graceful degradation
      console.error('Ad service unavailable after retries:', error.message);
      
      // Return empty ads instead of throwing - this prevents the entire page from failing
      return { ads: [] };
    }
  },
});

export default AdGateway();
