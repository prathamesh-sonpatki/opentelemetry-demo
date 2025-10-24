// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials } from '@grpc/grpc-js';
import { AdResponse, AdServiceClient } from '../../protos/demo';

const { AD_ADDR = '' } = process.env;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

const client = new AdServiceClient(AD_ADDR, ChannelCredentials.createInsecure());

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const AdGateway = () => ({
  async listAds(contextKeys: string[]) {
    let lastError;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        return await new Promise<AdResponse>((resolve, reject) =>
          client.getAds({ contextKeys: contextKeys }, (error, response) => {
            if (error) {
              if (error.code === 14) { // UNAVAILABLE
                reject(error);
              } else {
                reject(error);
              }
            } else {
              resolve(response);
            }
          })
        );
      } catch (error) {
        lastError = error;
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt); // Exponential backoff
          continue;
        }
      }
    }
    
    // If all retries failed, return empty ad list instead of failing
    console.error(`Failed to fetch ads after ${MAX_RETRIES} attempts:`, lastError);
    return { ads: [] };
  },
});

export default AdGateway();