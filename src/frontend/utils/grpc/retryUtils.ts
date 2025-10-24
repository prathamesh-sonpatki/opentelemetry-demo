// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { status } from '@grpc/grpc-js';

const RETRY_CODES = [
  status.UNAVAILABLE,
  status.DEADLINE_EXCEEDED,
  status.RESOURCE_EXHAUSTED
];

const DEFAULT_RETRY_OPTIONS = {
  maxRetries: 3,
  initialDelayMs: 100,
  maxDelayMs: 1000,
  backoffMultiplier: 1.5
};

export interface RetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
}

export async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const {
    maxRetries,
    initialDelayMs,
    maxDelayMs,
    backoffMultiplier
  } = { ...DEFAULT_RETRY_OPTIONS, ...options };

  let lastError: Error | null = null;
  let currentDelay = initialDelayMs;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Only retry on specific gRPC error codes
      if (!RETRY_CODES.includes(error?.code)) {
        throw error;
      }

      if (attempt === maxRetries) {
        break;
      }

      await new Promise(resolve => setTimeout(resolve, currentDelay));
      currentDelay = Math.min(currentDelay * backoffMultiplier, maxDelayMs);
    }
  }

  throw lastError;
}