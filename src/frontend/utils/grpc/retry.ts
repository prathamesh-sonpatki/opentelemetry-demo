// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Retry utility for gRPC calls with exponential backoff
 * Addresses production exceptions: GRPC Error 14 (UNAVAILABLE) and Error 13 (INTERNAL)
 */

export interface RetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
  retryableErrors?: string[];
}

const DEFAULT_OPTIONS: Required<RetryOptions> = {
  maxRetries: 3,
  initialDelayMs: 100,
  maxDelayMs: 5000,
  backoffMultiplier: 2,
  retryableErrors: ['14', 'UNAVAILABLE', 'ECONNREFUSED', 'ETIMEDOUT'],
};

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

const isRetryableError = (error: any, retryableErrors: string[]): boolean => {
  const errorString = error?.toString() || '';
  const errorMessage = error?.message || '';
  const errorCode = error?.code?.toString() || '';
  
  return retryableErrors.some(retryable => 
    errorString.includes(retryable) || 
    errorMessage.includes(retryable) || 
    errorCode === retryable
  );
};

export async function retryGrpcCall<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  let lastError: any;
  
  for (let attempt = 0; attempt <= opts.maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      
      // Don't retry if this is the last attempt or error is not retryable
      if (attempt === opts.maxRetries || !isRetryableError(error, opts.retryableErrors)) {
        throw error;
      }
      
      // Calculate delay with exponential backoff
      const delayMs = Math.min(
        opts.initialDelayMs * Math.pow(opts.backoffMultiplier, attempt),
        opts.maxDelayMs
      );
      
      console.warn(
        `gRPC call failed (attempt ${attempt + 1}/${opts.maxRetries + 1}), ` +
        `retrying in ${delayMs}ms. Error: ${error?.message || error}`
      );
      
      await delay(delayMs);
    }
  }
  
  throw lastError;
}
