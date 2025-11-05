// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ServiceError } from '@grpc/grpc-js';

export interface RetryOptions {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  retryableStatusCodes?: number[];
}

const DEFAULT_RETRY_OPTIONS: Required<RetryOptions> = {
  maxRetries: 3,
  baseDelayMs: 1000,
  maxDelayMs: 10000,
  retryableStatusCodes: [14, 2], // UNAVAILABLE, UNKNOWN
};

/**
 * Utility class for implementing retry logic with exponential backoff for gRPC calls.
 * Helps handle transient connection failures gracefully.
 */
export class GrpcRetry {
  private options: Required<RetryOptions>;

  constructor(options: RetryOptions = {}) {
    this.options = { ...DEFAULT_RETRY_OPTIONS, ...options };
  }

  /**
   * Executes a gRPC call with retry logic and exponential backoff.
   * 
   * @param operation - The gRPC operation to execute
   * @param operationName - Name of the operation (for logging)
   * @returns Promise resolving to the operation result
   */
  async execute<T>(operation: () => Promise<T>, operationName: string): Promise<T> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= this.options.maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error as Error;
        
        // Check if error is retryable
        if (!this.isRetryable(error)) {
          console.error(`[GrpcRetry] Non-retryable error in ${operationName}:`, error);
          throw error;
        }

        // Don't retry on last attempt
        if (attempt === this.options.maxRetries) {
          console.error(
            `[GrpcRetry] Max retries (${this.options.maxRetries}) reached for ${operationName}`,
            error
          );
          break;
        }

        // Calculate delay with exponential backoff
        const delay = Math.min(
          this.options.baseDelayMs * Math.pow(2, attempt),
          this.options.maxDelayMs
        );

        console.warn(
          `[GrpcRetry] Retry attempt ${attempt + 1}/${this.options.maxRetries} for ${operationName} after ${delay}ms delay`
        );

        await this.sleep(delay);
      }
    }

    throw lastError || new Error(`Failed to execute ${operationName} after ${this.options.maxRetries} retries`);
  }

  /**
   * Determines if an error is retryable based on gRPC status codes.
   */
  private isRetryable(error: any): boolean {
    if (!error) return false;

    // Check for gRPC ServiceError
    const serviceError = error as ServiceError;
    if (serviceError.code !== undefined) {
      return this.options.retryableStatusCodes.includes(serviceError.code);
    }

    // Check error message for common transient patterns
    const errorMessage = error.message?.toLowerCase() || '';
    const transientPatterns = [
      'econnrefused',
      'unavailable',
      'connection refused',
      'failed to connect',
    ];

    return transientPatterns.some(pattern => errorMessage.includes(pattern));
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

export default new GrpcRetry();
