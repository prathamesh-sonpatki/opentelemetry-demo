// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ServiceError, status as GrpcStatus } from '@grpc/grpc-js';

interface RetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
  retryableStatuses?: number[];
}

interface CircuitBreakerOptions {
  failureThreshold?: number;
  resetTimeoutMs?: number;
}

enum CircuitState {
  CLOSED,
  OPEN,
  HALF_OPEN,
}

class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private lastFailureTime: number = 0;
  private failureThreshold: number;
  private resetTimeoutMs: number;

  constructor(options: CircuitBreakerOptions = {}) {
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeoutMs = options.resetTimeoutMs || 60000; // 1 minute
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === CircuitState.OPEN) {
      const timeSinceLastFailure = Date.now() - this.lastFailureTime;
      if (timeSinceLastFailure > this.resetTimeoutMs) {
        console.log('[CircuitBreaker] Attempting to close circuit (half-open state)');
        this.state = CircuitState.HALF_OPEN;
      } else {
        throw new Error(
          `Circuit breaker is OPEN. Service unavailable. Retry after ${Math.ceil(
            (this.resetTimeoutMs - timeSinceLastFailure) / 1000
          )} seconds.`
        );
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  private onSuccess() {
    if (this.state === CircuitState.HALF_OPEN) {
      console.log('[CircuitBreaker] Circuit closed after successful call');
    }
    this.failureCount = 0;
    this.state = CircuitState.CLOSED;
  }

  private onFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.failureCount >= this.failureThreshold && this.state !== CircuitState.OPEN) {
      console.error(
        `[CircuitBreaker] Circuit opened after ${this.failureCount} failures. Service calls will be blocked.`
      );
      this.state = CircuitState.OPEN;
    }
  }

  getState(): string {
    return CircuitState[this.state];
  }
}

class GrpcResilienceHelper {
  private static circuitBreakers = new Map<string, CircuitBreaker>();

  private static getCircuitBreaker(serviceName: string): CircuitBreaker {
    if (!this.circuitBreakers.has(serviceName)) {
      this.circuitBreakers.set(
        serviceName,
        new CircuitBreaker({
          failureThreshold: 5,
          resetTimeoutMs: 60000,
        })
      );
    }
    return this.circuitBreakers.get(serviceName)!;
  }

  static async executeWithRetry<T>(
    fn: () => Promise<T>,
    serviceName: string,
    options: RetryOptions = {}
  ): Promise<T> {
    const {
      maxRetries = 3,
      initialDelayMs = 100,
      maxDelayMs = 5000,
      backoffMultiplier = 2,
      retryableStatuses = [
        GrpcStatus.UNAVAILABLE,
        GrpcStatus.DEADLINE_EXCEEDED,
        GrpcStatus.RESOURCE_EXHAUSTED,
        GrpcStatus.ABORTED,
      ],
    } = options;

    const circuitBreaker = this.getCircuitBreaker(serviceName);

    return circuitBreaker.execute(async () => {
      let lastError: Error | ServiceError | null = null;
      let delay = initialDelayMs;

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          if (attempt > 0) {
            console.log(
              `[GrpcResilience] Retry attempt ${attempt}/${maxRetries} for ${serviceName} after ${delay}ms delay`
            );
            await this.sleep(delay);
            delay = Math.min(delay * backoffMultiplier, maxDelayMs);
          }

          return await fn();
        } catch (error: any) {
          lastError = error;

          // Check if error is retryable
          const isGrpcError = error && typeof error.code === 'number';
          const isRetryable = isGrpcError && retryableStatuses.includes(error.code);

          console.error(
            `[GrpcResilience] Attempt ${attempt + 1}/${maxRetries + 1} failed for ${serviceName}:`,
            {
              code: error?.code,
              message: error?.message,
              isRetryable,
              circuitState: circuitBreaker.getState(),
            }
          );

          // Don't retry if error is not retryable or we've exhausted retries
          if (!isRetryable || attempt === maxRetries) {
            throw this.enhanceError(error, serviceName, attempt + 1);
          }
        }
      }

      throw lastError || new Error(`Unknown error occurred for ${serviceName}`);
    });
  }

  private static sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private static enhanceError(error: any, serviceName: string, attempts: number): Error {
    const enhancedError = new Error(
      `${serviceName} service error after ${attempts} attempts: ${error?.message || 'Unknown error'}`
    );
    (enhancedError as any).originalError = error;
    (enhancedError as any).code = error?.code || 'UNKNOWN';
    (enhancedError as any).serviceName = serviceName;
    (enhancedError as any).attempts = attempts;
    return enhancedError;
  }
}

export default GrpcResilienceHelper;
