// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import * as grpc from '@grpc/grpc-js';
import { Context } from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import { getTracer } from './telemetry/Instrumentation';

const INITIAL_RETRY_DELAY_MS = 1000;
const MAX_RETRY_DELAY_MS = 5000;
const MAX_RETRIES = 3;

interface RetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  shouldRetry?: (error: Error) => boolean;
}

export class GrpcClient {
  private static defaultRetryOptions: RetryOptions = {
    maxRetries: MAX_RETRIES,
    initialDelayMs: INITIAL_RETRY_DELAY_MS,
    maxDelayMs: MAX_RETRY_DELAY_MS,
    shouldRetry: (error: Error) => {
      if (error instanceof Error) {
        const grpcError = error as any;
        return grpcError.code === grpc.status.UNAVAILABLE || 
               grpcError.message?.includes('name resolver error');
      }
      return false;
    }
  };

  private static async sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private static calculateBackoff(attempt: number, initialDelay: number, maxDelay: number): number {
    const delay = Math.min(initialDelay * Math.pow(2, attempt), maxDelay);
    return delay + Math.random() * 100; // Add jitter
  }

  static async withRetry<T>(
    operation: () => Promise<T>,
    context?: Context,
    options: RetryOptions = {}
  ): Promise<T> {
    const {
      maxRetries,
      initialDelayMs,
      maxDelayMs,
      shouldRetry
    } = { ...this.defaultRetryOptions, ...options };

    let lastError: Error | null = null;
    const tracer = getTracer();

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error as Error;
        
        if (!shouldRetry(lastError) || attempt === maxRetries) {
          throw error;
        }

        // Record retry attempt in telemetry
        if (context) {
          const span = tracer.startSpan('grpc.retry', {
            attributes: {
              [SemanticAttributes.RPC_SYSTEM]: 'grpc',
              'retry.attempt': attempt + 1,
              'retry.max': maxRetries,
              'error.type': lastError.name,
              'error.message': lastError.message
            }
          });
          span.end();
        }

        const delay = this.calculateBackoff(attempt, initialDelayMs, maxDelayMs);
        await this.sleep(delay);
      }
    }

    throw lastError;
  }

  static createClient<T>(
    ClientConstructor: new (address: string, credentials: grpc.ChannelCredentials) => T,
    address: string,
    options: grpc.ChannelOptions = {}
  ): T {
    const credentials = grpc.credentials.createInsecure();
    
    // Add service config for retries
    const serviceConfig = {
      loadBalancingConfig: [{ round_robin: {} }],
      methodConfig: [{
        name: [{ service: ClientConstructor.name }],
        retryPolicy: {
          maxAttempts: 5,
          initialBackoff: '0.1s',
          maxBackoff: '5s',
          backoffMultiplier: 2,
          retryableStatusCodes: [grpc.status.UNAVAILABLE]
        }
      }]
    };

    const channelOptions: grpc.ChannelOptions = {
      'grpc.service_config': JSON.stringify(serviceConfig),
      'grpc.enable_retries': 1,
      'grpc.dns_min_time_between_resolutions_ms': 500,
      ...options
    };

    return new ClientConstructor(address, credentials, channelOptions);
  }
}