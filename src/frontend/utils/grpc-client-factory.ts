// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ChannelOptions, Client, ServiceError } from '@grpc/grpc-js';

/**
 * Configuration for gRPC client creation with resilience features
 */
interface GrpcClientConfig {
  address: string;
  maxRetries?: number;
  retryDelay?: number;
  timeout?: number;
}

/**
 * Default configuration values
 */
const DEFAULT_CONFIG = {
  maxRetries: 3,
  retryDelay: 1000, // 1 second
  timeout: 10000, // 10 seconds
};

/**
 * Channel options for improved connection reliability
 */
const getChannelOptions = (timeout: number): ChannelOptions => ({
  'grpc.keepalive_time_ms': 30000, // Send keepalive ping every 30 seconds
  'grpc.keepalive_timeout_ms': 10000, // Wait 10 seconds for keepalive response
  'grpc.keepalive_permit_without_calls': 1, // Allow keepalive pings when no calls
  'grpc.http2.min_time_between_pings_ms': 10000, // Minimum time between pings
  'grpc.http2.max_pings_without_data': 0, // No limit on pings without data
  'grpc.enable_retries': 1, // Enable retries
  'grpc.max_receive_message_length': 4 * 1024 * 1024, // 4MB max message size
  'grpc.initial_reconnect_backoff_ms': 1000, // 1 second initial backoff
  'grpc.max_reconnect_backoff_ms': 10000, // 10 seconds max backoff
});

/**
 * Creates a gRPC client with retry logic and improved error handling
 * 
 * @param ClientConstructor - The gRPC client class constructor
 * @param config - Configuration for the client
 * @returns Configured gRPC client instance
 */
export function createGrpcClient<T extends Client>(
  ClientConstructor: new (address: string, credentials: ChannelCredentials, options?: ChannelOptions) => T,
  config: GrpcClientConfig
): T {
  const { address, timeout = DEFAULT_CONFIG.timeout } = config;

  if (!address) {
    throw new Error('gRPC service address is required but not provided');
  }

  const credentials = ChannelCredentials.createInsecure();
  const options = getChannelOptions(timeout);

  return new ClientConstructor(address, credentials, options);
}

/**
 * Wraps a gRPC call with retry logic and timeout handling
 * 
 * @param callFn - The gRPC call function to execute
 * @param config - Retry configuration
 * @returns Promise that resolves with the response or rejects with an error
 */
export async function withRetry<T>(
  callFn: () => Promise<T>,
  config: Partial<GrpcClientConfig> = {}
): Promise<T> {
  const { maxRetries = DEFAULT_CONFIG.maxRetries, retryDelay = DEFAULT_CONFIG.retryDelay } = config;
  let lastError: Error;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await callFn();
    } catch (error) {
      lastError = error as Error;
      const serviceError = error as ServiceError;

      // Check if error is retryable (connection errors, unavailable, deadline exceeded)
      const isRetryable = 
        serviceError.code === 14 || // UNAVAILABLE
        serviceError.code === 4 ||  // DEADLINE_EXCEEDED
        serviceError.code === 8;    // RESOURCE_EXHAUSTED

      if (!isRetryable || attempt === maxRetries) {
        // Don't retry on non-retryable errors or if max retries reached
        throw error;
      }

      // Calculate exponential backoff delay
      const backoffDelay = retryDelay * Math.pow(2, attempt);
      console.warn(
        `gRPC call failed (attempt ${attempt + 1}/${maxRetries + 1}): ${serviceError.message}. ` +
        `Retrying in ${backoffDelay}ms...`
      );

      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, backoffDelay));
    }
  }

  throw lastError!;
}

/**
 * Helper to create a promisified gRPC method with retry logic
 * 
 * @param client - The gRPC client instance
 * @param methodName - Name of the method to call
 * @param request - Request object
 * @param config - Retry configuration
 * @returns Promise that resolves with the response
 */
export function callWithRetry<TRequest, TResponse>(
  client: any,
  methodName: string,
  request: TRequest,
  config: Partial<GrpcClientConfig> = {}
): Promise<TResponse> {
  const callFn = () => 
    new Promise<TResponse>((resolve, reject) => {
      client[methodName](request, (error: ServiceError | null, response: TResponse) => {
        if (error) {
          reject(error);
        } else {
          resolve(response);
        }
      });
    });

  return withRetry(callFn, config);
}
