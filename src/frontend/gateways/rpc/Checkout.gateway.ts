// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError, status as GrpcStatus } from '@grpc/grpc-js';
import {
  CheckoutServiceClient,
  PlaceOrderRequest,
  PlaceOrderResponse,
} from '../../protos/demo';

const { CHECKOUT_ADDR = '' } = process.env;

// Configuration for retry logic
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 2000;
const REQUEST_TIMEOUT_MS = 5000;

// Create client with keepalive settings to prevent connection drops
const client = new CheckoutServiceClient(
  CHECKOUT_ADDR,
  ChannelCredentials.createInsecure(),
  {
    'grpc.keepalive_time_ms': 30000,
    'grpc.keepalive_timeout_ms': 10000,
    'grpc.keepalive_permit_without_calls': 1,
    'grpc.http2.max_pings_without_data': 0,
    'grpc.http2.min_time_between_pings_ms': 10000,
  }
);

/**
 * Determines if an error is retryable
 */
function isRetryableError(error: ServiceError): boolean {
  const retryableStatuses = [
    GrpcStatus.UNAVAILABLE,
    GrpcStatus.DEADLINE_EXCEEDED,
    GrpcStatus.RESOURCE_EXHAUSTED,
    GrpcStatus.ABORTED,
  ];
  
  // Check for specific error codes
  if (error.code && retryableStatuses.includes(error.code)) {
    return true;
  }
  
  // Check for specific error messages indicating transient issues
  const errorMessage = error.message?.toLowerCase() || '';
  const retryableMessages = ['econnreset', 'econnrefused', 'etimedout', 'eof', 'socket hang up'];
  
  return retryableMessages.some(msg => errorMessage.includes(msg));
}

/**
 * Calculate exponential backoff delay with jitter
 */
function getRetryDelay(attempt: number): number {
  const exponentialDelay = Math.min(
    INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt),
    MAX_RETRY_DELAY_MS
  );
  
  // Add jitter to prevent thundering herd
  const jitter = Math.random() * exponentialDelay * 0.3;
  return exponentialDelay + jitter;
}

/**
 * Sleep for specified milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Enhanced promise wrapper with retry logic and timeout
 */
async function callWithRetry<TRequest, TResponse>(
  method: (request: TRequest, callback: (error: ServiceError | null, response?: TResponse) => void) => void,
  request: TRequest,
  operationName: string
): Promise<TResponse> {
  let lastError: ServiceError | null = null;
  
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await new Promise<TResponse>((resolve, reject) => {
        // Set timeout for the request
        const timeoutId = setTimeout(() => {
          reject(new Error(`Request timeout after ${REQUEST_TIMEOUT_MS}ms for ${operationName}`));
        }, REQUEST_TIMEOUT_MS);
        
        method.call(client, request, (error: ServiceError | null, response?: TResponse) => {
          clearTimeout(timeoutId);
          
          if (error) {
            // Enhance error with context
            const enhancedError = error as ServiceError & { attempt?: number; operation?: string };
            enhancedError.attempt = attempt + 1;
            enhancedError.operation = operationName;
            reject(enhancedError);
          } else {
            resolve(response as TResponse);
          }
        });
      });
      
      // Success - return response
      return response;
      
    } catch (error) {
      lastError = error as ServiceError;
      
      // Don't retry if this is not a retryable error or if we've exhausted retries
      if (!isRetryableError(lastError) || attempt === MAX_RETRIES) {
        console.error(`[CheckoutGateway] ${operationName} failed after ${attempt + 1} attempts:`, {
          code: lastError.code,
          message: lastError.message,
          details: lastError.details,
        });
        throw lastError;
      }
      
      // Calculate delay and retry
      const delay = getRetryDelay(attempt);
      console.warn(`[CheckoutGateway] ${operationName} failed (attempt ${attempt + 1}/${MAX_RETRIES + 1}), retrying in ${Math.round(delay)}ms...`, {
        error: lastError.message,
        code: lastError.code,
      });
      
      await sleep(delay);
    }
  }
  
  // This should never be reached, but TypeScript needs it
  throw lastError || new Error('Unknown error in retry logic');
}

const CheckoutGateway = () => ({
  async placeOrder(request: PlaceOrderRequest): Promise<PlaceOrderResponse> {
    return callWithRetry<PlaceOrderRequest, PlaceOrderResponse>(
      client.placeOrder,
      request,
      'placeOrder'
    );
  },
});

export default CheckoutGateway();
