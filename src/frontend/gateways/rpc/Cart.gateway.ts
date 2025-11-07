// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, ServiceError } from '@grpc/grpc-js';
import { Cart, CartItem, CartServiceClient, Empty } from '../../protos/demo';

const { CART_ADDR = '' } = process.env;

// Retry configuration
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY = 100; // ms
const MAX_RETRY_DELAY = 2000; // ms
const BACKOFF_MULTIPLIER = 2;

// Circuit breaker configuration
let failureCount = 0;
let lastFailureTime = 0;
const CIRCUIT_BREAKER_THRESHOLD = 5;
const CIRCUIT_BREAKER_TIMEOUT = 30000; // 30 seconds

// Create client with keepalive settings for better connection management
const client = new CartServiceClient(CART_ADDR, ChannelCredentials.createInsecure(), {
  'grpc.keepalive_time_ms': 10000,
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.http2.max_pings_without_data': 0,
});

/**
 * Check if circuit breaker is open (service unavailable)
 */
function isCircuitOpen(): boolean {
  if (failureCount >= CIRCUIT_BREAKER_THRESHOLD) {
    const timeSinceLastFailure = Date.now() - lastFailureTime;
    if (timeSinceLastFailure < CIRCUIT_BREAKER_TIMEOUT) {
      return true;
    }
    // Reset circuit breaker after timeout
    failureCount = 0;
  }
  return false;
}

/**
 * Record a failure and update circuit breaker state
 */
function recordFailure(): void {
  failureCount++;
  lastFailureTime = Date.now();
}

/**
 * Record a success and reset circuit breaker
 */
function recordSuccess(): void {
  failureCount = 0;
}

/**
 * Determine if error is retryable
 */
function isRetryableError(error: ServiceError): boolean {
  // Retry on UNAVAILABLE (14), DEADLINE_EXCEEDED (4), or connection errors
  return error.code === 14 || error.code === 4 || error.message.includes('ECONNREFUSED');
}

/**
 * Retry wrapper with exponential backoff
 */
async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  retries = MAX_RETRIES
): Promise<T> {
  // Check circuit breaker
  if (isCircuitOpen()) {
    const error = new Error('Cart service circuit breaker is open - service temporarily unavailable') as ServiceError;
    error.code = 14; // UNAVAILABLE
    throw error;
  }

  let lastError: ServiceError | undefined;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const result = await operation();
      recordSuccess();
      return result;
    } catch (error) {
      lastError = error as ServiceError;
      
      // Don't retry if error is not retryable or we're out of retries
      if (!isRetryableError(lastError) || attempt === retries) {
        recordFailure();
        throw error;
      }

      // Calculate delay with exponential backoff
      const delay = Math.min(
        INITIAL_RETRY_DELAY * Math.pow(BACKOFF_MULTIPLIER, attempt),
        MAX_RETRY_DELAY
      );

      console.warn(
        `Cart service request failed (attempt ${attempt + 1}/${retries + 1}): ${lastError.message}. Retrying in ${delay}ms...`
      );

      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  recordFailure();
  throw lastError;
}

const CartGateway = () => ({
  getCart(userId: string) {
    return retryWithBackoff<Cart>(
      () =>
        new Promise<Cart>((resolve, reject) => {
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 5); // 5 second timeout
          
          client.getCart(
            { userId },
            { deadline },
            (error, response) => (error ? reject(error) : resolve(response))
          );
        })
    );
  },
  addItem(userId: string, item: CartItem) {
    return retryWithBackoff<Empty>(
      () =>
        new Promise<Empty>((resolve, reject) => {
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 5);
          
          client.addItem(
            { userId, item },
            { deadline },
            (error, response) => (error ? reject(error) : resolve(response))
          );
        })
    );
  },
  emptyCart(userId: string) {
    return retryWithBackoff<Empty>(
      () =>
        new Promise<Empty>((resolve, reject) => {
          const deadline = new Date();
          deadline.setSeconds(deadline.getSeconds() + 5);
          
          client.emptyCart(
            { userId },
            { deadline },
            (error, response) => (error ? reject(error) : resolve(response))
          );
        })
    );
  },
});

export default CartGateway();
