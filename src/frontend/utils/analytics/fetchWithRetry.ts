// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

interface RetryConfig {
  maxRetries?: number;
  retryDelay?: number;
  shouldRetry?: (error: Error) => boolean;
}

const defaultConfig: RetryConfig = {
  maxRetries: 3,
  retryDelay: 1000,
  shouldRetry: (error: Error) => true,
};

/**
 * Wrapper for fetch calls to third-party analytics services with retry logic
 */
export async function fetchWithRetry(
  input: RequestInfo,
  init?: RequestInit,
  config: RetryConfig = {}
): Promise<Response> {
  const { maxRetries, retryDelay, shouldRetry } = { ...defaultConfig, ...config };
  let lastError: Error;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(input, {
        ...init,
        // Shorter timeout for analytics requests
        signal: AbortSignal.timeout(5000),
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      return response;
    } catch (error) {
      lastError = error as Error;
      
      // Don't retry if the error shouldn't be retried
      if (!shouldRetry(lastError)) {
        break;
      }

      // Don't wait on the last attempt
      if (attempt < maxRetries - 1) {
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }
  }

  // If all retries failed, log the error but don't throw to prevent UI errors
  console.warn(`Analytics request failed after ${maxRetries} attempts:`, lastError);
  
  // Return a mock successful response to prevent errors from propagating
  return new Response(null, { status: 200 });
}

/**
 * Helper to detect if a URL is for analytics purposes
 */
export function isAnalyticsUrl(url: string): boolean {
  const analyticsHosts = [
    'analytics.google.com',
    'www.google-analytics.com',
    'www.googletagmanager.com',
    'px.ads.linkedin.com',
    'snap.licdn.com'
  ];
  
  try {
    const urlObj = new URL(url);
    return analyticsHosts.some(host => urlObj.hostname.includes(host));
  } catch {
    return false;
  }
}