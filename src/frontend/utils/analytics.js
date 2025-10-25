// Analytics request wrapper with retry logic
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

export async function sendAnalyticsRequest(url, options) {
  let lastError;
  
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, {
        ...options,
        // Add timeout to prevent hanging requests
        signal: AbortSignal.timeout(5000)
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      return response;
    } catch (error) {
      lastError = error;
      console.warn(`Analytics request attempt ${attempt} failed:`, error);
      
      if (attempt < MAX_RETRIES) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * attempt));
      }
    }
  }
  
  // Log the final failure for monitoring
  console.error('Analytics request failed after all retries:', lastError);
  // Return a resolved promise to prevent UI errors
  return Promise.resolve();
}