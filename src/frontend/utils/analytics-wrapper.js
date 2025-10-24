// LinkedIn Analytics wrapper with error handling and retry logic
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // ms

class AnalyticsWrapper {
  static async trackEvent(eventData) {
    let retries = 0;
    
    while (retries < MAX_RETRIES) {
      try {
        const response = await fetch('https://px.ads.linkedin.com/wa/', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(eventData),
        });
        
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        return response;
      } catch (error) {
        retries++;
        console.warn(`LinkedIn analytics request failed (attempt ${retries}/${MAX_RETRIES}):`, error);
        
        if (retries === MAX_RETRIES) {
          // Log to monitoring system but don't throw
          console.error('LinkedIn analytics failed after max retries:', error);
          return null;
        }
        
        // Exponential backoff
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, retries - 1)));
      }
    }
  }
}

export default AnalyticsWrapper;