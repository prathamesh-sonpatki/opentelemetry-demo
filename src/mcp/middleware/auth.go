package middleware

import (
    "context"
    "net/http"
    "sync"
    "time"
)

type AuthMiddleware struct {
    tokenMutex     sync.RWMutex
    currentToken   string
    tokenExpiry    time.Time
    refreshToken   string
    tokenRefresher func(context.Context, string) (string, time.Time, error)
}

func NewAuthMiddleware(refreshToken string, tokenRefresher func(context.Context, string) (string, time.Time, error)) *AuthMiddleware {
    return &AuthMiddleware{
        refreshToken:   refreshToken,
        tokenRefresher: tokenRefresher,
    }
}

func (am *AuthMiddleware) ensureValidToken(ctx context.Context) error {
    am.tokenMutex.RLock()
    if am.currentToken != "" && time.Now().Before(am.tokenExpiry.Add(-30*time.Second)) {
        am.tokenMutex.RUnlock()
        return nil
    }
    am.tokenMutex.RUnlock()

    am.tokenMutex.Lock()
    defer am.tokenMutex.Unlock()

    // Double check after acquiring write lock
    if am.currentToken != "" && time.Now().Before(am.tokenExpiry.Add(-30*time.Second)) {
        return nil
    }

    token, expiry, err := am.tokenRefresher(ctx, am.refreshToken)
    if err != nil {
        return err
    }

    am.currentToken = token
    am.tokenExpiry = expiry
    return nil
}

func (am *AuthMiddleware) Middleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if err := am.ensureValidToken(r.Context()); err != nil {
            http.Error(w, "Failed to refresh auth token", http.StatusInternalServerError)
            return
        }

        am.tokenMutex.RLock()
        r.Header.Set("Authorization", "Bearer "+am.currentToken)
        am.tokenMutex.RUnlock()

        next.ServeHTTP(w, r)
    })
}