// Package centric is a small client for Centric PLM's REST API: session login and JSON GETs
// with retries.
package centric

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eemax/centric-fetch/internal/config"
)

const (
	centricDomain   = "centricsoftware.com"
	handlerPath     = "/csi-requesthandler"
	maxRetryAfter   = 5 * time.Minute
	maxErrorSnippet = 180
	userAgent       = "centric-fetch"
)

// AuthError means logging in failed; nothing else can work, so the run stops.
type AuthError struct{ Msg string }

func (e *AuthError) Error() string { return e.Msg }

// Error is a request that failed for good: a non-transient HTTP status, or a transient
// failure that outlasted every retry.
type Error struct {
	Msg    string
	Status int // 0 for transport errors
	// Transient: it failed on timeouts, 408, 429 or 5xx, so trying later may work.
	Transient bool
}

func (e *Error) Error() string { return e.Msg }

// Client talks to one Centric instance. Safe for concurrent use; the session token is shared
// and lives only in memory.
type Client struct {
	base  string
	creds config.Credentials
	cfg   config.HTTP
	http  *http.Client
	sleep func(context.Context, time.Duration) error

	mu    sync.Mutex
	token string
}

// New returns a client. transport may be nil.
func New(creds config.Credentials, cfg config.HTTP, transport http.RoundTripper) *Client {
	if transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConnsPerHost = 16
		transport = t
	}
	return &Client{
		base:  NormalizeBaseURL(creds.BaseURL),
		creds: creds,
		cfg:   cfg,
		http:  &http.Client{Timeout: cfg.Timeout, Transport: transport},
		sleep: sleepCtx,
	}
}

// SetSleep replaces the backoff sleep (tests).
func (c *Client) SetSleep(f func(context.Context, time.Duration) error) { c.sleep = f }

// URL builds an API URL, e.g. URL("v2", "styles").
func (c *Client) URL(apiVersion, path string) string {
	return c.base + "/api/" + apiVersion + "/" + strings.Trim(path, "/")
}

// NormalizeBaseURL accepts "acme", "acme.centricsoftware.com" or a full URL and returns the
// API root.
func NormalizeBaseURL(value string) string {
	raw := strings.TrimRight(strings.TrimSpace(value), "/")
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		host, path, hasPath := strings.Cut(strings.Trim(raw, "/"), "/")
		if !strings.Contains(host, ".") {
			host += "." + centricDomain
		}
		raw = "https://" + host
		if hasPath {
			raw += "/" + strings.TrimRight(path, "/")
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	host := strings.ToLower(u.Hostname())
	path := strings.TrimRight(u.Path, "/")
	if (host == centricDomain || strings.HasSuffix(host, "."+centricDomain)) &&
		(path == "" || strings.EqualFold(path, handlerPath)) {
		path = handlerPath
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	return u.Scheme + "://" + u.Host + path
}

// RetryFunc is told about each retry before its backoff.
type RetryFunc func(message string)

// GetJSON fetches url with query q and returns the body. Transport errors, 408, 429 and 5xx
// are retried; a 401 logs in again once.
func (c *Client) GetJSON(ctx context.Context, u string, q url.Values, onRetry RetryFunc) ([]byte, error) {
	full := u
	if len(q) > 0 {
		full += "?" + q.Encode()
	}
	return c.withRetries(ctx, u, onRetry, func() (*http.Response, error) {
		token, err := c.session(ctx, onRetry)
		if err != nil {
			return nil, err
		}
		resp, err := c.get(ctx, full, token)
		if err == nil && resp.StatusCode == http.StatusUnauthorized {
			drain(resp)
			c.dropToken(token)
			if token, err = c.session(ctx, onRetry); err != nil {
				return nil, err
			}
			resp, err = c.get(ctx, full, token)
		}
		return resp, err
	})
}

func (c *Client) get(ctx context.Context, u, token string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	return c.http.Do(req)
}

// withRetries sends until success, a permanent failure, or the attempts run out. The body
// is read inside the attempt, so a timeout while reading it is retried too.
func (c *Client) withRetries(ctx context.Context, u string, onRetry RetryFunc, send func() (*http.Response, error)) ([]byte, error) {
	var last string
	for attempt := 1; ; attempt++ {
		var retryAfter time.Duration
		resp, err := send()
		if err != nil {
			var auth *AuthError
			if errors.As(err, &auth) {
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			last = "transport error: " + transportMessage(err)
		} else {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch {
			case readErr != nil:
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				last = "transport error: " + transportMessage(readErr)
			case resp.StatusCode < 400:
				return body, nil
			case !transient(resp.StatusCode):
				return nil, &Error{
					Msg:    fmt.Sprintf("HTTP %d from %s: %s", resp.StatusCode, u, summarize(body)),
					Status: resp.StatusCode,
				}
			default:
				last = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, summarize(body))
				retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			}
		}
		if attempt >= c.cfg.Attempts {
			return nil, &Error{
				Msg:       fmt.Sprintf("request to %s failed after %d attempts (%s)", u, c.cfg.Attempts, last),
				Transient: true,
			}
		}
		delay := c.backoff(attempt)
		if retryAfter > delay {
			delay = retryAfter
		}
		if onRetry != nil {
			onRetry(fmt.Sprintf("%s; retry %d/%d in %s", last, attempt, c.cfg.Attempts-1, delay.Round(100*time.Millisecond)))
		}
		if err := c.sleep(ctx, delay); err != nil {
			return nil, err
		}
	}
}

// backoff is exponential with "equal jitter": half fixed, half random, so parallel workers
// that failed together don't retry together.
func (c *Client) backoff(attempt int) time.Duration {
	if c.cfg.BackoffBase <= 0 {
		return 0
	}
	d := c.cfg.BackoffMax
	if attempt-1 < 32 { // past that the shift overflows; the cap applies long before
		d = min(c.cfg.BackoffBase<<(attempt-1), c.cfg.BackoffMax)
	}
	if d <= 0 {
		d = c.cfg.BackoffMax
	}
	return d/2 + rand.N(d/2+1)
}

func (c *Client) session(ctx context.Context, onRetry RetryFunc) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" {
		return c.token, nil
	}
	token, err := c.login(ctx, onRetry)
	if err != nil {
		return "", err
	}
	c.token = token
	return token, nil
}

func (c *Client) dropToken(token string) {
	c.mu.Lock()
	if c.token == token {
		c.token = ""
	}
	c.mu.Unlock()
}

func (c *Client) login(ctx context.Context, onRetry RetryFunc) (string, error) {
	u := c.base + "/api/v2/session"
	payload, _ := json.Marshal(map[string]string{"username": c.creds.Username, "password": c.creds.Password})
	body, err := c.withRetries(ctx, u, onRetry, func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", userAgent)
		return c.http.Do(req)
	})
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", &AuthError{Msg: "login failed: " + err.Error()}
	}
	var resp struct {
		Token any `json:"token"`
	}
	if json.Unmarshal(body, &resp) != nil {
		return "", &AuthError{Msg: "login response was not valid JSON"}
	}
	token := strings.TrimSpace(fmt.Sprint(resp.Token))
	if resp.Token == nil {
		token = ""
	}
	// Centric answers e.g. "SecurityTokenURL=abc…".
	if _, after, ok := strings.Cut(token, "="); ok {
		token = strings.TrimSpace(after)
	}
	if token == "" {
		return "", &AuthError{Msg: "login response did not contain a token"}
	}
	return token, nil
}

func transient(status int) bool {
	return status == 408 || status == 429 || (status >= 500 && status <= 599)
}

func parseRetryAfter(v string) time.Duration {
	s, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil || s < 0 {
		return 0
	}
	d := time.Duration(s * float64(time.Second))
	return min(d, maxRetryAfter)
}

func transportMessage(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "Client.Timeout") {
		return "timed out"
	}
	return err.Error()
}

func summarize(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return "empty response body"
	}
	var obj map[string]any
	if json.Unmarshal([]byte(text), &obj) == nil {
		for _, key := range []string{"error", "message", "detail", "details"} {
			switch v := obj[key].(type) {
			case string, float64, bool:
				return fmt.Sprintf("%s=%v", key, v)
			}
		}
	}
	flat := []rune(strings.Join(strings.Fields(text), " "))
	if len(flat) > maxErrorSnippet {
		return string(flat[:maxErrorSnippet]) + "..."
	}
	return string(flat)
}

func drain(resp *http.Response) {
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
