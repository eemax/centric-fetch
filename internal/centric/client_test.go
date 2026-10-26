package centric

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/eemax/centric-fetch/internal/config"
)

func TestNormalizeBaseURL(t *testing.T) {
	for in, want := range map[string]string{
		"acme":                              "https://acme.centricsoftware.com/csi-requesthandler",
		"acme.centricsoftware.com":          "https://acme.centricsoftware.com/csi-requesthandler",
		"https://acme.centricsoftware.com/": "https://acme.centricsoftware.com/csi-requesthandler",
		"https://acme.centricsoftware.com/csi-requesthandler/": "https://acme.centricsoftware.com/csi-requesthandler",
		"http://127.0.0.1:8080":                                "http://127.0.0.1:8080",
		"plm.example.com/api-root":                             "https://plm.example.com/api-root",
	} {
		if got := NormalizeBaseURL(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/session" {
			w.Write([]byte(`{"token":"t"}`))
			return
		}
		calls++
		http.Error(w, `{"message":"no such thing"}`, 404)
	}))
	defer srv.Close()
	c := New(config.Credentials{BaseURL: srv.URL, Username: "u", Password: "p"}, config.HTTP{Timeout: time.Second, Attempts: 5}, nil)
	_, err := c.GetJSON(context.Background(), c.URL("v2", "x"), nil, nil)
	var ce *Error
	if !errors.As(err, &ce) || ce.Status != 404 || ce.Transient || calls != 1 || ce.Msg != "HTTP 404 from "+srv.URL+"/api/v2/x: message=no such thing" {
		t.Fatalf("err %v, calls %d", err, calls)
	}
}

func TestTimeoutIsRetriedThenTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/session" {
			w.Write([]byte(`{"token":"t"}`))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()
	c := New(config.Credentials{BaseURL: srv.URL, Username: "u", Password: "p"}, config.HTTP{Timeout: 50 * time.Millisecond, Attempts: 2}, nil)
	var retries []string
	_, err := c.GetJSON(context.Background(), c.URL("v2", "x"), nil, func(m string) { retries = append(retries, m) })
	var ce *Error
	if !errors.As(err, &ce) || !ce.Transient || len(retries) != 1 || retries[0] != "transport error: timed out; retry 1/1 in 0s" {
		t.Fatalf("err %v retries %q", err, retries)
	}
}

func TestBackoff(t *testing.T) {
	c := New(config.Credentials{}, config.HTTP{BackoffBase: 0, BackoffMax: time.Minute}, nil)
	if d := c.backoff(3); d != 0 {
		t.Fatalf("base 0: %s", d)
	}
	c = New(config.Credentials{}, config.HTTP{BackoffBase: 10 * time.Second, BackoffMax: 2 * time.Minute}, nil)
	for attempt, want := range map[int]time.Duration{1: 10 * time.Second, 2: 20 * time.Second, 4: 80 * time.Second, 5: 2 * time.Minute, 40: 2 * time.Minute} {
		if d := c.backoff(attempt); d < want/2 || d > want {
			t.Errorf("attempt %d: %s, want %s..%s", attempt, d, want/2, want)
		}
	}
}

func TestSummarizeKeepsRunesWhole(t *testing.T) {
	s := summarize([]byte(strings.Repeat("é", 300)))
	if !utf8.ValidString(s) || !strings.HasSuffix(s, "...") {
		t.Fatalf("%q", s)
	}
}
