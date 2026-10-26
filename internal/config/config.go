// Package config loads centric-fetch's JSON config and credentials.
package config

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultPath is used when neither --config nor $CENTRIC_FETCH_CONFIG is set.
const DefaultPath = "/etc/centric-fetch/config.json"

// Endpoint is one Centric object type to fetch.
type Endpoint struct {
	Name       string
	Path       string
	APIVersion string
	CountPath  string
	Limit      int
}

// HTTP controls requests to Centric. Every request is retried with exponential backoff and
// jitter on transport errors, 408, 429 and 5xx.
type HTTP struct {
	Timeout     time.Duration
	Attempts    int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// Config is the loaded configuration.
type Config struct {
	Path        string
	Database    string
	EnvFile     string
	Endpoints   []Endpoint
	Concurrency int
	Overlap     time.Duration
	HTTP        HTTP
	// A failed endpoint gets one more try at the end of the run after this pause (0: none).
	EndpointRetryAfter time.Duration
}

// Credentials for Centric.
type Credentials struct {
	BaseURL  string
	Username string
	Password string
}

type fileConfig struct {
	Database           string          `json:"database"`
	EnvFile            string          `json:"env_file"`
	Endpoints          []endpointEntry `json:"endpoints"`
	Concurrency        *int            `json:"concurrency"`
	OverlapMinutes     *int            `json:"overlap_minutes"`
	EndpointRetryAfter *float64        `json:"endpoint_retry_after_seconds"`
	HTTP               struct {
		TimeoutSeconds     *float64 `json:"timeout_seconds"`
		Attempts           *int     `json:"attempts"`
		BackoffBaseSeconds *float64 `json:"backoff_base_seconds"`
		BackoffMaxSeconds  *float64 `json:"backoff_max_seconds"`
	} `json:"http"`
}

// An endpoint entry is a catalog name, or {"name": …, "limit": …} to change its page size.
type endpointEntry struct {
	Name  string `json:"name"`
	Limit int    `json:"limit"`
}

func (e *endpointEntry) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return json.Unmarshal(data, &e.Name)
	}
	type plain endpointEntry
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode((*plain)(e))
}

// Resolve picks the config path: the flag, then $CENTRIC_FETCH_CONFIG, then DefaultPath.
func Resolve(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if env := os.Getenv("CENTRIC_FETCH_CONFIG"); env != "" {
		return env
	}
	return DefaultPath
}

// Load reads and validates the config file. Relative paths in it resolve against its folder.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var raw fileConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	abs := func(p string) string {
		if p == "" || filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(dir, p)
	}
	cfg := &Config{
		Path:               path,
		Database:           abs(raw.Database),
		EnvFile:            abs(raw.EnvFile),
		Concurrency:        intOr(raw.Concurrency, 3),
		Overlap:            time.Duration(intOr(raw.OverlapMinutes, 10)) * time.Minute,
		EndpointRetryAfter: seconds(raw.EndpointRetryAfter, 60),
		HTTP: HTTP{
			Timeout:     seconds(raw.HTTP.TimeoutSeconds, 90),
			Attempts:    intOr(raw.HTTP.Attempts, 6),
			BackoffBase: seconds(raw.HTTP.BackoffBaseSeconds, 10),
			BackoffMax:  seconds(raw.HTTP.BackoffMaxSeconds, 120),
		},
	}
	switch {
	case cfg.Database == "":
		return nil, fmt.Errorf("config %s: database is required", path)
	case cfg.Concurrency < 1:
		return nil, fmt.Errorf("config %s: concurrency must be at least 1", path)
	case cfg.EndpointRetryAfter < 0:
		return nil, fmt.Errorf("config %s: endpoint_retry_after_seconds can't be negative", path)
	case cfg.Overlap < 0:
		return nil, fmt.Errorf("config %s: overlap_minutes can't be negative", path)
	case cfg.HTTP.Attempts < 1 || cfg.HTTP.Timeout <= 0 || cfg.HTTP.BackoffBase < 0 || cfg.HTTP.BackoffMax < cfg.HTTP.BackoffBase:
		return nil, fmt.Errorf("config %s: http settings out of range", path)
	}
	entries := raw.Endpoints
	if len(entries) == 0 {
		for _, e := range Catalog {
			entries = append(entries, endpointEntry{Name: e.Name})
		}
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		ep, ok := catalogEndpoint(entry.Name)
		if !ok {
			return nil, fmt.Errorf("config %s: unknown endpoint %q", path, entry.Name)
		}
		if seen[ep.Name] {
			return nil, fmt.Errorf("config %s: endpoint %q listed twice", path, ep.Name)
		}
		seen[ep.Name] = true
		if entry.Limit < 0 {
			return nil, fmt.Errorf("config %s: %s: limit must be positive", path, ep.Name)
		}
		if entry.Limit > 0 {
			ep.Limit = entry.Limit
		}
		cfg.Endpoints = append(cfg.Endpoints, ep)
	}
	return cfg, nil
}

// Select returns the configured endpoints with these names (all when names is empty).
func (c *Config) Select(names []string) ([]Endpoint, error) {
	if len(names) == 0 {
		return c.Endpoints, nil
	}
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	var out []Endpoint
	for _, e := range c.Endpoints {
		if want[e.Name] {
			out = append(out, e)
			delete(want, e.Name)
		}
	}
	if len(want) > 0 {
		var missing []string
		for n := range want {
			missing = append(missing, n)
		}
		return nil, fmt.Errorf("endpoint(s) not in the config: %s", strings.Join(missing, ", "))
	}
	return out, nil
}

// LoadCredentials reads CENTRIC_BASE_URL/USERNAME/PASSWORD. The environment wins over the
// env file.
func (c *Config) LoadCredentials() (Credentials, error) {
	file := map[string]string{}
	if c.EnvFile != "" {
		var err error
		if file, err = readEnvFile(c.EnvFile); err != nil {
			return Credentials{}, err
		}
	}
	get := func(key string) string {
		if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return strings.TrimSpace(file[key])
	}
	creds := Credentials{get("CENTRIC_BASE_URL"), get("CENTRIC_USERNAME"), get("CENTRIC_PASSWORD")}
	var missing []string
	for _, kv := range [][2]string{{"CENTRIC_BASE_URL", creds.BaseURL}, {"CENTRIC_USERNAME", creds.Username}, {"CENTRIC_PASSWORD", creds.Password}} {
		if kv[1] == "" {
			missing = append(missing, kv[0])
		}
	}
	if len(missing) > 0 {
		where := "the environment"
		if c.EnvFile != "" {
			where += " or " + c.EnvFile
		}
		return Credentials{}, fmt.Errorf("missing %s in %s", strings.Join(missing, ", "), where)
	}
	return creds, nil
}

func readEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("env file: %w", err)
	}
	defer f.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(f)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("%s:%d: expected KEY=value", path, n)
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == value[len(value)-1] && (value[0] == '"' || value[0] == '\'') {
			value = value[1 : len(value)-1]
		}
		values[strings.TrimSpace(key)] = value
	}
	return values, scanner.Err()
}

func intOr(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}

func seconds(p *float64, def float64) time.Duration {
	v := def
	if p != nil {
		v = *p
	}
	return time.Duration(v * float64(time.Second))
}
