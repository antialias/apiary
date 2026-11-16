package main

// A LIVE check of the codex lane's prompt cache, against the real chatgpt.com
// backend. It skips unless CODEX_LIVE=1, so CI never touches the network.
//
//	CODEX_LIVE=1 go test -run TestCodexLiveCache -v .
//
// The real proxy stack (e2e class harness) serves, through a class whose only
// member is gpt on codex, a 6-turn growing Anthropic conversation behind a ~28k-token system prompt and a dozen tools, sent as
// The agent gateway does (with a fixed x-claude-code-session-id), and logs each turn's
// cache_read_input_tokens. The stand-in brain reads the bearer from this
// machine's Codex login (~/.codex/auth.json) at request time. It never
// refreshes it (refresh tokens rotate globally; a refresh would break that
// login), never copies the file, and never logs it.

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestCodexLiveCache(t *testing.T) {
	if os.Getenv("CODEX_LIVE") != "1" {
		t.Skip("live codex backend test: set CODEX_LIVE=1 to run")
	}
	home, _ := os.UserHomeDir()
	authPath := filepath.Join(home, ".codex", "auth.json")

	h, restore := newE2EClass(t)
	defer restore()
	fake, _ := url.Parse(brainURL)
	fakeBrain := httputil.NewSingleHostReverseProxy(fake)
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/openai-token" {
			fakeBrain.ServeHTTP(w, r)
			return
		}
		raw, err := os.ReadFile(authPath)
		if err != nil {
			http.Error(w, "no codex login", http.StatusServiceUnavailable)
			return
		}
		var a struct {
			Tokens struct {
				AccessToken string `json:"access_token"`
				AccountID   string `json:"account_id"`
			} `json:"tokens"`
		}
		if json.Unmarshal(raw, &a) != nil || a.Tokens.AccessToken == "" {
			http.Error(w, "unreadable codex login", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"account_id": a.Tokens.AccountID, "access_token": a.Tokens.AccessToken,
			"expires_at": float64(time.Now().Add(time.Hour).Unix()), "name": "live",
		})
	}))
	defer live.Close()
	brainURL = live.URL
	codexUpstream = "https://chatgpt.com"
	h.brainMu.Lock()
	h.classMembers = []map[string]any{
		{"id": "gpt/gpt-6.1-sol", "priority": 1, "kind": "provider", "provider": "gpt",
			"model": "gpt-6.1-sol", "context_window": 400000, "anthropic_token_ratio": 0.52,
			"effective": 50.0, "usable": true, "status": "ok"},
	}
	h.brainMu.Unlock()
	fetchPreferredSync()
	rebuildProxy(h.e2eEnv)

	// The repro's prefix: the proxy's own source, ~100k chars (~28k tokens),
	// behind a per-run nonce so no run can warm another's cache.
	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce)
	files, _ := filepath.Glob("*.go")
	sort.Strings(files)
	var src strings.Builder
	for _, f := range files {
		b, _ := os.ReadFile(f)
		src.Write(b)
		if src.Len() >= 100000 {
			break
		}
	}
	system := fmt.Sprintf("Session nonce %s. You are a terse assistant. Reference material follows; "+
		"answer only what the user asks.\n\n%s", hex.EncodeToString(nonce), src.String()[:100000])
	tools := []any{}
	for i := 0; i < 12; i++ {
		tools = append(tools, map[string]any{
			"name": fmt.Sprintf("tool_%d", i), "description": strings.Repeat(fmt.Sprintf("Reference tool number %d. ", i), 8),
			"input_schema": map[string]any{"type": "object",
				"properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		})
	}

	msgs := []any{}
	var laterIn, laterCached int
	t.Logf("turn  input  cache_read  hit")
	for turn := 1; turn <= 6; turn++ {
		msgs = append(msgs, map[string]any{"role": "user",
			"content": fmt.Sprintf("Turn %d: reply with exactly the word OK%d.", turn, turn)})
		body, _ := json.Marshal(map[string]any{
			"model": "claude-frontier", "max_tokens": 64, "system": system, "tools": tools,
			"messages": msgs, "output_config": map[string]any{"effort": "low"},
		})
		req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer CLIENT")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", "agent-gw")
		resp, err := (&http.Client{Timeout: 3 * time.Minute}).Do(req)
		if err != nil {
			t.Fatalf("turn %d: %v", turn, err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("turn %d: status %d: %.300s", turn, resp.StatusCode, raw)
		}
		var m struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			Usage struct {
				Input     int `json:"input_tokens"`
				CacheRead int `json:"cache_read_input_tokens"`
			} `json:"usage"`
		}
		_ = json.Unmarshal(raw, &m)
		reply := fmt.Sprintf("OK%d", turn)
		if len(m.Content) > 0 && strings.TrimSpace(m.Content[0].Text) != "" {
			reply = m.Content[0].Text
		}
		total := m.Usage.Input + m.Usage.CacheRead
		share := 0.0
		if total > 0 {
			share = float64(m.Usage.CacheRead) / float64(total)
		}
		t.Logf("%4d %6d %11d %5.1f%%", turn, m.Usage.Input, m.Usage.CacheRead, 100*share)
		if turn > 1 {
			laterIn += total
			laterCached += m.Usage.CacheRead
		}
		msgs = append(msgs, map[string]any{"role": "assistant", "content": reply})
		time.Sleep(2 * time.Second)
	}
	if laterIn == 0 {
		t.Fatal("turns 2..6 reported no input")
	}
	share := float64(laterCached) / float64(laterIn)
	t.Logf("turns 2..6 hit share %.1f%%", 100*share)
	// Measured 99.4% with session_id and 0% without; half is far from both.
	if share < 0.5 {
		t.Errorf("turns 2..6 hit share %.1f%%, want the cache to carry the conversation", 100*share)
	}
}
