// Apiary interest-signup stub.
//
// Serves the front door and the long page, and appends signup submissions to a JSONL file
// (SIGNUPS_FILE, default ./data/signups.jsonl). No accounts, no cookies, no
// analytics — this is a signal sweep, not onboarding. See ../docs/DESIGN.md §6.
package main

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

//go:embed static
var static embed.FS

type signup struct {
	ReceivedAt string   `json:"received_at"`
	Email      string   `json:"email"`
	Handle     string   `json:"handle,omitempty"`
	Uses       []string `json:"uses,omitempty"` // what runs in their hive today: claude-plan, codex, local…
	PlanTier   string   `json:"plan_tier,omitempty"`
	Hardware   string   `json:"hardware,omitempty"`
	Spend      string   `json:"spend,omitempty"`
	Pool       []string `json:"pool,omitempty"` // interest in the pool to come: share, draw, dev-time, funding
	Pain       string   `json:"pain,omitempty"`
	IPHash     string   `json:"ip_hash"` // sha256[:12] of remote IP — dedup signal, not identity
}

var (
	mu       sync.Mutex
	lastByIP = map[string]time.Time{}
)

func clientIP(r *http.Request) string {
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		return strings.TrimSpace(strings.Split(xf, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[:n]
	}
	return s
}

func clipAll(ss []string, n, max int) []string {
	var out []string
	for _, s := range ss {
		if s = clip(s, n); s != "" {
			out = append(out, s)
		}
		if len(out) == max {
			break
		}
	}
	return out
}

func handleSignup(file string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Email    string   `json:"email"`
			Handle   string   `json:"handle"`
			Uses     []string `json:"uses"`
			PlanTier string   `json:"plan_tier"`
			Hardware string   `json:"hardware"`
			Spend    string   `json:"spend"`
			Pool     []string `json:"pool"`
			Pain     string   `json:"pain"`
			Website  string   `json:"website"` // honeypot — humans never fill it
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if body.Website != "" { // bot: swallow silently
			w.WriteHeader(http.StatusAccepted)
			return
		}
		email := clip(body.Email, 254)
		at := strings.Index(email, "@")
		if at < 1 || !strings.Contains(email[at:], ".") {
			http.Error(w, "a real email is the one thing we need", http.StatusBadRequest)
			return
		}

		ip := clientIP(r)
		mu.Lock()
		if t, ok := lastByIP[ip]; ok && time.Since(t) < 10*time.Second {
			mu.Unlock()
			http.Error(w, "easy there — try again in a moment", http.StatusTooManyRequests)
			return
		}
		lastByIP[ip] = time.Now()
		mu.Unlock()

		sum := sha256.Sum256([]byte(ip))
		rec := signup{
			ReceivedAt: time.Now().UTC().Format(time.RFC3339),
			Email:      email,
			Handle:     clip(body.Handle, 80),
			Uses:       clipAll(body.Uses, 40, 8),
			PlanTier:   clip(body.PlanTier, 80),
			Hardware:   clip(body.Hardware, 200),
			Spend:      clip(body.Spend, 40),
			Pool:       clipAll(body.Pool, 40, 8),
			Pain:       clip(body.Pain, 2000),
			IPHash:     hex.EncodeToString(sum[:])[:12],
		}

		line, _ := json.Marshal(rec)
		mu.Lock()
		defer mu.Unlock()
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			log.Printf("signup open: %v", err)
			http.Error(w, "storage error — please try again", http.StatusInternalServerError)
			return
		}
		defer f.Close()
		if _, err := f.Write(append(line, '\n')); err != nil {
			log.Printf("signup write: %v", err)
			http.Error(w, "storage error — please try again", http.StatusInternalServerError)
			return
		}
		log.Printf("signup: %s (uses=%v pool=%v)", rec.IPHash, rec.Uses, rec.Pool)
		w.WriteHeader(http.StatusCreated)
	}
}

func main() {
	listen := os.Getenv("LISTEN")
	if listen == "" {
		listen = ":8080"
	}
	file := os.Getenv("SIGNUPS_FILE")
	if file == "" {
		file = "data/signups.jsonl"
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		log.Fatalf("mkdir %s: %v", filepath.Dir(file), err)
	}

	pages, err := fs.Sub(static, "static")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("apiary-site listening on %s, signups → %s", listen, file)
	log.Fatal(http.ListenAndServe(listen, site(pages, file)))
}

// site serves the pages: the one-screen front door at /, the long page at
// /about/, the plan, the og cards and the signup form.
func site(pages fs.FS, signups string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(pages)))
	mux.Handle("/og/", ogCards(pages))
	mux.HandleFunc("/signup", handleSignup(signups))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return oneHost(delivery(mux))
}

// oneHost sends www.apiary.wtf to apiary.wtf, path and query kept, so each page
// has one address. /healthz answers on any host.
func oneHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		bare, www := strings.CutPrefix(strings.ToLower(host), "www.")
		if !www || r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		code := http.StatusMovedPermanently
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			code = http.StatusPermanentRedirect // keeps a POST a POST
		}
		http.Redirect(w, r, "https://"+bare+r.URL.RequestURI(), code)
	})
}
