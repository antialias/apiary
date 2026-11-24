package main

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func testSite(t *testing.T) http.Handler {
	t.Helper()
	pages, err := fs.Sub(static, "static")
	if err != nil {
		t.Fatal(err)
	}
	return site(pages, filepath.Join(t.TempDir(), "signups.jsonl"))
}

func TestWWWGoesToTheBareDomain(t *testing.T) {
	h := testSite(t)
	for _, c := range []struct {
		method, url string
		code        int
		location    string
	}{
		{http.MethodGet, "https://www.apiary.wtf/", http.StatusMovedPermanently, "https://apiary.wtf/"},
		{http.MethodGet, "https://www.apiary.wtf/about/?from=x", http.StatusMovedPermanently, "https://apiary.wtf/about/?from=x"},
		{http.MethodGet, "https://WWW.apiary.wtf:443/plan/", http.StatusMovedPermanently, "https://apiary.wtf/plan/"},
		{http.MethodPost, "https://www.apiary.wtf/signup", http.StatusPermanentRedirect, "https://apiary.wtf/signup"},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(c.method, c.url, nil))
		if rec.Code != c.code || rec.Header().Get("Location") != c.location {
			t.Errorf("%s %s: %d → %q, want %d → %q", c.method, c.url, rec.Code, rec.Header().Get("Location"), c.code, c.location)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "https://www.apiary.wtf/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("www /healthz: %d %q, want 200 ok", rec.Code, rec.Body.String())
	}
}

func TestFrontDoorAndLongPage(t *testing.T) {
	h := testSite(t)
	get := func(url string) (int, string) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
		return rec.Code, rec.Body.String()
	}
	code, front := get("https://apiary.wtf/")
	if code != http.StatusOK {
		t.Fatalf("/: %d", code)
	}
	for _, want := range []string{"One hive", `href="/sign-in"`, `href="/about/"`, `href="/about/#join"`, `href="/plan/"`, `href="/privacy"`, "/og/front/hive-jars.webp", "/og/front.jpg"} {
		if !strings.Contains(front, want) {
			t.Errorf("front door lacks %s", want)
		}
	}
	code, about := get("https://apiary.wtf/about/")
	if code != http.StatusOK || !strings.Contains(about, `id="join"`) || !strings.Contains(about, `href="https://apiary.wtf/about/"`) {
		t.Errorf("/about/: %d, or no waggle-list form, or the wrong canonical address", code)
	}
	if code, _ := get("https://apiary.wtf/about"); code != http.StatusMovedPermanently {
		t.Errorf("/about: %d, want a redirect to /about/", code)
	}
	for _, asset := range []string{"/og/front/hive-jars.webp", "/og/front/hive-band.webp", "/og/front.jpg"} {
		if code, _ := get("https://apiary.wtf" + asset); code != http.StatusOK {
			t.Errorf("%s: %d", asset, code)
		}
	}
}
