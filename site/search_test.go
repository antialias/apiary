package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

// What search engines are told: the sitemap lists exactly the pages that may be in search,
// robots.txt points at it and blocks nothing, each page's description fits in a search result,
// and no page asks another host for anything (fonts and icons are served from here).
func TestSearchEnginesSeeEveryPublicPage(t *testing.T) {
	pages, err := fs.Sub(static, "static")
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	err = fs.WalkDir(pages, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".html") {
			return err
		}
		b, _ := fs.ReadFile(pages, p)
		doc := string(b)
		if !strings.Contains(metaOf(doc, "name", "robots"), "noindex") {
			want = append(want, firstOf(doc, `<link rel="canonical" href="([^"]*)"`))
		}
		if n := utf8.RuneCountInString(metaOf(doc, "name", "description")); n == 0 || n > 160 {
			t.Errorf("%s: its description is %d characters; search results show about 155", p, n)
		}
		outside := regexp.MustCompile(`<(?:link|script|img|source)\b[^>]*\b(?:href|src|srcset)="(?:https?:)?//([^/"]+)`)
		for _, m := range outside.FindAllStringSubmatch(doc, -1) {
			if m[1] != "apiary.wtf" {
				t.Errorf("%s: asks %s for something; serve it from this site", p, m[1])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sitemap, _ := fs.ReadFile(pages, "sitemap.xml")
	var listed []string
	for _, m := range regexp.MustCompile(`<loc>([^<]*)</loc>`).FindAllStringSubmatch(string(sitemap), -1) {
		listed = append(listed, m[1])
	}
	slices.Sort(listed)
	slices.Sort(want)
	if !slices.Equal(listed, want) {
		t.Errorf("sitemap.xml lists %q; the pages that may be in search (no noindex) are %q", listed, want)
	}
	robots, _ := fs.ReadFile(pages, "robots.txt")
	if !bytes.Contains(robots, []byte("Sitemap: https://apiary.wtf/sitemap.xml")) || regexp.MustCompile(`(?m)^Disallow: */`).Match(robots) {
		t.Errorf("robots.txt should point at the sitemap and block nothing; it says:\n%s", robots)
	}
}

func TestDeliveryCachesAndCompresses(t *testing.T) {
	h := testSite(t)
	const page, hour, year = "no-cache", "public, max-age=3600", "public, max-age=31536000, immutable"
	for _, c := range []struct {
		method, path    string
		gzip            bool
		encoding, cache string
		contains        string
	}{
		{"GET", "/", true, "gzip", page, "<title>"},
		{"GET", "/", false, "", page, "<title>"},
		{"HEAD", "/about/", true, "gzip", page, ""},
		{"GET", "/plan/plan.js", true, "gzip", hour, ""},
		{"GET", "/fonts/fonts.css", true, "gzip", hour, "@font-face"},
		{"GET", "/fonts/fraunces-latin.woff2", true, "", year, "wOF2"},
		{"GET", "/og/front/hive-jars.webp", true, "", hour, "WEBP"},
		{"GET", "/robots.txt", true, "gzip", hour, "Sitemap:"},
		{"GET", "/sitemap.xml", true, "gzip", hour, "<urlset"},
		{"GET", "/favicon.ico", true, "", hour, ""},
		{"GET", "/favicon.svg", true, "gzip", hour, "<polygon"},
		{"GET", "/apple-touch-icon.png", true, "", hour, "PNG"},
	} {
		req := httptest.NewRequest(c.method, "https://apiary.wtf"+c.path, nil)
		if c.gzip {
			req.Header.Set("Accept-Encoding", "gzip, deflate, br")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		name := c.method + " " + c.path
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d", name, rec.Code)
			continue
		}
		if got := rec.Header().Get("Content-Encoding"); got != c.encoding {
			t.Errorf("%s: Content-Encoding %q, want %q", name, got, c.encoding)
		}
		if got := rec.Header().Get("Cache-Control"); got != c.cache {
			t.Errorf("%s: Cache-Control %q, want %q", name, got, c.cache)
		}
		if c.encoding == "gzip" && rec.Header().Get("Content-Length") != "" {
			t.Errorf("%s: gzipped but still says the uncompressed Content-Length", name)
		}
		if c.method == http.MethodHead {
			continue
		}
		body := rec.Body.Bytes()
		if c.encoding == "gzip" {
			zr, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				t.Errorf("%s: not gzip: %v", name, err)
				continue
			}
			if body, err = io.ReadAll(zr); err != nil {
				t.Errorf("%s: broken gzip: %v", name, err)
			}
		}
		if !bytes.Contains(body, []byte(c.contains)) || len(body) == 0 {
			t.Errorf("%s: body (%d bytes) lacks %q", name, len(body), c.contains)
		}
	}
}
