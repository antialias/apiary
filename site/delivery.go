package main

import (
	"compress/gzip"
	"net/http"
	"path"
	"strings"
)

// delivery says how long browsers may keep what the site serves, and gzips text for clients that
// take it. Pages are checked on every visit, so a deploy shows at once. Fonts keep a year, since a
// font never changes under its name (a changed one gets a new name). Everything else keeps an hour.
func delivery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ext := path.Ext(r.URL.Path)
		switch {
		case strings.HasPrefix(r.URL.Path, "/fonts/") && ext == ".woff2":
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		case ext == "" || ext == ".html":
			w.Header().Set("Cache-Control", "no-cache")
		default:
			w.Header().Set("Cache-Control", "public, max-age=3600")
		}
		if !textual[ext] || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Add("Vary", "Accept-Encoding")
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		r.Header.Del("Range") // a byte range of the uncompressed file means nothing in gzip
		gw := &gzipped{ResponseWriter: w}
		defer gw.close()
		next.ServeHTTP(gw, r)
	})
}

// Pictures and fonts are compressed already; these are the kinds worth gzipping.
var textual = map[string]bool{"": true, ".html": true, ".css": true, ".js": true, ".svg": true, ".xml": true, ".txt": true}

type gzipped struct {
	http.ResponseWriter
	gz      *gzip.Writer
	started bool
}

func (g *gzipped) WriteHeader(code int) {
	if !g.started {
		g.started = true
		g.Header().Del("Content-Length")
		g.Header().Set("Content-Encoding", "gzip")
	}
	g.ResponseWriter.WriteHeader(code)
}

func (g *gzipped) Write(b []byte) (int, error) {
	if !g.started {
		g.WriteHeader(http.StatusOK)
	}
	if g.gz == nil {
		g.gz = gzip.NewWriter(g.ResponseWriter)
	}
	return g.gz.Write(b)
}

func (g *gzipped) close() {
	if g.gz != nil {
		g.gz.Close()
	}
}
