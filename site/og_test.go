package main

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
)

func TestOGCardsServeTheAnimatedCardOnlyToAppsThatPlayIt(t *testing.T) {
	pages := fstest.MapFS{
		"og/home.jpg": {Data: []byte("still home")},
		"og/home.gif": {Data: []byte("GIF89a animated home")},
		"og/plan.jpg": {Data: []byte("still plan")},
	}
	h := ogCards(pages)
	for _, c := range []struct{ path, ua, want, wantType string }{
		{"/og/home.jpg", "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)", "GIF89a animated home", "image/gif"},
		{"/og/home.jpg", "Slack-ImgProxy (+https://api.slack.com/robots)", "GIF89a animated home", "image/gif"},
		{"/og/home.jpg", "Mozilla/5.0 (compatible; Discordbot/2.0; +https://discordapp.com)", "GIF89a animated home", "image/gif"},
		{"/og/home.jpg", "Twitterbot/1.0", "still home", "image/jpeg"},
		{"/og/home.jpg", "facebookexternalhit/1.1", "still home", "image/jpeg"},
		// A page with no animated card gives every app its still.
		{"/og/plan.jpg", "Slackbot-LinkExpanding 1.0", "still plan", "image/jpeg"},
	} {
		req := httptest.NewRequest(http.MethodGet, c.path, nil)
		req.Header.Set("User-Agent", c.ua)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Body.String() != c.want {
			t.Errorf("%s for %q: %d %q, want 200 %q", c.path, c.ua, rec.Code, rec.Body.String(), c.want)
		}
		if got := rec.Header().Get("Content-Type"); got != c.wantType {
			t.Errorf("%s for %q: Content-Type %q, want %q", c.path, c.ua, got, c.wantType)
		}
		if rec.Header().Get("Vary") != "User-Agent" {
			t.Errorf("%s for %q: no Vary: User-Agent", c.path, c.ua)
		}
	}
}

// The hive-cam loops play in Safari only if they come as video/mp4 and answer a byte range.
// The image is FROM scratch, with no mime.types, so the type comes from Go sniffing the file.
func TestKeeperLoopsServeAsVideoWithRanges(t *testing.T) {
	pages, err := fs.Sub(static, "static")
	if err != nil {
		t.Fatal(err)
	}
	h := ogCards(pages)
	for _, k := range []string{"marigold", "basil", "society", "penny"} {
		req := httptest.NewRequest(http.MethodGet, "/og/keepers/"+k+".mp4", nil)
		req.Header.Set("Range", "bytes=0-99")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent || rec.Body.Len() != 100 {
			t.Errorf("%s: %d with %d bytes, want 206 with 100", k, rec.Code, rec.Body.Len())
		}
		if got := rec.Header().Get("Content-Type"); got != "video/mp4" {
			t.Errorf("%s: Content-Type %q, want video/mp4", k, got)
		}
		if _, err := fs.Stat(pages, "og/keepers/"+k+".webp"); err != nil {
			t.Errorf("%s: no poster photo: %v", k, err)
		}
	}
}
