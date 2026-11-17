package main

import (
	"io/fs"
	"log"
	"net/http"
	"strings"
)

// Link-preview cards. Every app that fetches a page's og:image, static/og/<page>.jpg, gets that
// still card, except the apps that play an animated preview: they get static/og/<page>.gif when
// the page has one. The GIF's first frame is the still card, so a frame-one-only app still shows
// the whole card. Facebook takes stills only and X shows a first frame, so they get the JPEG.
//
// The apps are known by the User-Agent of the fetch: Slack's unfurler and image proxy, and
// Discord's bot.
var animatedPreviewers = []string{"Slackbot", "Slack-ImgProxy", "Discordbot"}

func playsAnimation(userAgent string) bool {
	for _, a := range animatedPreviewers {
		if strings.Contains(userAgent, a) {
			return true
		}
	}
	return false
}

// ogCards serves /og/: the animated card to an app that plays one, the still card to every other.
func ogCards(pages fs.FS) http.Handler {
	files := http.FileServer(http.FS(pages))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "User-Agent")
		name, isCard := strings.CutSuffix(r.URL.Path, ".jpg")
		if isCard && playsAnimation(r.UserAgent()) {
			gif := strings.TrimPrefix(name, "/") + ".gif"
			if st, err := fs.Stat(pages, gif); err == nil && !st.IsDir() {
				log.Printf("og: %s → animated, for %q", r.URL.Path, r.UserAgent())
				r2 := r.Clone(r.Context())
				r2.URL.Path = "/" + gif
				files.ServeHTTP(w, r2)
				return
			}
		}
		if isCard {
			log.Printf("og: %s → still, for %q", r.URL.Path, r.UserAgent())
		}
		files.ServeHTTP(w, r)
	})
}
