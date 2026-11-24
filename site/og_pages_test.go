package main

import (
	"bytes"
	"html"
	"image/jpeg"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"testing"
)

// Every public page has its own link-preview card, and the card keeps up with
// the page: the tags are complete, the card exists at 1200x630, and its alt
// text quotes the page's headline. Change a headline and this fails until the
// card (site/og, render.sh) and its tags are redone in the same commit.
func TestEveryPageHasItsOwnCurrentPreviewCard(t *testing.T) {
	pages, err := fs.Sub(static, "static")
	if err != nil {
		t.Fatal(err)
	}
	meta := func(doc, attr, name string) string {
		m := regexp.MustCompile(`<meta ` + attr + `="` + regexp.QuoteMeta(name) + `" content="([^"]*)"`).FindStringSubmatch(doc)
		if m == nil {
			return ""
		}
		return html.UnescapeString(m[1])
	}
	first := func(doc, re string) string {
		m := regexp.MustCompile(re).FindStringSubmatch(doc)
		if m == nil {
			return ""
		}
		return m[1]
	}
	cardOf := map[string]string{}
	err = fs.WalkDir(pages, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".html") {
			return err
		}
		b, _ := fs.ReadFile(pages, p)
		doc := string(b)
		want := "https://apiary.wtf/" + strings.TrimSuffix(strings.TrimSuffix(p, "index.html"), "/")
		if !strings.HasSuffix(want, "/") {
			want += "/"
		}
		title := html.UnescapeString(first(doc, `<title>([^<]*)</title>`))
		if title == "" || meta(doc, "property", "og:title") != title {
			t.Errorf("%s: og:title %q, want the page's <title> %q", p, meta(doc, "property", "og:title"), title)
		}
		if c := first(doc, `<link rel="canonical" href="([^"]*)"`); c != want || meta(doc, "property", "og:url") != want {
			t.Errorf("%s: canonical %q and og:url %q, want both %q", p, c, meta(doc, "property", "og:url"), want)
		}
		for _, name := range []string{"og:description", "og:site_name", "og:type"} {
			if meta(doc, "property", name) == "" {
				t.Errorf("%s: no %s", p, name)
			}
		}
		if meta(doc, "name", "twitter:card") != "summary_large_image" {
			t.Errorf("%s: twitter:card should be summary_large_image", p)
		}

		img := meta(doc, "property", "og:image")
		card, ok := strings.CutPrefix(img, "https://apiary.wtf/")
		if !ok || !strings.HasPrefix(card, "og/") || path.Ext(card) != ".jpg" {
			t.Errorf("%s: og:image %q, want https://apiary.wtf/og/<page>.jpg", p, img)
			return nil
		}
		if other, taken := cardOf[card]; taken {
			t.Errorf("%s: shares its card %s with %s; each page gets its own", p, card, other)
		}
		cardOf[card] = p
		jb, err := fs.ReadFile(pages, card)
		if err != nil {
			t.Errorf("%s: card %s is missing", p, card)
			return nil
		}
		cfg, err := jpeg.DecodeConfig(bytes.NewReader(jb))
		if err != nil || cfg.Width != 1200 || cfg.Height != 630 {
			t.Errorf("%s: card %s is not a 1200x630 JPEG (%v, %dx%d)", p, card, err, cfg.Width, cfg.Height)
		}
		if meta(doc, "property", "og:image:width") != "1200" || meta(doc, "property", "og:image:height") != "630" {
			t.Errorf("%s: og:image:width/height should say 1200 and 630", p)
		}

		headline := plain(first(doc, `(?s)<h1[^>]*>(.*?)</h1>`))
		alt := plain(meta(doc, "property", "og:image:alt"))
		if headline == "" || !strings.HasPrefix(alt, "“"+headline+"”") {
			t.Errorf("%s: og:image:alt should open with the headline in quotes, “%s”; it says %q. Redo the card if the headline changed.", p, headline, alt)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cardOf) < 3 {
		t.Errorf("found %d pages; expected the front door, /about/ and /plan/ at least", len(cardOf))
	}
}

// plain is text as a reader sees it: no tags, entities decoded, one space
// between words, none before punctuation, and one kind of apostrophe.
func plain(s string) string {
	s = regexp.MustCompile(`<[^>]*>`).ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	s = strings.Join(strings.Fields(s), " ")
	s = regexp.MustCompile(` ([.,!?;:])`).ReplaceAllString(s, "$1")
	return strings.ReplaceAll(s, "’", "'")
}
