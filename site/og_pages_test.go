package main

import (
	"bytes"
	"html"
	"image"
	"image/draw"
	"image/gif"
	"image/jpeg"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"testing"
)

// Every public page has its own link-preview card, still and animated, and the
// card keeps up with the page: the tags are complete, the still is 1200x630,
// the GIF beside it has the same shape and visibly moves, and the alt text quotes the page's
// headline. Change a headline and this fails until the card (site/og:
// render.sh, motion.py, animate.sh) and its tags are redone in the same commit.
func TestEveryPageHasItsOwnCurrentPreviewCard(t *testing.T) {
	pages, err := fs.Sub(static, "static")
	if err != nil {
		t.Fatal(err)
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
		title := html.UnescapeString(firstOf(doc, `<title>([^<]*)</title>`))
		if title == "" || metaOf(doc, "property", "og:title") != title {
			t.Errorf("%s: og:title %q, want the page's <title> %q", p, metaOf(doc, "property", "og:title"), title)
		}
		if c := firstOf(doc, `<link rel="canonical" href="([^"]*)"`); c != want || metaOf(doc, "property", "og:url") != want {
			t.Errorf("%s: canonical %q and og:url %q, want both %q", p, c, metaOf(doc, "property", "og:url"), want)
		}
		for _, name := range []string{"og:description", "og:site_name", "og:type"} {
			if metaOf(doc, "property", name) == "" {
				t.Errorf("%s: no %s", p, name)
			}
		}
		if metaOf(doc, "name", "twitter:card") != "summary_large_image" {
			t.Errorf("%s: twitter:card should be summary_large_image", p)
		}

		img := metaOf(doc, "property", "og:image")
		card, ok := strings.CutPrefix(img, "https://apiary.wtf/")
		// A changed card gets a new ?v=<n>, so chat apps that kept the old one fetch it again.
		card, version, versioned := strings.Cut(card, "?")
		if !ok || !strings.HasPrefix(card, "og/") || path.Ext(card) != ".jpg" ||
			versioned && !regexp.MustCompile(`^v=[0-9]+$`).MatchString(version) {
			t.Errorf("%s: og:image %q, want https://apiary.wtf/og/<page>.jpg, or with ?v=<n> once it has changed", p, img)
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
		anim := strings.TrimSuffix(card, ".jpg") + ".gif"
		if gb, err := fs.ReadFile(pages, anim); err != nil {
			t.Errorf("%s: no animated card %s (og/motion.py, then og/animate.sh)", p, anim)
		} else if g, err := gif.DecodeAll(bytes.NewReader(gb)); err != nil {
			t.Errorf("%s: animated card %s is not a GIF: %v", p, anim, err)
		} else if g.Config.Width*630 != g.Config.Height*1200 {
			t.Errorf("%s: animated card %s is %dx%d, not shaped like the card", p, anim, g.Config.Width, g.Config.Height)
		} else if m := visibleMotion(g); m < 0.01 {
			t.Errorf("%s: animated card %s barely moves (%.1f%% of it visibly changes; at least 1%%): "+
				"in a chat app's preview it looks still", p, anim, 100*m)
		}
		if metaOf(doc, "property", "og:image:width") != "1200" || metaOf(doc, "property", "og:image:height") != "630" {
			t.Errorf("%s: og:image:width/height should say 1200 and 630", p)
		}

		headline := plain(firstOf(doc, `(?s)<h1[^>]*>(.*?)</h1>`))
		alt := plain(metaOf(doc, "property", "og:image:alt"))
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

// visibleMotion is the largest share of the picture, in any frame, that differs from the first
// frame by more than a fifth of full brightness: what a viewer sees move, not grain or a shimmer
// that the GIF's palette and a chat app's thumbnail wash out.
func visibleMotion(g *gif.GIF) float64 {
	bounds := image.Rect(0, 0, g.Config.Width, g.Config.Height)
	canvas := image.NewRGBA(bounds)
	var first []byte
	most := 0.0
	for i, frame := range g.Image {
		var saved []byte
		if g.Disposal != nil && g.Disposal[i] == gif.DisposalPrevious {
			saved = append([]byte(nil), canvas.Pix...)
		}
		draw.Draw(canvas, frame.Bounds(), frame, frame.Bounds().Min, draw.Over)
		if first == nil {
			first = append([]byte(nil), canvas.Pix...)
		}
		changed := 0
		for p := 0; p < len(first); p += 4 {
			for c := p; c < p+3; c++ {
				if d := int(canvas.Pix[c]) - int(first[c]); d > 51 || d < -51 {
					changed++
					break
				}
			}
		}
		most = max(most, float64(changed)/float64(bounds.Dx()*bounds.Dy()))
		switch {
		case saved != nil:
			copy(canvas.Pix, saved)
		case g.Disposal != nil && g.Disposal[i] == gif.DisposalBackground:
			draw.Draw(canvas, frame.Bounds(), image.Transparent, image.Point{}, draw.Src)
		}
	}
	return most
}

// metaOf is the content of a page's <meta attr="name">, unescaped, or "".
func metaOf(doc, attr, name string) string {
	m := regexp.MustCompile(`<meta ` + attr + `="` + regexp.QuoteMeta(name) + `" content="([^"]*)"`).FindStringSubmatch(doc)
	if m == nil {
		return ""
	}
	return html.UnescapeString(m[1])
}

// firstOf is the first group of re's first match in doc, or "".
func firstOf(doc, re string) string {
	m := regexp.MustCompile(re).FindStringSubmatch(doc)
	if m == nil {
		return ""
	}
	return m[1]
}
