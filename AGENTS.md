# Working in this repo

- The repo is public. No member names or emails, and no private hostnames or addresses, in code,
  docs or commit messages.
- The site: read `site/README.md` first.
- **Every public page keeps its link-preview card current, still and animated.** Changing a page's
  headline, pitch or look updates, in the same commit, its art and `PAGES` row (`site/og/card.html`),
  the still (`render.sh`), the clip if the art changed (`motion.py`), the animated card
  (`animate.sh`), and its `og:*` tags. The steps are in `site/README.md`. `site/og_pages_test.go`
  fails when a page's tags are incomplete, its card or its GIF is missing or misshapen, two pages
  share a card, or the card's alt text no longer quotes the page's headline.
