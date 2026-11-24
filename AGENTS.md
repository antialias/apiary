# Working in this repo

- The repo is public. No member names or emails, and no private hostnames or addresses, in code,
  docs or commit messages.
- The site: read `site/README.md` first.
- **Every public page keeps its link-preview card current.** Changing a page's headline, pitch or
  look updates its card (`site/og/`: art, the `PAGES` row in `card.html`, then `render.sh`) and its
  `og:*` tags in the same commit. `site/og_pages_test.go` fails when a page's tags are incomplete,
  its card is missing or not 1200x630, two pages share a card, or the card's alt text no longer
  quotes the page's headline.
