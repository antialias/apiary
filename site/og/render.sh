#!/bin/sh
# Renders each page's link-preview card (card.html) to static/og/<page>.jpg, 1200x630.
# Uses Playwright's own browser (npx playwright) and the network (the fonts come from Google Fonts).
# Run: site/og/render.sh [page ...]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out="$here/../static/og"
pages=${*:-"front home plan"}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$out"
for page in $pages; do
  npx --yes playwright screenshot --viewport-size=1200,630 --wait-for-timeout=4000 \
    "file://$here/card.html?page=$page" "$tmp/$page.png" >"$tmp/playwright.log" 2>&1 || true
  size=$(sips -g pixelWidth -g pixelHeight "$tmp/$page.png" 2>/dev/null | awk '/pixel/ {printf "%s ", $2}')
  if [ "$size" != "1200 630 " ]; then
    echo "render.sh: no 1200x630 screenshot for $page (got '$size'); Playwright said:" >&2
    tail -5 "$tmp/playwright.log" >&2
    exit 1
  fi
  sips -s format jpeg -s formatOptions 90 "$tmp/$page.png" --out "$out/$page.jpg" >/dev/null
  echo "$out/$page.jpg"
done
