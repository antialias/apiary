#!/bin/sh
# Renders each page's link-preview card (card.html) to static/og/<page>.jpg, 1200x630.
# Needs Google Chrome and the network (the fonts come from Google Fonts). Run: site/og/render.sh [page ...]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out="$here/../static/og"
chrome=${CHROME:-"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}
pages=${*:-"home plan"}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$out"
for page in $pages; do
  "$chrome" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=1 \
    --window-size=1200,630 --virtual-time-budget=10000 --user-data-dir="$tmp/profile" \
    --screenshot="$tmp/$page.png" "file://$here/card.html?page=$page" >"$tmp/chrome.log" 2>&1 || true
  # Chrome on macOS often writes the screenshot and then dies in teardown ("Teardown watchdog
  # expired"), exiting non-zero. So the check is the picture itself: written, and 1200x630.
  size=$(sips -g pixelWidth -g pixelHeight "$tmp/$page.png" 2>/dev/null | awk '/pixel/ {printf "%s ", $2}')
  if [ "$size" != "1200 630 " ]; then
    echo "render.sh: no 1200x630 screenshot for $page (got '$size'); Chrome said:" >&2
    tail -5 "$tmp/chrome.log" >&2
    exit 1
  fi
  sips -s format jpeg -s formatOptions 90 "$tmp/$page.png" --out "$out/$page.jpg" >/dev/null
  echo "$out/$page.jpg"
done
