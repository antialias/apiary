#!/bin/sh
# Makes a page's animated card, static/og/<page>.gif, from a clip of its art.
#
# The clip, og/motion/<page>.mp4, is image-to-video from og/art/<page>.jpg, so its first frame is
# the still card's art. This lays the card's words (card.html?layer=words) over the clip, fades the
# last moments back into the first frame so the loop is seamless, and writes a GIF whose first
# frame is the still card. Needs ffmpeg, ffprobe and Google Chrome (for the words).
# Run: site/og/animate.sh <page> [width, default 720] [frames per second, default 10]
set -eu
page=$1
width=${2:-720}
here=$(cd "$(dirname "$0")" && pwd)
clip="$here/motion/$page.mp4"
out="$here/../static/og/$page.gif"
chrome=${CHROME:-"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}
fps=${3:-10}
fade=0.6
[ -f "$clip" ] || { echo "animate.sh: no clip at $clip" >&2; exit 1; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# The words, on a transparent 1200x630 page. Chrome on macOS often dies in teardown after writing
# the screenshot, so the check is the picture itself (as in render.sh).
"$chrome" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=1 \
  --window-size=1200,630 --virtual-time-budget=10000 --default-background-color=00000000 \
  --user-data-dir="$tmp/profile" --screenshot="$tmp/words.png" \
  "file://$here/card.html?page=$page&layer=words" >"$tmp/chrome.log" 2>&1 || true
size=$(sips -g pixelWidth -g pixelHeight "$tmp/words.png" 2>/dev/null | awk '/pixel/ {printf "%s ", $2}')
if [ "$size" != "1200 630 " ]; then
  echo "animate.sh: no 1200x630 words layer for $page (got '$size'); Chrome said:" >&2
  tail -5 "$tmp/chrome.log" >&2
  exit 1
fi

dur=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$clip")
offset=$(awk -v d="$dur" -v f="$fade" 'BEGIN { printf "%.3f", d - f }')
ffmpeg -v error -y -i "$clip" -frames:v 1 "$tmp/first.png"
# Cover 1200x630 the way the card's CSS does (scale to width, centre vertically).
# hqdn3d calms the clip's film grain, which otherwise changes every pixel of every frame and
# makes the GIF several times bigger.
cover="scale=1200:-2,crop=1200:630,setsar=1,fps=$fps,hqdn3d=3:3:16:16,format=yuv420p"
# The first frame, held only as long as the fade: xfade ends at offset plus its second input.
ffmpeg -v error -y -i "$clip" -loop 1 -t "$fade" -i "$tmp/first.png" -i "$tmp/words.png" -filter_complex "
  [0:v]$cover[clip];
  [1:v]$cover[first];
  [clip][first]xfade=transition=fade:duration=$fade:offset=$offset[looped];
  [looped][2:v]overlay=0:0,scale=$width:-2:flags=lanczos,split[a][b];
  [a]palettegen=max_colors=256:stats_mode=diff[pal];
  [b][pal]paletteuse=dither=bayer:bayer_scale=3:diff_mode=rectangle" -loop 0 "$out"
echo "$out ($(wc -c <"$out" | tr -d ' ') bytes, ${dur}s)"
