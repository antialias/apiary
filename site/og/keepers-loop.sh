#!/bin/sh
# Makes a keeper's hive-cam loop, static/og/keepers/<name>.mp4, from its clip.
#
# The clip, og/motion/keepers/<name>.mp4, is a five-second image-to-video clip made with the keeper's
# photo (static/og/keepers/<name>.webp) as both its first and its last frame, so it ends where it
# began, and the page shows the photo as the poster.
#
# The model's last frame is close to the first but not identical (a shade darker, the grass a little
# different), so this spreads the difference across the clip: frame n gets n/(frames-1) of
# (first - last), blurred, added. Blurred, it carries only the drift in light and colour, never a
# ghost of a pose or a bee, and the last frame then matches the first in everything but small
# detail, which differs no more than one frame does from the next. Then it drops that last frame,
# so the loop runs straight from the second-last frame back to the first, with no blend and no jump.
#
# The model also eases into and out of the keyframe: the opening and closing frames barely move. On a
# clip with motion that reads as a pause at every restart, so it drops them: after the first frame,
# and before the last, each frame that moves less than a quarter of the clip's typical frame-to-frame
# change (at most a dozen at each end). A clip that is still throughout loses only frozen frames.
#
# Last, it holds the grain still: each pixel is averaged with up to four seconds of its neighbours
# in time, stopping where it really changes, with the loop wrapped round so the first frames see the
# last ones. Otherwise the encoder, which carries the grain forward from the opening keyframe, shows
# a faint shimmer each time the loop restarts and that keyframe comes round again.
#
# The result is small (640 square, no sound): on the page it fills a screen a few hundred pixels
# wide, under scan lines. Needs ffmpeg, ffprobe.
# Run: site/og/keepers-loop.sh <name>
set -eu
name=$1
here=$(cd "$(dirname "$0")" && pwd)
clip="$here/motion/keepers/$name.mp4"
out="$here/../static/og/keepers/$name.mp4"
[ -f "$clip" ] || { echo "keepers-loop.sh: no clip at $clip" >&2; exit 1; }
n=$(ffprobe -v error -select_streams v:0 -count_frames -show_entries stream=nb_read_frames -of csv=p=0 "$clip")
last=$((n - 1))
# How much each frame moves from the one before (mean level change at 240 square, grey), then how
# many barely-moving frames to drop after the first (lead) and before the last (trail).
moves=$(mktemp); trap 'rm -f "$moves"' EXIT
ffmpeg -v error -i "$clip" -vf "scale=240:240,format=gray,tblend=all_mode=difference,signalstats,metadata=print:key=lavfi.signalstats.YAVG:file=$moves" -f null -
sed -n 's/.*YAVG=//p' "$moves" > "$moves.v"; mv "$moves.v" "$moves"
typical=$(sort -g "$moves" | awk '{a[NR] = $1} END {print a[int((NR + 1) / 2)]}')
set -- $(awk -v slow="$typical" '{v[NR] = $1} END {
  slow /= 4; lead = 0; while (lead < 12 && v[lead + 1] < slow) lead++
  trail = 0; while (trail < 12 && v[NR - trail] < slow) trail++
  print lead, trail }' "$moves")
lead=$1 trail=$2
keep=$((last - lead - (trail > 0 ? trail - 1 : 0)))
ramp="clip(N/$last,0,1)"
ffmpeg -v error -y -i "$clip" -filter_complex "
  [0:v]setpts=N/24/TB,scale=640:640:flags=lanczos,format=gbrp,split=3[c][a][b];
  [a]trim=end_frame=1,loop=loop=-1:size=1,setpts=N/24/TB,trim=end_frame=$n,split[f0a][f0b];
  [b]trim=start_frame=$last,setpts=PTS-STARTPTS,loop=loop=-1:size=1,setpts=N/24/TB,trim=end_frame=$n,split[fla][flb];
  [f0a][fla]blend=all_mode=subtract,gblur=sigma=24[up];
  [flb][f0b]blend=all_mode=subtract,gblur=sigma=24[down];
  [c][up]blend=all_expr='clip(A+B*$ramp,0,255)'[c1];
  [c1][down]blend=all_expr='clip(A-B*$ramp,0,255)',trim=end_frame=$last,
    select='not(between(n,1,$lead))*not(between(n,$((last - trail + 1)),$((last - 1))))',
    setpts=N/24/TB,split=3[l1][l2][l3];
  [l1]trim=start_frame=$((keep - 60)),setpts=PTS-STARTPTS[tail];
  [l3]trim=end_frame=60,setpts=PTS-STARTPTS[head];
  [tail][l2][head]concat=n=3:v=1,setpts=N/24/TB,
    atadenoise=0a=0.045:0b=0.3:1a=0.045:1b=0.3:2a=0.045:2b=0.3:s=99,
    trim=start_frame=60:end_frame=$((60 + keep)),setpts=N/24/TB,format=yuv420p[v]" \
  -map "[v]" -an -r 24 -c:v libx264 -profile:v high -preset slow -crf 26 -movflags +faststart "$out"
echo "$out: dropped $lead + $((keep < last - lead ? last - lead - keep : 0)) slow frames; $(ffprobe -v error -show_entries format=duration,size -of csv=p=0 "$out")"
