#!/bin/sh
# init.sh: first-time setup of Apiary's state, run once as
#
#     docker compose run --rm init
#
# It runs in the brain image with the `data` volume at /creds, and:
#   - writes /creds/switch_key and /creds/client_secret, 32 random bytes each, hex;
#   - seeds /creds/classes.json from the brain's classes.example.json, only when absent;
#   - makes /creds/proxy, where the proxy keeps its class state;
#   - prints the two secrets, this once.
# It overwrites nothing: if either secret is already there it changes nothing and exits 1.
#
# The proxy image runs as distroless's nonroot user (uid 65532), so the files it reads and
# the directory it writes are that user's. The brain runs as root and reads them too.
set -eu

DIR=${APIARY_STATE_DIR:-/creds}
EXAMPLE=${APIARY_CLASSES_EXAMPLE:-/app/classes.example.json}
PROXY_UID=65532

say() { printf '%s\n' "$*"; }
die() {
	say "init: $*" >&2
	exit 1
}

[ -d "$DIR" ] || die "$DIR is not a directory; run this as: docker compose run --rm init"

for f in switch_key client_secret; do
	if [ -e "$DIR/$f" ] || [ -L "$DIR/$f" ]; then
		die "$DIR/$f already exists, so Apiary is already initialised; nothing was changed.
To read the secrets again:
  docker compose exec brain cat /creds/client_secret
  docker compose exec brain cat /creds/switch_key"
	fi
done
[ -f "$EXAMPLE" ] || die "no classes example at $EXAMPLE; nothing was changed"

umask 077
set -C # noclobber: a file that appeared meanwhile is never overwritten

secret() { python3 -c 'import secrets; print(secrets.token_hex(32))'; }

write_secret() {
	secret >"$DIR/$1"
	chown "$PROXY_UID" "$DIR/$1"
	chmod 0400 "$DIR/$1"
}

write_secret switch_key
write_secret client_secret

if [ -e "$DIR/classes.json" ] || [ -L "$DIR/classes.json" ]; then
	classes="kept the classes.json already there"
else
	cat "$EXAMPLE" >"$DIR/classes.json"
	chmod 0600 "$DIR/classes.json"
	classes="seeded classes.json from the example: claude-fast, claude-capable, claude-frontier"
fi

if [ ! -d "$DIR/proxy" ]; then
	mkdir "$DIR/proxy"
fi
chown "$PROXY_UID" "$DIR/proxy"
chmod 0700 "$DIR/proxy"

say "Apiary is initialised: wrote switch_key and client_secret, $classes."
say ""
say "These are printed only now. They stay on the data volume (see the README to read them again)."
say ""
say "  Client secret: Claude Code's ANTHROPIC_AUTH_TOKEN, the proxy's only password"
say "    $(cat "$DIR/client_secret")"
say ""
say "  Switch key: unlocks the brain's /auth page, where accounts are added"
say "    $(cat "$DIR/switch_key")"
say ""
say "Next:"
say "  docker compose up -d"
say "  open http://127.0.0.1:9115/auth, paste the switch key when asked, and add an account"
