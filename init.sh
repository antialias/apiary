#!/bin/sh
# init.sh: first-time setup of Apiary's state, run once as
#
#     docker compose run --rm init
#
# It runs in the brain image with the `data` volume at /creds, and:
#   - writes /creds/switch_key and /creds/client_secret, 32 random bytes each, hex;
#   - writes /creds/key, the brain's sealing key (credstore.py: every tenant file is AES-256-GCM
#     sealed with it; the brain refuses to start on a plain one), 32 random bytes, hex, 0400;
#   - writes /creds/tenants.json with this machine's one tenant (tenancy.py: a fresh ten_ id, lan,
#     content tap), whose data the brain keeps under /creds/<tenant>/ and serves at /t/<tenant>/…;
#   - seeds the tenant's classes document, /creds/<tenant>/classes.json, from the brain's
#     classes.example.json (plain: it holds no secret, and you edit it by hand); its classes are
#     marked onboarding, so a member who joins starts with a copy;
#   - makes /creds/proxy, where the proxy keeps its class state;
#   - prints the two secrets and the tenant id, this once (the id is not a secret).
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

for f in switch_key client_secret key tenants.json; do
	if [ -e "$DIR/$f" ] || [ -L "$DIR/$f" ]; then
		die "$DIR/$f already exists, so Apiary is already initialised; nothing was changed.
To read the secrets and the tenant id again:
  docker compose exec brain cat /creds/client_secret
  docker compose exec brain cat /creds/switch_key
  docker compose exec brain cat /creds/tenants.json"
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
# The sealing key is the brain's alone (it runs as root); the proxy never opens a sealed file.
secret >"$DIR/key"
chmod 0400 "$DIR/key"

TENANT=$(python3 -c 'import tenancy; print(tenancy.new_tenant_id())')
printf '[{"tenant": "%s", "name": "apiary", "lan": true, "tap": "content"}]\n' "$TENANT" >"$DIR/tenants.json"
chmod 0600 "$DIR/tenants.json"
mkdir "$DIR/$TENANT"
chmod 0700 "$DIR/$TENANT"

cat "$EXAMPLE" >"$DIR/$TENANT/classes.json"
chmod 0600 "$DIR/$TENANT/classes.json"

if [ ! -d "$DIR/proxy" ]; then
	mkdir "$DIR/proxy"
fi
chown "$PROXY_UID" "$DIR/proxy"
chmod 0700 "$DIR/proxy"

say "Apiary is initialised: wrote switch_key, client_secret, the sealing key and tenants.json, and seeded the tenant's classes.json from the example: claude-fast, claude-capable, claude-frontier."
say ""
say "These are printed only now. They stay on the data volume (see the README to read them again)."
say ""
say "  Client secret: Claude Code's ANTHROPIC_AUTH_TOKEN, the proxy's only password"
say "    $(cat "$DIR/client_secret")"
say ""
say "  Switch key: the brain's key; the accounts command reads it from the volume itself"
say "    $(cat "$DIR/switch_key")"
say ""
say "  Tenant: this machine's one tenant; every brain route is under /t/<tenant>/"
say "    $TENANT"
say ""
say "Next:"
say "  docker compose up -d"
say "  docker compose exec brain accounts sign-in claude    (or: accounts add anthropic-api <name>)"
