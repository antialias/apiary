#!/usr/bin/env bash
# smoke/run.sh: the compose smoke test (Actions job compose-smoke). Run from the repository root:
#
#     bash smoke/run.sh
#
# It builds both images from this checkout, runs init, brings the stack up with
# docker-compose.smoke.yml (a stub Anthropic API in the compose network) and checks:
#   1. init refuses a second run;
#   2. the proxy's /_switch/healthz and the brain's /t/<tenant>/preferred answer, the tenant the
#      one init wrote to /creds/tenants.json;
#   3. GET /v1/models without the client secret is refused, and with it lists the three classes;
#   4. the brain serves no page: /t/<tenant>/auth is 404;
#   5. an anthropic-api key added with the accounts command (`accounts add`, the key on stdin) is
#      checked and serves, `accounts list` shows it with its key's last four characters, and it
#      serves a claude-fast request: 200, and the stub saw that key as x-api-key and no
#      Authorization header;
#   6. neither image holds a household path.
# Nothing reaches the Anthropic API or any network beyond the compose one. Containers, volumes,
# networks and the two local images are removed on exit, pass or fail.
set -euo pipefail

PROJECT=apiary-smoke
DC=(docker compose -p "$PROJECT" -f docker-compose.yml -f docker-compose.smoke.yml)
PROXY=http://127.0.0.1:8787
BRAIN=http://127.0.0.1:9115
STUB=http://127.0.0.1:18080
KEY_NAME=smoke-key
API_KEY=made-up-smoke-key-$RANDOM$RANDOM

fail() {
	echo "smoke: FAILED: $*" >&2
	echo "---- logs" >&2
	"${DC[@]}" logs --no-color --tail 80 >&2 || true
	exit 1
}
step() { echo "==> $*"; }

cleanup() {
	"${DC[@]}" --profile init down -v --remove-orphans >/dev/null 2>&1 || true
	docker image rm apiary-smoke/brain:local apiary-smoke/proxy:local >/dev/null 2>&1 || true
}
trap cleanup EXIT

# json EXPR: evaluate a Python expression over the JSON document on stdin (bound to d).
json() { python3 -c 'import json, sys; d = json.load(sys.stdin); print(eval(sys.argv[1]))' "$1"; }

# wait_for WHAT CMD...: retry CMD for up to 60 s.
wait_for() {
	local what=$1
	shift
	for _ in $(seq 60); do
		if "$@" >/dev/null 2>&1; then return 0; fi
		sleep 1
	done
	fail "$what did not happen within 60 s"
}

step "build both images from this checkout"
"${DC[@]}" build

step "init"
"${DC[@]}" run --rm init
if "${DC[@]}" run --rm init >/dev/null 2>&1; then
	fail "a second init ran; it must refuse to overwrite"
fi

step "up -d"
"${DC[@]}" up -d
SECRET=$("${DC[@]}" exec -T brain cat /creds/client_secret)
SWITCH_KEY=$("${DC[@]}" exec -T brain cat /creds/switch_key)
[ "${#SECRET}" -eq 64 ] && [ "${#SWITCH_KEY}" -eq 64 ] || fail "init's secrets are not 32 bytes of hex"
TENANT=$("${DC[@]}" exec -T brain cat /creds/tenants.json | json 'd[0]["tenant"]')
[ "${#TENANT}" -eq 30 ] || fail "init's tenants.json names no tenant"
BRAIN_T="$BRAIN/t/$TENANT"

step "the proxy's healthz and the brain answer"
wait_for "proxy /_switch/healthz" curl -fsS "$PROXY/_switch/healthz"
wait_for "brain /t/<tenant>/preferred" curl -fsS "$BRAIN_T/preferred"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BRAIN/preferred")
[ "$code" = 404 ] || fail "the brain's bare /preferred answered $code, not 404: a route without a tenant is refused"

step "GET /v1/models: refused without the client secret, the three classes with it"
code=$(curl -s -o /dev/null -w '%{http_code}' "$PROXY/v1/models")
[ "$code" = 401 ] || fail "GET /v1/models without the client secret answered $code, not 401"
models_ok() {
	curl -fsS -H "Authorization: Bearer $SECRET" "$PROXY/v1/models" |
		json '{"claude-fast", "claude-capable", "claude-frontier"} <= {m["id"] for m in d["data"]}' |
		grep -qx True
}
wait_for "the three classes in /v1/models" models_ok

step "the brain serves no page"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "X-Switch-Key: $SWITCH_KEY" "$BRAIN_T/auth")
[ "$code" = 404 ] || fail "the brain's /t/<tenant>/auth answered $code, not 404: it serves no page"

step "add a made-up anthropic-api key with the accounts command"
ADDED=$(printf '%s\n' "$API_KEY" | "${DC[@]}" exec -T brain accounts add anthropic-api "$KEY_NAME") ||
	fail "accounts add did not finish with a key that serves: $ADDED"
printf '%s\n' "$ADDED"
printf '%s\n' "$ADDED" | grep -qx "added anthropic-api account $KEY_NAME" || fail "accounts add did not add the key"
printf '%s\n' "$ADDED" | grep -qx "$KEY_NAME serves" || fail "accounts add did not say the key serves"
case $ADDED in *"$API_KEY"* | *"$SWITCH_KEY"*) fail "accounts add printed a key" ;; esac
LISTED=$("${DC[@]}" exec -T brain accounts list) || fail "accounts list failed: $LISTED"
printf '%s\n' "$LISTED"
printf '%s\n' "$LISTED" | grep -qx "  Your Claude subscriptions are used first while they have room; after them, these keys are used in name order\." ||
	fail "accounts list does not say how API keys are picked"
printf '%s\n' "$LISTED" | grep -qx "  1\. $KEY_NAME  …${API_KEY: -4}  serving" ||
	fail "accounts list does not show $KEY_NAME serving with its key's last four"
case $LISTED in *"$API_KEY"* | *"$SWITCH_KEY"*) fail "accounts list printed a key" ;; esac
# ...and it landed sealed: the file opens only through the brain's store.
"${DC[@]}" exec -T brain head -c 16 "/creds/$TENANT/anthropic-api.json" | grep -q 'switch-sealed/1' ||
	fail "the enrolled key was not written sealed"

step "a claude-fast request through the proxy reaches the stub with that key"
message() {
	[ "$(curl -s -o "$REPLY" -w '%{http_code}' -X POST \
		-H "Authorization: Bearer $SECRET" -H 'Content-Type: application/json' \
		-H 'anthropic-version: 2023-06-01' \
		-d '{"model": "claude-fast", "max_tokens": 16, "messages": [{"role": "user", "content": "hi"}]}' \
		"$PROXY/v1/messages")" = 200 ]
}
REPLY=$(mktemp)
for _ in $(seq 60); do
	if message; then break; fi
	sleep 1
done
message || fail "POST /v1/messages did not answer 200 within 60 s; the last reply: $(cat "$REPLY")"
seen=$(curl -fsS "$STUB/_seen")
printf '%s' "$seen" | API_KEY=$API_KEY python3 -c '
import json, os, sys
calls = [c for c in json.load(sys.stdin) if c["method"] == "POST" and c["path"].startswith("/v1/messages")]
if not calls:
    sys.exit("the stub saw no POST /v1/messages")
h = calls[-1]["headers"]
if h.get("x-api-key") != os.environ["API_KEY"]:
    sys.exit("the stub did not see the enrolled key as x-api-key")
if "authorization" in h:
    sys.exit("the stub saw an Authorization header")
print("stub saw x-api-key = the enrolled key, and no Authorization")
' || fail "the request did not reach the stub as an api-key request"

step "no household path in either image"
for image in apiary-smoke/brain:local apiary-smoke/proxy:local; do
	cid=$(docker create "$image")
	paths=$(docker export "$cid" | tar -t)
	docker rm "$cid" >/dev/null
	if printf '%s\n' "$paths" | grep -E '(^|/)household(/|$)|_household' >&2; then
		fail "$image holds a household path"
	fi
done

echo "smoke: OK"
