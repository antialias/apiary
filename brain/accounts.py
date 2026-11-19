#!/usr/bin/env python3
"""accounts (shared): manage a tenant's provider accounts from a terminal.

The brain serves no page. Scry's Sources page is where a household manages its
accounts; a self-hosted Apiary has no Scry, so this command does the same
things through the same brain endpoints Sources uses, and no others:

    GET  /t/<tenant>/auth/providers          the accounts and their health
    GET  /t/<tenant>/preferred               which are in use, and their notices
    POST /t/<tenant>/auth/<provider>/begin|complete|remove

It runs inside the brain's container, `docker compose exec brain accounts …`,
and reads what the brain reads: the switch key from SWITCH_KEY or
SWITCH_KEY_FILE, the tenant from SWITCH_CREDS_DIR/tenants.json (its one `lan`
record), the brain at 127.0.0.1:$PORT. It never prints the switch key, and a
pasted API key is read from the terminal with no echo (or from stdin when it
is not a terminal), never from the command line. Stdlib only.
"""
import argparse
import getpass
import json
import os
import sys
import time
import urllib.error
import urllib.request

USAGE = """accounts list
accounts add <provider> <name>          the API key is read from stdin, not shown
accounts sign-in claude|codex [name]    sign in with a Claude or ChatGPT login
accounts remove <provider> <name>

Run it in the brain's container: docker compose exec brain accounts list
(add -T to `exec` when piping a key in)."""

# Friendlier names for the two subscription providers; any provider id the
# brain lists is accepted as it is.
ALIASES = {"claude": "anthropic", "codex": "gpt"}
# How each shared provider picks among its accounts, in Sources' words where
# Sources has them (lib/copy/sources.ts: ruleKeys, rulePool). None of them
# follows an order a person sets: the brain keeps the account in use until it
# can't serve, and Anthropic API keys come after the Claude accounts, by name.
_STAYS = ("The account in use stays in use until it runs low or can't be used; "
          "then the least used of the others takes over.")
RULES = {
    "anthropic": ("Each lane stays on the account it is on until that one runs low or can't be "
                  "used; then the least used of the others takes over."),
    "gpt": _STAYS,
    "kimi": _STAYS,
    "anthropic-api": ("Your Claude subscriptions are used first while they have room; after them, "
                      "these keys are used in name order."),
}
BY_NAME = {"anthropic-api"}
WAIT_S = 90     # how long an add or a sign-in waits for its first check
STEP_S = 2.0    # between two looks
TIMEOUT_S = 30  # one request; a key's or a code's probe can take seconds


class Refused(Exception):
    """The brain said no, or could not be reached: its words, for the person."""


def _read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def switch_key():
    """The switch key, as the brain reads it, stripped. A key that still holds
    a control character or a line break is refused, in words that never
    carry it: it could not go in a header, and urllib's complaint would."""
    key = (os.environ.get("SWITCH_KEY", "") or _read_file(os.environ.get("SWITCH_KEY_FILE", ""))).strip()
    if not key:
        raise Refused("no switch key: SWITCH_KEY_FILE names no readable file")
    if any(ord(c) < 0x20 or ord(c) == 0x7f for c in key):
        raise Refused("the switch key holds a control character or a line break; fix the file init wrote")
    return key


def _tenant(creds_dir):
    path = os.path.join(creds_dir, "tenants.json")
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError) as e:
        raise Refused(f"cannot read {path} ({type(e).__name__}); run init first") from None
    lan = [r.get("tenant") for r in doc if isinstance(r, dict) and r.get("lan") is True]
    if len(lan) != 1 or not lan[0]:
        raise Refused(f"{path} names no single tenant; pass --tenant")
    return lan[0]


class Brain:
    """The brain's tenant routes, with the switch key on every request."""

    def __init__(self, base, tenant, key):
        self.base = base.rstrip("/") + f"/t/{tenant}"
        self.key = key

    def call(self, method, path, body=None, headers=None):
        """-> (status, JSON body or {}, response headers). Raises Refused when
        the brain can't be reached."""
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("X-Switch-Key", self.key)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
                status, raw, hdrs = r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            status, raw, hdrs = e.code, e.read(), e.headers
        except (urllib.error.URLError, OSError) as e:
            # The socket's reason ("Connection refused", "timed out") says why;
            # it is the OS's words, never the request's headers.
            why = e.reason if isinstance(e, urllib.error.URLError) else e
            why = " ".join(str(getattr(why, "strerror", None) or why).split())[:120]
            raise Refused(f"the brain at {self.base.split('/t/')[0]} did not answer "
                          f"({why or type(e).__name__}); is it up?") from None
        except Exception as e:  # noqa: BLE001 — never a traceback: it could carry a header
            raise Refused(f"the request to the brain failed ({type(e).__name__})") from None
        try:
            out = json.loads(raw.decode() or "{}")
        except ValueError:
            out = {}
        if status == 401:
            raise Refused("the brain refused the switch key")
        return status, out if isinstance(out, dict) else {}, hdrs

    def get(self, path):
        status, out, _ = self.call("GET", path)
        if status != 200:
            raise Refused(_said(out, f"GET {path} answered {status}"))
        return out

    def auth(self, pid, step, body):
        status, out, _ = self.call("POST", f"/auth/{pid}/{step}", body)
        if status != 200:
            raise Refused(_said(out, f"{step} answered {status}"))
        return out


def _said(out, fallback):
    errs = out.get("errors")
    if isinstance(errs, list) and errs:
        return "; ".join(map(str, errs))
    return str(out.get("error") or fallback)


# ---- reading -------------------------------------------------------------------

def _rows(provider):
    """The provider's account rows; the keyring's unreadable row is not one."""
    return [r for r in provider.get("accounts") or []
            if r.get("last_error_kind") != "unreadable" and not str(r.get("name", "")).endswith(":keyring")]


def in_use(pid, preferred):
    """The provider's accounts the brain names as in use: its pool's incumbent,
    or for the Claude accounts each lane's."""
    if pid in BY_NAME:
        return set()
    if pid == "anthropic":
        return {ln.get("account") for ln in (preferred.get("lanes") or {}).values()
                if isinstance(ln, dict) and ln.get("account")}
    acct = ((preferred.get("providers") or {}).get(pid) or {}).get("account")
    return {acct} if acct else set()


def ordered(pid, provider, preferred):
    """The provider's account names in the order its rule takes them: API keys
    by name; otherwise the ones in use first, then the rest in the brain's order."""
    names = [r["name"] for r in _rows(provider)]
    if pid in BY_NAME:
        return sorted(names)
    using = in_use(pid, preferred)
    return [n for n in names if n in using] + [n for n in names if n not in using]


def _key_ends(fingerprint):
    fp = fingerprint or ""
    return fp[-4:] if len(fp) == 11 and fp[4:7] == "..." else None


def status_word(row, preferred):
    """What the account is doing, in a few words."""
    name = row["name"]
    if row.get("health") == "auth_dead":
        return "refused: " + (row.get("err") or "its credential was refused")
    standing = (preferred.get("accounts") or {}).get(name) or {}
    notice = (((preferred.get("notices") or {}).get("accounts") or {}).get(name) or {}).get("state")
    if not row.get("poll_ok") and row.get("err"):
        return "failing: " + row["err"]
    if standing.get("as_of") is None and not row.get("good_ts"):
        return "not checked yet"
    if notice == "dry":
        return "out of credit"
    if row.get("health") == "degraded":
        return "serving (last check is stale)"
    return "serving"


# ---- the subcommands ------------------------------------------------------------

def _providers(brain):
    return (brain.get("/auth/providers").get("providers") or {})


def _pid(name, providers):
    pid = ALIASES.get(name, name)
    if pid not in providers:
        raise Refused(f"unknown provider {name!r}; the brain has: {', '.join(providers)}")
    return pid


def cmd_list(brain, out):
    providers = _providers(brain)
    preferred = brain.get("/preferred")
    for pid, p in providers.items():
        rows = {r["name"]: r for r in _rows(p)}
        out.write(f"{pid}  {p.get('label') or pid}\n")
        if RULES.get(pid):
            out.write(f"  {RULES[pid]}\n")
        for r in p.get("accounts") or []:
            if r["name"] not in rows:
                out.write(f"   ! {r.get('err') or r['name']}\n")
        names = ordered(pid, p, preferred)
        using = in_use(pid, preferred)
        if not names:
            out.write("   (no accounts)\n")
        width = max((len(n) for n in names), default=0)
        for i, name in enumerate(names, 1):
            ends = _key_ends(rows[name].get("key_fingerprint"))
            ends = f"…{ends}" if ends else "     "
            word = status_word(rows[name], preferred)
            if name in using and word == "serving":
                word = "serving, in use"
            out.write(f"  {i}. {name:<{width}}  {ends}  {word}\n")
    return 0


def _read_key(prompt):
    if sys.stdin.isatty():
        return getpass.getpass(prompt).strip()
    return sys.stdin.readline().strip()


def cmd_add(brain, out, provider, name):
    providers = _providers(brain)
    pid = _pid(provider, providers)
    flow = (providers[pid].get("enroll") or {}).get("flow")
    if flow != "paste-key":
        word = {"anthropic": "claude", "gpt": "codex"}.get(pid, pid)
        raise Refused(f"{pid} accounts are signed in, not pasted: accounts sign-in {word}")
    key = _read_key(f"{providers[pid].get('label') or pid} key for {name} (not shown): ")
    if not key:
        raise Refused("no key was given; nothing was saved")
    begun = brain.auth(pid, "begin", {})
    done = brain.auth(pid, "complete", {"state": begun.get("state"), "name": name, "api_key": key})
    out.write(f"{'replaced the key of' if done.get('reauthorized') else 'added'} {pid} account {done.get('account') or name}\n")
    return _await_check(brain, out, pid, done.get("account") or name, done.get("saved_at"))


def _ask(out, prompt):
    out.write(prompt)
    out.flush()
    line = sys.stdin.readline()
    if not line:
        raise Refused("nothing was pasted; nothing was saved")
    return line.strip()


def cmd_sign_in(brain, out, which, name):
    providers = _providers(brain)
    pid = ALIASES.get(which)
    if pid is None:
        raise Refused("sign-in is for claude or codex")
    if pid not in providers:
        raise Refused(f"this brain has no {which} provider")
    p = providers[pid]
    existing = {r["name"] for r in _rows(p)}
    if pid == "gpt":
        out.write("Warning: this ChatGPT login's refresh grant rotates strictly. Once this sign-in\n"
                  "finishes, the brain must be its only refresher: refreshing the same grant anywhere\n"
                  "else, such as a laptop's `codex login`, kills it. Sign in here with a login you\n"
                  "don't use elsewhere, or sign that one in again on the laptop afterwards and lose\n"
                  "it here.\n\n")
        if not name:
            name = _ask(out, "A name for this login (lowercase, e.g. gpt-b): ")
    elif name and name not in existing:
        raise Refused(f"{name!r} is not a Claude account here; a new one is named by its email, "
                      "so leave the name off to add one")
    begun = brain.auth(pid, "begin", {"account": name if name in existing else None})
    instructions = (p.get("enroll") or {}).get("instructions") or ""
    out.write(("Signing in " + name + " again.\n" if name in existing else ""))
    out.write(f"{instructions}\n\nOpen this link:\n\n  {begun.get('authorize_url')}\n\n")
    out.flush()
    if pid == "gpt":
        fields = {"name": name, "redirect_url": _ask(out, "Paste the full localhost URL: ")}
    else:
        fields = {"code": _ask(out, "Paste the code: ")}
    done = brain.auth(pid, "complete", dict(fields, state=begun.get("state")))
    account = done.get("account") or name
    out.write(f"{'signed in again' if done.get('reauthorized') else 'added'}: {pid} account {account}\n")
    if done.get("note"):
        out.write(f"{done['note']}\n")
    return _await_check(brain, out, pid, account, done.get("saved_at"))


def cmd_remove(brain, out, provider, name):
    pid = _pid(provider, _providers(brain))
    done = brain.auth(pid, "remove", {"account": name})
    out.write(f"removed {pid} account {done.get('account') or name}\n")
    return 0


def _await_check(brain, out, pid, account, saved_at):
    """Wait up to WAIT_S for the account's first check after the save: a good
    reading taken after saved_at (the brain's complete answer; every poll
    stamps its time before it reads a credential, so only the new one's
    readings are later), or one more failed poll than it had just after the
    save. Says whether it serves."""
    if not isinstance(saved_at, (int, float)):
        raise Refused("the brain's answer carries no saved_at; is the brain older than this command?")
    out.write(f"waiting up to {WAIT_S} s for its first check…\n")
    out.flush()
    base = None
    deadline = time.monotonic() + WAIT_S
    while True:
        p = _providers(brain).get(pid) or {}
        row = next((r for r in _rows(p) if r["name"] == account), None)
        if row is not None:
            if base is None:
                base = row.get("consecutive_failures") or 0
            if (row.get("consecutive_failures") or 0) > base:
                out.write(f"{account} does not serve: {row.get('err') or 'its check failed'}\n")
                return 1
            good = row.get("good_ts")
            if isinstance(good, (int, float)) and good > saved_at:
                word = status_word(row, brain.get("/preferred"))
                out.write(f"{account} {'serves' if word == 'serving' else 'was checked: ' + word}\n")
                return 0 if word == "serving" else 1
        if time.monotonic() >= deadline:
            out.write(f"{account} was not checked within {WAIT_S} s; `accounts list` shows it when it is\n")
            return 1
        time.sleep(STEP_S)


def main(argv=None, out=None):
    out = out or sys.stdout
    ap = argparse.ArgumentParser(prog="accounts", usage=USAGE, add_help=True)
    ap.add_argument("--tenant", help="the tenant (default: tenants.json's one lan tenant)")
    ap.add_argument("--brain", default=f"http://127.0.0.1:{os.environ.get('PORT', '9115')}",
                    help="the brain's address (default: this container's)")
    ap.add_argument("command", nargs="?")
    ap.add_argument("args", nargs="*")
    a = ap.parse_args(argv)
    shape = {"list": (0, 0), "add": (2, 2), "sign-in": (1, 2), "remove": (2, 2)}
    if a.command not in shape:
        sys.stderr.write(USAGE + "\n")
        return 2
    lo, hi = shape[a.command]
    if len(a.args) < lo or (hi is not None and len(a.args) > hi):
        sys.stderr.write(USAGE + "\n")
        return 2
    try:
        key = switch_key()
        tenant = a.tenant or _tenant(os.environ.get("SWITCH_CREDS_DIR", "/creds"))
        brain = Brain(a.brain, tenant, key)
        if a.command == "list":
            return cmd_list(brain, out)
        if a.command == "add":
            return cmd_add(brain, out, *a.args)
        if a.command == "sign-in":
            return cmd_sign_in(brain, out, a.args[0], a.args[1] if len(a.args) > 1 else None)
        return cmd_remove(brain, out, *a.args)
    except Refused as e:
        sys.stderr.write(f"accounts: {e}\n")
        return 1
    except KeyboardInterrupt:
        sys.stderr.write("\naccounts: stopped; nothing more was sent\n")
        return 130


if __name__ == "__main__":
    sys.exit(main())
