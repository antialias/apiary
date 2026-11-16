"""The Anthropic API driver (shared): accounts that are static API keys, each
billed per token by its own console organization. A key is enrolled on the
/auth page (a name and the key), kept in ANTHROPIC_API_CREDS_FILE, and
dispensed to the routing proxy by GET /api-key?account=. Its accounts serve
the lanes beside the account loop's (`lanes`): the proxy sends a lane's
request on one with x-api-key.

There is no usage endpoint to poll for a key's spend, so the reading is an
open, unmetered allowance, like the local model's: the poll (a free GET
/v1/models) proves the key still works, and that is the whole answer."""
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request

import claude_usage_exporter as core
from providers import register

PROVIDER = "anthropic-api"
ANTHROPIC_API_CREDS_FILE = os.environ.get("ANTHROPIC_API_CREDS_FILE", "/creds/anthropic-api.json")
ANTHROPIC_API_BASE = os.environ.get("ANTHROPIC_API_BASE", "https://api.anthropic.com").rstrip("/")
ANTHROPIC_VERSION = "2023-06-01"
POT = "balance"

# A name is a roster key like any other account's: what /api-key, the routes
# and the standings name it by.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@+-]{0,63}$")

# The enrolled keys, mtime-cached so an out-of-band edit of the file is adopted
# at the next read. _keys: None = not loaded or no file.
_keys_lock = threading.Lock()
_keys = None
_keys_mtime = None


def _load():
    """{name: key} from the creds file, in file order; {} without one. A file
    that is not a list of {name, api_key} reads as no keys, and says so."""
    global _keys, _keys_mtime
    with _keys_lock:
        try:
            mtime = os.stat(ANTHROPIC_API_CREDS_FILE).st_mtime
        except OSError:
            _keys, _keys_mtime = None, None
            return {}
        if _keys is None or mtime != _keys_mtime:
            keys = {}
            try:
                with open(ANTHROPIC_API_CREDS_FILE) as f:
                    raw = json.load(f)
                for e in raw if isinstance(raw, list) else []:
                    if isinstance(e, dict) and isinstance(e.get("name"), str) \
                            and isinstance(e.get("api_key"), str) and e["api_key"].strip():
                        keys[e["name"]] = e["api_key"].strip()
            except Exception as e:  # noqa: BLE001
                core.log(f"WARN {ANTHROPIC_API_CREDS_FILE} unreadable: {type(e).__name__}")
            _keys, _keys_mtime = keys, mtime
        return dict(_keys)


def _persist(keys):
    """Write {name: key} atomically, readable by its owner alone. Raises
    ValueError (operator-readable) on I/O failure."""
    global _keys, _keys_mtime
    body = json.dumps([{"name": n, "api_key": k} for n, k in keys.items()], indent=1) + "\n"
    with _keys_lock:
        tmp = ANTHROPIC_API_CREDS_FILE + ".tmp"
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(body)
            os.chmod(tmp, 0o600)
            os.replace(tmp, ANTHROPIC_API_CREDS_FILE)
        except Exception as e:  # noqa: BLE001
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise ValueError(f"cannot write {ANTHROPIC_API_CREDS_FILE}: {e}") from e
        _keys, _keys_mtime = dict(keys), os.stat(ANTHROPIC_API_CREDS_FILE).st_mtime


def _names():
    return list(_load())


def _get_models(key):
    """GET /v1/models: free, and refused with 401 for a key that no longer works."""
    req = urllib.request.Request(
        ANTHROPIC_API_BASE + "/v1/models",
        headers={"x-api-key": key, "anthropic-version": ANTHROPIC_VERSION,
                 "Accept": "application/json"})
    with core.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _reading(as_of):
    """An api account's §1.5 reading: an open allowance with no windows,
    marked unmetered (no endpoint reports what a key may still spend), and its
    money pot shut to the dial check, which never draws it: the key's spend is
    the allowance itself."""
    balance = {"id": POT, "type": "money", "draw": 2, "unit": None, "unit_usd": None,
               "entitlement": {"state": "none", "basis": "derived"},
               "open": False, "open_basis": "derived", "limits": []}
    return {"provider": PROVIDER, "kind": "api", "as_of": as_of,
            "source": "GET /v1/models", "ok": True,
            "plan": {"label": "API key", "basis": "derived"},
            "pots": [core._allowance_pot([], open_=True, open_basis="derived", unmetered=True),
                     balance],
            "beside": [], "models": {}}


def poll(prev=None, account=None):  # noqa: ARG001 — uniform pool dispatch (#71)
    """One account's poll result (never raises), shaped as every provider's."""
    out = {"name": account, "kind": "provider", "provider": PROVIDER,
           "ok": 0, "ts": time.time(), "good_ts": None, "stale": 0,
           "http": 0, "err": "", "retry_after": None, "reading": None}
    key = _load().get(account)
    if not key:
        out["err"] = f"no key for {account!r} in {ANTHROPIC_API_CREDS_FILE}"
        return out
    try:
        _get_models(key)
    except urllib.error.HTTPError as e:
        out["http"] = e.code
        out["err"] = f"models HTTP {e.code}"
        out["retry_after"] = core._retry_after(e)
        return out
    except Exception as e:  # noqa: BLE001
        out["err"] = f"models {type(e).__name__}"
        return out
    out["reading"] = _reading(out["ts"])
    out["ok"] = 1
    return out


def _auth_status(now):
    """One row per enrolled key: its poll health and fingerprint, never the key."""
    keys = _load()
    rows = core._account_health(list(keys), now)["accounts"]
    for r in rows:
        r["source"] = "file"
        r["key_fingerprint"] = core._key_fingerprint(keys.get(r["name"], ""))
    return rows


def _auth_begin(account=None):
    """A bare one-shot state: there is no authorization to visit."""
    return None, core._auth_grant_mint(PROVIDER, "reauth" if account else "add", account=account)


def _name_error(name, keys):
    """Why `name` cannot be this provider's account, or None. A name another
    provider's target or the account loop already holds would be two accounts
    under one roster key."""
    if not _NAME_RE.match(name):
        return "name must be 1-64 letters, digits or ._@+- and start with a letter or digit"
    if name in keys:
        return None
    if name in core.PROVIDERS or name in core._account_loop_names():
        return f"{name!r} already names another account"
    taken = core._provider_poll_targets().get(name)
    if taken and taken[0] != PROVIDER:
        return f"{name!r} already names a {taken[0]} account"
    return None


def _auth_complete(grant, fields):
    name = str(fields.get("name") or grant.get("account") or "").strip()
    key = str(fields.get("api_key") or "").strip()
    if not name:
        raise ValueError("name is required")
    if not key:
        raise ValueError("api_key is required")
    keys = _load()
    err = _name_error(name, keys)
    if err:
        raise ValueError(err + " — nothing was saved")
    # Validate BEFORE persist. The error carries the code and the fingerprint,
    # never the key or upstream prose that could echo it.
    try:
        _get_models(key)
    except urllib.error.HTTPError as e:
        raise ValueError(f"key validation HTTP {e.code} (fingerprint "
                         f"{core._key_fingerprint(key)}) — nothing was saved") from e
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"key validation {type(e).__name__} — nothing was saved") from e
    replaced = name in keys
    keys[name] = key
    _persist(keys)
    core.log(f"{PROVIDER} key {'replaced' if replaced else 'enrolled'} for {name} "
             f"(fingerprint {core._key_fingerprint(key)})")
    with core._poll_health_lock:
        core._poll_fails.pop(name, None)
        core._poll_schedule.pop(name, None)  # due at once
    core._poll_wakeup.set()
    return {"account": name, "enrolled": True, "reauthorized": replaced}


register(PROVIDER, {
    "poll": poll,
    "enabled": lambda: bool(_load()),
    "accounts": _names,
    "api_key": lambda account: _load().get(account, ""),
    "api_pot": POT,
    # Its accounts serve the lanes beside the account loop's (_routes).
    "lanes": True,
    "auth": {
        "kind": "static-key",
        "label": "Anthropic API",
        "custody": {"rotation": "none",
                    "note": "Static API keys, held by this brain alone: the routing "
                            "proxy is dispensed each one by GET /api-key."},
        "enroll": {"flow": "paste-key", "can_add": True,
                   "instructions": "Name the account and paste its API key. The key is "
                                   "validated with a model list before anything is saved; "
                                   "a name already enrolled has its key replaced.",
                   "fields": [{"name": "name", "label": "Account name", "secret": False},
                              {"name": "api_key", "label": "API key", "secret": True}]},
        "status": _auth_status,
        "begin": _auth_begin,
        "complete": _auth_complete,
    },
})
