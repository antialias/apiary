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
ANTHROPIC_API_CREDS_FILE = "anthropic-api.json"  # in each tenant's dir
ANTHROPIC_API_BASE = os.environ.get("ANTHROPIC_API_BASE", "https://api.anthropic.com").rstrip("/")
ANTHROPIC_VERSION = "2023-06-01"
POT = "balance"

# A name is a roster key like any other account's: what /api-key, the routes
# and the standings name it by.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@+-]{0,63}$")

# The enrolled keys, mtime-cached so an out-of-band edit of the file is adopted
# at the next read. keys: None = not loaded or no file.
class _Store:
    """One tenant's enrolled keys (tenant.stores["anthropic-api"], _st)."""

    def __init__(self, t):
        self.file = t.path(ANTHROPIC_API_CREDS_FILE)
        self.keys_lock = threading.Lock()
        # Held from read to write by every edit of the file (enrol, remove), so
        # an edit never writes back a copy another edit has since changed.
        self.edit_lock = threading.Lock()
        self.keys = None
        self.keys_mtime = None


def _st(t):
    return t.store_for(PROVIDER, _Store)


def _load(t):
    """{name: key} from the creds file, in file order; {} without one. A file
    that is not a list of {name, api_key} reads as no keys, and says so."""
    with _st(t).keys_lock:
        mtime = core._store.mtime(_st(t).file)
        if mtime is None:
            _st(t).keys, _st(t).keys_mtime = None, None
            return {}
        if _st(t).keys is None or mtime != _st(t).keys_mtime:
            keys = {}
            try:
                raw = core._store.read_json(_st(t).file)
                for e in raw if isinstance(raw, list) else []:
                    if isinstance(e, dict) and isinstance(e.get("name"), str) \
                            and isinstance(e.get("api_key"), str) and e["api_key"].strip():
                        keys[e["name"]] = e["api_key"].strip()
            except Exception as e:  # noqa: BLE001
                core.log(f"WARN {_st(t).file} unreadable: {type(e).__name__}")
            _st(t).keys, _st(t).keys_mtime = keys, mtime
        return dict(_st(t).keys)


def _persist(t, keys):
    """Write {name: key} atomically, readable by its owner alone. Raises
    ValueError (operator-readable) on I/O failure."""
    doc = [{"name": n, "api_key": k} for n, k in keys.items()]
    with _st(t).keys_lock:
        try:
            core._store.write_json(_st(t).file, doc, indent=1)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"the keys could not be stored ({type(e).__name__}) — nothing was changed") from e
        _st(t).keys, _st(t).keys_mtime = dict(keys), core._store.mtime(_st(t).file)


def _names(t):
    return list(_load(t))


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


def poll(t, prev=None, account=None):  # noqa: ARG001 — uniform pool dispatch (#71)
    """One account's poll result (never raises), shaped as every provider's."""
    out = {"name": account, "kind": "provider", "provider": PROVIDER,
           "ok": 0, "ts": time.time(), "good_ts": None, "stale": 0,
           "http": 0, "err": "", "retry_after": None, "reading": None}
    key = _load(t).get(account)
    if not key:
        out["err"] = f"no key for {account!r} in {_st(t).file}"
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


def _auth_status(t, now):
    """One row per enrolled key: its poll health and fingerprint, never the key."""
    keys = _load(t)
    rows = core._account_health(t, list(keys), now)["accounts"]
    for r in rows:
        r["source"] = "file"
        r["key_fingerprint"] = core._key_fingerprint(keys.get(r["name"], ""))
    return rows


def _auth_begin(t, account=None):
    """A bare one-shot state: there is no authorization to visit."""
    return None, core._auth_grant_mint(t, PROVIDER, "reauth" if account else "add", account=account)


def _name_error(t, name, keys):
    """Why `name` cannot be this provider's account, or None. A name another
    provider's target or the account loop already holds would be two accounts
    under one roster key."""
    if not _NAME_RE.match(name):
        return "name must be 1-64 letters, digits or ._@+- and start with a letter or digit"
    if name in keys:
        return None
    if name in core.PROVIDERS or name in core._account_loop_names(t):
        return f"{name!r} already names another account"
    taken = core._provider_poll_targets(t).get(name)
    if taken and taken[0] != PROVIDER:
        return f"{name!r} already names a {taken[0]} account"
    return None


def _auth_complete(t, grant, fields):
    name = str(fields.get("name") or grant.get("account") or "").strip()
    key = str(fields.get("api_key") or "").strip()
    if not name:
        raise ValueError("name is required")
    if not key:
        raise ValueError("api_key is required")
    keys = _load(t)
    err = _name_error(t, name, keys)
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
    # Read again under the edit lock: the probe took seconds, and a key
    # removed (or added) meanwhile is not written back from the first read.
    with _st(t).edit_lock:
        keys = _load(t)
        err = _name_error(t, name, keys)
        if err:
            raise ValueError(err + " — nothing was saved")
        replaced = name in keys
        keys[name] = key
        _persist(t, keys)
    core.log(f"{PROVIDER} key {'replaced' if replaced else 'enrolled'} for {name} "
             f"(fingerprint {core._key_fingerprint(key)})")
    with t.poll_health_lock:
        t.poll_fails.pop(name, None)
        t.poll_schedule.pop(name, None)  # due at once
    core._poll_wakeup.set()
    return {"account": name, "enrolled": True, "reauthorized": replaced}


def _auth_remove(t, account):
    """Drop one key from the file, and the file with its last key."""
    with _st(t).edit_lock:
        keys = _load(t)
        if keys.pop(account, None) is None:
            raise ValueError(f"{account!r} is not enrolled — nothing was removed")
        if keys:
            _persist(t, keys)
        else:
            with _st(t).keys_lock:
                core._remove_credentials(_st(t).file)
                _st(t).keys, _st(t).keys_mtime = None, None
    core.log(f"{PROVIDER} key removed for {account}")


register(PROVIDER, {
    "poll": poll,
    "enabled": lambda t: bool(_load(t)),
    "accounts": _names,
    "api_key": lambda t, account: _load(t).get(account, ""),
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
        "remove": _auth_remove,
    },
})
