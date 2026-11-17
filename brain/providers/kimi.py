"""The Kimi for Coding driver (shared): a static-key provider whose quota the
brain polls on GET /coding/v1/usages; its key is enrolled on the tenant's
/auth page into the tenant's kimi.json (there is no process-wide key: a key
belongs to a tenant)."""
import base64
import copy
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import claude_usage_exporter as core
from providers import register, register_route


# ---- provider quota pollers (Phase 1 of cross-provider class switching) ----
# These are additive, brain-only metrics: they do NOT alter /preferred in Phase 1.
# The Kimi key is the same sk-kimi-* key the proxy uses for the tenant; never
# commit it, and fingerprint it in logs.
KIMI_USAGES_URL = os.environ.get("KIMI_USAGES_URL", "https://api.kimi.com/coding/v1/usages")


# Kimi enrollment (#72): the enrolled key lives in the tenant's dir.
KIMI_CREDS_FILE = "kimi.json"


# ---- Kimi-for-Coding quota poller --------------------------------------------

# Kimi key resolution (#72): the tenant's enrolled file key, mtime-cached like
# every pooled creds file so an out-of-band edit is adopted live.
# file_key: None = not loaded / no file, "" = file present but keyless.
class _Store:
    """One tenant's kimi key (tenant.stores["kimi"], _st)."""

    def __init__(self, t):
        self.file = t.path(KIMI_CREDS_FILE)
        self.file_key = None
        self.file_mtime = None


def _st(t):
    return t.store_for("kimi", _Store)


def _kimi_key(t):
    """The Moonshot key the brain polls with for this tenant: the enrolled
    file's value, "" without one."""
    with t.refresh_lock:
        mtime = core._store.mtime(_st(t).file)
        if mtime is None:
            _st(t).file_key = None
            _st(t).file_mtime = None
            return ""
        if _st(t).file_key is None or mtime != _st(t).file_mtime:
            try:
                raw = core._store.read_json(_st(t).file)
                _st(t).file_key = (raw.get("api_key") or "") if isinstance(raw, dict) else ""
            except Exception as e:  # noqa: BLE001
                core.log(f"WARN kimi creds file unreadable: {e}")
                _st(t).file_key = ""
            _st(t).file_mtime = mtime
        return _st(t).file_key or ""


def _kimi_key_source(t):
    """Where the active kimi key comes from — declared on /auth/providers."""
    return "file" if _kimi_key(t) else "none"


def _persist_kimi_key(t, key):
    """Atomically write the enrolled kimi key. Raises ValueError (operator-readable)
    on I/O failure; logs the fingerprint only, never the key."""
    with t.refresh_lock:
        try:
            core._store.write_json(_st(t).file, {"api_key": key})
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"cannot write {_st(t).file}: {e}") from e
        _st(t).file_key = key
        _st(t).file_mtime = core._store.mtime(_st(t).file)
    core.log(f"kimi key enrolled (fingerprint {core._key_fingerprint(key)})")


def _get_kimi_usages(key):
    req = urllib.request.Request(
        KIMI_USAGES_URL,
        headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
    )
    with core.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _parse_kimi_window(raw):
    """Extract used/limit/remaining/resets_at from a Kimi window object.

    Kimi returns numeric values as strings; normalize to floats and epoch times.
    The LIVE payload carries limit + remaining but NO used,
    so a missing side is derived from the other two (used = limit - remaining,
    clamped >= 0) — without this, `used` silently defaulted to 0 downstream and
    Kimi read as permanently free (#26 pre-flight fix). Underivable stays absent:
    unknown must surface as None, never as 0.
    """
    detail = raw.get("detail") or raw
    used = detail.get("used")
    limit = detail.get("limit")
    remaining = detail.get("remaining")
    reset_time = detail.get("resetTime")
    out = {}
    for k, v in (("used", used), ("limit", limit), ("remaining", remaining)):
        if v is not None:
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                pass
    if "used" not in out and "limit" in out and "remaining" in out:
        out["used"] = max(0.0, out["limit"] - out["remaining"])
    elif "remaining" not in out and "limit" in out and "used" in out:
        out["remaining"] = max(0.0, out["limit"] - out["used"])
    if reset_time is not None:
        out["resets_at"] = core._iso_to_epoch(str(reset_time))
    if "used" in out and "limit" in out and out["limit"] > 0:
        out["utilization"] = 100.0 * out["used"] / out["limit"]
    return out


# Kimi (GET /coding/v1/usages, 12 §2): numbers arrive as strings; `usage` is
# the weekly window (kimi-cli labels it so), limits[] the windows by
# {duration, timeUnit}, totalQuota the monthly pool shared with every Kimi
# feature (K2: its fields are read, what drains it is not, so it never gates),
# parallel the concurrency cap. A new plan has no weekly window (K1): that is
# a reading without one, not a failure.
_KIMI_TIME_UNITS = {"TIME_UNIT_SECOND": 1, "TIME_UNIT_MINUTE": 60,
                    "TIME_UNIT_HOUR": 3600, "TIME_UNIT_DAY": 86400}


def _kimi_window(wid, period_s, parsed, as_of):
    return core._window(wid, period_s, parsed.get("utilization"), parsed.get("resets_at"), as_of,
                   opens="first_use" if period_s == 18000 else None,
                   **{k: parsed[k] for k in ("used", "limit", "remaining") if k in parsed})


def _kimi_reading(raw, as_of):
    """GET /usages -> (reading, err). `{}` stays a failed read until C9 reads
    it as not entitled (K3): an empty answer is no reading of any limit."""
    if not isinstance(raw, dict) or not (raw.get("usage") or raw.get("limits")):
        return None, "usage read carries no window ({}: not entitled until C9 reads it)"
    windows = {}
    for lim in raw.get("limits") or []:
        if not isinstance(lim, dict):
            continue
        win = lim.get("window") or {}
        try:
            secs = int(win.get("duration")) * _KIMI_TIME_UNITS[win.get("timeUnit")]
        except (KeyError, TypeError, ValueError):
            core.log(f"kimi: limits[] window {win!r} has no known length; not read")
            continue
        wid, period_s = core._slot_id(secs, windows)
        windows[wid] = _kimi_window(wid, period_s, _parse_kimi_window(lim), as_of)
    usage = raw.get("usage")
    if isinstance(usage, dict) and usage:
        wid = "seven_day" if "seven_day" not in windows else "window:7d#usage"
        windows[wid] = dict(_kimi_window(wid, 604800, _parse_kimi_window(usage), as_of),
                            opens="anchored")
    others = []
    total = raw.get("totalQuota")
    if isinstance(total, dict) and total:
        t = _parse_kimi_window(total)
        if t:
            others.append({"id": "total_quota", "kind": "cycle_pool", "period": "1mo",
                           "resets_at": t.get("resets_at"), "reset_basis": "anniversary",
                           "used": t.get("used"), "limit": t.get("limit"),
                           "remaining": t.get("remaining"), "used_pct": t.get("utilization"),
                           "basis": "unknown_semantics", "as_of": as_of})
    beside = []
    par = raw.get("parallel")
    if isinstance(par, dict) and par.get("limit") is not None:
        try:
            beside.append({"kind": "concurrency", "limit": float(par["limit"]),
                           "on_exceed": "refuse", "basis": "exact"})
        except (TypeError, ValueError):
            pass
    mem = raw.get("membership")
    level = str(mem["level"]) if isinstance(mem, dict) and mem.get("level") else None
    plan = {"label": level, "basis": "exact" if level else "unknown"}
    for k, name in (("subType", "sub_type"), ("scope", "scope")):
        if raw.get(k) is not None:
            plan[name] = raw[k]
    # Extra Usage has no API (12 §2): the pot exists, nothing about it is known.
    credits = {"id": core.MONEY_POT, "type": "money", "draw": 2, "unit": None, "unit_usd": None,
               "entitlement": {"state": "unknown", "basis": "unknown"},
               "open": None, "open_basis": None,
               "limits": [{"id": "balance", "kind": "balance", "amount": None,
                           "basis": "unknown"}]}
    return {"provider": "kimi", "kind": "subscription", "as_of": as_of,
            "source": "GET /coding/v1/usages", "ok": True, "plan": plan,
            "pots": [core._allowance_pot(windows.values(), others=others), credits],
            "beside": beside, "models": {}}, ""


def poll_kimi(t, prev=None, account=None):  # noqa: ARG001 — uniform pool dispatch (#71)
    """Return one Kimi poll result (never raises):
      name, kind, provider, ok, ts, good_ts, stale, http, err, retry_after, reading
    reading is the §1.5 normalized reading (_kimi_reading), None on a failure."""
    out = {"name": "kimi", "kind": "provider", "provider": "kimi",
           "ok": 0, "ts": time.time(), "good_ts": None, "stale": 0,
           "http": 0, "err": "", "retry_after": None, "reading": None}
    key = _kimi_key(t)
    if not key:
        out["err"] = "kimi key not configured (enroll one on /auth)"
        return out
    raw = None
    try:
        raw = _get_kimi_usages(key)
    except urllib.error.HTTPError as e:
        out["http"] = e.code
        out["err"] = f"usage HTTP {e.code}"
        out["retry_after"] = core._retry_after(e)
        return out
    except Exception as e:  # noqa: BLE001
        out["err"] = f"usage {type(e).__name__}: {e}"
        return out
    reading, err = _kimi_reading(raw, out["ts"])
    if reading is None:
        out["err"] = err
        return out
    out["reading"] = reading
    out["ok"] = 1
    return out


def _kimi_auth_status(t, now):
    """A single row (kimi is one key, not a pool) carrying where the key comes
    from and its fingerprint — never the key."""
    key = _kimi_key(t)
    if not key:
        return []
    rows = core._account_health(t, ["kimi"], now)["accounts"]
    for r in rows:
        r["source"] = _kimi_key_source(t)
        r["key_fingerprint"] = core._key_fingerprint(key)
    return rows


def _kimi_auth_begin(t, account=None):
    """No redirect grant here — mint a bare one-shot state so complete has the same
    shape (and TTL) as every other provider's flow."""
    return None, core._auth_grant_mint(t, "kimi", "add", account=account)


def _kimi_auth_complete(t, grant, fields):  # noqa: ARG001 — uniform authority signature
    key = str(fields.get("api_key") or "").strip()
    if not key:
        raise ValueError("api_key is required")
    # Validate BEFORE persist: one usage call proves the key. Error text carries
    # the exception TYPE and fingerprint only — never the key, never upstream
    # prose that could echo it.
    try:
        raw = _get_kimi_usages(key)
    except urllib.error.HTTPError as e:
        raise ValueError(f"key validation HTTP {e.code} (fingerprint "
                         f"{core._key_fingerprint(key)}) — nothing was saved") from e
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"key validation {type(e).__name__} — nothing was saved") from e
    if _kimi_reading(raw, time.time())[0] is None:
        # A new plan without a weekly window enrolls (K1); `{}` reads no
        # window at all, and is not entitled until C9 says so.
        raise ValueError("key was accepted but returned no usable quota payload — nothing was saved")
    _persist_kimi_key(t, key)
    with t.poll_health_lock:
        t.poll_fails.pop("kimi", None)
        t.poll_schedule.pop("kimi", None)  # due immediately
    core._poll_wakeup.set()
    return {"account": "kimi", "enrolled": True, "reauthorized": False,
            "note": "the routing proxy holds its own copy of this key "
                    "(SWITCH_KIMI_KEY) — update the proxy env separately"}


register("kimi", {
    "poll": poll_kimi, "enabled": lambda t: bool(_kimi_key(t)),
    "auth": {
        "kind": "static-key",
        "label": "Kimi for Coding (Moonshot)",
        "custody": {"rotation": "none",
                    "note": "A static API key. The routing proxy injects its OWN "
                            "copy (SWITCH_KIMI_KEY); enrolling here fixes quota "
                            "polling only — update the proxy env separately."},
        "enroll": {"flow": "paste-key", "can_add": True,
                   "instructions": "Paste the sk-kimi-* key. It is validated with a "
                                   "usage probe before anything is saved.",
                   "fields": [{"name": "api_key", "label": "API key", "secret": True}]},
        "status": _kimi_auth_status,
        "begin": _kimi_auth_begin,
        "complete": _kimi_auth_complete,
    },
    # Sized from the brain's own polls (quota-model 66 §7).
    "sizing": lambda wid, kind: "poll" if kind in ("window", "cycle_pool") else None,
})
