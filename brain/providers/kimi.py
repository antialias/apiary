"""The Kimi for Coding driver (shared): a static-key provider whose quota the
brain polls on GET /coding/v1/usages; its keys are named accounts on the
keyring (providers/keyring.py), enrolled through the tenant's /auth endpoints
(Scry's Sources page, or the `accounts` command) into the tenant's kimi.json
(there is no process-wide key: a key belongs to a tenant)."""
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
from providers import keyring


# ---- provider quota pollers (Phase 1 of cross-provider class switching) ----
# These are additive, brain-only metrics: they do NOT alter /preferred in Phase 1.
# The Kimi key is the same sk-kimi-* key the proxy uses for the tenant; never
# commit it, and fingerprint it in logs.
KIMI_USAGES_URL = os.environ.get("KIMI_USAGES_URL", "https://api.kimi.com/coding/v1/usages")


# Kimi enrollment (#72): the tenant's keys live in the tenant's dir, on the
# keyring (providers/keyring.py): several named Kimi accounts per tenant.
KIMI_CREDS_FILE = "kimi.json"


# ---- Kimi-for-Coding quota poller --------------------------------------------

def _get_kimi_usages(key):
    req = urllib.request.Request(
        KIMI_USAGES_URL,
        headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
    )
    with core.key_urlopen(req, timeout=15) as resp:
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


def _probe(key):
    """Enrolment's probe: the poll's own read must give a reading. A new plan
    without a weekly window enrolls (K1); `{}` reads no window at all, and is
    not entitled until C9 says so."""
    if _kimi_reading(_get_kimi_usages(key), time.time())[0] is None:
        return "returned no usable quota payload"
    return None


# The proxy signs this tenant's Kimi requests with an account's key, dispensed
# by GET /api-key?account=<name> like an api account's; it holds none of its own.
KEYS = keyring.register(
    "kimi", "Kimi for Coding (Moonshot)", KIMI_CREDS_FILE,
    lambda key: _get_kimi_usages(key), _kimi_reading, _probe,
    "Name the account and paste its sk-kimi-* key. It is validated with a usage probe "
    "before anything is saved; a name already enrolled has its key replaced.",
    "Static API keys, held by this brain alone: the routing proxy is dispensed each one by "
    "GET /api-key.",
    what="usage",
    # Sized from the brain's own polls (quota-model 66 §7).
    sizing=lambda wid, kind: "poll" if kind in ("window", "cycle_pool") else None)
