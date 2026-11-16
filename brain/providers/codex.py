"""The OpenAI codex driver: ChatGPT-subscription chains, polled on
the usage reads the codex CLI's /status makes and dispensed to the proxy by
GET /openai-token. The refresh token rotates strictly: this brain is the sole
refresher of each chain.
"""
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


# OpenAI codex (ChatGPT subscription) quota — #30/#31. Enabled iff the creds
# file exists (seeded out-of-band; see README), re-checked every poller loop so
# seeding after deploy is picked up live. The refresh token ROTATES on every
# use, so this exporter is the SOLE refresher of the chain — the file lives
# apart from accounts.json so account pruning can never touch it. Quota comes
# from two FREE reads, the ones the codex CLI's /status makes: GET wham/usage
# (windows, rate_limit.allowed, credits, spend_control) and GET
# wham/rate-limit-reset-credits (the reset options). Both answer headless with no
# Cloudflare challenge (quota-model 11 S6/S8). They replace
# a POST /responses inference that was billed in credits whenever the window was
# spent (quota-model 11 D5).
#
# The two reads' response shapes, abridged to what is read here:
#
#   wham/usage
#     plan_type
#     rate_limit {allowed, limit_reached,
#                 primary_window {used_percent, limit_window_seconds,
#                                 reset_after_seconds, reset_at},
#                 secondary_window (same, or null)}
#     additional_rate_limits [{limit_name, metered_feature, rate_limit}] | null
#     credits {has_credits, unlimited, overage_limit_reached,
#              balance (decimal STRING, in credits), approx_*_messages}
#     spend_control {reached, individual_limit}
#
#   wham/rate-limit-reset-credits
#     available_count, total_earned_count, immediate_reset_purchase_eligible
#
# Neither is an inference, so neither draws on the window or on credits.
OPENAI_CODEX_CREDS_FILE = os.environ.get("OPENAI_CODEX_CREDS_FILE", "/creds/openai-codex.json")
OPENAI_TOKEN_URL = os.environ.get("OPENAI_TOKEN_URL", "https://auth.openai.com/oauth/token")
OPENAI_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"  # codex CLI's public client id
OPENAI_USAGE_URL = os.environ.get(
    "OPENAI_USAGE_URL", "https://chatgpt.com/backend-api/wham/usage")
OPENAI_RESET_CREDITS_URL = os.environ.get(
    "OPENAI_RESET_CREDITS_URL", "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits")
OPENAI_REFRESH_MARGIN = int(os.environ.get("OPENAI_REFRESH_MARGIN", "3600"))  # access TTL ~10d

# ---- codex enrollment endpoints (#72) ----------------------------------------
# The authorize URL, scopes, and extra flow params are ASSUMED from the codex
# CLI's public OAuth flow and NOT yet live-verified — the first real enrollment
# is the validation. Getting one wrong is visible (a 422 with the upstream
# reason, surfaced to the operator) and stateless (nothing persists until the usage
# probe passes), so a bad assumption costs one retry, not a wedged pool.
# CODEX_TOKEN_URL is the SAME endpoint the live refresh path already exercises
# (JSON body accepted there — if the authorization_code grant refuses JSON,
# flip _codex_auth_complete's exchange to form-encoding).
CODEX_AUTHORIZE = os.environ.get("CODEX_AUTHORIZE_URL", "https://auth.openai.com/oauth/authorize")
CODEX_TOKEN_URL = OPENAI_TOKEN_URL
# The codex CLI's registered redirect. Never served here: the operator's browser
# fails to load localhost:1455 and they paste the full URL back (the code rides
# its query string; the exchange only needs redirect_uri to MATCH, not resolve).
CODEX_REDIRECT = "http://localhost:1455/auth/callback"
CODEX_SCOPE = "openid profile email offline_access"


# Codex credits per 1M tokens (11 §3 [1], DOCUMENTED): input, cached input,
# output. Output counts every billed output token, reasoning included. A model
# with no entry is charged at the highest rate of each kind (basis
# upper_bound): a budget may overcount, never undercount. Speed modes weigh
# the charge (Fast 2x, Ultrafast 6x of credits); a request whose speed the
# proxy cannot name is charged at the heaviest weight, upper_bound too.
# GPT-Image-2 is here for the image door's entries (C4): image and text
# tokens, each input / cached / output.
CODEX_CREDIT_RATES = {
    "gpt-6-astra": {"input": 250.0, "cached": 25.0, "output": 1250.0},
    "gpt-6.1-sol": {"input": 50.0, "cached": 2.5, "output": 250.0},
}
CODEX_IMAGE_RATES = {
    "gpt-image-2": {"image": {"input": 200.0, "cached": 50.0, "output": 750.0},
                    "text": {"input": 125.0, "cached": 31.25, "output": 250.0}},
}
CODEX_SPEED_WEIGHTS = {"standard": 1.0, "fast": 2.0, "ultrafast": 6.0}


# ---- generic OAuth refresh-token machinery (#30) -----------------------------
# Mirrors the Anthropic _refresh/_ensure_token semantics for providers whose
# refresh token rotates on every use (codex today). The live Anthropic path
# deliberately stays on its own code; a third OAuth provider should adopt these.

def _oauth_refresh(creds, token_url, client_id, persist_fn):
    """POST a refresh_token grant, update `creds` in place, persist, return the
    access token. Raises on HTTP/parse errors (callers surface poll failures)."""
    body = json.dumps({"grant_type": "refresh_token",
                       "refresh_token": creds["refresh_token"],
                       "client_id": client_id}).encode()
    req = urllib.request.Request(token_url, data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with core.urlopen(req, timeout=20) as resp:
        t = json.loads(resp.read().decode("utf-8"))
    creds["access_token"] = t["access_token"]
    creds["expires_at"] = time.time() + int(t.get("expires_in", 864000))
    if t.get("refresh_token"):
        creds["refresh_token"] = t["refresh_token"]  # rotates every refresh
    if t.get("earliest_refresh_at") is not None:
        creds["earliest_refresh_at"] = t["earliest_refresh_at"]
    persist_fn()
    return creds["access_token"]


def _oauth_ensure_token(creds, token_url, client_id, persist_fn, margin=3600):
    """Return a valid access token, refreshing inside `margin` seconds of expiry.

    Honors the server's earliest_refresh_at throttle (epoch or ISO): an early,
    margin-driven refresh is deferred while the throttle window is open, but a
    hard-expired token refreshes regardless (nothing better to do)."""
    with core._refresh_lock:
        now = time.time()
        exp = creds.get("expires_at", 0)
        if creds.get("access_token") and now < exp - margin:
            return creds["access_token"]
        earliest = creds.get("earliest_refresh_at")
        earliest = (float(earliest) if isinstance(earliest, (int, float))
                    else core._iso_to_epoch(earliest)) or 0
        if creds.get("access_token") and now < exp and now < earliest:
            return creds["access_token"]  # throttled; current token still valid
        return _oauth_refresh(creds, token_url, client_id, persist_fn)


# Live codex credential POOL {name: {refresh_token, access_token, expires_at,
# account_id}} (#71); lazy-loaded on first use. The on-disk file accepts two
# shapes: the legacy single dict (pool of one under the synthesized name
# "gpt" — matching the poll target name, so a pool of one is byte-identical
# to the pre-pool behavior) and an array of entries each carrying a unique
# "name". Touched from the poller thread AND /openai-token handler threads
# (#33) — loads AND refreshes serialize under the shared _refresh_lock, which
# also closes the load-vs-rotation race that could resurrect a rotated-out
# refresh token (STRICT rotation: writing a stale RT kills the chain).
_openai_pool = None            # {name: creds-dict} or None
_openai_pool_shape = "legacy"  # "legacy" | "array" — governs _persist's shape
_openai_pool_mtime = None      # st_mtime of the loaded file (live adoption)


def _parse_openai_pool(raw):
    """File JSON -> (pool, shape). Array entries need name + refresh_token and
    a name that doesn't collide with an Anthropic account or another provider
    id — _state is one flat namespace, keyed by target name."""
    if isinstance(raw, dict):
        return {"gpt": raw}, "legacy"
    if not isinstance(raw, list):
        core.log("WARN openai creds file is neither a dict nor an array — ignored")
        return None, "legacy"
    pool = {}
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict) or not entry.get("name") \
                or not entry.get("refresh_token"):
            core.log(f"WARN openai creds entry {i} skipped: needs name + refresh_token")
            continue
        n = entry["name"]
        if n in pool:
            core.log(f"WARN openai creds entry {i} skipped: duplicate name {n!r}")
            continue
        if n in core._account_loop_names() or n in (set(core.PROVIDERS) - {"gpt"}):
            core.log(f"WARN openai creds entry {i} skipped: name {n!r} collides with "
                f"an account/provider target")
            continue
        pool[n] = entry
    return pool, "array"


def _load_openai_creds(force=False):
    """Return the codex creds pool {name: creds}, or None when there is no file.

    Cached, with an mtime guard: an out-of-band rewrite (operator re-seeds a
    dead chain, or converts legacy->array to add an account) is adopted on the
    next call — live, no restart. force=True re-reads unconditionally (the
    401 escape hatch; also covers mtime-granularity aliasing)."""
    global _openai_pool, _openai_pool_shape, _openai_pool_mtime
    with core._refresh_lock:
        try:
            mtime = os.stat(OPENAI_CODEX_CREDS_FILE).st_mtime
        except OSError:
            _openai_pool = None
            _openai_pool_mtime = None
            return None
        if _openai_pool is not None and not force and mtime == _openai_pool_mtime:
            return _openai_pool
        try:
            with open(OPENAI_CODEX_CREDS_FILE) as f:
                raw = json.load(f)
        except OSError:
            _openai_pool = None
            _openai_pool_mtime = None
            return None
        except ValueError as e:
            core.log(f"WARN openai creds file unreadable: {e}")
            _openai_pool = None
            _openai_pool_mtime = None
            return None
        _openai_pool, _openai_pool_shape = _parse_openai_pool(raw)
        _openai_pool_mtime = mtime
        return _openai_pool


def _persist_openai_creds():
    """Atomic, SHAPE-PRESERVING rewrite of the codex creds file after a token
    rotation. A legacy dict file is written back as the single dict — a
    rolled-back brain calls .get() on the parsed file and would crash on a
    list, so array form is only ever on disk when the operator seeded array
    form. Callers hold _refresh_lock (persist runs inside _oauth_refresh)."""
    global _openai_pool_mtime
    if not _openai_pool:
        return
    try:
        if _openai_pool_shape == "legacy":
            data = next(iter(_openai_pool.values()))
        else:
            data = list(_openai_pool.values())
        tmp = OPENAI_CODEX_CREDS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, OPENAI_CODEX_CREDS_FILE)
        _openai_pool_mtime = os.stat(OPENAI_CODEX_CREDS_FILE).st_mtime
    except Exception as e:  # noqa: BLE001
        core.log(f"WARN could not persist rotated openai tokens: {e}")


def _openai_pool_entry(pool, account):
    """Resolve one chain from the pool: a named account exactly (None when
    absent), or — account None/"" — the legacy singleton semantics: "gpt" when
    present (every legacy pool), else the sole entry, else the sorted-first."""
    if not pool:
        return None, None
    if account:
        return (account, pool[account]) if account in pool else (None, None)
    if "gpt" in pool:
        return "gpt", pool["gpt"]
    name = sorted(pool)[0]
    return name, pool[name]


def _openai_account_names():
    """Poll-target names for the gpt provider: None = legacy single-target mode
    (one target named "gpt", account arg None), else the array entries' names."""
    pool = _load_openai_creds()
    if pool is None or _openai_pool_shape == "legacy":
        return None
    return sorted(pool)


def _codex_get(url, token, account_id):
    """GET one wham endpoint as the codex CLI does and return its parsed JSON.
    The headers are the ones the codex CLI sends: bearer, workspace id, and the
    codex-cli originator/agent pair."""
    req = urllib.request.Request(
        url, method="GET",
        headers={
            "Authorization": f"Bearer {token}",
            "ChatGPT-Account-Id": account_id or "",
            "Accept": "application/json",
            "originator": "codex-cli",
            "User-Agent": "codex-cli/0.137.0",
        })
    with core.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_codex_usage(token, account_id):
    return _codex_get(OPENAI_USAGE_URL, token, account_id)


def _get_codex_reset_credits(token, account_id):
    return _codex_get(OPENAI_RESET_CREDITS_URL, token, account_id)


def _codex_window_raw(raw):
    """One wham window snapshot -> (used_percent, length_s, reset_at), or None
    when it is absent or does not apply (a zero length). Codex reports percent
    used only, no absolute counts; it can overrun 100 (11 §3)."""
    if not isinstance(raw, dict):
        return None
    try:
        pct = float(raw.get("used_percent"))
        secs = int(raw.get("limit_window_seconds") or 0)
    except (TypeError, ValueError):
        return None
    if secs <= 0 or not math.isfinite(pct):
        return None
    try:
        resets = float(raw["reset_at"])
    except (KeyError, TypeError, ValueError):
        resets = None
    return pct, secs, resets


def _codex_windows(rl, as_of, taken, scope=None, gate=None, prefix=None):
    """The windows of one wham rate_limit object. Unscoped (the account's own
    rate_limit) they take their slot by length; a scoped one (an
    additional_rate_limits entry) is scoped:<name>:<period> and carries that
    limit's own gate. reset_basis is early_possible: codex has ended periods
    early, so reset_at is an upper bound (11 §3)."""
    out = []
    for key in ("primary_window", "secondary_window"):
        got = _codex_window_raw(rl.get(key))
        if got is None:
            continue
        pct, secs, resets = got
        if prefix is None:
            wid, period_s = core._slot_id(secs, taken)
        else:
            wid, period_s = f"{prefix}:{core._period_label(secs)}", secs
        facts = {"used": pct, "limit": 100.0, "remaining": max(0.0, 100.0 - pct)}
        if prefix is not None:
            facts.update(open=gate, open_basis="exact" if gate is not None else None)
        w = core._window(wid, period_s, pct, resets, as_of, scope=scope, opens="first_use",
                    reset_basis="early_possible", **facts)
        taken[wid] = w
        out.append(w)
    return out


def _codex_credits_pot(raw, spend_control):
    """wham/usage `credits` (+ spend_control) -> the credits pot, or None when
    the read carries no credits object.

    The gate is (has_credits ∨ unlimited) ∧ ¬overage_limit_reached ∧
    ¬spend_control.reached. The combination is INFERRED (quota-model 11 §3),
    so open_basis is derived and it is never called the provider's gate: a
    refusal that contradicts it wins. `has_credits` means "credits tracking is
    enabled", not "funded": codex's own client prints "Available" at a blank or
    zero balance (11 S4), so the balance is ADVISORY, shown and never a gate
    (50 §1.5, M5). It arrives as a decimal string in credits; unparseable stays
    None, never 0. overage_limit_reached reads as the auto top-up's monthly
    cap (INFERRED, 11 §5); spend_control is the owner's own cap."""
    if not isinstance(raw, dict):
        return None
    sc = spend_control if isinstance(spend_control, dict) else {}
    has, unlimited = raw.get("has_credits") is True, raw.get("unlimited") is True
    overage, reached = raw.get("overage_limit_reached") is True, sc.get("reached") is True
    try:
        balance = float(raw.get("balance"))
    except (TypeError, ValueError):
        balance = None
    if balance is not None and not math.isfinite(balance):
        balance = None
    return {"id": core.MONEY_POT, "type": "money", "draw": 2, "unit": "codex_credit",
            "unit_usd": None, "entitlement": {"state": "active", "basis": "derived"},
            "open": bool((has or unlimited) and not overage and not reached),
            "open_basis": "derived", "has_credits": has, "unlimited": unlimited,
            "limits": [
                {"id": "balance", "kind": "balance", "amount": balance, "advisory": True,
                 "basis": "exact" if balance is not None else "unknown"},
                {"id": "topup_cap", "kind": "spend_cap", "setter": "owner@provider",
                 "reached": overage, "limit": None, "basis": "derived"},
                {"id": "spend_control", "kind": "spend_cap", "setter": "owner@provider",
                 "reached": reached, "limit": sc.get("individual_limit"),
                 "basis": "exact" if spend_control is not None else "unknown"}]}


def _codex_reset_options(raw):
    """wham/rate-limit-reset-credits → the allowance's reset options. They are
    offered when a class is unavailable and never feed capacity; how a reset is
    redeemed or bought is UNKNOWN (11 S8)."""
    out = {"reset_credits": None, "purchase_eligible": None, "basis": "exact"}
    if isinstance(raw, dict):
        if isinstance(raw.get("available_count"), (int, float)):
            out["reset_credits"] = int(raw["available_count"])
        if isinstance(raw.get("immediate_reset_purchase_eligible"), bool):
            out["purchase_eligible"] = raw["immediate_reset_purchase_eligible"]
    return out


def _codex_reading(raw, reset_options, as_of, name="gpt"):
    """wham/usage (+ the reset-credits options) -> (reading, err).

    The allowance's `open` is rate_limit.allowed, the provider's own gate
    (exact). Its windows are slotted by limit_window_seconds. Each
    additional_rate_limits entry is read generically (O-D7): its windows are
    scoped to the limit's own name, carry its own allowed gate, and bind the
    route whose scope (a provider member's model) that name is.
    model_usage is kept as stated; what it means is UNKNOWN (11 §7)."""
    rl = raw.get("rate_limit") if isinstance(raw, dict) else None
    if not isinstance(rl, dict) or not isinstance(rl.get("allowed"), bool):
        return None, "usage read carries no rate_limit.allowed"
    taken = {}
    windows = _codex_windows(rl, as_of, taken)
    for i, x in enumerate(raw.get("additional_rate_limits") or []):
        if not isinstance(x, dict):
            core.log(f"{name}: codex additional_rate_limits[{i}] is not an object; not read")
            continue
        lname = x.get("limit_name") or x.get("metered_feature") or f"additional_{i}"
        xrl = x.get("rate_limit") if isinstance(x.get("rate_limit"), dict) else {}
        gate = xrl["allowed"] if isinstance(xrl.get("allowed"), bool) else None
        got = _codex_windows(xrl, as_of, taken, scope=str(lname), gate=gate,
                             prefix=f"scoped:{lname}")
        if not got:
            # A limit with a gate and no window still binds its model: closed
            # while it says so, with no reset known.
            wid = f"scoped:{lname}"
            got = [core._window(wid, None, None, None, as_of, scope=str(lname), open=gate,
                           open_basis="exact" if gate is not None else None)]
        for w in got:
            w["feature"] = x.get("metered_feature")
            w["limit_reached"] = xrl.get("limit_reached") is True
        windows += got
    models = {}
    for m, v in (raw.get("model_usage") or {}).items() if isinstance(raw.get("model_usage"), dict) else ():
        if isinstance(v, dict):
            models[m] = {"available": v.get("available"), "available_at": v.get("available_at"),
                         "credits_would_enable": v.get("credits_would_enable"),
                         "basis": "unknown_semantics"}
    plan = raw.get("plan_type")
    pots = [core._allowance_pot(windows, open_=rl["allowed"], open_basis="exact",
                           limit_reached=rl.get("limit_reached") is True,
                           reset_options=reset_options)]
    credits = _codex_credits_pot(raw.get("credits"), raw.get("spend_control"))
    if credits is not None:
        pots.append(credits)
    return {"provider": "gpt", "kind": "subscription", "as_of": as_of,
            "source": "GET wham/usage + GET wham/rate-limit-reset-credits", "ok": True,
            "plan": {"label": str(plan) if plan else None, "basis": "exact" if plan else "unknown"},
            "pots": pots, "beside": [], "models": models}, ""


def poll_gpt(prev=None, account=None):
    """Return one codex/ChatGPT-subscription poll result (never raises).

    Emits provider="gpt" — NOT "openai" — because the class-member join
    (_class_member_targets) matches poll provider == classes.json member
    provider by exact string, and the live frontier member declares "gpt".
    `account` picks one chain out of the creds pool (#71) and becomes the
    result's name; None = the legacy singleton, name "gpt" — so a pool of one
    is byte-identical to the pre-pool poll.

    Reads GET wham/usage, then GET wham/rate-limit-reset-credits (see the
    section comment), into the §1.5 reading (_codex_reading)."""
    pool = _load_openai_creds()
    name, creds = _openai_pool_entry(pool, account)
    out = {"name": name or account or "gpt", "kind": "provider", "provider": "gpt",
           "ok": 0, "ts": time.time(), "good_ts": None, "stale": 0,
           "http": 0, "err": "", "retry_after": None, "reading": None,
           "source": "GET wham/usage + GET wham/rate-limit-reset-credits"}
    if not creds or not creds.get("refresh_token"):
        out["err"] = f"no codex creds at {OPENAI_CODEX_CREDS_FILE}"
        return out
    account_id = creds.get("account_id") or ""

    # Acquire a token: normal path, then one retry after force-re-reading the
    # creds file (adopts an externally re-seeded chain if our copy's RT died).
    tok = None
    for attempt in (0, 1):
        try:
            tok = _oauth_ensure_token(creds, OPENAI_TOKEN_URL, OPENAI_CLIENT_ID,
                                      _persist_openai_creds,
                                      margin=OPENAI_REFRESH_MARGIN)
            break
        except urllib.error.HTTPError as e:
            out["http"] = e.code
            out["err"] = f"token refresh HTTP {e.code}"
            out["retry_after"] = core._retry_after(e)
            if e.code != 401 or attempt:
                return out
            _, creds = _openai_pool_entry(_load_openai_creds(force=True), account)
            if not creds or not creds.get("refresh_token"):
                return out
            account_id = creds.get("account_id") or ""
        except Exception as e:  # noqa: BLE001
            out["err"] = f"token refresh {type(e).__name__}: {e}"
            return out

    raw = None
    try:
        raw = _get_codex_usage(tok, account_id)
    except urllib.error.HTTPError as e:
        if e.code != 401:
            out["http"] = e.code
            out["err"] = f"usage HTTP {e.code}"
            out["retry_after"] = core._retry_after(e)
            return out
        # Access token rejected mid-lifetime: force one refresh and retry once.
        try:
            with core._refresh_lock:
                tok = _oauth_refresh(creds, OPENAI_TOKEN_URL, OPENAI_CLIENT_ID,
                                     _persist_openai_creds)
            raw = _get_codex_usage(tok, account_id)
        except urllib.error.HTTPError as e2:
            out["http"] = e2.code
            out["err"] = f"usage HTTP {e2.code}"
            out["retry_after"] = core._retry_after(e2)
            return out
        except Exception as e2:  # noqa: BLE001
            out["err"] = f"usage {type(e2).__name__}: {e2}"
            return out
    except Exception as e:  # noqa: BLE001
        out["err"] = f"usage {type(e).__name__}: {e}"
        return out

    reading, err = _codex_reading(raw, None, out["ts"], out["name"])
    if reading is None:
        out["err"] = err
        return out
    # The second read only offers options; it never gates. A failure leaves them
    # unknown, says why, and does not fail the poll that already measured.
    try:
        options = _codex_reset_options(_get_codex_reset_credits(tok, account_id))
    except urllib.error.HTTPError as e:
        options = {"reset_credits": None, "purchase_eligible": None,
                   "basis": "unknown", "err": f"HTTP {e.code}"}
        core.log(f"{out['name']}: codex reset-credits read HTTP {e.code}; options unknown")
    except Exception as e:  # noqa: BLE001
        options = {"reset_credits": None, "purchase_eligible": None,
                   "basis": "unknown", "err": type(e).__name__}
        core.log(f"{out['name']}: codex reset-credits read {type(e).__name__}; options unknown")
    core._reading_pot(reading, core.ALLOWANCE_POT)["reset_options"] = options
    out["reading"] = reading
    out["ok"] = 1
    return out


def _jwt_claims(token):
    """A JWT's payload claims, WITHOUT signature verification — the token just
    arrived over TLS from the token endpoint we posted to, so its origin is the
    transport's guarantee; only the workspace-id claim inside is needed. {} on
    any malformation."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:  # noqa: BLE001
        return {}


_CODEX_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def _codex_name_error(name):
    """Why `name` can't label a codex chain, or None. Mirrors the
    _parse_openai_pool skip rules — the pool loader silently DROPS a colliding
    entry on the next load, so a name that would vanish must be refused at
    enrollment instead. An existing chain name is fine: that's a re-enroll."""
    if not _CODEX_NAME_RE.match(name or ""):
        return "name must be lowercase letters/digits/dot/dash/underscore (e.g. gpt-b)"
    if name in core._account_loop_names():
        return f"{name!r} collides with an Anthropic account"
    if name in (set(core.PROVIDERS) - {"gpt"}):
        return f"{name!r} collides with a provider id"
    return None


def _codex_pool_enroll(entry):
    """Append (or replace, same name) one chain in the codex pool and persist.
    Re-enrolling the legacy singleton under its own name keeps the legacy dict
    shape (rollback-neutral repair); anything else persists ARRAY form —
    converting a legacy file is an EXPLICIT enrollment side effect, logged
    loudly, equivalent to the operator's documented out-of-band conversion
    (rollback floor becomes the first pool-aware brain). Ordinary rotation
    persists (_persist_openai_creds) stay shape-preserving either way.
    Returns True when an existing chain was replaced."""
    global _openai_pool, _openai_pool_shape
    with core._refresh_lock:
        cur = _load_openai_creds() or {}
        replaced = entry["name"] in cur
        # Replace-don't-mutate: a stale in-flight refresh holds the OLD dict;
        # copying every entry makes its later mutations invisible rather than
        # resurrected. dict(e, name=n) is load-bearing on legacy->array
        # conversion: the legacy singleton's entry has no "name" key (the file
        # was a bare dict), and a nameless entry in the persisted array would
        # be silently DROPPED by the next load — losing the original chain.
        new_pool = {n: dict(e, name=n) for n, e in cur.items()}
        new_pool[entry["name"]] = dict(entry)
        if not (_openai_pool_shape == "legacy" and replaced and set(new_pool) == {"gpt"}):
            if _openai_pool_shape == "legacy" and cur:
                core.log(f"codex enrollment converts {OPENAI_CODEX_CREDS_FILE} legacy->array — "
                    "a pre-#71 brain can no longer read it (hand-revert to a single "
                    "dict before rolling back past 4ecbe41)")
            _openai_pool_shape = "array"
        _openai_pool = new_pool
        _persist_openai_creds()
    core.log(f"codex chain {entry['name']!r} {'re-enrolled' if replaced else 'enrolled'} "
        f"(pool size {len(new_pool)})")
    return replaced


def _gpt_auth_status(now):
    """One row per pool chain (the legacy singleton shows as 'gpt'). Health is
    the same per-target poll bookkeeping the poller writes — a dead chain reads
    auth_dead here exactly as an Anthropic account would."""
    with core._refresh_lock:
        pool = _load_openai_creds()
        if pool is None:
            return []
        names = sorted(pool)
        exp = {n: (pool.get(n) or {}).get("expires_at") for n in names}
    accounts = core._account_health(names, now)["accounts"]
    for a in accounts:
        a["token_expires_at"] = exp.get(a["name"]) or None
    return accounts


def _codex_auth_begin(account=None):
    """Mint the codex-CLI-shaped PKCE authorize URL (constants block: ASSUMED,
    fail-visible). `account` is advisory — the chain's identity is the `name`
    field at complete time."""
    verifier = core._b64url(secrets.token_bytes(32))
    state = core._auth_grant_mint("gpt", "reauth" if account else "add",
                             account=account, verifier=verifier)
    params = {"response_type": "code", "client_id": OPENAI_CLIENT_ID,
              "redirect_uri": CODEX_REDIRECT, "scope": CODEX_SCOPE,
              "code_challenge": core._b64url(hashlib.sha256(verifier.encode()).digest()),
              "code_challenge_method": "S256", "state": state,
              "id_token_add_organizations": "false",
              "codex_cli_simplified_flow": "true"}
    return CODEX_AUTHORIZE + "?" + urllib.parse.urlencode(params), state


def _codex_auth_complete(grant, fields):
    """The paste-the-redirect-URL exchange. Order is load-bearing:
    name validation FIRST (a bad name must not burn the one-shot code), then
    URL parse + state match, then the code exchange, then a usage PROBE on the
    new chain — only a probe-passing chain is persisted, so a wrong flow
    assumption (constants block) yields a clean 422 and zero state."""
    name = str(fields.get("name") or "").strip()
    err = _codex_name_error(name)
    if err:
        raise ValueError(err)
    q = parse_qs(urlparse(str(fields.get("redirect_url") or "").strip()).query)
    code = (q.get("code") or [""])[0]
    if not code:
        raise ValueError("that URL carries no ?code= — paste the FULL localhost URL "
                         "from the browser's address bar")
    if (q.get("state") or [""])[0] != grant["state"]:
        raise ValueError("this URL is from a different attempt — begin again and "
                         "paste the fresh redirect URL")
    body = json.dumps({"grant_type": "authorization_code", "code": code,
                       "redirect_uri": CODEX_REDIRECT, "client_id": OPENAI_CLIENT_ID,
                       "code_verifier": grant["verifier"]}).encode()
    req = urllib.request.Request(CODEX_TOKEN_URL, data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with core.urlopen(req, timeout=20) as r:
            tok = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise ValueError(f"code exchange HTTP {e.code} — nothing was saved; begin again") from e
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"code exchange {type(e).__name__} — nothing was saved; begin again") from e
    rt, at = tok.get("refresh_token"), tok.get("access_token")
    if not rt or not at:
        raise ValueError("exchange returned no refresh_token/access_token")
    account_id = ((_jwt_claims(tok.get("id_token") or "").get("https://api.openai.com/auth")
                   or {}).get("chatgpt_account_id") or "")
    if not account_id:
        raise ValueError("id_token carries no chatgpt_account_id claim — cannot "
                         "address the codex backend; nothing was saved")
    # The probe is the poller's own free read: a spent window still answers 200
    # (with rate_limit.allowed false), so any HTTP error here is a real failure.
    try:
        raw = _get_codex_usage(at, account_id)
    except urllib.error.HTTPError as e:
        raise ValueError(f"usage probe HTTP {e.code} — nothing was saved; begin again") from e
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"usage probe {type(e).__name__} — nothing was saved; begin again") from e
    rl = raw.get("rate_limit") if isinstance(raw, dict) else None
    if not isinstance(rl, dict) or not isinstance(rl.get("allowed"), bool):
        raise ValueError("usage probe returned no rate_limit — nothing was saved; begin again")
    replaced = _codex_pool_enroll({
        "name": name, "refresh_token": rt, "access_token": at,
        "expires_at": time.time() + int(tok.get("expires_in", 864000)),
        "account_id": account_id})
    with core._poll_health_lock:
        core._poll_fails.pop(name, None)
        core._poll_schedule.pop(name, None)  # due immediately
    core._poll_wakeup.set()
    return {"account": name, "enrolled": not replaced, "reauthorized": replaced}


def _openai_token(self, parsed):
    """Dispense a fresh codex (ChatGPT-subscription) access token plus the
    account id the caller must send as ChatGPT-Account-Id — the id is a
    mandatory request header on the codex endpoint, not metadata. Secret-
    guarded like /token: this hands out an inference bearer. (#33)

    ?account=<name> picks one chain out of the creds pool (#71); no param =
    the provider pool's INCUMBENT (legacy pool-of-one: the sole chain —
    byte-identical to the pre-pool response, modulo the additive `name`
    key; `account` stays deliberately absent — account_id is the ChatGPT
    workspace UUID header and an `account` key would be ambiguous next to
    it). Unknown account -> 404, mirroring /token.

    The consumer (switch-proxy codex lane) never refreshes — STRICT
    rotation, this process owns every chain. Short-TTL caching consumer-side
    is safe: rotation does not invalidate outstanding access tokens."""
    if not core.SWITCH_KEY:
        return self._json(503, {"error": "SWITCH_KEY not configured"})
    if not hmac.compare_digest(self.headers.get("X-Switch-Key", ""), core.SWITCH_KEY):
        self.send_response(401)
        self.end_headers()
        return
    pool = _load_openai_creds()
    if not pool:
        return self._json(503, {"error": f"no codex creds at {OPENAI_CODEX_CREDS_FILE}"})
    account = (parse_qs(parsed.query).get("account") or [""])[0]
    if account and account not in pool:
        return self._json(404, {"error": f"unknown codex account {account!r}"})
    if not account:
        # The pool incumbent, when the policy has bootstrapped one; else the
        # legacy singleton resolution (sole entry / "gpt" / sorted-first).
        with core._plock:
            incumbent = (core._preferred.get("providers", {}).get("gpt") or {}).get("account")
        if incumbent in pool:
            account = incumbent
    name, creds = _openai_pool_entry(pool, account)
    if not creds or not creds.get("refresh_token"):
        return self._json(503, {"error": f"no codex creds at {OPENAI_CODEX_CREDS_FILE}"})
    tok = None
    # Same acquire shape as poll_gpt: normal path, then one retry after a
    # force-re-read (adopts an externally re-seeded chain if our RT died).
    for attempt in (0, 1):
        try:
            tok = _oauth_ensure_token(creds, OPENAI_TOKEN_URL, OPENAI_CLIENT_ID,
                                      _persist_openai_creds,
                                      margin=OPENAI_REFRESH_MARGIN)
            break
        except urllib.error.HTTPError as e:
            if e.code != 401 or attempt:
                return self._json(502, {"error": f"token refresh HTTP {e.code}"})
            name, creds = _openai_pool_entry(_load_openai_creds(force=True), account)
            if not creds or not creds.get("refresh_token"):
                return self._json(503, {"error": "codex creds unusable after re-read"})
        except Exception as e:  # noqa: BLE001
            return self._json(502, {"error": f"token refresh failed: {type(e).__name__}"})
    return self._json(200, {"name": name,
                            "account_id": creds.get("account_id") or "",
                            "access_token": tok,
                            "expires_at": creds.get("expires_at")})


register("gpt", {
    "poll": poll_gpt, "enabled": lambda: os.path.exists(OPENAI_CODEX_CREDS_FILE),
    "accounts": _openai_account_names,
    "auth": {
        "kind": "oauth-rotating",
        "label": "OpenAI codex (ChatGPT subscription)",
        "custody": {"rotation": "strict",
                    "note": "STRICT rotation — the moment enrollment finishes, this "
                            "brain is the SOLE refresher of the chain. Refreshing the "
                            "same grant anywhere else (codex login on a laptop) kills it."},
        "enroll": {"flow": "paste-url", "can_add": True,
                   "instructions": "Open the authorization link and approve. The browser "
                                   "will fail to load a localhost page — that is expected. "
                                   "Copy the FULL URL from the address bar and paste it "
                                   "here with a name for the new chain.",
                   "fields": [{"name": "name", "label": "chain name (e.g. gpt-b)", "secret": False},
                              {"name": "redirect_url", "label": "pasted localhost URL", "secret": False}]},
        "status": _gpt_auth_status,
        "begin": _codex_auth_begin,
        "complete": _codex_auth_complete,
    },
    # Codex credits (C6) are charged from tokens: chat tokens (models, weighted
    # by speed) and the image tool's tokens (images), each per per_tokens.
    "ledger": {"unit": "codex_credit", "tables": ("per_tokens", "models", "speed_weights", "images"),
               "rates": {"per_tokens": 1000000, "models": CODEX_CREDIT_RATES,
                         "images": CODEX_IMAGE_RATES, "speed_weights": CODEX_SPEED_WEIGHTS}},
    "modality": {"gpt-image-2": "image"},
    # Sized from the headers every response carries (quota-model 66 §7).
    "sizing": lambda wid, kind: "header" if kind == "window" else None,
})
register_route("GET", "/openai-token", _openai_token)
