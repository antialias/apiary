"""The Anthropic subscription driver: Claude accounts signed in
with a Claude login, polled on the subscription usage endpoints and dispensed
to the proxy as bearer tokens.

Auth: each account bootstraps from a user:profile-scoped refresh token (an
independent grant from any laptop login, so its rotation never disturbs one).
The 8h access token is refreshed on demand; the refresh token ROTATES on every
refresh, so the new one is persisted back to the creds file immediately. A
`claude setup-token` credential is inference-only and 403s on /usage.

Credentials file, each tenant's own (accounts.json in the tenant's dir, read
and written through the core's sealed store), never committed, mounted RW so
rotated refresh tokens persist across restarts:
    [{"name": "user@example.com", "refresh_token": "..."}]
`name` is the STABLE Prometheus `account` label; it is NOT re-derived from the
API, so a transient failure can't churn the label.

The usage endpoint is rate-limited (429s under fast polling): the profile
(email/tier labels) is fetched once and cached, halving the request rate.

Owns the account loop (core._account_loop), GET /token, and the anthropic
authority on the /auth surface. Accounts are
managed on Scry's Sources page, or with the brain image's `accounts` command
in the self-hosted edition; the brain serves no page.
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


ACCOUNTS_FILE = "accounts.json"  # in each tenant's dir
# The client version the polls name. The provider offers reset grants only to a
# client version it supports (cedar_ember says ineligible_reason "cli_version"
# below it), so this follows a current Claude Code release.
CC_VERSION = os.environ.get("CLAUDE_CODE_VERSION", "2.1.329")
API = "https://api.anthropic.com"
USAGE_PATH = "/api/oauth/usage?cedar_ember=1"
TOKEN_URL = "https://api.anthropic.com/v1/oauth/token"
CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
REFRESH_MARGIN = 120  # refresh this many seconds before the access token expires


class _Store:
    """One tenant's Anthropic accounts (tenant.stores["anthropic"], _st):
    nothing of them is held at module level."""

    def __init__(self, t):
        self.file = t.path(ACCOUNTS_FILE)
        # name -> live credential dict {refresh_token, access_token, expires_at}.
        self.creds = {}
        # Incremented whenever re-authorization replaces an account's credential. A poll
        # that started on an older generation must not publish its now-obsolete result.
        self.cred_generation = {}
        # Each account's last access-token refresh, for the accounts page:
        # name -> {at, why, old_left_s, fp}. old_left_s is what the replaced token had
        # left (None when there was none). Written by _refresh under the tenant's
        # refresh_lock; in memory only.
        self.last_refresh = {}
        # The switch proxy's last report that Anthropic refused its copy of an
        # account's access token (GET /token?refused_fp=): name -> {at, fp,
        # matched_current}, matched_current saying the refused copy was the brain's
        # current token rather than a stale one. For the accounts page; in memory only.
        # forced_refresh_at is when a refusal last made the brain refresh the account
        # (FORCED_REFRESH_MIN_S apart at most). Both are guarded by
        # proxy_refusals_lock, a leaf lock: refresh_lock is held across network calls.
        self.proxy_refusals = {}
        self.forced_refresh_at = {}
        self.proxy_refusals_lock = threading.Lock()


def _st(t):
    """The tenant's store, made at its first use."""
    return t.store_for("anthropic", _Store)


def _read_file(t):
    """The creds file's list, [] without one; raises on an unreadable one."""
    arr = core._store.read_json(_st(t).file)
    return arr if isinstance(arr, list) else []


def _write_file(t, arr):
    core._store.write_json(_st(t).file, arr)


def _hdr(extra=None):
    h = {"anthropic-beta": "oauth-2025-04-20", "anthropic-version": "2023-06-01",
         "User-Agent": f"claude-cli/{CC_VERSION} (external, cli)", "Accept": "application/json"}
    if extra:
        h.update(extra)
    return h


def _get(path, token):
    req = urllib.request.Request(API + path, headers=_hdr({"Authorization": f"Bearer {token}"}))
    with core.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


# What a grant is, beside its tokens: the scopes it was granted and the plan
# behind it. Carried on the account's _creds entry and its accounts.json row only
# once known (a grant made before they were recorded has neither), and never a
# secret.
_GRANT_FACTS = ("scopes", "subscription_type")


def _persist_creds(t):
    """Write current refresh/access tokens back to the creds file (atomic)."""
    try:
        with t.refresh_lock:
            arr = [{"name": n, "refresh_token": c.get("refresh_token"),
                    "access_token": c.get("access_token"), "expires_at": c.get("expires_at"),
                    **{k: c[k] for k in _GRANT_FACTS if c.get(k)}}
                   for n, c in _st(t).creds.items()]
            _write_file(t, arr)
    except Exception as e:  # noqa: BLE001
        core.log(f"WARN could not persist rotated tokens: {e}")


def _token_fp(token):
    """An access token's fingerprint for logs and the accounts page: sha256[:8],
    the switch proxy's fp() (main.go), so both logs name the same token. Never
    the token. (_key_fingerprint can't tell OAuth tokens apart: they share
    their first and last characters.)"""
    if not token:
        return "(none)"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:8]




def _refresh(t, name, why="poll"):
    """Refresh the access token for `name`. Persists the rotated refresh token.

    A refresh makes Anthropic revoke the previous access token, so every copy
    dispensed before it (the proxy's, a cloud session's) dies with it: each one
    is logged with `why` (who asked), what the old token had left and the new
    one's fingerprint."""
    c = _st(t).creds[name]
    old_tok, old_exp = c.get("access_token"), c.get("expires_at") or 0
    body = json.dumps({"grant_type": "refresh_token",
                       "refresh_token": c["refresh_token"], "client_id": CLIENT_ID}).encode()
    req = urllib.request.Request(TOKEN_URL, data=body, method="POST",
                                 headers=_hdr({"Content-Type": "application/json"}))
    with core.urlopen(req, timeout=20) as resp:
        grant = json.loads(resp.read().decode("utf-8"))
    now = time.time()
    c["access_token"] = grant["access_token"]
    c["expires_at"] = now + int(grant.get("expires_in", 28800))
    if grant.get("refresh_token"):
        c["refresh_token"] = grant["refresh_token"]  # rotates every refresh
    scopes = _scope_list(grant.get("scope"))
    if scopes:
        c["scopes"] = scopes  # what the grant carries now; a refresh keeps the granted set
    _persist_creds(t)
    old_left = max(0.0, old_exp - now) if old_tok else None
    fp = _token_fp(c["access_token"])
    _st(t).last_refresh[name] = {"at": now, "why": why,
                           "old_left_s": None if old_left is None else round(old_left), "fp": fp}
    core.log(f"{name}: token refreshed ({why}) — old had {round((old_left or 0) / 60)}m left, "
        f"new expires {time.strftime('%H:%M', time.localtime(c['expires_at']))}, fp {fp}")
    return c["access_token"]


def _scope_list(scope):
    """A token response's space-separated `scope` as a sorted list, or None."""
    if not isinstance(scope, str):
        return None
    return sorted(set(scope.split())) or None


def _ensure_token(t, name, margin=None, why="poll"):
    """The account's access token, refreshed first when it has less than `margin`
    seconds left (default REFRESH_MARGIN). The proxy's /token uses the default;
    a caller that hands the token on asks for more, so what it starts never
    starts on a token about to die. `why` names the caller in the refresh's log line."""
    margin = REFRESH_MARGIN if margin is None else margin
    with t.refresh_lock:
        c = _st(t).creds[name]
        if not c.get("access_token") or time.time() >= (c.get("expires_at", 0) - margin):
            return _refresh(t, name, why=why)
        return c["access_token"]


FORCED_REFRESH_MIN_S = 60


def _proxy_refused(t, name, refused_fp):
    """GET /token?refused_fp=: Anthropic answered 401 to the proxy's copy of
    `name`'s access token, the one with that fingerprint. Records the refusal,
    logs one line, and returns the token to dispense.

    A refused copy that is not the current token was only stale (an early
    refresh revoked it): the current token is dispensed. A refused current
    token is checked once against the usage endpoint and refreshed only if
    that also answers 401, at most once a minute: a blind refresh would revoke
    every copy the cloud sessions hold. Either way the quota poll is woken, so
    a dead grant shows as auth_dead through the poll's own path.

    The token is read and the refusal recorded under refresh_lock, which a new
    grant's splice holds: a grant saved meanwhile clears the record after it,
    never before it (_reauth_exchange_grant)."""
    now = time.time()
    with t.refresh_lock:
        current = (_st(t).creds.get(name) or {}).get("access_token")
        current_fp = _token_fp(current)
        matched = bool(current) and refused_fp == current_fp
        with _st(t).proxy_refusals_lock:
            _st(t).proxy_refusals[name] = {"at": now, "fp": refused_fp, "matched_current": matched}
    head = f"{name}: proxy refused fp {refused_fp}"
    if not matched:
        core.log(f"{head} — a stale copy; dispensing the current token, fp {current_fp}")
        return _ensure_token(t, name, why="token")
    try:
        _get("/api/oauth/usage", current)
        checked = "HTTP 200"
    except urllib.error.HTTPError as e:
        checked = f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        checked = type(e).__name__
    refresh = False
    replaced = False
    if checked == "HTTP 401":
        # Marked only while the refused token is still the account's: a grant
        # saved during the usage check replaced it, and its account owes no wait.
        with t.refresh_lock:
            replaced = (_st(t).creds.get(name) or {}).get("access_token") != current
            if not replaced:
                with _st(t).proxy_refusals_lock:
                    last = _st(t).forced_refresh_at.get(name)
                    refresh = last is None or now - last >= FORCED_REFRESH_MIN_S
                    if refresh:
                        _st(t).forced_refresh_at[name] = now
        outcome = ("its token was replaced meanwhile, not refreshing" if replaced
                   else "refreshing" if refresh
                   else f"refreshed {round(now - last)}s ago, not again yet")
    else:
        outcome = "not refreshing"
    core.log(f"{head} — the current token; usage check {checked}: {outcome}")
    try:
        if refresh:
            with t.refresh_lock:
                if (_st(t).creds.get(name) or {}).get("access_token") == current:
                    _refresh(t, name, why="forced: proxy refused")
        return _ensure_token(t, name, why="token")
    finally:
        with t.poll_health_lock:
            t.poll_schedule.pop(name, None)  # due at once
        core._poll_wakeup.set()


def poll_account(t, name, known_meta=None):
    """Return one Anthropic account's poll result (never raises); its reading
    is the §1.5 normalized reading (_anthropic_reading), None on a failure.

    known_meta = {"email","tier"} already discovered for this account; when both
    are known we SKIP the /profile call (it's stable) to halve the request rate
    against the rate-limited API.
    """
    out = {"name": name, "ok": 0, "ts": time.time(),
           "email": name, "tier": "", "reading": None, "http": 0, "err": "",
           "retry_after": None}
    try:
        token = _ensure_token(t, name, why="poll")
    except urllib.error.HTTPError as e:
        out["http"] = e.code
        out["err"] = f"refresh HTTP {e.code} (account authorization rejected)"
        return out
    except Exception as e:  # noqa: BLE001
        out["err"] = f"refresh {type(e).__name__}: {e}"
        return out
    # profile is best-effort (labels) and stable — fetch once, then reuse cache.
    if known_meta and known_meta.get("email") and known_meta.get("tier"):
        out["email"], out["tier"] = known_meta["email"], known_meta["tier"]
    else:
        try:
            profile = _get("/api/oauth/profile", token)
            out["email"] = (profile.get("account") or {}).get("email", "") or name
            out["tier"] = (profile.get("organization") or {}).get("rate_limit_tier", "") or ""
        except urllib.error.HTTPError as e:
            out["retry_after"] = core._retry_after(e)
            core.log(f"{name}: profile lookup failed (HTTP {e.code}); labeling by config name")
        except Exception as e:  # noqa: BLE001
            core.log(f"{name}: profile lookup failed ({e}); labeling by config name")
    try:
        # ?cedar_ember=1 adds the reset programs to the same read; never
        # at_wall=1, which asks the provider for the at-limit offer.
        out["reading"] = core._anthropic_reading(_get(USAGE_PATH, token), out["ts"], out["tier"])
        out["ok"] = 1
    except urllib.error.HTTPError as e:
        out["http"] = e.code
        out["err"] = f"usage HTTP {e.code}"
        out["retry_after"] = core._retry_after(e)
    except Exception as e:  # noqa: BLE001
        out["err"] = f"usage {type(e).__name__}: {e}"
    return out


def _load_creds(t):
    """(Re)load the creds file into _creds, preserving live cached tokens."""
    with t.refresh_lock:
        try:
            arr = _read_file(t)
        except Exception as e:  # noqa: BLE001
            core.log(f"cannot read {_st(t).file}: {e}")
            return []
        names = []
        for a in arr:
            n = a.get("name")
            if not n or not a.get("refresh_token"):
                continue
            names.append(n)
            facts = {k: a[k] for k in _GRANT_FACTS if a.get(k)}
            if n not in _st(t).creds:
                _st(t).creds[n] = {"refresh_token": a["refresh_token"],
                             "access_token": a.get("access_token"),
                             "expires_at": a.get("expires_at", 0), **facts}
            elif (a["refresh_token"] != _st(t).creds[n]["refresh_token"]
                  and not _st(t).creds[n].get("access_token")):
                # external update before we ever refreshed — adopt it, and what
                # it says about its own grant (none of the old one's facts)
                _st(t).creds[n]["refresh_token"] = a["refresh_token"]
                for k in _GRANT_FACTS:
                    _st(t).creds[n].pop(k, None)
                _st(t).creds[n].update(facts)
        # Prune accounts no longer in the file (e.g. renamed): otherwise
        # _persist_creds writes them back as orphan duplicates that share a rotating
        # refresh token with the live entry and 401 each other. Only prune after a
        # successful, non-empty read.
        if names:
            for stale in set(_st(t).creds) - set(names):
                _st(t).creds.pop(stale, None)
                _st(t).cred_generation.pop(stale, None)
        return names


# ---- self-service authorization (/auth, #72) -------------------------------------
# A dead refresh token used to be remediable only by running claude_oauth.py
# and splicing accounts.json by hand. These
# endpoints move the whole PKCE flow into the brain behind the same X-Switch-Key
# guard as /token, so the fix needs no terminal splice. The verifier and
# the resulting refresh token never leave this process.
#
# #72 generalizes this per provider: every PROVIDERS row carries an `auth`
# authority record (kind, custody, enrollment descriptor, begin/complete/status
# callables), served through ONE surface — GET /auth/providers plus
# POST /auth/<pid>/begin|complete — so adding a provider means adding a row,
# never another bespoke flow. Enrollment (ADDING an account to a pool) is a
# per-provider, per-mode choice; a reauth-mode anthropic grant (begin naming an
# account) still only ever REPLACES.

OAUTH_AUTHORIZE = "https://claude.ai/oauth/authorize"
OAUTH_REDIRECT = "https://console.anthropic.com/oauth/code/callback"
# The interactive-login scope set: exactly what Claude Code itself asks for at
# `claude auth login` (2.1.317), so every account's grant here is a Claude Code
# login and any of them can sign a Claude Code session in. `user:profile` is the one
# the usage endpoints 403 without, and `claude setup-token` doesn't carry it
# (README §Provision).
OAUTH_SCOPE = ("org:create_api_key user:profile user:inference user:sessions:claude_code "
               "user:mcp_servers user:file_upload user:plugins")


# The profile's organization type → Claude Code's `subscriptionType`, which a
# session's credentials file carries (the manager writes it there).
_SUBSCRIPTION_TYPES = {"claude_max": "max", "claude_pro": "pro",
                       "claude_team": "team", "claude_enterprise": "enterprise"}


def _profile_facts(access_token):
    """(email, subscription_type) from the profile endpoint; (None, None) when it
    refuses. subscription_type is None for an organization type Claude Code does
    not name."""
    try:
        p = _get("/api/oauth/profile", access_token)
    except Exception:  # noqa: BLE001
        return None, None
    org_type = (p.get("organization") or {}).get("organization_type")
    return ((p.get("account") or {}).get("email") or None,
            _SUBSCRIPTION_TYPES.get(org_type))


def _verify_profile(access_token):
    """Prove the grant carries user:profile by hitting the profile endpoint; return
    the account's email (the accounts.json key) or None. This is the check that
    keeps an inference-only token out of the creds file — it would poll-403
    forever and look like an outage."""
    return _profile_facts(access_token)[0]


def _reauth_begin(t, account, mode="reauth"):
    """Mint a PKCE authorize URL for `account`. Returns (url, state). The
    default mode is a re-authorization that only ever replaces; mode="add" (account=None)
    is the #72 enrollment path — identity is decided at complete time by the
    VERIFIED profile email, never by operator input."""
    verifier = core._b64url(secrets.token_bytes(32))
    state = core._auth_grant_mint(t, "anthropic", mode, account=account, verifier=verifier)
    params = {"code": "true", "client_id": CLIENT_ID, "response_type": "code",
              "redirect_uri": OAUTH_REDIRECT, "scope": OAUTH_SCOPE,
              "code_challenge": core._b64url(hashlib.sha256(verifier.encode()).digest()),
              "code_challenge_method": "S256", "state": state}
    return OAUTH_AUTHORIZE + "?" + urllib.parse.urlencode(params), state


def _reauth_exchange_grant(t, grant, code):
    """Exchange the code against a consumed grant, verify the grant can reach the
    usage endpoints, and persist the refresh token keyed by the VERIFIED email —
    replace when configured, append when unknown AND the grant was minted as an
    enrollment (mode="add"). Raises ValueError with an operator-readable reason
    on any failure. Returns (email, enrolled)."""
    body = json.dumps({"grant_type": "authorization_code", "code": code,
                       "state": grant["state"], "client_id": CLIENT_ID,
                       "redirect_uri": OAUTH_REDIRECT,
                       "code_verifier": grant["verifier"]}).encode()
    req = urllib.request.Request(TOKEN_URL, data=body, method="POST",
                                 headers=_hdr({"Content-Type": "application/json"}))
    try:
        with core.urlopen(req, timeout=20) as r:
            tok = json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise ValueError(f"exchange HTTP {e.code}") from e
    rt = tok.get("refresh_token")
    if not rt:
        raise ValueError("exchange returned no refresh_token")
    email, subscription_type = _profile_facts(tok.get("access_token", ""))
    if not email:
        raise ValueError("grant lacks user:profile (403 on profile) — cannot poll usage")
    facts = {"scopes": _scope_list(tok.get("scope")), "subscription_type": subscription_type}
    enrolled = _persist_anthropic_grant(t, email, rt, grant.get("mode", "reauth"),
                                        {k: v for k, v in facts.items() if v})
    # A new grant ends the account's live refusal: the refused token was the
    # old grant's. A refusal the proxy reports after this is recorded again.
    with _st(t).proxy_refusals_lock:
        _st(t).proxy_refusals.pop(email, None)
        _st(t).forced_refresh_at.pop(email, None)
    with t.poll_health_lock:
        t.poll_fails.pop(email, None)
        t.poll_schedule.pop(email, None)  # due immediately
    core._poll_wakeup.set()
    return email, enrolled


def _persist_anthropic_grant(t, email, rt, mode, facts=None):
    """Route a verified grant to the right persistence. A configured email is
    always a REPLACE (safe re-auth, even mid-enrollment — approving as an
    existing account must repair that account, not duplicate it). An unknown
    email appends only when the flow was begun as an enrollment; a reauth-mode
    grant keeps _splice_refresh_token's refusal, so a re-authorization never adds.
    `facts` is the grant's _GRANT_FACTS (scopes, subscription_type) as far as
    the exchange knew them. Returns True when a new account was enrolled."""
    if mode == "add" and email not in _configured_account_names(t):
        _append_account(t, email, rt, facts)
        return True
    _splice_refresh_token(t, email, rt, facts)
    return False


def _configured_account_names(t):
    """Account names currently in accounts.json (fallback: the live cache when
    the file is unreadable — _append_account will then surface the real error)."""
    with t.refresh_lock:
        try:
            return {a.get("name") for a in _read_file(t) if isinstance(a, dict)}
        except Exception:  # noqa: BLE001
            return set(_st(t).creds)


def _splice_refresh_token(t, email, refresh_token, facts=None):
    """Replace one configured account's persisted AND live credential atomically.

    Never adds an account or touches another entry. The shared refresh lock prevents
    an in-flight token rotation from writing the dead chain back after reauth.
    """
    with t.refresh_lock:
        try:
            arr = _read_file(t)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"the accounts could not be read ({type(e).__name__})") from e
        facts = dict(facts or {})
        hit = False
        for a in arr:
            if a.get("name") == email:
                a["refresh_token"] = refresh_token
                a.pop("access_token", None)
                a.pop("expires_at", None)
                for k in _GRANT_FACTS:  # the old grant's, not this one's
                    a.pop(k, None)
                a.update(facts)
                hit = True
        if not hit:
            raise ValueError(f"{email} is not a configured account — refusing to add it")
        try:
            _write_file(t, arr)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"the accounts could not be updated ({type(e).__name__})") from e
        _st(t).creds[email] = {"refresh_token": refresh_token,
                         "access_token": None, "expires_at": 0, **facts}
        _st(t).cred_generation[email] = _st(t).cred_generation.get(email, 0) + 1


def _append_account(t, email, refresh_token, facts=None):
    """Enroll a NEW Anthropic account (#72): append to accounts.json and the live
    cache atomically. Identity is the VERIFIED profile email — approving as the
    wrong account enrolls that account, visibly, rather than corrupting an
    existing entry. Only add-mode grants reach here (_persist_anthropic_grant);
    the never-add rule survives as reauth-mode policy."""
    with t.refresh_lock:
        try:
            arr = _read_file(t)  # a first enrollment may legitimately create the file
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"the accounts could not be read ({type(e).__name__})") from e
        if any(a.get("name") == email for a in arr if isinstance(a, dict)):
            raise ValueError(f"{email} is already configured — re-authorize it instead")
        if email in core.PROVIDERS or email in core._provider_poll_targets(t):
            # _state is one flat namespace keyed by target name (#71); a
            # colliding name would fuse two targets' poll bookkeeping.
            raise ValueError(f"{email!r} collides with a provider target name")
        facts = dict(facts or {})
        arr.append({"name": email, "refresh_token": refresh_token, **facts})
        try:
            _write_file(t, arr)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"the accounts could not be updated ({type(e).__name__})") from e
        _st(t).creds[email] = {"refresh_token": refresh_token,
                        "access_token": None, "expires_at": 0, **facts}
        _st(t).cred_generation[email] = _st(t).cred_generation.get(email, 0) + 1
    core.log(f"enrolled new anthropic account {email}")


def _anthropic_auth_status(t, now):
    """One row per configured account: _account_health verbatim plus the
    (secret-free) cached-token expiry, and whatever an annotator adds
    (_status_annotators).

    The proxy's view rides along, for the page only (never `health`, never
    /preferred): `proxy_refusal` {at, fp, matched_current}, the proxy's last
    report that Anthropic refused its copy of the token, or None; `served_at`,
    the last served request's head_at, or None (a refusal after it means the
    proxy is still being refused); `last_refresh` {at, why}, or None."""
    with t.refresh_lock:
        names = list(_st(t).creds)
        exp = {n: (_st(t).creds.get(n) or {}).get("expires_at") for n in names}
        scopes = {n: (_st(t).creds.get(n) or {}).get("scopes") for n in names}
        refreshed = {n: {"at": r["at"], "why": r["why"]} for n, r in _st(t).last_refresh.items()}
    with _st(t).proxy_refusals_lock:
        refusals = {n: dict(r) for n, r in _st(t).proxy_refusals.items()}
    with t.served_lock:
        served_at = dict(t.served["head_at"])
    accounts = core._account_health(t, names, now)["accounts"]
    for a in accounts:
        a["token_expires_at"] = exp.get(a["name"]) or None
        a["proxy_refusal"] = refusals.get(a["name"])
        a["served_at"] = served_at.get(a["name"])
        a["last_refresh"] = refreshed.get(a["name"])
    for annotate in _status_annotators:
        annotate(t, accounts, scopes)
    return accounts


def _anthropic_auth_begin(t, account=None):
    return _reauth_begin(t, account, mode="reauth" if account else "add")


def _anthropic_auth_complete(t, grant, fields):
    code = str(fields.get("code") or "").split("#")[0].split("&")[0].strip()
    if not code:
        raise ValueError("code is required — paste the code shown after approving")
    email, enrolled = _reauth_exchange_grant(t, grant, code)
    return {"account": email, "enrolled": enrolled, "reauthorized": not enrolled}


def _anthropic_auth_remove(t, account):
    """Drop one account from accounts.json (the file with the last one) and
    from the live cache. Its generation moves on, so a poll of it already in
    flight is discarded rather than published."""
    with t.refresh_lock:
        try:
            arr = _read_file(t)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"the accounts could not be read ({type(e).__name__})") from e
        rest = [a for a in arr if not (isinstance(a, dict) and a.get("name") == account)]
        if len(rest) == len(arr):
            raise ValueError(f"{account} is not connected — nothing was removed")
        try:
            if rest:
                _write_file(t, rest)
            else:
                core._remove_credentials(_st(t).file)
        except OSError as e:
            raise ValueError(f"the accounts could not be updated ({type(e).__name__})") from e
        _st(t).creds.pop(account, None)
        _st(t).cred_generation[account] = _st(t).cred_generation.get(account, 0) + 1
        _st(t).last_refresh.pop(account, None)
    with _st(t).proxy_refusals_lock:
        _st(t).proxy_refusals.pop(account, None)
        _st(t).forced_refresh_at.pop(account, None)
    core.log(f"removed anthropic account {account}")


def _token(t, self, parsed):
    """Dispense a fresh access token for an account. Secret-guarded — this hands
    out an inference bearer, so it must never be reachable without SWITCH_KEY.
    ?refused_fp=<sha256[:8]> is the proxy saying Anthropic refused its copy
    with that fingerprint (_proxy_refused); the answer has the same shape."""
    if not core.SWITCH_KEY:
        return self._json(503, {"error": "SWITCH_KEY not configured"})
    if not hmac.compare_digest(self.headers.get("X-Switch-Key", ""), core.SWITCH_KEY):
        self.send_response(401)
        self.end_headers()
        return
    qs = parse_qs(parsed.query)
    account = (qs.get("account") or [""])[0]
    refused_fp = re.sub(r"[^0-9A-Za-z()]", "", (qs.get("refused_fp") or [""])[0])[:64]
    if account == "preferred":
        with t.plock:
            account = t.preferred.get("account")
    if not account or account not in _st(t).creds:
        return self._json(404, {"error": f"unknown account {account!r}"})
    try:
        tok = (_proxy_refused(t, account, refused_fp) if refused_fp
               else _ensure_token(t, account, why="token"))
    except Exception as e:  # noqa: BLE001
        return self._json(502, {"error": f"token refresh failed: {type(e).__name__}"})
    exp = (_st(t).creds.get(account) or {}).get("expires_at")
    self._json(200, {"account": account, "access_token": tok, "expires_at": exp})


def _start(t):
    """At boot: say when the creds file is missing, then load it, so the state
    loader restores only the configured accounts."""
    if not os.path.exists(_st(t).file):
        core.log(f"WARNING: {_st(t).file} not found — exporter serves poll_success=0 until it appears.")
    _load_creds(t)


# Called with the tenant, the status rows and {name: scopes} after
# _anthropic_auth_status fills them: each may add its own facts to a row. A
# list of callables registered at import (not state: the inspect walk allows it).
_status_annotators = []


# The anthropic row is a FACADE (poll=None): its accounts are polled by the
# account loop and routed by the lane machinery. The row exists so
# authorization addresses every provider through one registry. The loop's
# callables look the functions up at call time, so a test's stub of one is seen.
register("anthropic", {
    "poll": None,
    "enabled": lambda t: True,
    "account_loop": {
        "start": lambda t: _start(t),
        "load": lambda t: _load_creds(t),
        "names": lambda t: list(_st(t).creds),
        "poll": lambda t, name, known_meta: poll_account(t, name, known_meta),
        "generation": lambda t, name: _st(t).cred_generation.get(name, 0),
        "expires_at": lambda t, name: (_st(t).creds.get(name) or {}).get("expires_at"),
    },
    "auth": {
        "kind": "oauth-pkce",
        "label": "Anthropic (Claude subscription)",
        "custody": {"rotation": "independent",
                    "note": "Independent grants coexist — authorizing here never "
                            "disturbs a laptop login."},
        "enroll": {"flow": "paste-code", "can_add": True,
                   "instructions": "Open the authorization link, approve as the "
                                   "intended account, then paste the code shown "
                                   "(code#state). The account is identified by the "
                                   "verified email, not by what you type.",
                   "fields": [{"name": "code", "label": "code#state", "secret": False}]},
        "status": _anthropic_auth_status,
        "begin": _anthropic_auth_begin,
        "complete": _anthropic_auth_complete,
        "remove": _anthropic_auth_remove,
    },
})
register_route("GET", "/token", _token)
