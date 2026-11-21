#!/usr/bin/env python3
"""claude-usage-exporter — the routing brain's core: Prometheus exporter, quota
model and lane/class policy for the switch proxy.

The core holds no provider. Each driver (providers/ for the shared ones, an
edition's own beside them) registers itself with providers.register(): how to
poll its accounts, enrol them, and dispense their credentials. An entry point
(brain.py: the shared drivers) imports the drivers it runs, then calls main().

Design: stdlib-only files,
bind-mounted into a stock python:slim container and run directly (no image
build). A single background thread polls every POLL_INTERVAL and caches the
rendered exposition; /metrics returns the cache, so Prometheus scrapes never
reach a provider directly.

Credentials files live under /creds, never committed, mounted RW so rotated
tokens persist across restarts. An account's `name` is the STABLE Prometheus
`account` label (e.g. "user@example.com"): it is NOT re-derived from a provider,
so a transient failure can't churn the label.

Providers rate-limit their usage reads, so:
  * polls default to every 5 min (plenty of resolution for a 7-day window),
  * a 429/5xx backs that account off exponentially (honoring Retry-After),
  * the last successful snapshot is retained and kept in the exposition
    through transient failures (poll_success flips to 0 and data_age_seconds
    climbs, but the utilization gauges don't vanish — so alerts don't flap blind).

Env:
    PORT                 (default 9115)
    POLL_INTERVAL        seconds between polls per account/provider (default 300)
    POLL_MAX_BACKOFF     cap on each 429/5xx backoff wait, seconds (default
                         1500); a poll with no HTTP response retries at
                         POLL_INTERVAL
    (each driver documents its own: credentials files, upstream URLs)
    KIMI_USAGES_URL      Override for https://api.kimi.com/coding/v1/usages
    SWITCH_CREDS_DIR     Where the brain's data lives (default /creds): the
                         registry tenants.json, lanes.json, the switch key, and
                         one dir per tenant, /creds/<tenant>/, with that
                         tenant's documents (its classes.json among them; see
                         classes.example.json) and credentials.
    SWITCH_CREDS_KEY_FILE The sealing key (credstore.py): set, every tenant
                         file is AES-256-GCM sealed and a plain one is refused;
                         unset, files are plain and a sealed one is refused.
    SWITCH_BRIDGE_URL    The alert bridge the prepaid notices post to. Unset:
                         the edition's default (none in the shared edition);
                         set but empty: the notices are off.
    SWITCH_SESSION_IDLE  Seconds before an idle class-session binding is dropped
                         (#26), default 21600. Hygiene only — priority
                         restoration is the reset-boundary drift-up's job.
"""
import base64
import bisect
import copy
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import shutil
import signal
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request

# Patch seam for tests: network goes through this name so tests can stub it
# module-locally without mutating urllib globally (the tests' own HTTP client uses the real one).
urlopen = urllib.request.urlopen


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuses every redirect: urllib re-sends a request's headers, a secret
    in them included, to wherever a 3xx points (https to http too). The 3xx
    is raised as an HTTPError naming only its status: no Location, no
    header of the request."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ARG002
        if fp is not None:
            fp.close()
        raise urllib.error.HTTPError(urlparse(req.full_url)._replace(query="", fragment="").geturl(), code,
                                     f"redirect {code} not followed", {}, None)


# Every request that carries a provider's key or token in its headers goes
# through this name: it follows no redirect. A patch seam like urlopen.
key_urlopen = urllib.request.build_opener(_NoRedirect).open
from collections import OrderedDict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# The driver registry (providers/__init__.py): every provider-specific table the
# core reads is filled by the driver modules an entry point imports.
#   PROVIDERS          provider id -> row (poll, enabled, accounts, auth, ...)
#   _API_POTS          an api-kind provider's money pot name (§1.2); every pot
#                      id is "<account>/<pot>"
#   _COUNTABLE_UNITS   the pots whose charges can be counted (§1.7), by the
#                      provider of their account, with their native unit
#   _LEDGER_RATES      the rates the proxy charges by, per countable unit
#                      (ledger.rates). A unit whose charges need a table it
#                      lacks would have them entered uncounted, so a budget on
#                      its pot is refused (_money_problems) and reads no
#                      (unknown) (_budget_gate)
#   _LEDGER_RATE_TABLES the tables each unit's charges need (ledger.rate_tables),
#                      published so the brain's gate, the proxy's and the
#                      proxy's deploy preflight check exactly these. A unit not
#                      listed has no rule, and none of its charges are counted
#   _MODEL_MODALITY    (provider, model) -> what the model makes, when not chat
#   ROUTES             (method, path) -> a driver's HTTP handler
from providers import (PROVIDERS, ROUTES, _API_POTS, _COUNTABLE_UNITS,  # noqa: E402
                       _LEDGER_RATE_TABLES, _LEDGER_RATES, _MODEL_MODALITY)
import credstore  # noqa: E402 — the sealed store every tenant file goes through
import notices  # noqa: E402 — the prepaid "needs payment" notices
import tenancy  # noqa: E402 — Tenant and the registry

PORT = int(os.environ.get("PORT", "9115"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "300"))
# A provider that answered with an error and no Retry-After backs off toward
# this. It caps each wait, not the time since the last good reading: the waits
# add up (300, 600, 1200 s: asks at 300, 900 and 2100 s after the reading), so
# an account that keeps failing goes stale past SWITCH_STALE_MAX and the stale
# rule decides (quota-model 50 §1.6). Keeping one wait under 1800 bounds how
# long a recovered account waits to be read again (§2.10: a 3600 s cap can
# hold every Anthropic account unread for an hour).
POLL_MAX_BACKOFF = int(os.environ.get("POLL_MAX_BACKOFF", "1500"))


def _read_file(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return ""


# ---- switchover brain config (the /preferred policy + guarded /token dispenser) ----
# The proxy (services/claude-switch-proxy) polls /preferred to learn which account
# has headroom and pulls a fresh token from /token to route requests there. This
# exporter is the SOLE token refresher, so there's one authority and no rotation race.
CREDS_DIR = os.environ.get("SWITCH_CREDS_DIR", "/creds")
SWITCH_KEY = os.environ.get("SWITCH_KEY", "") or _read_file(os.environ.get("SWITCH_KEY_FILE", ""))
# N equal accounts balanced by utilization — no account is privileged ("home"/"native"
# is gone). A lane stays on its incumbent account and only leaves when forced, moving to
# whichever remaining account is coolest. See _compute_lane.
SW_HIGH = float(os.environ.get("SWITCH_HIGH_PCT", "80"))     # weekly % at which we consider leaving an account
SW_MARGIN = float(os.environ.get("SWITCH_MARGIN_PCT", "15")) # min headroom advantage (pts) to justify a switch
SW_HARD = float(os.environ.get("SWITCH_HARD_PCT", "95"))     # weekly % that forces a switch to any lower account
SW_DWELL = int(os.environ.get("SWITCH_MIN_DWELL", "900"))    # min seconds between switches (anti-flap)
STALE_MAX = int(os.environ.get("SWITCH_STALE_MAX", "1800"))  # exclude accounts whose data is older than this
SESSION_IDLE = int(os.environ.get("SWITCH_SESSION_IDLE", "21600"))  # drop class-session bindings idle longer than this (6h > the 5h window, so drift bookkeeping outlives a window)

# Per-model routing lanes: the proxy extracts the request's "model" and routes each
# lane to its own account independently (fable can ride one account while haiku
# stays on another). `match` = lowercased substrings of the request model-id (proxy-side);
# `scope_model` = EXACT join key against usage limits[].scope.model.display_name
# (brain-side) so a lane's effective utilization is max(overall 7d, the 5-hour
# rolling session, its scoped cap) — every limit that would 429 a request for that
# model, so we never route onto an account that can't actually serve it.
# Override with a JSON array at SWITCH_LANES_FILE; the "default" lane (no match, no
# scope = max(7d, 5-hour session) = the old single-lane policy plus the session gate)
# is always appended and cannot be redefined. Rules served to the proxy in order;
# first match wins.
LANES_FILE = os.environ.get("SWITCH_LANES_FILE") or os.path.join(CREDS_DIR, "lanes.json")
DEFAULT_LANES = [
    {"lane": "fable",  "match": ["fable"],  "scope_model": "Fable"},
    {"lane": "opus",   "match": ["opus"],   "scope_model": "Opus"},
    {"lane": "sonnet", "match": ["sonnet"], "scope_model": "Sonnet"},
    {"lane": "haiku",  "match": ["haiku"],  "scope_model": "Haiku"},
]

# Anthropic's other names for a scoped weekly cap, keyed by the poll's
# limits[].scope.model.display_name: the per-response header window token
# (anthropic-ratelimit-unified-<token>-*) and the representative-claim. Published
# on each scoped window (/preferred targets[*].windows["scoped:<name>"].aliases)
# so the proxy reads a header window by meaning, never by a name substring (C5,
# quota-model 10 D1). DECLARED, not derived: a scoped cap shares its reset with the
# account's 7d window (Fable's does, 10 §2), so matching resets cannot tell one
# window from another, and a claim only arrives on a refusal, which the brain never
# sees. Each entry is a capture: Fable = header 7d_oi + claim
# seven_day_overage_included (proxy log: 7d_oi
# rejected at 1.0 while 7d read allowed_warning at 0.89, same reset as the poll's
# scoped Fable). A family with no entry (Opus, Sonnet, Mythos: tokens never seen,
# 10 U4) publishes aliases null and the proxy keeps its header window "unmapped".
SCOPE_ALIASES = {
    "Fable": {"header": ["7d_oi"], "claim": ["seven_day_overage_included"]},
}

# Model classes (#25): OPT-IN substitution groups. A class is an ordered
# user-priority list of interchangeable members — Anthropic lanes and provider
# models — that the proxy will advertise as ONE synthetic picker row
# (claude-<class>, #27) while the brain points sessions at the best member
# (#26's policy). Named models keep today's account-only switching untouched.
# Each tenant's own document, <tenant dir>/classes.json: no compiled-in
# classes, so a tenant without one routes no class. Schema (see
# classes.example.json):
#   [{"class": "frontier",            # ^[a-z]+(-[a-z]+)*$ — becomes claude-<class>
#     "display": "Frontier (auto)",   # picker display_name (#27); default = name
#     "guidance": "Top tier for open design and hard decisions",  # optional, null = none
#     # Guidance is stripped, non-empty, <=300 chars, with no ASCII controls/DEL.
#     "members": [
#       {"priority": 1, "kind": "anthropic", "lane": "opus", "context_window": 200000},
#       {"priority": 2, "kind": "provider", "provider": "kimi", "model": "kimi-k3",
#        "context_window": 256000, "anthropic_token_ratio": 0.61}]}]
# A class may carry "request_kind": "chat" (default) or "image" (quota-model 50
# §2.9, C4: a class of the image door; "images" is its default and must be one).
# An image class has provider members only, and they declare no
# context_window, anthropic_token_ratio or calibration (it has no token
# window), so its window fields normalize to null. Every member's model makes
# what its class's requests are (_MODEL_MODALITY).
# Every chat member DECLARES context_window — never inferred (the model-registry
# spike rule). Provider members also declare anthropic_token_ratio (r_p, #44):
# tokens the provider reports for a body ÷ tokens Anthropic reports for the
# same body (C1-calibrated; served on /preferred and /class-bind, the proxy's
# one source for scaling a provider hop's usage). r_anthropic = 1.0 by definition and may not
# be authored. Each member gains a derived effective_window =
# floor(context_window / r_p) — its window in ANTHROPIC units — and the class's
# own window is DERIVED = min(member effective windows), served as derived:
# there is no floor (#135) — the proxy advertises a class's window per id
# (/v1/models context_window), so a 64k local class says 64k rather than
# pretending to 200k. Claude Code itself still assumes 200k for a claude-* id
# it does not know; the README's local-class section carries that caveat.
# Owner settings (quota-model 50 §1.9): any member may carry `pay` (the money
# pot ids it may draw, default []) and `stop_at` ({window, level}, default none);
# a provider member may carry calibrated_at / calibration_ref (where its r_p
# came from). The document may instead be {"classes": [...], "lanes": {"<lane>":
# {"pay", "stop_at"}}} to give bare lanes the same settings; a bare array still
# loads. Pot ranking and topup live in the money document (MONEY_FILE).
# The LAN tenant's document may mark a class "onboarding": true, offered to new
# members: a tenant made by PUT /tenants starts with a copy of those classes
# (_starter), and a member's document may not carry the key.

# The tenants (tenancy.py): every other document is one tenant's, in its dir
# under CREDS_DIR, read and written through the sealed store (credstore.py).
# The registry is filled by boot(); a Tenant is made by _tenant_fresh.
_store = credstore.Store.from_env()
_registry = None  # tenancy.Registry, set by boot()

# Every subscription account has these two pots until api accounts arrive (C8):
# the allowance its windows measure, then the credits the provider draws next.
ALLOWANCE_POT = "allowance"
MONEY_POT = "credits"


def _money_pot(provider):
    """The name of an account's money pot, by its provider."""
    return _API_POTS.get(provider, MONEY_POT)


def _account_kind(provider):
    """subscription | api (§1.2), by provider: what an account is before any
    reading says so."""
    return "api" if provider in _API_POTS else "subscription"

# The runtime-config API (#92, ui-revamp §6): request bodies carry WHOLE
# documents, so these endpoints must not inherit _read_json_body's 16 KB
# enrollment-sized cap. ≥ 256 KB per the spec; 512 KiB leaves headroom.
DOC_BODY_MAX = 512 * 1024

SEVERITY = {"normal": 0, "warning": 1, "critical": 2}
UA = None  # set in main

# ---- per-tenant state ----------------------------------------------------------
# Every piece of state that is one tenant's lives on its Tenant (tenancy.py),
# made here. The field comments below are the lock discipline, unchanged from
# the single-tenant brain; each tenant has its own locks, and no lock of one
# tenant is ever held while another's is taken.
#
#   state          name -> latest render-facing poll result. Guarded by t.lock.
#   refresh_lock   serializes credential-file/cache replacement and token
#                  refresh; re-entrant because the drivers' _load_creds and
#                  _persist_creds are also called from refresh-locked paths.
#   preferred      the switchover decision state (which account the proxy
#                  should route to), guarded by t.plock. "pinned" forces the
#                  choice and disables the auto-policy until cleared. The flat
#                  top-level keys mirror the "default" lane; per-lane state
#                  lives in "lanes", per-class in "classes".
#   session_journal / journal_seq
#                  the class pass's recent session moves as (seq, move),
#                  newest last, guarded by t.plock. A PUT of the owner settings
#                  reports the moves made after it swapped them in (_doc_put).
#   poll_fails / poll_schedule
#                  per-target poll health, guarded by t.poll_health_lock. The
#                  poller thread is the only writer; the HTTP handlers read.
#   recheck_epochs the owner's re-checks (quota-model 50 §2.6, C7): account ->
#                  the instant of its last /recheck or enrolment (C11),
#                  published as the account's recheck_epoch; the proxy drops
#                  every refusal record on the account older than it, and an
#                  advance drops its cached credential (a re-added account is a
#                  new identity). Guarded by t.poll_health_lock; kept in
#                  t.recheck_file (_save_rechecks_locked, _load_rechecks).
#   enrol_lock     a remove (POST /auth/<pid>/remove) holds it from its
#                  driver's remove through _forget_account; an enrolment
#                  (/auth/<pid>/complete) takes it for its epoch advance, after
#                  its driver wrote the account. A keyring enrolment also holds
#                  it from its name check through its write, so two providers'
#                  enrolments never take one name. So a remove's forget never
#                  lands after an overlapping re-add's advance: an enrolled
#                  account always has an epoch newer than any forget. Outermost:
#                  taken before any lock a driver or the core takes.
_start_time = time.time()
# Re-authorization sets this after clearing one account's schedule; a tenant's
# creation or removal sets it too. One poller serves every tenant. Event
# semantics retain a signal raised while the poller is busy.
_poll_wakeup = threading.Event()


def _tenant_fresh(tid, name, lan, tap, dir_):
    """A Tenant with every state field at its start value: what the single-
    tenant brain had at import. The documents are read from its dir
    (consumers, money); the rest (state, rechecks, ledger, sizing, notices) is
    read by _tenant_boot, as main() did."""
    t = tenancy.Tenant(tid, name, lan, tap, dir_, _store)
    t.state = {}
    _load_classes(t)
    t.preferred = {"account": None, "since": 0.0, "reason": "init", "last_switch": 0.0,
                   "pinned": None, "lanes": {}, "classes": {}, "providers": {}}
    t.session_journal = deque(maxlen=256)
    t.journal_seq = 0
    t.poll_fails = {}
    t.poll_schedule = {}
    t.recheck_epochs = {}
    t.consumers_doc = {"consumers": []}
    t.consumers_warnings = []
    t.consumers = _load_consumers(t)
    t.money_doc = {}
    t.money_warnings = []
    t.money = _load_money(t)
    t.ledger = _ledger_fresh()
    t.reported = {}
    t.shut_reports = {}
    t.holds = {}
    t.limit_holds = {}
    t.served = _served_fresh()
    t.sizing_gate = {"closing": False, "inflight": 0, "frozen": None, "set_aside": None}
    t.sizing = _sizing_fresh()
    t.events_pub = {"seq": 0, "data": None, "digest": None, "ledger_as_of": None}
    t.events_subs = set()
    t.reauth_pending = {}
    t.notices = notices.Notices(t.notices_file, log=lambda m: log(f"{tid}: {m}"),
                                bridged=lan, store=_store)
    return t


def _tenant_boot(t):
    """Read a tenant's persisted state (main() did this once for the one
    tenant): the switch state, the ledger, the re-checks, the notices and the
    sizing, then start its notices worker."""
    _load_state(t)
    with t.lock:
        m = t.money
        pots = m["pots"]
        pay_order = m["pay_order"]
    log(f"{t.id}: money: {len(pay_order)} in pay_order, {len(pots)} pots configured "
        f"({t.money_file})")
    _load_ledger(t)
    _load_rechecks(t)
    n = t.notices.load()
    log(f"{t.id}: notices: {n} prepaid accounts remembered ({t.notices_file}; bridge "
        f"{'on' if t.notices.bridged and notices.bridge_url() else 'off'})")
    t.notices.start()
    _sizing_load(t)
    log(f"{t.id}: consumers: {len(t.consumers)} configured ({t.consumers_file})")


def _tenant_retire(t):
    """Stop a tenant's threads and drop what it holds (DELETE /tenants/<t>):
    its subscribers are told to close, its notices worker stopped. The
    registry has already forgotten it, so no request or poll reaches it
    again; its dir is removed by the registry after this."""
    t.stopped = True
    t.notices.stop()
    with t.events_cond:
        t.events_cond.notify_all()
    _poll_wakeup.set()


def _classify_poll_error(res):
    """Bucket a failed poll result into the error kind a remediation hangs off.

    `auth` means the refresh or usage call was refused with 400/401 — the refresh
    token is dead and only re-authorization fixes it. `rate_limit` (429) self-clears
    via the backoff. Everything else is `transient`. The poll result only carries the
    HTTP code + a prose err string, so the classification keys off the code."""
    if res.get("ok"):
        return "none"
    if res.get("http") in (400, 401):
        return "auth"
    if res.get("http") == 429:
        return "rate_limit"
    return "transient"


def _account_health(t, names, now):
    """Health rows + worst rollup for `names` (poll-target names).

    worst: "auth_dead" if any account's last error was an auth refusal (a human must
    re-authorize — nothing self-clears), else "degraded" if any account is serving
    stale last-good data, else "ok". This is the building block: each provider authority's
    status() calls it with its own roster, and /preferred's `account_health` is the
    cross-provider union via _unified_account_health."""
    order = {"ok": 0, "degraded": 1, "auth_dead": 2}
    accounts = []
    worst = "ok"
    with t.lock:
        snap = {n: t.state.get(n) for n in names}
    with t.poll_health_lock:
        fails = dict(t.poll_fails)
        schedule = dict(t.poll_schedule)
    for n in names:
        r = snap.get(n) or {}
        kind = _classify_poll_error(r)
        if kind == "auth":
            health = "auth_dead"
        elif r.get("stale"):
            health = "degraded"
        else:
            health = "ok"
        if order[health] > order[worst]:
            worst = health
        retry_at = schedule.get(n)
        accounts.append({
            "name": n,
            "poll_ok": int(bool(r.get("ok"))),
            "health": health,
            "last_error_kind": kind,
            "consecutive_failures": fails.get(n, 0),
            "stale": int(bool(r.get("stale"))),
            "good_ts": r.get("good_ts"),
            "age_s": round(now - r["good_ts"], 1) if r.get("good_ts") else None,
            "next_retry_in_s": round(retry_at - now, 1) if retry_at and retry_at > now else 0,
            "err": r.get("err") or "",
        })
    return {"worst": worst, "accounts": accounts}


# The per-account keys _account_health emits. _unified_account_health projects
# authority status() rows down to these + `provider`: the auth-page extras
# (token expiry, key fingerprint) belong to the key-guarded /auth/providers,
# and /preferred is unguarded — nothing beyond poll health may ride along.
_HEALTH_ROW_KEYS = ("name", "poll_ok", "health", "last_error_kind",
                    "consecutive_failures", "stale", "good_ts", "age_s",
                    "next_retry_in_s", "err")


def _unified_account_health(t, now):
    """/preferred `account_health`: every account every provider authority
    manages, not just anthropic's. Pre-#72 this was anthropic-only ("providers
    have no re-auth flow"); now each PROVIDERS row's status() reports the same
    per-target poll health for its roster (gpt chains, the kimi key), so a dead
    provider's chain must trip the same `worst` rollup a dead anthropic refresh
    token does — a strip that stays green while quota routing degrades is the
    dashboard asserting the wrong half of the picture. Rows gain `provider` so
    the UI can label the roster; `worst` re-rolls across the union (each
    status() rolled up only its own slice)."""
    order = {"ok": 0, "degraded": 1, "auth_dead": 2}
    accounts, worst = [], "ok"
    for pid, p in PROVIDERS.items():
        a = p.get("auth")
        if not a:
            continue
        for row in a["status"](t, now):
            slim = {k: row.get(k) for k in _HEALTH_ROW_KEYS}
            slim["provider"] = pid
            if order.get(slim.get("health"), 0) > order[worst]:
                worst = slim["health"]
            accounts.append(slim)
    return {"worst": worst, "accounts": accounts}


def log(msg):
    print(f"[claude-usage] {msg}", flush=True)


def _load_lanes():
    """Load lane definitions (LANES_FILE overrides DEFAULT_LANES); always append the
    implicit "default" lane last so the proxy's first-match walk has a catch-all."""
    lanes = None
    try:
        with open(LANES_FILE) as f:
            lanes = json.load(f)
    except FileNotFoundError:
        pass
    except Exception as e:  # noqa: BLE001
        log(f"WARN bad lanes file {LANES_FILE}: {e} — using built-in defaults")
    if not isinstance(lanes, list) or not lanes:
        lanes = DEFAULT_LANES
    out, seen = [], set()
    for lc in lanes:
        if not isinstance(lc, dict):
            continue
        name = str(lc.get("lane") or "").strip()
        if not name or name == "default" or name in seen:
            continue
        seen.add(name)
        out.append({"lane": name,
                    "match": [str(m).lower() for m in (lc.get("match") or []) if m],
                    "scope_model": lc.get("scope_model") or None})
    out.append({"lane": "default", "match": [], "scope_model": None})
    return out


_lanes = _load_lanes()
_lane_names = [lc["lane"] for lc in _lanes]

_CLASS_NAME_RE = re.compile(r"^[a-z][a-z0-9-]*$")  # a consumer's id
# A class's id: letters in hyphenated words, no digit. The proxy advertises
# claude-<class> only when it could later refuse the id as class-shaped
# (claude-switch-proxy classShapedRe), and a digit is not class-shaped there, so
# a class with one would load and never be offered.
_CLASS_ID_RE = re.compile(r"^[a-z]+(-[a-z]+)*$")
_CLASS_ID_RULE = "class name must match ^[a-z]+(-[a-z]+)*$, with no digit (it becomes the claude-<class> picker id)"


def _guidance_error(guidance):
    """Optional class prose must never contain controls, even at its edges."""
    if guidance is None:
        return None
    if not isinstance(guidance, str):
        return "guidance must be a string or null"
    if not guidance.strip():
        return "guidance must be non-empty after stripping whitespace"
    if len(guidance.strip()) > 300:
        return "guidance must be at most 300 characters after stripping whitespace"
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in guidance):
        return "guidance must be one line with no ASCII control characters or DEL"
    return None


def _member_id(m):
    """Stable member identity for bindings/pins — survives priority renumbering
    in classes.json: 'lane:<lane>' for anthropic members, '<provider>/<model>'
    for provider members. A route of a class is "<class>/<member id>"
    (quota-model 50 §1.9)."""
    if m.get("kind") == "anthropic":
        return f"lane:{m['lane']}"
    return f"{m.get('provider')}/{m.get('model')}"


# A budget is pot-grain (§1.9): it lives in the money document, on the pot it
# caps, and a route's own budget in that pot's route_budgets. A classes
# document carrying one would have it accepted and ignored, so it is refused.
_BUDGET_KEYS = ("budget", "route_budgets")
_BUDGET_REASON = ("a budget lives in the money document: pots[<pot id>].budget, or "
                  "pots[<pot id>].route_budgets[<route id>] for one route")
# The keys a class and a member may carry (§1.9: nothing is dropped or
# ignored, so an unknown key, a typo included, is refused at PUT).
_CLASS_KEYS = frozenset(("class", "display", "guidance", "request_kind", "members", "onboarding"))
# The LAN tenant's mark on a class it offers to new members (_starter); a
# member's document cannot offer anything, so it may not carry the key.
# The previous brain's visibility mark (one classes document for every
# tenant), retired by migrate_classes.py; a revision from before it carries it.
_LAN_ONLY_REASON = ("lan_only is retired: each tenant has its own classes, and `onboarding` "
                    "marks the classes offered to new members (lan_only: true is onboarding: false; "
                    "migrate_classes.py rewrote the document)")
_ONBOARDING_MEMBER_REASON = ("onboarding marks a class the operator offers to new members; "
                             "only the LAN tenant's document carries it")

# What a class's requests are (quota-model 50 §2.9): "chat" (the default) or
# "image", the image door's class. A token window and r_p measure chat bodies,
# so an image member declares neither, and only a provider makes images.
_REQUEST_KINDS = ("chat", "image")
# The image door's default class (the proxy's hpImagesDefaultClass): a request
# that names no class (model gpt-image-*) is routed here, so a chat class under
# this name would serve the door nothing. Other image classes, such as the
# one-provider images-runway, are reached by name (the request's model or its
# X-Switch-Class header).
IMAGE_CLASS = "images"


def _model_modality(provider, model):
    return _MODEL_MODALITY.get((provider, model), "chat")


def _image_models():
    return sorted(f"{p}/{m}" for (p, m), mod in _MODEL_MODALITY.items() if mod == "image")
# The members' keys that only describe a chat body's tokens.
_CHAT_ONLY_MEMBER_KEYS = ("context_window", "anthropic_token_ratio", "calibrated_at", "calibration_ref")
_MEMBER_KEYS = frozenset((
    "priority", "kind", "lane", "model", "provider", "context_window",
    "anthropic_token_ratio", "reserve_ceiling", "calibrated_at", "calibration_ref",
    "pay", "stop_at"))
_SPEND_CREDITS_REASON = ("spend_credits is retired: `pay` lists the money pots a route may "
                         "draw (run migrate_pay.py to translate it)")


# A route's pay may name every account of a provider at once (C13):
# "<provider>/*" is each of the tenant's accounts of that provider by its
# money pot, so a document grants pots by provider, and a starter copied into
# another tenant (_starter) pays from that tenant's own accounts. _routes
# expands it; a route's pay is exact ids.
PAY_ALL = "*"


def _route_settings_error(s, where):
    """Shape check of one route's owner settings (§1.9), shared by class members
    and the `lanes` map: `pay` is a list of distinct pot-id strings or
    "<provider>/*" (never "*" for an account), `stop_at` is
    {window, level}. Whether the pots and the window exist depends on the
    accounts, so _settings_problems checks that at PUT, never at load."""
    pay = s.get("pay")
    if pay is not None:
        if (not isinstance(pay, list)
                or any(not isinstance(p, str) or "/" not in p for p in pay)):
            return f"{where}: pay must be an array of pot ids (<account>/<pot> or <provider>/*)"
        for p in pay:
            acct, pot = _split_pot(p)
            if PAY_ALL in acct:
                return f"{where}: pay {p!r}: '*' stands for a provider's accounts (<provider>/*), never for an account"
            if pot == PAY_ALL and (not acct or "/" in acct):
                return f"{where}: pay {p!r}: <provider>/* names one provider by its id"
        if len(set(pay)) != len(pay):
            return f"{where}: pay lists a pot twice"
    st = s.get("stop_at")
    if st is not None:
        if not isinstance(st, dict) or set(st) != {"window", "level"}:
            return f"{where}: stop_at must be {{window, level}}"
        if not isinstance(st["window"], str) or not st["window"].strip():
            return f"{where}: stop_at.window must be a period (5h, 7d), a scope or 'effective'"
        lv = st["level"]
        # The chained comparison also refuses NaN and ±inf (1e999 parses as
        # inf): every proxy decodes this number from /preferred.
        if isinstance(lv, bool) or not isinstance(lv, (int, float)) or not 0 <= lv <= 100:
            return f"{where}: stop_at.level must be a finite number in [0, 100]"
    return None


def _class_error(c, seen, *, lan, check_guidance=True, strict=False):
    """Why this class config entry is invalid (a reason string), or None if it's
    fine. Validation is per-class so one bad entry never takes out the rest.
    strict (PUT/validate) also refuses keys whose meaning is retired or not yet
    built; the tolerant load warns about those in _normalize_classes instead,
    so a document written for the previous brain still loads its classes.
    lan: the document is the LAN tenant's, the only one that may carry
    onboarding."""
    if not isinstance(c, dict):
        return "not an object"
    name = str(c.get("class") or "")
    if not _CLASS_ID_RE.match(name):
        return _CLASS_ID_RULE
    if name in seen:
        return "duplicate class name"
    if name in _lane_names:
        return f"name collides with lane {name!r}"
    if strict:
        if "lan_only" in c:
            return _LAN_ONLY_REASON
        if "onboarding" in c and not lan:
            return _ONBOARDING_MEMBER_REASON
        for k in c:
            if k in _BUDGET_KEYS:
                return f"{k} — {_BUDGET_REASON}"
            if k not in _CLASS_KEYS:
                return (f"unknown key {k!r} (a class carries {', '.join(sorted(_CLASS_KEYS))}; "
                        "pay and stop_at belong on a member)")
    if check_guidance:
        reason = _guidance_error(c.get("guidance"))
        if reason:
            return reason
    # onboarding: the LAN tenant offers the class to new members (_starter).
    # Optional; absent = not offered.
    if "onboarding" in c and not isinstance(c["onboarding"], bool):
        return "onboarding must be a boolean"
    rk = c.get("request_kind", "chat")
    if rk not in _REQUEST_KINDS:
        return f"request_kind must be one of {', '.join(_REQUEST_KINDS)}"
    image = rk == "image"
    if name == IMAGE_CLASS and not image:
        return f"class {IMAGE_CLASS!r} is the image door's default: its request_kind must be image"
    members = c.get("members")
    if not isinstance(members, list) or not members:
        return "members must be a non-empty array"
    prios = set()
    for i, m in enumerate(members):
        where = f"members[{i}]"
        if not isinstance(m, dict):
            return f"{where}: not an object"
        p = m.get("priority")
        if isinstance(p, bool) or not isinstance(p, int) or p < 1:
            return f"{where}: priority must be a positive int"
        if p in prios:
            return f"{where}: duplicate priority {p}"
        prios.add(p)
        if image:
            # Waived, and refused when written (§1.9: nothing is ignored): an
            # image has no token window to declare and no tokenizer to calibrate.
            if m.get("kind") != "provider":
                return f"{where}: an image class has provider members only (an anthropic lane makes no images)"
            for k in _CHAT_ONLY_MEMBER_KEYS:
                if k in m:
                    return f"{where}: {k} measures a chat body's tokens — an image member has none"
        cw = m.get("context_window")
        if not image and (isinstance(cw, bool) or not isinstance(cw, int) or cw <= 0):
            return f"{where}: context_window must be a declared positive int (never inferred)"
        # Optional reserve ceiling (#33): the utilization % past which this member
        # stops being a PREFERENCE, holding the rest of its window back. Absent or
        # null = no ceiling. Validated here rather than at use because the policy
        # compares it against a float inside the poller thread — a string ceiling
        # would raise there and kill polling, far from the config that caused it.
        rc = m.get("reserve_ceiling")
        if rc is not None:
            if isinstance(rc, bool) or not isinstance(rc, (int, float)):
                return f"{where}: reserve_ceiling must be a number or null"
            if not 0 <= rc <= 100:
                return f"{where}: reserve_ceiling must be a percent in [0, 100], got {rc}"
        reason = _route_settings_error(m, where)
        if reason:
            return reason
        if strict:
            if "spend_credits" in m:
                return f"{where}: {_SPEND_CREDITS_REASON}"
            for k in _BUDGET_KEYS:
                if k in m:
                    return f"{where}: {k} — {_BUDGET_REASON}"
            for k in m:
                if k not in _MEMBER_KEYS:
                    return f"{where}: unknown key {k!r}"
        kind = m.get("kind")
        if kind == "anthropic":
            if m.get("lane") not in _lane_names:
                return f"{where}: unknown lane {m.get('lane')!r} (have {_lane_names})"
            # The proxy must rewrite the class picker id (claude-<class>) to a REAL
            # Anthropic model id before forwarding — lanes only carry match substrings,
            # so the concrete id has to be declared here (#27).
            mv = m.get("model")
            if not isinstance(mv, str) or not mv.strip():
                return f"{where}: model must be a non-empty string (the Anthropic model id the proxy rewrites to)"
            if "anthropic_token_ratio" in m:
                return (f"{where}: anthropic_token_ratio is 1.0 by definition on an "
                        "anthropic member — do not declare it")
            for k in ("calibrated_at", "calibration_ref"):
                if k in m:
                    return f"{where}: {k} records an r_p calibration — provider members only"
        elif kind == "provider":
            for field in ("provider", "model"):
                v = m.get(field)
                if not isinstance(v, str) or not v.strip():
                    return f"{where}: {field} must be a non-empty string"
            mod = _model_modality(m["provider"], m["model"])
            if mod != rk:
                takes = f" ({', '.join(_image_models())})" if image else ""
                return (f"{where}: {m['provider']}/{m['model']} serves {mod} requests; "
                        f"a class of request_kind {rk} takes {rk} models only{takes}")
            # r_p (#44): required on every chat provider member — declared,
            # never inferred, C1-calibrated; the proxy scales a hop's usage by
            # it. Missing drops the class rather than silently minning across
            # tokenizers with different units.
            r = m.get("anthropic_token_ratio")
            if not image and (isinstance(r, bool) or not isinstance(r, (int, float)) or not 0 < r <= 4):
                return (f"{where}: anthropic_token_ratio must be a number in (0, 4] "
                        "(r_p = provider-reported tokens per Anthropic token; run C1)")
            # Where r_p came from, carried here since the named models.json rows
            # that held it are gone (C2). Optional; display only.
            for k in ("calibrated_at", "calibration_ref"):
                v = m.get(k)
                if v is not None and (not isinstance(v, str) or not v.strip()):
                    return f"{where}: {k} must be a non-empty string when present"
        else:
            return f"{where}: kind must be 'anthropic' or 'provider'"
    return None


def _norm_route_settings(s):
    """(pay, stop_at) of a shape-checked member or lane entry: pay a list
    (default [], never spends), stop_at {window, level: float} or None."""
    st = s.get("stop_at")
    stop = None if st is None else {"window": st["window"].strip(), "level": float(st["level"])}
    return list(s.get("pay") or []), stop


def _normalize_classes(raw, warnings=None, *, lan):
    """Parsed classes JSON (a list) -> a tenant's normalized classes. Tolerant
    by contract: an invalid CLASS is dropped with a logged reason while the
    rest load — config errors must never take the brain down. Each message is
    also appended to `warnings` so GET /classes can surface it (#92). Members
    are normalized (only schema keys survive) and sorted by priority; each
    class gains the DERIVED context_window = min(member effective windows). A
    provider member with no live poller is NOT a load error — it's a runtime
    degradation the /preferred classes view reports as status="no_target".
    Does NOT stamp window changes — callers stamp before installing."""
    if warnings is None:
        warnings = []

    def warn(msg):
        log(f"WARN classes: {msg}")
        warnings.append(msg)

    out, seen = [], set()
    for c in raw:
        # Routing errors still drop the class; prose errors must not.
        reason = _class_error(c, seen, lan=lan, check_guidance=False)
        if reason:
            ident = c.get("class") if isinstance(c, dict) else c
            warn(f"dropped {ident!r} — {reason}")
            continue
        # A key the class does not carry (a retired one, as the previous
        # brain's per-tenant visibility mark, or a typo) is loaded without,
        # said out loud; a strict PUT refuses it.
        for k in c:
            if k == "lan_only":
                warn(f"{c['class']}: {_LAN_ONLY_REASON} — ignored")
            elif k not in _CLASS_KEYS and k not in _BUDGET_KEYS:
                warn(f"{c['class']}: unknown key {k!r} ignored")
        if "onboarding" in c and not lan:
            warn(f"{c['class']}: {_ONBOARDING_MEMBER_REASON} — ignored")
        guidance = c.get("guidance")
        reason = _guidance_error(guidance)
        if reason:
            warn(f"{c['class']!r} — {reason}; loaded with guidance None")
            guidance = None
        elif guidance is not None:
            guidance = guidance.strip()
        rk = c.get("request_kind", "chat")
        image = rk == "image"
        members = []
        for m in sorted(c["members"], key=lambda m: m["priority"]):
            keep = (("priority", "kind", "context_window", "lane", "model") if m["kind"] == "anthropic"
                    else ("priority", "kind", "provider", "model") if image
                    else ("priority", "kind", "context_window", "provider", "model"))
            norm = {k: m[k] for k in keep}
            # reserve_ceiling is OPTIONAL, so it must NOT join `keep` — every key
            # there is guaranteed by _class_error, and this comprehension would
            # KeyError on any member that omits an optional one. That raise is
            # outside the try above (which only wraps the file read), at import
            # time: the brain would not start. Set it explicitly instead, and
            # always emit the key so the member shape downstream is uniform.
            rc = m.get("reserve_ceiling")
            norm["reserve_ceiling"] = None if rc is None else float(rc)
            norm["pay"], norm["stop_at"] = _norm_route_settings(m)
            # The previous brain's money switch. Its value is never read: until
            # the document is migrated the member pays nothing, said out loud.
            if "spend_credits" in m:
                warn(f"{c['class']}/{_member_id(m)}: {_SPEND_CREDITS_REASON} — "
                     "ignored, this member pays nothing")
            for k in _BUDGET_KEYS:
                if k in m:
                    warn(f"{c['class']}/{_member_id(m)}: {k} ignored — {_BUDGET_REASON}")
            if m["kind"] == "provider":
                # Present only with a value: a reader's schema takes an
                # absent provenance as "not calibrated"; a null would be a
                # second spelling of the same fact.
                for k in ("calibrated_at", "calibration_ref"):
                    if isinstance(m.get(k), str):
                        norm[k] = m[k].strip()
            if image:
                # An image member has no token window (§2.9): the keys stay,
                # null, so the member shape downstream is uniform.
                norm["context_window"] = norm["anthropic_token_ratio"] = norm["effective_window"] = None
                members.append(norm)
                continue
            # r_p (#44): guaranteed by _class_error on provider members, defined
            # as 1.0 for anthropic ones (and forbidden in the file). Every member
            # carries both keys so downstream never branches on kind for units:
            # effective_window is the member's window in ANTHROPIC units.
            norm["anthropic_token_ratio"] = (1.0 if m["kind"] == "anthropic"
                                             else float(m["anthropic_token_ratio"]))
            norm["effective_window"] = int(norm["context_window"] / norm["anthropic_token_ratio"])
            members.append(norm)
        name = c["class"]
        seen.add(name)
        if image:
            out.append({"class": name, "display": str(c.get("display") or name),
                        "guidance": guidance, "request_kind": rk, "context_window": None,
                        "floor_member": None, "units": None, "members": members})
            continue
        # The governing window is min() over ALL members' effective windows —
        # not currently-usable ones, so quota-driven rebinding never moves it
        # (a transcript sized to gpt's 666k effective cannot re-serve on opus).
        floor_m = min(members, key=lambda m: m["effective_window"])
        derived = floor_m["effective_window"]
        # Units follow the floor member (#44 §160): "floor == Anthropic
        # tokenizer" is a contingent fact of today's config, not an invariant.
        floor_id = _member_id(floor_m)
        units = "anthropic" if floor_m["kind"] == "anthropic" else floor_id
        if units != "anthropic":
            warn(f"{name} floor member {floor_id} is not anthropic — "
                 "window units follow its tokenizer")
        if derived < 200000:
            warn(f"{name} derived window {derived} is below 200k — advertised as is; "
                 "a client that assumes 200k for a claude-* id can overrun it")
        out.append({"class": name,
                    "display": str(c.get("display") or name),
                    "guidance": guidance,
                    "request_kind": rk,
                    "context_window": derived,
                    "floor_member": floor_id,
                    "units": units,
                    "members": members})
    return out


def _split_classes_doc(doc):
    """(class array, lanes map) of a classes document, or None when the shape
    is unusable. The document is {"classes": [...], "lanes": {...}} (§1.9:
    the bare lanes' route settings ride beside the classes), and a bare array
    of classes still loads, as consumers already allows."""
    if isinstance(doc, list):
        return doc, {}
    if isinstance(doc, dict) and isinstance(doc.get("classes"), list):
        return doc["classes"], doc.get("lanes")
    return None


def _lane_settings_error(lane, s, *, strict):
    """Why one `lanes` entry is invalid, or None."""
    where = f"lanes[{lane!r}]"
    if lane not in _lane_names:
        return f"{where}: unknown lane (have {_lane_names})"
    if not isinstance(s, dict):
        return f"{where}: must be an object {{pay, stop_at}}"
    for k in s:
        if k in _BUDGET_KEYS:
            if strict:
                return f"{where}: {k} — {_BUDGET_REASON}"
        elif k not in ("pay", "stop_at"):
            return f"{where}: unknown key {k!r} (a lane carries pay and stop_at)"
    return _route_settings_error(s, where)


def _normalize_lane_settings(raw, warnings):
    """The `lanes` map -> {lane: {pay, stop_at}} for the lanes it configures.
    Tolerant, like _normalize_classes: a bad entry is dropped with a warning
    and its lane keeps the defaults (pay [], no stop_at)."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        msg = "lanes must be an object {<lane>: {pay, stop_at}} — ignored"
        log(f"WARN classes: {msg}")
        warnings.append(msg)
        return {}
    out = {}
    for lane, s in raw.items():
        reason = _lane_settings_error(lane, s, strict=False)
        if reason:
            msg = f"dropped {reason}"
            log(f"WARN classes: {msg}")
            warnings.append(msg)
            continue
        for k in _BUDGET_KEYS:
            if k in s:
                msg = f"lane:{lane}: {k} ignored — {_BUDGET_REASON}"
                log(f"WARN classes: {msg}")
                warnings.append(msg)
        pay, stop = _norm_route_settings(s)
        out[lane] = {"pay": pay, "stop_at": stop}
    return out


def _normalize_classes_doc(doc, warnings, *, lan):
    """A whole classes document -> (classes, lane settings), tolerant."""
    arr, lanes = _split_classes_doc(doc)
    return _normalize_classes(arr, warnings, lan=lan), _normalize_lane_settings(lanes, warnings)


def _validate_classes_doc(t, doc):
    """STRICT whole-document validation of tenant t's classes document for
    PUT/validate (#92): every error a tolerant load would merely drop-and-warn
    on is a rejection here — an editor save must never silently lose a class.
    The owner settings are checked against t's accounts (_settings_problems),
    so a pot or a window that does not exist is refused, never stored and
    ignored. Returns a list of error strings (empty = valid)."""
    split = _split_classes_doc(doc)
    if split is None:
        return ['document must be {"classes": [...], "lanes": {...}} or a JSON array of class objects']
    arr, lanes = split
    errors, seen = [], set()
    if isinstance(doc, dict):
        for k in doc:
            if k in _BUDGET_KEYS:
                errors.append(f"{k} — {_BUDGET_REASON}")
            elif k not in ("classes", "lanes"):
                errors.append(f"unknown top-level key {k!r} (a classes document has classes and lanes)")
    for i, c in enumerate(arr):
        reason = _class_error(c, seen, lan=t.lan, strict=True)
        if reason:
            ident = c.get("class") if isinstance(c, dict) else None
            where = f"classes[{i}]" + (f" ({ident!r})" if ident else "")
            errors.append(f"{where}: {reason}")
        name = c.get("class") if isinstance(c, dict) else None
        if isinstance(name, str) and _CLASS_ID_RE.match(name):
            seen.add(name)  # later duplicates must error even if this entry did
    if lanes is not None and not isinstance(lanes, dict):
        errors.append("lanes must be an object {<lane>: {pay, stop_at}}")
    else:
        for lane, s in (lanes or {}).items():
            reason = _lane_settings_error(lane, s, strict=True)
            if reason:
                errors.append(reason)
    if errors:
        return errors
    classes, lane_settings = _normalize_classes_doc(doc, [], lan=t.lan)
    return _settings_problems(t, classes, lane_settings)[0]


def _drop_non_finite(v, warnings, where="$"):
    """A hand-edited file's document with every NaN or ±inf removed (the key,
    or the list item), each one warned: ignored at load, as spend_credits is.
    Python's JSON reader accepts them and no proxy can decode them, so none
    may reach a served payload or the document's revision (a fixing PUT needs
    that revision). A PUT carrying one is refused (_doc_check)."""
    if isinstance(v, float) and not math.isfinite(v):
        return v
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            p = f"{where}[{k!r}]"
            if isinstance(x, float) and not math.isfinite(x):
                warnings.append(f"{p}: non-finite number {x} ignored")
                continue
            out[k] = _drop_non_finite(x, warnings, p)
        return out
    if isinstance(v, list):
        out = []
        for i, x in enumerate(v):
            if isinstance(x, float) and not math.isfinite(x):
                warnings.append(f"{where}[{i}]: non-finite number {x} ignored")
                continue
            out.append(_drop_non_finite(x, warnings, f"{where}[{i}]"))
        return out
    return v


def _leftover_root_classes():
    """The warning for a classes.json left at the creds root, or None. A brain
    before per-tenant classes read it there; this one never does, so it is a
    document not moved (an upgrade that skipped the migration) and routing
    runs on the LAN tenant's own."""
    path = os.path.join(CREDS_DIR, "classes.json")
    if not os.path.exists(path):
        return None
    return (f"{path} is the previous brain's one classes document, and this brain does not read it: "
            "stop the brain and run `python3 migrate_classes.py migrate --creds-dir <the creds dir>` "
            "to move it into the LAN tenant's dir (it refuses while that dir holds a classes.json too: "
            "keep the one that is live, remove the other)")


def _load_classes(t):
    """Load tenant t's classes document from its dir (its creation, at boot or
    on PUT /tenants). A missing file means no classes; an unreadable one logs
    and loads none. Sets t.classes, t.classes_doc (the raw document: what the
    revision hashes and GET /classes serves, so a hand-edit changes the
    revision by construction), t.classes_warnings and t.lane_settings, and
    stamps window changes. Runs once the drivers have registered: a class is
    checked against their model modalities."""
    warnings = []
    raw = None
    path = t.classes_file
    try:
        raw = _doc_read(t, "classes")
    except Exception as e:  # noqa: BLE001
        log(f"WARN {t.id}: bad classes file {path}: {e} — no classes loaded")
        warnings.append(f"bad classes file: {e} — no classes loaded")
        raw = None
    if raw is not None:
        raw = _drop_non_finite(raw, warnings)
        for w in warnings:
            log(f"WARN {t.id}: classes file {path}: {w}")
    if raw is not None and _split_classes_doc(raw) is None:
        msg = 'top level must be {"classes": [...], "lanes": {...}} or an array — no classes loaded'
        log(f"WARN {t.id}: classes file {path}: {msg}")
        warnings.append(msg)
        raw = None
    if t.lan:
        leftover = _leftover_root_classes()
        if leftover:
            log(f"WARN {t.id}: {leftover}")
            warnings.append(leftover)
    if raw is None:
        t.classes, t.classes_doc, t.classes_warnings, t.lane_settings = [], [], warnings, {}
        return
    out, lane_settings = _normalize_classes_doc(raw, warnings, lan=t.lan)
    _stamp_class_window_changes(t, out)
    t.classes, t.classes_doc, t.classes_warnings, t.lane_settings = out, raw, warnings, lane_settings


def _stamp_class_window_changes(t, classes):
    """#44 §10: make "when did this class's window last change" answerable.
    Persists {class: {window, previous, changed_at}} in tenant t's
    classes.windows.json and stamps previous_context_window / window_changed_at
    onto each class. Runs at load and at every install of a document;
    best-effort on a read-only or missing state dir, never fatal."""
    state_path = t.windows_file
    prev = {}
    try:
        with open(state_path) as f:
            prev = json.load(f)
        if not isinstance(prev, dict):
            prev = {}
    except Exception:  # noqa: BLE001 — absent/corrupt state = no history
        prev = {}
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    state = {}
    for c in classes:
        old = prev.get(c["class"]) or {}
        if old.get("window") == c["context_window"]:
            c["previous_context_window"] = old.get("previous")
            c["window_changed_at"] = old.get("changed_at")
        else:
            c["previous_context_window"] = old.get("window")
            c["window_changed_at"] = now_iso if old.get("window") is not None else None
        state[c["class"]] = {"window": c["context_window"],
                             "previous": c["previous_context_window"],
                             "changed_at": c["window_changed_at"]}
    try:
        tmp = state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, state_path)
    except Exception as e:  # noqa: BLE001
        log(f"WARN classes: cannot persist window history {state_path}: {e}")


# ---- the starter and the catalogue (CLASSES-PLAN D1, D4) ------------------------
# Both derive from the LAN tenant's live document: the operator routes what
# members are offered. A provider is one a member can connect when Sources
# offers a flow for it, which is exactly a provider with an `auth` record
# (_auth_providers_payload lists those); a lane rides Claude accounts, so the
# anthropic provider's record decides it.

def _lan_tenant():
    """The LAN tenant (the registry holds exactly one)."""
    return _registry.lan()[0]


def _connectable(provider):
    """Can a member connect an account of this provider on Sources?"""
    return bool((PROVIDERS.get(provider) or {}).get("auth"))


def _starter_pay(pay, provider):
    """(pay as a member's document carries it, [dropped exact pot ids]): a
    "<provider>/*" kept; an exact pot that is the provider's money pot becomes
    "<provider>/*", so the member pays from their own accounts of it; any
    other exact pot (an allowance, another provider's) is dropped."""
    out, dropped = [], []
    for p in pay:
        _, pot = _split_pot(p)
        if pot == PAY_ALL:
            each = p
        elif pot == _money_pot(provider):
            each = f"{provider}/{PAY_ALL}"
        else:
            dropped.append(p)
            continue
        if each not in out:
            out.append(each)
    return out, dropped


# What a starter copies, and nothing else: a key the strict PUT would refuse
# (one a load only warned of) never reaches a member's document. onboarding is
# the LAN tenant's mark and calibration_ref the operator's note.
_STARTER_CLASS_KEYS = _CLASS_KEYS - {"onboarding", "members"}
_STARTER_MEMBER_KEYS = _MEMBER_KEYS - {"calibration_ref"}
_STARTER_LANE_KEYS = frozenset(("stop_at",))  # pay is translated apart


def _starter(lan_doc, live):
    """(starter document, dropped) a new member starts with: a copy of each
    class of the LAN tenant's document `lan_doc` marked onboarding: true and
    live there (its name in `live`: a class the LAN tenant's own load dropped
    is not the operator's to offer). Each copy keeps display, guidance,
    request_kind and its rungs' windows, ratios, calibrated_at,
    reserve_ceiling and stop_at; calibration_ref (the operator's note),
    onboarding and any key a class or rung does not carry are not copied, so
    every starter passes the strict PUT. Pays translate (_starter_pay); a rung whose
    provider a member cannot connect is removed; a class left with no rung is
    not copied. The `lanes` map is copied with the same pay translation.
    `dropped` names each loss, [{class, member?, pay?, why}], why one of
    exact_pay, not_connectable, empty; a bare lane's pay names class null and
    member lane:<lane>. The join, GET /classes/starter and migrate_classes.py
    all derive it here."""
    arr, lanes = _split_classes_doc(lan_doc) or ([], {})
    out, dropped = [], []
    for c in arr:
        if not isinstance(c, dict) or c.get("onboarding") is not True or c.get("class") not in live:
            continue
        name = c["class"]
        members = []
        for m in c.get("members") or []:
            mid = _member_id(m)
            provider = "anthropic" if m.get("kind") == "anthropic" else m.get("provider")
            if not _connectable(provider):
                dropped.append({"class": name, "member": mid, "why": "not_connectable"})
                continue
            rung = {k: copy.deepcopy(v) for k, v in m.items() if k in _STARTER_MEMBER_KEYS}
            if "pay" in m:
                rung["pay"], lost = _starter_pay(m["pay"], provider)
                dropped += [{"class": name, "member": mid, "pay": p, "why": "exact_pay"} for p in lost]
                if not rung["pay"]:
                    del rung["pay"]
            members.append(rung)
        if not members:
            dropped.append({"class": name, "why": "empty"})
            continue
        out.append(dict({k: copy.deepcopy(v) for k, v in c.items()
                         if k in _STARTER_CLASS_KEYS}, members=members))
    lanes_out = {}
    for lane, st in (lanes if isinstance(lanes, dict) else {}).items():
        if _lane_settings_error(lane, st, strict=False):
            continue  # the LAN tenant's load dropped it, with a warning
        entry = {k: copy.deepcopy(v) for k, v in st.items() if k in _STARTER_LANE_KEYS}
        if "pay" in st:
            pay, lost = _starter_pay(st["pay"], "anthropic")
            dropped += [{"class": None, "member": f"lane:{lane}", "pay": p, "why": "exact_pay"}
                        for p in lost]
            if pay:
                entry["pay"] = pay
        if entry:
            lanes_out[lane] = entry
    return {"classes": out, "lanes": lanes_out}, dropped


def _tenant_starter():
    """(starter document, dropped) from the LAN tenant's live document now."""
    lan = _lan_tenant()
    with lan.plock:
        doc, live = lan.classes_doc, {c["class"] for c in lan.classes}
    return _starter(doc, live)


def _catalog():
    """GET /classes/catalog: what a member may add to a ladder, derived from
    the LAN tenant's live classes (CLASSES-PLAN D4). lanes: each lane a rung
    of those classes rides, with the first such rung's model and
    context_window in document order (a lane none of them uses is offered to
    nobody); models: every provider member of them, the first declaration of
    each in document order, with its context_window, anthropic_token_ratio,
    calibrated_at and modality (calibration_ref is the operator's note). Only
    what a member can connect: a lane when Claude accounts can be, a model
    when its provider can be."""
    lan = _lan_tenant()
    with lan.plock:
        doc, classes = lan.classes_doc, lan.classes
    live = {c["class"]: {_member_id(m): m for m in c["members"]} for c in classes}
    arr, _ = _split_classes_doc(doc) or ([], {})
    lanes, models = {}, {}
    for c in arr:  # the document's order, not the loader's priority sort
        rungs = live.get(c.get("class")) if isinstance(c, dict) else None
        for raw in (c.get("members") or []) if rungs else []:
            m = rungs.get(_member_id(raw)) if isinstance(raw, dict) else None
            if m is None:  # a rung the load dropped
                continue
            if m["kind"] == "anthropic":
                if m["lane"] not in lanes and _connectable("anthropic"):
                    lanes[m["lane"]] = {"model": m["model"], "context_window": m["context_window"]}
                continue
            mid = _member_id(m)
            if mid in models or not _connectable(m["provider"]):
                continue
            models[mid] = {"context_window": m["context_window"],
                           "anthropic_token_ratio": m["anthropic_token_ratio"],
                           "calibrated_at": m.get("calibrated_at"),
                           "modality": _model_modality(m["provider"], m["model"])}
    return {"lanes": lanes, "models": models}


def _new_lane_state():
    return {"account": None, "since": 0.0, "reason": "init", "last_switch": 0.0, "pinned": None}


def _new_class_state():
    return {"pinned": None, "sessions": {}}


# ---- the consumers document (#95, ui-revamp §5) --------------------------------
# ONE id grammar everywhere: a consumer member ref is "lane:<lane>" |
# "class:<id>". The third form, "<provider>/<model>" (a named picker row that
# dialled a provider with no policy), went with those rows in C2 (quota-model 50
# §2.2 row 10): every dial now passes the dial check, so a provider model is
# reached through a class.

_NAMED_REF_REASON = ("direct <provider>/<model> refs are retired (C2): reach a provider "
                     "model through a class")


def _parse_ref(ref):
    """Parse one consumer member ref -> ("lane"|"class", name), or None when
    the string fits no form."""
    if not isinstance(ref, str) or not ref:
        return None
    if ref.startswith("lane:"):
        rest = ref[len("lane:"):]
        return ("lane", rest) if rest else None
    if ref.startswith("class:"):
        rest = ref[len("class:"):]
        return ("class", rest) if rest else None
    return None


def _is_named_ref(ref):
    """The retired "<provider>/<model>" form, told apart so its refusal names
    the reason rather than calling it malformed."""
    if not isinstance(ref, str) or "/" not in ref:
        return False
    prov, model = ref.split("/", 1)
    return bool(prov and model and ":" not in prov)


def _consumer_error(c, seen_ids):
    """Why this consumer entry is invalid (a reason string), or None. Mirrors
    _class_error: per-entry so one bad consumer never takes out the rest."""
    if not isinstance(c, dict):
        return "not an object"
    cid = str(c.get("id") or "")
    if not _CLASS_NAME_RE.match(cid):
        return "id must match ^[a-z][a-z0-9-]*$"
    if cid in seen_ids:
        return "duplicate consumer id"
    rs = c.get("request_shape")
    if rs not in ("claude-code", "other", "image"):
        return "request_shape must be 'claude-code', 'other' or 'image'"
    members = c.get("members")
    if not isinstance(members, list):
        return "members must be an array of refs"
    display = c.get("display")
    if display is not None and not isinstance(display, str):
        return "display must be a string"
    return None


def _normalize_consumers(doc, strict):
    """Consumers document -> (errors, warnings, normalized list).

    strict=True (PUT/validate): every structural problem — bad entry, invalid
    ref grammar, a top-level ref claimed by two consumers — is an ERROR; the
    save is rejected. strict=False (file load): the same problems WARN and
    drop-don't-crash (duplicate refs resolve first-wins), so a bad hand-edit
    can never take the exporter down. Dangling refs are NEVER an error in
    either mode — cross-doc create/delete ordering must not deadlock (§5);
    they surface as status "dangling" in the runtime view.

    Accepts {"consumers": [...]} or a bare array (the §5 example's shape)."""
    if isinstance(doc, list):
        doc = {"consumers": doc}
    if not isinstance(doc, dict) or not isinstance(doc.get("consumers"), list):
        msg = 'document must be {"consumers": [...]} (or a bare array of consumers)'
        return ([msg], [], []) if strict else ([], [msg], [])
    errors, warnings, out = [], [], []
    seen_ids, claimed = set(), {}

    def problem(msg):
        (errors if strict else warnings).append(msg)

    for i, c in enumerate(doc["consumers"]):
        reason = _consumer_error(c, seen_ids)
        if reason:
            ident = c.get("id") if isinstance(c, dict) else None
            where = f"consumers[{i}]" + (f" ({ident!r})" if ident else "")
            problem(f"{where}: {reason} — dropped" if not strict else f"{where}: {reason}")
            if strict and isinstance(ident, str) and _CLASS_NAME_RE.match(ident):
                # Mirrors _validate_classes_doc: a later duplicate of this id
                # must ALSO error even though this entry did, so one editor
                # round-trip reports both problems. Strict only — the tolerant
                # load keeps a later valid entry when the first was dropped.
                seen_ids.add(ident)
            continue
        seen_ids.add(c["id"])
        kept, cwarn = [], []
        for ref in c["members"]:
            if _parse_ref(ref) is None:
                msg = (f"consumer {c['id']!r}: ref {ref!r}: {_NAMED_REF_REASON}"
                       if _is_named_ref(ref) else
                       f"consumer {c['id']!r}: invalid ref {ref!r} (lane:<lane> | class:<id>)")
                problem(msg if strict else msg + " — dropped")
                continue
            if ref in claimed:
                # At-most-one consumer per TOP-LEVEL ref (§5). Strict = error;
                # load = first-wins so a hand-edit can never crash the brain.
                msg = (f"consumer {c['id']!r}: ref {ref!r} already claimed by "
                       f"{claimed[ref]!r} (at most one consumer per ref)")
                if strict:
                    errors.append(msg)
                else:
                    m = msg + " — first-wins, dropped here"
                    warnings.append(m)
                    cwarn.append(m)
                continue
            claimed[ref] = c["id"]
            kept.append(ref)
        out.append({"id": c["id"],
                    "display": str(c.get("display") or c["id"]),
                    "request_shape": c["request_shape"],
                    "members": kept,
                    "warnings": cwarn})
    return errors, warnings, out


def _load_consumers(t):
    """Load the tenant's consumers document (its creation + the reload path).
    Absent file = no consumers, ZERO warnings — the feature is opt-in.
    Unreadable file / bad shape = WARN and no consumers, never a crash."""
    warnings = []
    raw = None
    try:
        raw = _store.read_json(t.consumers_file)
    except Exception as e:  # noqa: BLE001
        log(f"WARN bad consumers file {t.consumers_file}: {e} — no consumers loaded")
        t.consumers_doc = {"consumers": []}
        t.consumers_warnings = [f"bad consumers file: {e} — no consumers loaded"]
        return []
    if raw is None:
        t.consumers_doc, t.consumers_warnings = {"consumers": []}, []
        return []
    dropped = []
    raw = _drop_non_finite(raw, dropped)
    _, warnings, out = _normalize_consumers(raw, strict=False)
    warnings = dropped + warnings
    for w in warnings:
        log(f"WARN consumers: {w}")
    t.consumers_doc = raw if isinstance(raw, dict) else {"consumers": raw}
    t.consumers_warnings = warnings
    return out


def _consumers_view(consumers, classes):
    """The runtime consumers view: per-member status computed against the LIVE
    classes/lanes (dangling-ness is time-varying — a class hot-swap can orphan
    a ref). Statuses per §5: "ok" | "dangling" (unknown lane/class). Also
    raises the request-shape WARN: an anthropic-kind member inside a class
    whose consumer isn't claude-code-shaped is a dead hop (§5 enforcement
    semantics)."""
    class_names = {c["class"] for c in classes}
    anthropic_classes = {c["class"] for c in classes
                         if any(m.get("kind") == "anthropic" for m in c["members"])}
    image_classes = {c["class"] for c in classes if c["request_kind"] == "image"}
    out = []
    for c in consumers:
        warnings = list(c.get("warnings") or [])
        members = []
        for ref in c["members"]:
            kind, key = _parse_ref(ref)
            if kind == "lane":
                status = "ok" if key in _lane_names else "dangling"
                if status == "dangling":
                    warnings.append(f"dangling ref {ref!r}: unknown lane {key!r}")
                elif c["request_shape"] == "image":
                    warnings.append(f"lane {key!r} makes no images — a dead ref for this image consumer")
            else:
                status = "ok" if key in class_names else "dangling"
                if status == "dangling":
                    warnings.append(f"dangling ref {ref!r}: no such class (create it, or edit this consumer)")
                elif (c["request_shape"] == "image") != (key in image_classes):
                    # §2.9: the image door serves only image classes, and the
                    # chat doors never serve one.
                    warnings.append(
                        f"class {key!r} serves {'image' if key in image_classes else 'chat'} requests "
                        f"but request_shape is {c['request_shape']!r} — a dead ref for this consumer")
                elif c["request_shape"] != "claude-code" and key in anthropic_classes:
                    warnings.append(
                        f"class {key!r} has anthropic members but request_shape is "
                        f"{c['request_shape']!r} — anthropic rungs are a dead hop for this consumer")
            members.append({"ref": ref, "status": status})
        out.append({"id": c["id"], "display": c["display"],
                    "request_shape": c["request_shape"],
                    "members": members, "warnings": warnings})
    return out


def _consumer_claims(consumers):
    """{("lane", name) | ("class", name): consumer id} — which consumer claims
    each top-level ref (first-wins is already enforced at normalize time).
    Feeds the derived `consumer` field on /preferred class + lane entries."""
    out = {}
    for c in consumers:
        for ref in c["members"]:
            parsed = _parse_ref(ref)
            if parsed and parsed not in out:
                out[parsed] = c["id"]
    return out


# ---- the money document (quota-model 50 §1.9) ---------------------------------
# {"pay_order": [pot ids], "pots": {"<pot id>": {"topup": "off"|"on",
#  "budget": n, "route_budgets": {"<route id>": n}}}}. A pot id
# is "<account>/<pot>"; which pots an account has follows from its kind (§1.2),
# never from this document: every subscription account has an allowance
# and a credits pot. pay_order ranks the paid tier and never admits a route
# (only a route's `pay` does); topup is the owner's declaration of whether the
# provider refills an empty pot by itself, and only moves a refusal's `until`.
# budget (C6) caps this month's ledger spend on the pot, for every route that
# draws it; route_budgets caps one route's. Both apply; unset is no budget,
# 0 closes the pot. Only a pot whose charges the ledger counts may carry one
# (_COUNTABLE_UNITS).

_TOPUP_VALUES = ("off", "on")
_POT_KEYS = ("topup", "budget", "route_budgets")


def _split_pot(pid):
    """(account, pot) of a pot id. Account names never carry a slash, so the
    last one splits."""
    acct, _, pot = pid.rpartition("/")
    return acct, pot


def _normalize_money(doc, strict):
    """Money document -> (errors, warnings, normalized). The shape rules only:
    whether each pot exists depends on the accounts (_money_problems). strict
    (PUT/validate) errors on every problem; the load drops the bad part with a
    warning, like _normalize_consumers. Absent (None) = the defaults."""
    errors, warnings = [], []
    out = {"pay_order": [], "pots": {}}

    def problem(msg, kept=None):
        if strict:
            errors.append(msg)
        else:
            warnings.append(f"{msg} — {kept or 'dropped'}")

    if doc is None and not strict:
        return errors, warnings, out
    if not isinstance(doc, dict):
        problem('document must be {"pay_order": [...], "pots": {...}}')
        return errors, warnings, out
    for k in doc:
        if k in _BUDGET_KEYS:
            problem(f"{k} — a budget is per pot: pots[<pot id>].{k}")
        elif k not in ("pay_order", "pots"):
            problem(f"unknown top-level key {k!r} (a money document has pay_order and pots)")
    order = doc.get("pay_order")
    if order is not None:
        if not isinstance(order, list) or any(not isinstance(p, str) or "/" not in p for p in order):
            problem("pay_order must be an array of pot ids (<account>/<pot>)")
        else:
            for p in order:
                if p in out["pay_order"]:
                    problem(f"pay_order lists {p!r} twice", kept="the first kept")
                else:
                    out["pay_order"].append(p)
    pots = doc.get("pots")
    if pots is not None and not isinstance(pots, dict):
        problem("pots must be an object {<pot id>: {topup, budget, route_budgets}}")
        pots = None
    for pid, e in (pots or {}).items():
        where = f"pots[{pid!r}]"
        if "/" not in pid:
            problem(f"{where}: a pot id is <account>/<pot>")
            continue
        if not isinstance(e, dict):
            problem(f"{where}: must be an object {{topup, budget, route_budgets}}")
            continue
        bad = next((f"{where}: unknown key {k!r} (a pot carries topup, budget and route_budgets)"
                    for k in e if k not in _POT_KEYS), None)
        if bad:
            problem(bad)
            continue
        if _split_pot(pid)[1] == ALLOWANCE_POT and any(k in e for k in _POT_KEYS):
            # §1.9: refused outright, "off" too; an allowance has no top-up to
            # set, and its budget is its window.
            problem(f"{where}: {', '.join(k for k in _POT_KEYS if k in e)} applies to money "
                    "pots; an allowance refills at its reset")
            continue
        topup = e.get("topup", "off")
        if topup not in _TOPUP_VALUES:
            problem(f"{where}: topup must be 'off' or 'on'")
            continue
        budget = e.get("budget")
        if budget is not None and not _is_amount(budget):
            problem(f"{where}: budget must be a number >= 0 in the pot's own unit, per month (UTC)")
            continue
        rb = e.get("route_budgets")
        if rb is not None and not (isinstance(rb, dict) and all(
                isinstance(r, str) and r and _is_amount(v) for r, v in rb.items())):
            problem(f"{where}: route_budgets must be an object {{<route id>: number >= 0}}")
            continue
        norm = {"topup": topup}
        if budget is not None:
            norm["budget"] = float(budget)
        if rb:
            norm["route_budgets"] = {r: float(v) for r, v in rb.items()}
        out["pots"][pid] = norm
    return errors, warnings, out


def _is_amount(v):
    """A budget amount: a finite number >= 0, never a bool."""
    return (not isinstance(v, bool) and isinstance(v, (int, float))
            and math.isfinite(v) and v >= 0)


def _load_money(t):
    """Load the tenant's money document (its creation + the reload path).
    Absent file = the defaults with no warnings; unreadable = warn and
    defaults. Pots are not checked against accounts here: nothing is polled
    yet at creation, and a pot that names no account binds nothing (dangling)."""
    try:
        raw = _store.read_json(t.money_file)
    except Exception as e:  # noqa: BLE001
        log(f"WARN bad money file {t.money_file}: {e} — defaults (no pay_order, topup off)")
        t.money_doc = {}
        t.money_warnings = [f"bad money file: {e} — defaults (no pay_order, topup off)"]
        return {"pay_order": [], "pots": {}}
    if raw is None:
        t.money_doc, t.money_warnings = {}, []
        return {"pay_order": [], "pots": {}}
    dropped = []
    raw = _drop_non_finite(raw, dropped)
    _, warnings, out = _normalize_money(raw, strict=False)
    warnings = dropped + warnings
    for w in warnings:
        log(f"WARN money: {w}")
    t.money_doc = raw if isinstance(raw, dict) else {}
    t.money_warnings = warnings
    return out


# ---- the ledger (quota-model 50 §1.7, C6) --------------------------------------
#
# Our own charges on money pots, as the proxy counted them: one entry per
# request served on a countable money pot, and one per hop abandoned on one
# with no usage returned (charge null: "uncounted", shown in reconciliation,
# never in a budget). The proxy computes each charge from the rates published
# here (ledger.rates on /preferred) and sends it as a `ledger` event in
# POST /reports; entries are deduplicated by id, so a resend after a lost
# reply counts once. The file is append-only JSONL beside the money document;
# a restart reads it back.
#
#   entry {id, at, account, pot, route, model, charge (number | null), unit,
#          basis (exact | derived | upper_bound | lower_bound),
#          reason (uncounted only), usage {...} | null, speed (str | null)}
#
# lower_bound is a charge for what the response reported when part of its cost
# has no published price to bound it by (an image with no image usage, 11 §3):
# it counts, and a budget the entry reaches reads no (unknown) for the rest of
# the period ("unpriced"), since its true spend may be higher.
#
# It feeds the budget gate (dial_check): this month's spend per pot and per
# route. It never infers a charge from a balance change; the fall in balance
# between readings, less our ledger, is shown as "spent elsewhere or
# uncounted" and gates nothing (reconciliation, below).

# The file is the tenant's ledger.jsonl: an append-only log, one entry a
# line, plain whatever the store's mode (it holds charges, never a secret, and
# its recovery below is by line boundary).


# API list prices, USD per 1M tokens, by the exact model id the proxy reports
# and the speed asked for (quota-model 65 §2): the yardstick a `served` event
# is priced by (_served_price). Fetched 2026-11-03, Fable 5.2 and Mythos 5.2 on
# 2026-11-16; each price is sourced and labelled in quota-model 10 (Anthropic)
# and 11 (OpenAI), "List prices".
# A token kind a row has no price for is unpriced when its count is above 0:
# cache writes are priced by lifetime only (Anthropic 5-minute/1-hour, OpenAI
# 30-minute/12-hour), so a `cache_write` total is "cache write split unknown".
# Every model the provider's price page lists that its API still serves, by
# every id a client names it by (an alias and its dated id alike). A fast row
# is the model's fast input/output prices with the caching multipliers on top,
# as Anthropic documents. Not here: Kimi (its price page rendered empty, 12),
# gpt-image-2 (the event carries no image tokens) and retired models.
_ANTH_PRICES = ("input", "cache_write_5m", "cache_write_1h", "cache_read", "output")
_OPENAI_PRICES = ("input", "cache_write_30m", "cache_write_12h", "cache_read", "output")
_OPUS_45 = {"standard": dict(zip(_ANTH_PRICES, (5.0, 6.25, 10.0, 0.5, 25.0)))}
_OPUS_48 = dict(_OPUS_45, fast=dict(zip(_ANTH_PRICES, (10.0, 12.5, 20.0, 1.0, 50.0))))
_SONNET_45 = {"standard": dict(zip(_ANTH_PRICES, (3.0, 3.75, 6.0, 0.3, 15.0)))}
API_LIST_PRICES = {
    "claude-fable-5-2": {"standard": dict(zip(_ANTH_PRICES, (8.0, 10.0, 16.0, 0.2, 40.0))),
                         "fast": dict(zip(_ANTH_PRICES, (16.0, 20.0, 32.0, 0.4, 80.0)))},
    "claude-fable-5-1": {"standard": dict(zip(_ANTH_PRICES, (10.0, 12.5, 20.0, 0.25, 50.0)))},
    "claude-fable-5": {"standard": dict(zip(_ANTH_PRICES, (10.0, 12.5, 20.0, 1.0, 50.0)))},
    "claude-opus-5-5": {"standard": dict(zip(_ANTH_PRICES, (4.0, 5.0, 8.0, 0.2, 20.0))),
                        "fast": dict(zip(_ANTH_PRICES, (8.0, 10.0, 16.0, 0.4, 40.0)))},
    "claude-opus-5": _OPUS_48, "claude-opus-4-8": _OPUS_48,
    "claude-opus-4-7": _OPUS_45, "claude-opus-4-6": _OPUS_45,
    "claude-opus-4-5": _OPUS_45, "claude-opus-4-5-20251101": _OPUS_45,
    "claude-sonnet-5-5": {"standard": dict(zip(_ANTH_PRICES, (2.0, 2.5, 4.0, 0.1, 10.0)))},
    "claude-sonnet-5": {"standard": dict(zip(_ANTH_PRICES, (2.0, 2.5, 4.0, 0.2, 10.0)))},
    "claude-sonnet-4-6": _SONNET_45, "claude-sonnet-4-5": _SONNET_45,
    "claude-sonnet-4-5-20250929": _SONNET_45,
    "claude-haiku-5-5": {"standard": dict(zip(_ANTH_PRICES, (0.1, 0.125, 0.2, 0.01, 0.5)))},
    "claude-haiku-4-5": {"standard": dict(zip(_ANTH_PRICES, (1.0, 1.25, 2.0, 0.1, 5.0)))},
    "gpt-6-astra": {"standard": dict(zip(_OPENAI_PRICES, (10.0, 12.5, 25.0, 1.0, 50.0))),
                    "fast": dict(zip(_OPENAI_PRICES, (20.0, 25.0, 50.0, 2.0, 100.0))),
                    "ultrafast": dict(zip(_OPENAI_PRICES, (60.0, 75.0, 150.0, 6.0, 300.0)))},
    "gpt-6.1-sol": {"standard": dict(zip(_OPENAI_PRICES, (2.0, 2.5, 5.0, 0.1, 10.0))),
                    "fast": dict(zip(_OPENAI_PRICES, (4.0, 5.0, 10.0, 0.2, 20.0))),
                    "ultrafast": dict(zip(_OPENAI_PRICES, (12.0, 15.0, 30.0, 0.6, 60.0)))},
}
API_LIST_PRICES["claude-haiku-4-5-20251001"] = API_LIST_PRICES["claude-haiku-4-5"]
API_LIST_PRICES["claude-mythos-5-2"] = API_LIST_PRICES["claude-fable-5-2"]
API_LIST_PRICES["claude-mythos-5-1"] = API_LIST_PRICES["claude-fable-5-1"]
API_LIST_PRICES["claude-mythos-5"] = API_LIST_PRICES["claude-fable-5"]
# Prices by prompt length: a request whose prompt (all of its input: input,
# cache reads and cache writes) is over its model's threshold pays the second
# set of prices for all of its tokens, output included, each request on its
# own. OpenAI's models over 272K tokens (11); Claude Haiku 5.5 over 100K (10).
API_LONG_CONTEXT = {
    "gpt-6-astra": (272000, {
        "standard": dict(zip(_OPENAI_PRICES, (20.0, 25.0, 50.0, 2.0, 75.0))),
        "fast": dict(zip(_OPENAI_PRICES, (40.0, 50.0, 100.0, 4.0, 150.0))),
        "ultrafast": dict(zip(_OPENAI_PRICES, (120.0, 150.0, 300.0, 12.0, 450.0)))}),
    "gpt-6.1-sol": (272000, {
        "standard": dict(zip(_OPENAI_PRICES, (4.0, 5.0, 10.0, 0.2, 15.0))),
        "fast": dict(zip(_OPENAI_PRICES, (8.0, 10.0, 20.0, 0.4, 30.0))),
        "ultrafast": dict(zip(_OPENAI_PRICES, (24.0, 30.0, 60.0, 1.2, 90.0)))}),
    "claude-haiku-5-5": (100000, {
        "standard": dict(zip(_ANTH_PRICES, (0.5, 0.625, 1.0, 0.05, 2.5)))}),
}
def _prices_payload():
    """The /preferred `prices` key: the API list prices above, the yardstick a
    `served` event is priced by (_served_priced), published unchanged in
    meaning so a console prices a path at API price by the same table. models is
    API_LIST_PRICES ({model id: {speed: {token kind: USD per 1M}}}, aliases
    included); long_context is API_LONG_CONTEXT with each (threshold, rows)
    pair named: {model id: {over: prompt tokens, speeds: rows}}. Fresh
    dicts, so a caller of the document never reaches the tables."""
    return {"models": {m: {sp: dict(row) for sp, row in speeds.items()}
                       for m, speeds in API_LIST_PRICES.items()},
            "long_context": {m: {"over": over,
                                 "speeds": {sp: dict(row) for sp, row in speeds.items()}}
                             for m, (over, speeds) in API_LONG_CONTEXT.items()}}


def _rates_complete(unit, rates, tables=None):
    """True when the unit's published rates carry every table its charges
    need. tables is the published rule (a ledger's rate_tables); None reads
    this brain's own."""
    need = (_LEDGER_RATE_TABLES if tables is None else tables).get(unit)
    if need is None:
        return False
    return all(isinstance(rates, dict) and rates.get(k) for k in need)

# An entry's `at` is epoch seconds, at most this far ahead of now (one host's
# clock, the proxy beside the brain) and, when posted, no earlier than the
# start of the last budget period: dedupe keeps ids that far back
# (_ledger_prune), so an older entry's resend would be counted twice and
# money_spent_total could not be trusted. Milliseconds are refused.
LEDGER_AT_AHEAD_S = 300

_LEDGER_BASES = ("exact", "derived", "upper_bound", "lower_bound")
_LEDGER_KEYS = frozenset(("id", "at", "account", "pot", "route", "model", "charge", "unit",
                          "basis", "reason", "usage", "speed"))

# seq: entries in the file (an entry's seq is its line); ids: {id: period
# start}, kept for this period and the last, which is as far back as a resend
# reaches; spend: {(period start, pot id, route): charge sum}; uncounted: the
# same key -> count; unpriced: the same key -> count of lower_bound entries;
# total: {(pot id, route): all-time sum}, money_spent_total; recon: {pot id:
# {since, at, last, falls, ledger}} (reconciliation); counted_at: {pot id: the
# newest counted entry's at}: the pot served a paid request then, which ends a
# prepaid account's dry (notices.py) and a shut report on a pot nothing reads
# (_report_money_floor).
def _ledger_fresh():
    return {"seq": 0, "ids": {}, "spend": {}, "uncounted": {}, "unpriced": {}, "total": {},
            "recon": {}, "unreadable": 0, "counted_at": {}}


def _ledger_entry_problem(e, roster, now):
    """Why one posted entry is refused, or None. Nothing is dropped or
    ignored: an unknown key, an `at` that is not epoch seconds near now, a
    pot whose charges are not countable, or a charge that is not a finite
    number >= 0 (or null with a reason) refuses the whole POST."""
    why = _ledger_line_problem(e, now)
    if why:
        return why
    if e["at"] < _budget_period(_budget_period(now)[0] - 1)[0]:
        return ("at must be epoch seconds in this budget period or the last "
                "(a resend of anything older would be counted twice)")
    unit = _COUNTABLE_UNITS.get(roster.get(e["account"]))
    if unit is None or e["pot"] != _money_pot(roster.get(e["account"])):
        return f"{e['account']}/{e['pot']}: not a pot whose charges the ledger counts (§1.7)"
    if e["unit"] != unit:
        return f"unit must be {unit!r}, the pot's own"
    return None


def _ledger_line_problem(e, now):
    """Why one entry is not a ledger entry, or None: every rule of a POST but
    the ones that change with time (the roster, how far back `at` may be). A
    line of the file is held to it at load, so no line, however it got
    there, reaches the aggregates unchecked."""
    if not isinstance(e, dict):
        return "an entry is an object"
    bad = sorted(set(e) - _LEDGER_KEYS)
    if bad:
        return f"unknown key(s) {bad}"
    if not (isinstance(e.get("id"), str) and 0 < len(e["id"]) <= 200):
        return "id must be a string of 1-200 chars (the dedupe key)"
    at = _num(e.get("at"))
    if at is None or not 0 <= at <= now + LEDGER_AT_AHEAD_S:
        return (f"at must be epoch seconds, no later than {LEDGER_AT_AHEAD_S}s "
                "from now (milliseconds are not)")
    acct, pot = e.get("account"), e.get("pot")
    if not (isinstance(acct, str) and acct) or pot not in {MONEY_POT, *_API_POTS.values()}:
        return f"{acct}/{pot}: not a pot whose charges the ledger counts (§1.7)"
    if not (isinstance(e.get("route"), str) and e["route"]):
        return "route must be a route id"
    if not (isinstance(e.get("unit"), str) and e["unit"]):
        return "unit must be the pot's own"
    if e.get("charge") is None:
        if not (isinstance(e.get("reason"), str) and e["reason"]):
            return "an uncounted entry (charge null) carries its reason"
    elif not _is_amount(e["charge"]):
        return "charge must be a finite number >= 0, or null for an uncounted hop"
    elif e.get("basis") not in _LEDGER_BASES:
        return f"basis must be one of {list(_LEDGER_BASES)}"
    for k, typ in (("model", str), ("speed", str), ("usage", dict)):
        if e.get(k) is not None and not isinstance(e[k], typ):
            return f"{k} must be {typ.__name__} or null"
    return None


def _ledger_add(t, e):
    """Fold one entry into the aggregates. Caller holds _ledger_lock."""
    start = _budget_period(e["at"])[0]
    pid, route = f"{e['account']}/{e['pot']}", e["route"]
    t.ledger["seq"] += 1
    t.ledger["ids"][e["id"]] = start
    key = (start, pid, route)
    if e.get("charge") is None:
        t.ledger["uncounted"][key] = t.ledger["uncounted"].get(key, 0) + 1
        return
    t.ledger["spend"][key] = t.ledger["spend"].get(key, 0.0) + e["charge"]
    counted = t.ledger.setdefault("counted_at", {})
    if e["at"] > counted.get(pid, 0):
        counted[pid] = e["at"]
    if e.get("basis") == "lower_bound":
        t.ledger["unpriced"][key] = t.ledger["unpriced"].get(key, 0) + 1
    t.ledger["total"][(pid, route)] = t.ledger["total"].get((pid, route), 0.0) + e["charge"]
    rc = t.ledger["recon"].get(pid)
    if rc is not None and e["at"] >= rc["since"]:
        rc["ledger"] += e["charge"]


def _ledger_prune(t, now):
    """Forget dedupe ids older than last period, as far back as a POST's `at`
    may be (_ledger_entry_problem). Caller holds _ledger_lock."""
    start = _budget_period(now)[0]
    prev = _budget_period(start - 1)[0]
    t.ledger["ids"] = {i: p for i, p in t.ledger["ids"].items() if p >= prev}


def _load_ledger(t):
    """Read LEDGER_FILE back at startup. No line can stop the brain starting,
    and none is skipped quietly:
      - a final line with no newline is an append that failed (its POST got
        a 500, so the proxy resends it): dropped, and cut before the next
        append (_ledger_append);
      - a line seen before (by id) is a resend of an append that failed after
        writing it: counted once;
      - any other line that does not parse, or fails _ledger_line_problem, is
        logged with its number and counted in ledger.unreadable_lines. It may
        hide spend, so while one exists every budget reads no (unknown)."""
    try:
        f = open(t.ledger_file, "rb")
    except FileNotFoundError:
        return
    except OSError as e:
        log(f"WARN ledger: cannot read {t.ledger_file}: {e}")
        with t.ledger_lock:
            t.ledger["unreadable"] += 1
        return
    now, ids, dups = time.time(), set(), 0
    with f, t.ledger_lock:
        for n, line in enumerate(f, 1):
            if not line.endswith(b"\n"):
                log(f"WARN ledger: {t.ledger_file}:{n} has no newline (an append that failed); "
                    "dropped, and cut before the next append")
                break
            try:
                e = json.loads(line.decode("utf-8"))
                why = _ledger_line_problem(e, now)
                if why:
                    raise ValueError(why)
            except (ValueError, RecursionError) as err:
                log(f"WARN ledger: {t.ledger_file}:{n} unreadable ({err}); not counted, and "
                    "every budget reads no (unknown) while it is there")
                t.ledger["unreadable"] += 1
                t.ledger["seq"] += 1
                continue
            if e["id"] in ids:
                dups += 1
                t.ledger["seq"] += 1
                continue
            ids.add(e["id"])
            _ledger_add(t, e)
        _ledger_prune(t, now)
        log(f"ledger: {t.ledger['seq']} lines read from {t.ledger_file}"
            + (f", {dups} resent after a failed append (counted once)" if dups else ""))


def _ledger_append(t, lines):
    """Append encoded lines to LEDGER_FILE, starting on a line boundary: a
    partial line at the end (an append that failed part-way) is cut first,
    so it cannot join the next line into one unreadable line. Raises OSError.
    Caller holds _ledger_lock."""
    with open(t.ledger_file, "ab+") as f:
        end = f.seek(0, os.SEEK_END)
        cut = end
        while cut > 0:
            step = min(cut, 65536)
            f.seek(cut - step)
            chunk = f.read(step)
            if cut == end and chunk.endswith(b"\n"):
                break
            nl = chunk.rfind(b"\n")
            if nl >= 0:
                cut = cut - step + nl + 1
                break
            cut -= step
        if cut != end:
            log(f"WARN ledger: cut {end - cut} bytes of a partial line at the end of {t.ledger_file}")
            f.truncate(cut)
        f.write(b"".join(lines))
        f.flush()
        os.fsync(f.fileno())


def ledger_accept(t, entries, roster, now):
    """Each entry on its own (quota-model 64 §5, POST /reports' ledger kind):
    -> ([(result, why)] in order, seq). A bad entry is `refused`, with what
    is wrong with it; an id already held (this budget period or the last), or
    one earlier in the batch, is `ignored`; the rest go to the file in one
    append and are `applied`, or all `retry` when it fails. seq is the
    ledger's after the append."""
    results = [None] * len(entries)
    for i, e in enumerate(entries):
        why = _ledger_entry_problem(e, roster, now)
        if why:
            results[i] = ("refused", why)
    with t.ledger_lock:
        seen, new, idx = set(), [], []
        for i, e in enumerate(entries):
            if results[i] is not None:
                continue
            if e["id"] in t.ledger["ids"] or e["id"] in seen:
                results[i] = ("ignored", "a ledger id the brain already has")
                continue
            seen.add(e["id"])
            new.append({k: e.get(k) for k in sorted(_LEDGER_KEYS)})
            idx.append(i)
        if new:
            try:
                _ledger_append(t, [(json.dumps(e, sort_keys=True, allow_nan=False) + "\n").encode()
                                for e in new])
            except OSError as err:
                for i in idx:
                    results[i] = ("retry", f"cannot append {t.ledger_file}: {err}")
                return results, t.ledger["seq"]
            for e in new:
                _ledger_add(t, e)
            _ledger_prune(t, now)
            for i in idx:
                results[i] = ("applied", None)
        return results, t.ledger["seq"]


def _ledger_note_balance(t, pid, balance, at):
    """Reconciliation (§1.7): the falls in a countable pot's balance between
    good readings, since this process first read it. A rise is a refill and
    is never our spending; the falls less our ledger since then are "spent
    elsewhere or uncounted". A surface only: it gates nothing."""
    if balance is None:
        return
    with t.ledger_lock:
        rc = t.ledger["recon"].get(pid)
        if rc is None:
            t.ledger["recon"][pid] = {"since": at, "at": at, "last": balance, "falls": 0.0,
                                     "ledger": 0.0}
            return
        if at <= rc["at"]:
            return
        if balance < rc["last"]:
            rc["falls"] += rc["last"] - balance
        rc["last"], rc["at"] = balance, at


def _ledger_payload(t, now, roster, money):
    """The /preferred `ledger` block: this budget period, when it was read,
    the file's seq (the proxy drops a posted entry once seq covers it), the
    rates the proxy charges by and the tables each unit's charges need, and
    per countable pot its unit, this period's
    spend in total and per route, its uncounted hops, the owner's budgets, and
    the reconciliation."""
    start, end = _budget_period(now)
    with t.ledger_lock:
        spend = {k: v for k, v in t.ledger["spend"].items() if k[0] == start}
        unc = {k: v for k, v in t.ledger["uncounted"].items() if k[0] == start}
        unpriced = {k: v for k, v in t.ledger["unpriced"].items() if k[0] == start}
        recon = {p: dict(r) for p, r in t.ledger["recon"].items()}
        seq, unreadable = t.ledger["seq"], t.ledger["unreadable"]
    pots = {f"{a}/{_money_pot(pv)}": _COUNTABLE_UNITS[pv] for a, pv in roster.items()
            if pv in _COUNTABLE_UNITS}
    out = {}
    for pid in sorted(set(pots) | {k[1] for k in spend} | {k[1] for k in unc}):
        st = money["pots"].get(pid) or {}
        routes = {k[2]: round(v, 6) for k, v in spend.items() if k[1] == pid}
        rc = recon.get(pid)
        out[pid] = {
            "unit": pots.get(pid), "period_spend": round(sum(routes.values()), 6),
            "routes": dict(sorted(routes.items())),
            "uncounted": sum(v for k, v in unc.items() if k[1] == pid),
            "unpriced": dict(sorted((k[2], v) for k, v in unpriced.items() if k[1] == pid)),
            "budget": st.get("budget"), "route_budgets": st.get("route_budgets") or {},
            "reconciliation": None if rc is None else {
                "since": round(rc["since"]), "balance_falls": round(rc["falls"], 6),
                "ledger": round(rc["ledger"], 6),
                "elsewhere_or_uncounted": round(rc["falls"] - rc["ledger"], 6)}}
    return {"period": {"start": start, "end": end}, "as_of": round(now, 3), "seq": seq,
            "unreadable_lines": unreadable,
            "rates": _LEDGER_RATES,
            "rate_tables": {u: list(tb) for u, tb in _LEDGER_RATE_TABLES.items()},
            "pots": out}


def _ledger_totals(t):
    """{(pot id, route): all-time counted charge}, for money_spent_total."""
    with t.ledger_lock:
        return dict(t.ledger["total"])


# ---- document revisions + hot-swap (#92, ui-revamp §6) -------------------------

def _doc_revision(doc):
    """Content-hash revision: sha256 over the canonicalized document, first 16
    hex chars. Survives restarts and changes under hand-edits by construction
    — no counter to persist or desync."""
    canon = json.dumps(doc, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


# Per-document WRITE locks: the If-Match check-and-write must be serialized —
# the brain is a ThreadingHTTPServer, handlers are concurrent, and an
# unserialized check-and-write is a lost-update race (§6). Never held while
# waiting on _plock-holders that need these (nothing does). Each is the
# tenant's own (t.doc_locks).
_DOC_KINDS = ("classes", "consumers", "money")


def _doc_lock(t, kind):
    """The lock a document's PUT, validate and reload hold: the tenant's."""
    return t.doc_locks[kind]


def _doc_path(t, kind):
    """The file a document loads from and is written to: the tenant's."""
    return {"classes": t.classes_file, "consumers": t.consumers_file, "money": t.money_file}[kind]


def _doc_read(t, kind):
    """The document's file as parsed JSON, None without one. classes is a
    plain file in every store's mode (tenancy.FILES); the others go through
    the store."""
    path = _doc_path(t, kind)
    if kind == "classes":
        try:
            with open(path) as f:
                return json.load(f)
        except FileNotFoundError:
            return None
    return _store.read_json(path)


def _doc_write(t, kind, canon):
    """Write the document atomically, in the form its reader expects."""
    path = _doc_path(t, kind)
    if kind == "classes":
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(canon, f, indent=2, allow_nan=False)
            f.write("\n")
        os.replace(tmp, path)
        return
    _store.write_bytes(path, (json.dumps(canon, indent=2, allow_nan=False) + "\n").encode("utf-8"))


def _append_history(path, rev, actor, reason, doc):
    """Append one revision record to <path>.history.jsonl. Best-effort: the
    document write is the source of truth, so a history failure logs and the
    operation proceeds."""
    entry = {"rev": rev, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "actor": actor, "reason": reason, "doc": doc}
    try:
        with open(path + ".history.jsonl", "a") as f:
            f.write(json.dumps(entry) + "\n")
        return True
    except Exception as e:  # noqa: BLE001
        log(f"WARN could not append doc history {path}.history.jsonl: {e}")
        return False


def _classes_derived(classes):
    """Per-class derived fields for the /classes API: everything the brain
    computes that the document doesn't declare."""
    return {c["class"]: {
        "display": c["display"],
        "request_kind": c["request_kind"],
        "context_window": c["context_window"],
        "floor_member": c.get("floor_member"),
        "units": c.get("units"),
        "previous_context_window": c.get("previous_context_window"),
        "window_changed_at": c.get("window_changed_at"),
        "members": [{"id": _member_id(m), "route": f"{c['class']}/{_member_id(m)}",
                     "priority": m["priority"],
                     "effective_window": m["effective_window"],
                     "reserve_ceiling": m["reserve_ceiling"],
                     "pay": m["pay"], "stop_at": m["stop_at"]} for m in c["members"]],
    } for c in classes}


def _classes_live_effect(t, new_classes):
    """What installing `new_classes` as tenant t's would do to its LIVE routing
    state (the validate dry-run's live-effect panel, and the PUT response's
    receipt): deleted/new classes, bindings and pins that would be invalidated
    (their member vanished — the proxy bind cache rebinds within ~15 s),
    window changes vs the live config, and consumers left with a dangling
    class ref (§5: deleting a referenced class SUCCEEDS with a warning). Only
    t's: its sessions, pins and consumers. Takes t.plock."""
    new_by = {c["class"]: c for c in new_classes}
    with t.plock:
        old_names = [c["class"] for c in t.classes]
        old_windows = {c["class"]: c["context_window"] for c in t.classes}
        state = {cn: {"pinned": cs.get("pinned"),
                      "sessions": {sid: s.get("member")
                                   for sid, s in cs.get("sessions", {}).items()}}
                 for cn, cs in t.preferred.get("classes", {}).items()}
        consumers = t.consumers
    deleted = [n for n in old_names if n not in new_by]
    added = [c["class"] for c in new_classes if c["class"] not in old_names]
    window_changes = [{"class": n, "from": old_windows[n], "to": new_by[n]["context_window"]}
                     for n in old_names
                     if n in new_by and new_by[n]["context_window"] != old_windows[n]]
    invalidated_pins, invalidated_bindings, consumer_warnings = [], [], []
    for cname, cs in state.items():
        member_ids = ({_member_id(m) for m in new_by[cname]["members"]}
                      if cname in new_by else set())
        if cs["pinned"] and cs["pinned"] not in member_ids:
            invalidated_pins.append({"class": cname, "member": cs["pinned"]})
        for sid, member in cs["sessions"].items():
            if member not in member_ids:
                invalidated_bindings.append({"class": cname, "session": sid, "member": member})
    for n in deleted:
        for c in consumers:
            if f"class:{n}" in c["members"]:
                consumer_warnings.append(f"consumer {c['id']!r} references deleted "
                                         f"class {n!r} — its ref goes dangling until edited")
    return {"deleted_classes": deleted, "new_classes": added,
            "window_changes": window_changes,
            "invalidated_pins": invalidated_pins,
            "invalidated_bindings": invalidated_bindings,
            "consumer_warnings": consumer_warnings}


def _swap_classes(t, new_classes, doc, warnings, lane_settings):
    """Hot-swap tenant t's live class config (§6 hot-swap notes). Caller holds
    t.doc_locks["classes"]. Under one t.plock hold the four fields are
    replaced and t's runtime state reconciled: state for deleted classes
    dropped, fresh state for new classes, pins/bindings to members that
    vanished invalidated (the proxy bind cache rebinds within ~15 s); no
    reader ever sees a half-installed config. Restart never required. Returns
    t's session journal seq at the swap: the moves after it are the new
    config's (_doc_put)."""
    with t.plock:
        t.classes = new_classes
        t.classes_doc = doc
        t.classes_warnings = warnings
        t.lane_settings = lane_settings
        _reconcile_class_state(t.preferred.setdefault("classes", {}), new_classes,
                               lambda text: log(f"{t.id}: classes swap: {text}"))
        _save_state(t)
        return t.journal_seq


def _reconcile_class_state(cls_state, classes, note):
    """Fit class runtime state to a class config, as a swap installs it: state
    for deleted classes dropped, fresh state for new classes, pins and bindings
    to members that vanished dropped. note(text) is told each. The preview runs
    it on a copy (_session_preview). Caller holds the tenant's plock."""
    by_name = {c["class"]: c for c in classes}
    for cname in list(cls_state):
        if cname not in by_name:
            del cls_state[cname]
            note(f"dropped state for deleted class {cname!r}")
    for cname, c in by_name.items():
        cs = cls_state.setdefault(cname, _new_class_state())
        member_ids = {_member_id(m) for m in c["members"]}
        if cs.get("pinned") and cs["pinned"] not in member_ids:
            note(f"class[{cname}] pin {cs['pinned']!r} invalidated (member removed)")
            cs["pinned"] = None
        for sid in [sid for sid, s in cs["sessions"].items()
                    if s.get("member") not in member_ids]:
            note(f"class[{cname}] session {sid[:16]} binding to "
                 f"{cs['sessions'][sid].get('member')!r} invalidated (member removed)")
            del cs["sessions"][sid]


def _swap_consumers(t, new_consumers, doc, warnings):
    """Hot-swap the live consumers config. Caller holds t.doc_locks["consumers"].
    Consumers carry NO runtime state (no sessions, no pins), so the swap is
    just the atomic reference replace under _plock for reader consistency."""
    with t.plock:
        t.consumers = new_consumers
        t.consumers_doc = doc
        t.consumers_warnings = warnings


def _swap_money(t, new_money, doc, warnings):
    """Hot-swap the live money settings. Caller holds t.doc_locks["money"].
    Money carries no runtime state of its own: the next check reads it.
    Returns the session journal's seq at the swap, as _swap_classes does."""
    with t.plock:
        t.money = new_money
        t.money_doc = doc
        t.money_warnings = warnings
        return t.journal_seq


def _live_doc(t, kind):
    """The live raw document of a kind. Caller holds t.plock."""
    return {"classes": t.classes_doc, "consumers": t.consumers_doc, "money": t.money_doc}[kind]


def _live_revision(t, kind):
    """The live document's content-hash revision (If-Match's comparand)."""
    with t.plock:
        return _doc_revision(_live_doc(t, kind))


def _money_derived(t, money):
    """The money document's derived view for the editor: every money pot the
    accounts have now, and each configured pot's status against them."""
    with t.lock:
        snap = list(t.state.values())
    roster = _account_roster(t, snap)
    pots = set(_money_pots(roster)) | {f"{a}/{ALLOWANCE_POT}" for a in roster}
    return {"money_pots": _money_pots(roster),
            "pay_order": [{"pot": p, "status": "ok" if p in pots else "dangling"}
                          for p in money["pay_order"]],
            "pots": {p: dict(e, status="ok" if p in pots else "dangling")
                     for p, e in money["pots"].items()}}


def _doc_snapshot(t, kind):
    """The GET /classes|/consumers body: live raw doc + content-hash revision +
    derived state + load warnings."""
    with t.plock:
        if kind == "classes":
            doc, warnings = t.classes_doc, list(t.classes_warnings)
            derived = _classes_derived(t.classes)
            return {"doc": doc, "revision": _doc_revision(doc),
                    "derived": derived, "warnings": warnings}
        if kind == "consumers":
            doc, warnings = t.consumers_doc, list(t.consumers_warnings)
            derived = _consumers_view(t.consumers, t.classes)
        else:
            doc, warnings, money = t.money_doc, list(t.money_warnings), t.money
    if kind == "money":
        # Against the accounts known now: an orphaned setting shows dangling.
        derived = _money_derived(t, money)
        with t.lock:
            roster = _account_roster(t, list(t.state.values()))
        with t.plock:
            routes = _routes(t.classes, t.lane_settings, roster)
        errors, live = _money_problems(money, roster, routes)
        warnings += [f"dangling: {e}" for e in errors] + live
    return {"doc": doc, "revision": _doc_revision(doc),
            "derived": derived, "warnings": warnings}


def _non_finite_path(v, where="$"):
    """The path of the first NaN or ±inf number in a parsed document, or None.
    Python's JSON reader accepts NaN, Infinity and 1e999; no proxy can decode
    them back, so no document that carries one is stored."""
    if isinstance(v, float) and not math.isfinite(v):
        return where
    if isinstance(v, dict):
        items = v.items()
    elif isinstance(v, list):
        items = enumerate(v)
    else:
        return None
    for k, x in items:
        p = _non_finite_path(x, f"{where}[{k!r}]" if isinstance(k, str) else f"{where}[{k}]")
        if p:
            return p
    return None


def _doc_check(t, kind, doc):
    """STRICT validation + normalization for a candidate document (PUT/validate
    path). -> (errors, warnings, normalized, canonical_doc). canonical_doc is
    what gets hashed/written: classes = the document as given (array or
    {classes, lanes}); consumers = the {"consumers": [...]} wrapper (a bare
    §5-example array is normalized so the revision is shape-independent);
    money = the object as given. normalized for classes is (classes, lane
    settings). The owner settings are checked against the accounts the brain
    knows now; what they accept with a reason comes back as warnings."""
    bad = _non_finite_path(doc)
    if bad:
        return [f"{bad}: numbers must be finite (no NaN or Infinity)"], [], None, None
    if kind == "classes":
        errors = _validate_classes_doc(t, doc)
        if errors:
            return errors, [], None, None
        warnings = []
        classes, lanes = _normalize_classes_doc(doc, warnings, lan=t.lan)
        warnings += _settings_problems(t, classes, lanes)[1]
        return [], warnings, (classes, lanes), doc
    if kind == "money":
        errors, warnings, normalized = _normalize_money(doc, strict=True)
        if not errors:
            with t.lock:
                roster = _account_roster(t, list(t.state.values()))
            with t.plock:
                routes = _routes(t.classes, t.lane_settings, roster)
            errors, warnings = _money_problems(normalized, roster, routes)
        if errors:
            return errors, warnings, None, None
        return [], warnings, normalized, doc
    errors, warnings, normalized = _normalize_consumers(doc, strict=True)
    if errors:
        return errors, warnings, None, None
    return [], warnings, normalized, (doc if isinstance(doc, dict) else {"consumers": doc})


def _doc_live_effect(t, kind, normalized):
    """The validate dry-run's live-effect block. Classes carry runtime state
    (bindings/pins/windows) — see _classes_live_effect. Both owner-settings
    kinds add the session moves a save makes (_session_preview). Consumers
    carry none; their live effect is just which refs would be dangling
    against today's config."""
    if kind == "classes":
        return {**_classes_live_effect(t, normalized[0]), **_session_preview(t, kind, normalized)}
    if kind == "money":
        return _session_preview(t, kind, normalized)
    with t.plock:
        classes = t.classes
    view = _consumers_view(normalized, classes)
    return {"dangling_refs": [m["ref"] for c in view for m in c["members"]
                              if m["status"] == "dangling"]}


def _session_preview(t, kind, normalized):
    """What saving this owner-settings document would do to the bound class
    sessions now (quota-model 50 §2.3): the class pass itself, on a copy of
    the live sessions reconciled as the swap would, under the candidate
    settings, recording instead of acting. `session_moves` are the moves that
    pass makes, each with the reason the session will carry and the
    destination's check: the repays and rebinds the change causes, and any
    move due then anyway. `held_by_pin` are the sessions on money a pin holds
    through a change of their class's pay settings; they move when the pin
    ends. The lane and pool incumbents are the last pass's. Takes the locks
    itself."""
    with t.lock:
        snap = list(t.state.values())
    now = time.time()
    events = []
    with t.plock:
        if kind == "classes":
            (classes, lanes), money = normalized, t.money
        else:
            classes, lanes, money = t.classes, t.lane_settings, normalized
        states = copy.deepcopy(t.preferred.get("classes", {}))
        _reconcile_class_state(states, classes, lambda text: None)
        ctx = _settings_ctx(t, snap, now, classes, lanes, money)
        _compute_class_sessions(t, snap, now, ctx, classes, states,
                                lambda what, ev: events.append((what, ev)))
    return {"session_moves": [ev for what, ev in events if what == "move"],
            "held_by_pin": [ev for what, ev in events if what == "held"]}


def _retry_after(http_error):
    """Parse a Retry-After header (seconds form) from an HTTPError, else None."""
    try:
        ra = http_error.headers.get("Retry-After") if http_error.headers else None
        if ra and str(ra).strip().isdigit():
            return int(str(ra).strip())
    except Exception:  # noqa: BLE001
        pass
    return None


def _iso_to_epoch(s):
    if not s:
        return None
    if not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None


def _key_fingerprint(key):
    """Return a short fingerprint of a secret key for logging; never the value."""
    if not key:
        return "(none)"
    if len(key) <= 12:
        return "***"
    return f"{key[:4]}...{key[-4:]}"


# ---- the normalized reading (quota-model 50 §1.5, C6) -------------------------
#
# One reading per account per poll. It replaced the implicit usage{five_hour,
# seven_day, scoped, credits} contract (20 §1.1): every poller hands its
# provider's raw response to that provider's adapter, which owns every quirk
# (fractions or percent, strings or numbers, windows labelled by length) and
# keeps unknown unknown: a value the response does not carry is None, never 0.
#
#   {provider, kind, as_of, source, ok,
#    plan    {label, basis},
#    pots    [{id, type, draw, entitlement {state, basis}, open, open_basis,
#              limits [...], unit?, unit_usd?, events?, events_basis?, events_used?}],
#    beside  [{kind, limit, on_exceed, basis}],      concurrency (Kimi parallel)
#    models  {<model>: {available, ..., basis}}}     a provider's model_usage
#
# A window limit is {id, kind "window", period, period_s, refill, anchor, scope,
# aliases, used_pct, resets_at, reset_basis, basis, as_of}, plus what the
# provider states besides: used/limit/remaining, its own gate `open` (an OpenAI plan's
# additional_rate_limits), and the Anthropic limits[] entries it was read from
# (`sources`, which the claude_usage_limit_* families render). Window ids are
# the names the records, the header overlay and the alert-rule labels already
# key on: five_hour, seven_day, scoped:<scope>; a length that is neither is
# window:<period>, and an Anthropic limits[] kind nobody has named is
# limit:<kind>[:<scope>] (A-D7). Other limits (Kimi's monthly pool, a money
# pot's balance and caps) carry their own kind.

_WINDOW_SLOTS = ((18000, "five_hour"), (604800, "seven_day"))


def _period_label(secs):
    """A window length in the provider's own terms: 18000 -> 5h, 604800 -> 7d,
    anything else by its largest whole unit."""
    if not isinstance(secs, (int, float)) or secs <= 0:
        return None
    secs = int(secs)
    for unit, n in (("d", 86400), ("h", 3600), ("m", 60)):
        if secs % n == 0:
            return f"{secs // n}{unit}"
    return f"{secs}s"


def _num(v):
    """A finite number, or None: a bool, a string or a NaN is not a reading."""
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        return None
    return float(v)


def _window(wid, period_s, used_pct, resets_at, as_of, scope=None, aliases=None,
            refill=None, anchor=None, reset_basis="exact", **facts):
    """One window limit of a reading. basis says whether its use is known.
    refill is how the clock brings use back (at_once: all of it at resets_at;
    sliding; continuous), anchor what sets its schedule (fixed: a time of the
    provider's choosing; first_use: the first request after the last period
    ended; moved_by_events); None where the adapter does not know (69 §2.1)."""
    w = {"id": wid, "kind": "window", "period": _period_label(period_s),
         "period_s": int(period_s) if period_s else None, "refill": refill, "anchor": anchor,
         "scope": scope, "aliases": aliases, "used_pct": used_pct,
         "resets_at": round(resets_at) if resets_at is not None else None,
         "reset_basis": reset_basis if resets_at is not None else None,
         "basis": "exact" if used_pct is not None else "unknown", "as_of": as_of}
    w.update(facts)
    return w


def _slot_id(period_s, taken):
    """The window id for an unscoped window of this length: its contract slot
    when it fits one (with a server's rounding slack) and the slot is free, else
    window:<period>."""
    for length, label in _WINDOW_SLOTS:
        if abs(period_s - length) <= _WINDOW_SLOT_SLACK_S and label not in taken:
            return label, length
    wid = f"window:{_period_label(period_s)}"
    n = 2
    while wid in taken:
        wid, n = f"window:{_period_label(period_s)}#{n}", n + 1
    return wid, period_s


def _reading_pot(reading, pid):
    for p in (reading or {}).get("pots") or []:
        if p.get("id") == pid:
            return p
    return None


def _reading_windows(reading):
    """{window id: window} of a reading's allowance, in reading order."""
    pot = _reading_pot(reading, ALLOWANCE_POT) or {}
    return {lim["id"]: lim for lim in pot.get("limits") or [] if lim.get("kind") == "window"}


def _reading_limit(pot, lid):
    for lim in (pot or {}).get("limits") or []:
        if lim.get("id") == lid:
            return lim
    return None


# Anthropic (GET /api/oauth/usage, 10 §2). Top-level five_hour and seven_day
# carry utilization in percent and an ISO reset (null while the 5h window is
# idle); limits[] is the generic list {kind, group, percent, severity,
# resets_at, scope.model.display_name, is_active}. session and weekly_all are
# the same windows as the top-level pair; weekly_scoped is a model family's
# weekly cap; any other kind is read too and binds (A-D7), by its group's
# length where the group names one.
_ANTHROPIC_SLOTS = {"session": ("five_hour", 18000, "first_use"),
                    "weekly_all": ("seven_day", 604800, "fixed")}
_ANTHROPIC_GROUP_PERIODS = {"session": 18000, "weekly": 604800}


def _allowance_pot(windows, open_=None, open_basis=None, others=(), events=None,
                   events_basis=None, **facts):
    """A reading's allowance pot. events are the ways its use comes back other
    than the clock (69 §2.2), and events_basis says whether they were read
    (read), could not be (unread, with why) or there is none to read (none):
    an adapter that reads no reset mechanism leaves both to their default."""
    pot = {"id": ALLOWANCE_POT, "type": "allowance", "draw": 1,
           "entitlement": {"state": "active", "basis": "derived"},
           "open": open_, "open_basis": open_basis,
           "limits": list(windows) + list(others),
           "events": list(events or []),
           "events_basis": events_basis or {"state": "none", "why": "no reset mechanism read"}}
    pot.update(facts)
    return pot


# ---- events (quota-model 69 §2.2) ---------------------------------------------
#
# An event changes a pot other than the clock: a reset grant, a bought reset.
# It sits on the pot it acts on (pots[*].events[]):
#   {id, program, label, trigger, effects [...], anchor, anchor_basis,
#    supply {left, total, from, to, order, cooldown_until, per_period, basis},
#    usable {now, why_not, only_at_limit, min_used_pct, not_while_closed,
#            not_with_overage, paused, basis},
#    auto_use, provider_name}
# trigger is owner (the owner redeems it) or owner_pays (the owner buys it);
# an effect is {window, effect, basis} with effect refill (the window's use
# goes to 0), give_back (points, or share_of's share, come off it), add_lot
# ({lot}, on a money pot) or limit ({factor | points, until}); a provider's
# window the adapter cannot name is {window: null, provider_name, effect,
# basis: unknown}. basis is stated (the response says it), inferred (from the
# provider's documentation or a match, the rule beside it) or unknown.
_EVENT_TRIGGERS = ("owner", "owner_pays", "provider")
_EVENT_EFFECTS = ("refill", "give_back", "add_lot", "limit")
_EVENT_ANCHORS = ("kept", "restarts_now", "restarts_at_use", "unknown")
_EVENT_BASES = ("stated", "inferred", "unknown")
_EVENTS_STATES = ("read", "unread", "none")


def _event_problem(ev):
    """Why one event is not in the reading's shape, or None."""
    if not isinstance(ev, dict):
        return "an event must be an object"
    if not (isinstance(ev.get("id"), str) and ev["id"]):
        return "an event's id must be a string"
    if ev.get("trigger") not in _EVENT_TRIGGERS:
        return f"{ev['id']}: trigger must be one of {', '.join(_EVENT_TRIGGERS)}"
    if ev.get("anchor") not in _EVENT_ANCHORS:
        return f"{ev['id']}: anchor must be one of {', '.join(_EVENT_ANCHORS)}"
    if not isinstance(ev.get("label"), (str, type(None))):
        return f"{ev['id']}: label must be a string, or null"
    effects = ev.get("effects")
    if not isinstance(effects, list):
        return f"{ev['id']}: effects must be a list"
    for e in effects:
        if not isinstance(e, dict) or e.get("effect") not in _EVENT_EFFECTS:
            return f"{ev['id']}: an effect must be one of {', '.join(_EVENT_EFFECTS)}"
        if e.get("basis") not in _EVENT_BASES:
            return f"{ev['id']}: an effect's basis must be one of {', '.join(_EVENT_BASES)}"
        if e.get("window") is None:
            if e["basis"] != "unknown" or not isinstance(e.get("provider_name"), str):
                return f"{ev['id']}: an unnamed window is unknown and carries its provider_name"
        elif not isinstance(e["window"], str):
            return f"{ev['id']}: an effect's window must be a window id, or null"
        if e["effect"] == "give_back" and e.get("points") is None and e.get("share_of") is None \
                and e["basis"] != "unknown":
            return f"{ev['id']}: a give_back states its points or share_of, or is unknown"
        if e["effect"] == "limit" and e.get("factor") is None and e.get("points") is None:
            return f"{ev['id']}: a limit effect carries its factor or points"
    sup, use = ev.get("supply"), ev.get("usable")
    if not isinstance(sup, dict) or not isinstance(use, dict):
        return f"{ev['id']}: supply and usable must be objects"
    for k in ("left", "total", "from", "to", "order", "cooldown_until"):
        if sup.get(k) is not None and _num(sup[k]) is None:
            return f"{ev['id']}: supply.{k} must be a number, or null"
    if use.get("now") not in (True, False, None):
        return f"{ev['id']}: usable.now must be true, false or null"
    if not isinstance(use.get("not_while_closed", []), list):
        return f"{ev['id']}: usable.not_while_closed must be a list of window ids"
    return None


def _events_problem(pot):
    """Why a pot's events are not in the reading's shape, or None: each event
    as _event_problem, ids unique, and events_basis {state, why}."""
    events, basis = pot.get("events"), pot.get("events_basis")
    if not isinstance(events, list):
        return "events must be a list"
    if not isinstance(basis, dict) or basis.get("state") not in _EVENTS_STATES:
        return f"events_basis.state must be one of {', '.join(_EVENTS_STATES)}"
    if basis["state"] == "none" and events:
        return "events listed under events_basis none"
    seen = set()
    for ev in events:
        why = _event_problem(ev)
        if why:
            return why
        if ev["id"] in seen:
            return f"{ev['id']}: listed twice"
        seen.add(ev["id"])
    return None


def _checked_events(reading, name):
    """The reading with each pot's events checked: a pot whose events are not
    in the shape (_events_problem) keeps none, says unread with the reason,
    and the log names it. The reading is changed in place and returned."""
    for pot in (reading or {}).get("pots") or []:
        if "events" not in pot and "events_basis" not in pot:
            continue
        why = _events_problem(pot)
        if why:
            log(f"{name}: {pot.get('id')} events refused: {why}")
            pot["events"] = []
            pot["events_basis"] = {"state": "unread", "why": f"invalid: {why}"}
    return reading


def _epoch_s(v):
    """An ISO time or epoch seconds as whole epoch seconds, or None."""
    t = _iso_to_epoch(v) if isinstance(v, str) else _num(v)
    return round(t) if t is not None else None


# Anthropic's reset programs, in /api/oauth/usage?cedar_ember=1 (69 §3.1).
# cedar_ember is the reset grants: {eligible, ineligible_reason, at_limit,
# exhausted[], grants[], next_grant_id, weekly_resets_at, cooldown_until}.
# The provider offers grants only to a client version it supports: a
# User-Agent below it reads `ineligible_reason: "cli_version"` and no grants,
# which the reading says as events unread, never as an account offered none.
# A grant's `type` is the provider's vocabulary and not what it clears; the
# adapter reads clears[] and gives_back_weekly. juniper_tide is the weekly
# 5-hour reset, read to the client's field names (eligible, ineligible_reason,
# next_available_at, arm): its shape on the server is not yet confirmed, so a
# block of any other shape is unread, never guessed.
_ANTHROPIC_CLEARS = {"five_hour": "five_hour", "seven_day": "seven_day"}
_JUNIPER_WAITS = ("not_at_wall", "weekly_limit", "extra_usage")
# Program blocks in the response the adapter reads none of: the first
# non-null value of each is named in the log once, so a new offer is seen.
_ANTHROPIC_PROGRAMS_UNREAD = (
    "amber_cistern", "amber_gauge", "amber_ladder", "brass_thimble", "cinder_cove", "copper_kite",
    "granite_teacup", "harbor_lantern", "iguana_necktie", "indicator", "maple_sundial",
    "nimbus_quill", "omelette_promotional", "pace", "promo_credits", "starry_waterfall",
    "tangelo", "wattle_ember")
_programs_seen = set()


def _anthropic_programs_seen(raw):
    """Name, once per process, each unread program block that carries a value."""
    new = [k for k in _ANTHROPIC_PROGRAMS_UNREAD if raw.get(k) is not None and k not in _programs_seen]
    if new:
        _programs_seen.update(new)
        log(f"anthropic: usage carries program blocks the brain does not read: {', '.join(new)}")


def _scope_model_name(sc):
    """A limits[] entry's scope.model.display_name, or None for any other shape."""
    model = sc.get("model") if isinstance(sc, dict) else None
    dn = model.get("display_name") if isinstance(model, dict) else None
    return dn if isinstance(dn, str) and dn else None


def _anthropic_scoped_index(raw):
    """{scoped window id: its index in limits[]}: the index weekly_scoped_shares
    names a scoped window by."""
    out = {}
    for i, lim in enumerate(raw.get("limits") or []):
        if isinstance(lim, dict) and lim.get("kind") == "weekly_scoped":
            sc = lim.get("scope")
            dn = _scope_model_name(sc)
            if dn:
                out.setdefault(f"scoped:{dn}", i)
    return out


def _anthropic_window_of(name, percent_used, windows):
    """The window id a cedar_ember window name stands for, or None. five_hour
    and seven_day by name; any other name is the one scoped window whose
    used_pct equals the grant's percent_used for it (INFERRED: the provider's
    seven_day_overage_included is the weekly Fable cap, matched so on every
    account read). None when no scoped window, or more than one, matches."""
    if name in _ANTHROPIC_CLEARS:
        return _ANTHROPIC_CLEARS[name]
    pct = _num((percent_used or {}).get(name)) if isinstance(percent_used, dict) else None
    if pct is None:
        return None
    hits = [wid for wid, w in windows.items()
            if wid.startswith("scoped:") and w.get("used_pct") is not None and w["used_pct"] == pct]
    return hits[0] if len(hits) == 1 else None


def _grant_shape_problem(g):
    """Why one cedar_ember grant is not in the shape the adapter reads, or None."""
    if not isinstance(g, dict):
        return "not an object"
    if not (isinstance(g.get("id"), str) and g["id"]):
        return "id is not a string"
    for k in ("clears", "blocking"):
        v = g.get(k)
        if v is not None and not (isinstance(v, list) and all(isinstance(x, str) for x in v)):
            return f"{k} is not a list of names"
    if g.get("percent_used") is not None and not isinstance(g["percent_used"], dict):
        return "percent_used is not an object"
    for k in ("resets_left", "resets_total"):
        if g.get(k) is not None and _num(g[k]) is None:
            return f"{k} is not a number"
    for k in ("label", "unavailable_reason", "starts_at", "ends_at"):
        if g.get(k) is not None and not isinstance(g[k], str):
            return f"{k} is not a string"
    return None


def _anthropic_shares(raw):
    """{limit_index: used_percent_of_weekly} of weekly_scoped_shares; an
    entry of another shape is left out, and a list of another shape is none."""
    shares = raw.get("weekly_scoped_shares")
    out = {}
    for s in shares if isinstance(shares, list) else ():
        if isinstance(s, dict) and isinstance(s.get("limit_index"), int) \
                and not isinstance(s["limit_index"], bool):
            out[s["limit_index"]] = _num(s.get("used_percent_of_weekly"))
    return out


def _cedar_event(g, block, raw, windows):
    """One cedar_ember grant with uses left -> its event (69 §3.1)."""
    pu = g.get("percent_used")
    effects, scoped = [], []
    for name in g.get("clears") or []:
        wid = _anthropic_window_of(name, pu, windows)
        if wid is None:
            effects.append({"window": None, "provider_name": str(name), "effect": "refill",
                            "basis": "unknown"})
            continue
        effects.append({"window": wid, "effect": "refill", "basis": "stated"})
        if wid.startswith("scoped:"):
            scoped.append(wid)
    if g.get("gives_back_weekly") is True:
        # The scoped window's share of the weekly comes back to the weekly;
        # the provider states that share as weekly_scoped_shares, by the
        # scoped window's index in limits[].
        idx = _anthropic_scoped_index(raw)
        shares = _anthropic_shares(raw)
        for wid in scoped or [None]:
            pts = shares.get(idx.get(wid)) if wid is not None else None
            effects.append({"window": "seven_day", "effect": "give_back", "share_of": wid,
                            "points": pts, "basis": "inferred" if wid else "unknown",
                            "points_basis": "stated" if pts is not None else "unknown"})
        # The provider documents that such a reset also refills a 5-hour
        # window the account has; clears[] does not list it. With no 5-hour
        # window read (idle), there is nothing to refill.
        if "five_hour" in windows and not any(e["window"] == "five_hour" for e in effects):
            effects.append({"window": "five_hour", "effect": "refill", "basis": "inferred"})
    weekly = _epoch_s(block.get("weekly_resets_at"))
    seven = (windows.get("seven_day") or {}).get("resets_at")
    blocking = [w for w in (_anthropic_window_of(n, pu, windows) for n in g.get("blocking") or [])
                if w is not None]
    why = g.get("unavailable_reason")
    if not blocking and isinstance(why, str) and why.startswith("blocked_"):
        w = _anthropic_window_of(why[len("blocked_"):], pu, windows)
        blocking = [w] if w is not None else []
    usable_now = g.get("usable_now")
    return {"id": f"cedar_ember:{g.get('id')}", "program": "cedar_ember",
            "label": g.get("label") if isinstance(g.get("label"), str) else None,
            "trigger": "owner", "effects": effects,
            # The provider documents that weekly limits keep their day and
            # time; stated when the block's weekly_resets_at is the weekly's.
            "anchor": "kept",
            "anchor_basis": "stated" if weekly is not None and seven is not None
                            and abs(weekly - seven) <= 1 else "inferred",
            "supply": {"left": _num(g.get("resets_left")), "total": _num(g.get("resets_total")),
                       "from": _epoch_s(g.get("starts_at")), "to": _epoch_s(g.get("ends_at")),
                       "order": 1 if g.get("id") == block.get("next_grant_id") else None,
                       "cooldown_until": _epoch_s(block.get("cooldown_until")),
                       "per_period": None, "basis": "stated"},
            "usable": {"now": usable_now if isinstance(usable_now, bool) else None,
                       "why_not": why if isinstance(why, str) else None,
                       "only_at_limit": g.get("use_requires_limit") is True,
                       "min_used_pct": None, "not_while_closed": blocking,
                       "not_with_overage": False, "paused": g.get("paused") is True,
                       "basis": "stated"},
            "auto_use": None,
            "provider_name": {"clears": list(g.get("clears") or []), "type": g.get("type"),
                              "percent_used": pu if isinstance(pu, dict) else None,
                              "blocking": list(g.get("blocking") or [])}}


def _juniper_event(block):
    """juniper_tide (the weekly 5-hour reset) -> (event or None, unread why or
    None). Read to the client's field names; a block without a boolean
    `eligible` is a shape the adapter cannot read."""
    if not isinstance(block, dict) or not isinstance(block.get("eligible"), bool):
        return None, "juniper_tide shape"
    why = block.get("ineligible_reason")
    if not block["eligible"] and why not in _JUNIPER_WAITS:
        return None, None  # not offered to this account
    return {"id": "juniper_tide", "program": "juniper_tide", "label": "Reset your 5-hour limit",
            "trigger": "owner",
            # Documented: it clears the 5-hour window and still counts toward
            # the weekly, so the weekly is untouched.
            "effects": [{"window": "five_hour", "effect": "refill", "basis": "stated"}],
            "anchor": "unknown", "anchor_basis": "unknown",
            "supply": {"left": 1 if block["eligible"] else None, "total": 1, "from": None,
                       "to": None, "order": None,
                       "cooldown_until": _epoch_s(block.get("next_available_at")),
                       "per_period": "7d", "basis": "inferred"},
            "usable": {"now": block["eligible"], "why_not": why if isinstance(why, str) else None,
                       "only_at_limit": True, "min_used_pct": None,
                       "not_while_closed": ["seven_day"], "not_with_overage": True,
                       "paused": False, "basis": "inferred"},
            "auto_use": None,
            "provider_name": {"arm": block.get("arm"), "ineligible_reason": why}}, None


def _anthropic_events(raw, windows, name="anthropic"):
    """The allowance's events from the reset programs (69 §3.1): {events,
    events_basis, events_used}. A grant with no use left is history, counted
    in events_used, not an event."""
    events, used, unread = [], 0, []
    read, none_why = False, None
    shares = raw.get("weekly_scoped_shares")
    if shares is not None and len(_anthropic_shares(raw)) != (len(shares) if isinstance(shares, list) else -1):
        unread.append("weekly_scoped_shares shape")
        log(f"{name}: weekly_scoped_shares of a shape the brain does not read; events unread")
    ce = raw.get("cedar_ember")
    if isinstance(ce, dict):
        if ce.get("eligible") is True and isinstance(ce.get("grants"), list):
            read = True
            for i, g in enumerate(ce["grants"]):
                why = _grant_shape_problem(g)
                if why is None:
                    left, total = _num(g.get("resets_left")), _num(g.get("resets_total"))
                    if left is not None and left <= 0:
                        if total is not None:
                            used += int(max(0.0, total - left))
                        continue
                    try:
                        ev = _cedar_event(g, ce, raw, windows)
                    except (TypeError, ValueError, AttributeError, KeyError, OverflowError) as e:
                        why = f"{type(e).__name__}: {e}"
                    else:
                        if total is not None:
                            used += int(max(0.0, total - left))
                        events.append(ev)
                        continue
                unread.append("cedar_ember shape")
                log(f"{name}: cedar_ember grants[{i}] not read ({why}); events unread")
        elif ce.get("eligible") is False:
            why = ce.get("ineligible_reason")
            if why == "cli_version":
                unread.append("cli_version")
            else:
                none_why = str(why) if why else "ineligible"
        else:
            unread.append("cedar_ember shape")
            log(f"{name}: cedar_ember block of a shape the brain does not read; events unread")
    elif ce is not None:
        unread.append("cedar_ember shape")
        log(f"{name}: cedar_ember block of a shape the brain does not read; events unread")
    jt = raw.get("juniper_tide")
    if jt is not None:
        ev, why = _juniper_event(jt)
        if why:
            unread.append(why)
            log(f"{name}: juniper_tide block of a shape the brain does not read; events unread")
        elif ev is not None:
            read = True
            events.append(ev)
    if unread:
        # What could be read is kept; unread says the list may be short.
        return {"events": events,
                "events_basis": {"state": "unread", "why": "; ".join(dict.fromkeys(unread))},
                "events_used": used if read else None}
    if read:
        return {"events": events, "events_basis": {"state": "read", "why": None},
                "events_used": used}
    return {"events": [], "events_basis": {"state": "none",
                                           "why": none_why or "no reset program in the reading"},
            "events_used": None}


def _anthropic_reading(raw, as_of, tier=""):
    """GET /api/oauth/usage -> the §1.5 reading of one Anthropic subscription."""
    raw = raw if isinstance(raw, dict) else {}
    windows = {}
    for wid, period_s, anchor in (("five_hour", 18000, "first_use"),
                                  ("seven_day", 604800, "fixed")):
        w = raw.get(wid)
        if isinstance(w, dict):
            windows[wid] = _window(wid, period_s, _num(w.get("utilization")),
                                   _iso_to_epoch(w.get("resets_at")), as_of,
                                   refill="at_once", anchor=anchor, sources=[])
    for lim in raw.get("limits") or []:
        if not isinstance(lim, dict):
            continue
        kind, group = str(lim.get("kind") or ""), str(lim.get("group") or "")
        sc = lim.get("scope")
        dn = _scope_model_name(sc)
        sev = lim.get("severity")
        src = {"kind": kind, "group": group, "scope_model": dn or "",
               "percent": _num(lim.get("percent")), "severity": sev if isinstance(sev, str) else None,
               "is_active": lim.get("is_active") is True,
               "resets_at": _iso_to_epoch(lim.get("resets_at"))}
        refill = "at_once"
        if kind in _ANTHROPIC_SLOTS and not dn:
            wid, period_s, anchor = _ANTHROPIC_SLOTS[kind]
        elif kind == "weekly_scoped" and dn:
            # A scoped cap is weekly and shares the account's 7d reset (10 §2).
            wid, period_s, anchor = f"scoped:{dn}", 604800, "fixed"
        else:
            wid = f"limit:{kind or 'unnamed'}" + (f":{dn}" if dn else "")
            period_s, refill, anchor = _ANTHROPIC_GROUP_PERIODS.get(group), None, None
        if wid not in windows:
            windows[wid] = _window(wid, period_s, src["percent"], src["resets_at"], as_of,
                                   scope=dn, aliases=SCOPE_ALIASES.get(dn) if dn else None,
                                   refill=refill, anchor=anchor, sources=[])
        windows[wid]["sources"].append(src)
    # A window is carried only where the body reports it, top-level or in
    # limits[]. A null or absent five_hour with no session entry is a window
    # not reported: the 5h window is null while idle and opens at first use
    # (10 §2), so it binds nothing and is not unknown. A reported window whose
    # utilization is null is unknown, never 0 (§1.5).
    _anthropic_programs_seen(raw)
    try:
        events = _anthropic_events(raw, windows)
    except Exception as e:  # noqa: BLE001 — a reset block never costs the windows
        log(f"anthropic: reset programs not read ({type(e).__name__}: {e}); events unread")
        events = {"events": [], "events_basis": {"state": "unread", "why": "reset programs shape"},
                  "events_used": None}
    pots = [_allowance_pot(windows.values(), **events)]
    extra = raw.get("extra_usage")
    if isinstance(extra, dict):
        # Usage credits (10 §5): on or off by is_enabled; how much is left is
        # unknown (no field carries the balance, U6), and the units of
        # used_credits and monthly_limit are not known (U7), so the cap says
        # nothing about closed.
        enabled = extra["is_enabled"] if isinstance(extra.get("is_enabled"), bool) else None
        pots.append({
            "id": MONEY_POT, "type": "money", "draw": 2, "unit": "usd", "unit_usd": 1.0,
            "entitlement": {"state": "unknown", "basis": "unknown"},
            "open": enabled, "open_basis": "exact" if enabled is not None else None,
            "disabled_reason": extra.get("disabled_reason"),
            "limits": [
                {"id": "balance", "kind": "balance", "amount": None, "basis": "unknown"},
                {"id": "monthly_limit", "kind": "spend_cap", "setter": "owner@provider",
                 "limit": _num(extra.get("monthly_limit")), "used": _num(extra.get("used_credits")),
                 "used_pct": _num(extra.get("utilization")), "unit": None, "basis": "exact"}]})
    return {"provider": "anthropic", "kind": "subscription", "as_of": as_of,
            "source": "GET /api/oauth/usage", "ok": True,
            "plan": {"label": tier or None, "basis": "derived" if tier else "unknown"},
            "pots": pots, "beside": [], "models": {}}


# ---- effective, from the reading (§1.5) ----------------------------------------

def _scope_binds(window_scope, scope):
    """Does a window of this scope bind a route of that scope? An unscoped
    window binds every route; a scoped one the route of its own scope,
    compared case-blind (the Fable cap binds the fable lane and no other)."""
    return window_scope is None or (scope is not None and
                                    str(window_scope).lower() == str(scope).lower())


def _effective_of(windows, gate):
    """The maximum used_pct of the binding windows, or None when there is none
    or any of them is unknown (unknown is never 0). A closed gate, the
    account's or a window's own, floors it at 100: the percent alone is not
    the gate (a provider has been seen overrunning 100, 11 §3), and a closed gate
    is known spent even where its window's percent is not."""
    if not windows:
        return None
    vals = [w.get("used_pct") for w in windows]
    closed = gate is False or any(w.get("open") is False for w in windows)
    if any(v is None for v in vals) and not closed:
        return None
    return max([v for v in vals if v is not None] + ([100.0] if closed else []))


def _reading_effective(res, scope):
    """`effective` of one poll result for a route of this scope: the windows
    that bind it, from the result's reading; None with no reading."""
    rd = res.get("reading") if res else None
    if not rd:
        return None
    ws = [w for w in _reading_windows(rd).values() if _scope_binds(w.get("scope"), scope)]
    return _effective_of(ws, (_reading_pot(rd, ALLOWANCE_POT) or {}).get("open"))


def _provider_effective(res):
    """A provider account's account-wide `effective`: its unscoped windows
    only, the pool pick's and the provider_* families' number. A per-model
    window binds only its own model's route (_reading_effective)."""
    return _reading_effective(res, None)


def _reading_line(res):
    """One good poll's reading on one log line: every window's use, the gates
    and the balance, so the log alone shows what each read measured and
    whether idle polls move the balance (quota-model 50 §5.2)."""
    rd = res["reading"]
    parts = [f"{wid}={'n/a' if w.get('used_pct') is None else w['used_pct']}%"
             for wid, w in _reading_windows(rd).items()] or ["no window"]
    allow = _reading_pot(rd, ALLOWANCE_POT) or {}
    if allow.get("open") is not None:
        parts.append(f"allowed={allow['open']}")
    credits = _reading_pot(rd, _money_pot(rd.get("provider")))
    if credits and credits.get("open") is not None:
        parts.append(f"{credits['id']}_open={credits['open']}")
        bal = (_reading_limit(credits, "balance") or {}).get("amount")
        if bal is not None:
            parts.append(f"balance={bal}")
    if res.get("kind") == "provider":
        eff = _provider_effective(res)
        parts.append(f"effective={'n/a' if eff is None else eff}%")
    else:
        parts.append(f"tier={res.get('tier')}")
    if res.get("source"):
        parts.append(f"via {res['source']}")
    return " ".join(parts)


def _pool_pick(results, incumbent):
    """The poll result that speaks for a provider's account pool (#71): the
    incumbent's when it is polled, else the coolest with a known effective
    (ties by name; unknowns last). Reduces to the sole result for a pool of
    one — which is what keeps the pre-pool rendering byte-identical. None on
    an empty list."""
    if not results:
        return None
    hit = next((r for r in results if r["name"] == incumbent), None)
    if hit is not None:
        return hit
    return min(results, key=lambda r: (_provider_effective(r) is None,
                                       _provider_effective(r) or 0.0, r["name"]))


# A window is labelled by its length, never by its position (quota-model 11
# D6): a plan may have one weekly window and no 5-hour one, and a plan with both
# must not have them swapped. A length that is neither is its own window,
# window:<period>, never forced into a slot it does not fit (_slot_id). The
# slack absorbs a server rounding the length.
_WINDOW_SLOT_SLACK_S = 180


# ---- switchover policy --------------------------------------------------------

# What a restart keeps of each poll result: the last good reading and when it
# was taken. Never a token: credentials live in the creds files alone.
_READING_KEYS = ("name", "kind", "provider", "email", "tier", "good_ts", "reading", "source")


def _readings_snapshot(t):
    """Every account's last good reading, for _save_state."""
    with t.lock:
        return [{k: r[k] for k in _READING_KEYS if k in r}
                for r in t.state.values() if r.get("good_ts") and r.get("reading")]


_OPENS_ANCHOR = {"first_use": "first_use", "anchored": "fixed"}


def _restored_reading(rd, name):
    """A saved reading in today's shape, in place: one saved before C17 has
    its allowance's reset_options dropped and its events unread (the next
    poll reads them), and each window's opens becomes its anchor, with refill
    unknown. Then its events are checked as a poll's are (_checked_events)."""
    for pot in rd.get("pots") or [] if isinstance(rd, dict) else ():
        if not isinstance(pot, dict) or pot.get("type") != "allowance":
            continue
        pot.pop("reset_options", None)
        if "events" not in pot:
            pot.update(events=[], events_basis={"state": "unread", "why": "saved before events"},
                       events_used=None)
        for w in pot.get("limits") or []:
            if isinstance(w, dict) and w.get("kind") == "window" and "refill" not in w:
                opens = w.pop("opens", None)
                w["refill"], w["anchor"] = None, _OPENS_ANCHOR.get(opens)
    return _checked_events(rd, name)


def _restore_readings(t, readings):
    """Seed _state from saved readings at startup: each is served with its true
    age (good_ts) and marked stale until this process polls the account, so
    the stale rule decides, not a blank as_of (quota-model 50 §1.4). Only an
    account still configured (the creds file's, or an enabled provider
    target) is restored: any other has no credentials and no poll to retire
    it, so it would sit in the roster forever. An account already polled by
    this process keeps its own result. A reading saved before C6 carries the
    raw `usage` it replaced and no `reading`: it is not restored, and the
    account reads never measured until its first poll."""
    configured = set(_account_loop_names(t)) | set(_provider_poll_targets(t))
    n, old = 0, []
    with t.lock:
        for r in readings if isinstance(readings, list) else ():
            if isinstance(r, dict) and "usage" in r and "reading" not in r:
                old.append(r.get("name"))
                continue
            if not (isinstance(r, dict) and isinstance(r.get("name"), str)
                    and isinstance(r.get("good_ts"), (int, float)) and isinstance(r.get("reading"), dict)):
                continue
            if r["name"] not in configured or r["name"] in t.state:
                continue
            _restored_reading(r["reading"], r["name"])
            t.state[r["name"]] = dict(r, ok=0, ts=r["good_ts"], stale=1, http=0, retry_after=None,
                                     err="restored at restart; not polled since")
            n += 1
    if old:
        log(f"saved readings of {old} predate the normalized reading (C6); not restored, "
            "never measured until polled")
    return n


def _save_state(t):
    """Persist the preferred-account decision so an exporter restart doesn't flap
    it, and the last good readings so a restart keeps their age (§1.4).
    Caller holds _plock (never _lock): _preferred is the policy's."""
    try:
        _store.write_json(t.state_file, dict(t.preferred, readings=_readings_snapshot(t)))
    except Exception as e:  # noqa: BLE001
        log(f"WARN could not persist switch state: {e}")


def _migrate_state(d, lane_names, class_names=(), provider_ids=()):
    """Normalize a loaded state dict (v1 flat, or v2 with lanes) to v2 for the
    configured lanes. Pure — no globals, no I/O — so both directions are testable.

    v1 -> v2: every lane is seeded from the legacy flat fields, so dwell continuity
    and an existing global pin survive the upgrade (old pin => all-lane pin). A lane
    added to the config later gets the same seeding. The flat keys are kept verbatim
    (they mirror the default lane), so a ROLLED-BACK brain — whose _load_state copies
    only the keys it knows — reads this file unchanged. Lanes dropped from the config
    are dropped from state.

    v3 (#26) adds an additive `classes` key: per-class pin + session→member
    bindings. Classes no longer configured are dropped; malformed sessions are
    dropped; a session bound to a member no longer in the class is KEPT here and
    rebound by the policy (which can log a reason — this function can't). A
    rolled-back brain simply loses the key: sessions rebind on the next cycle
    after roll-forward, legacy keys untouched.

    v4 (#71) adds an additive `providers` key: per-provider pool state (same
    shape as a lane's). Providers no longer configured are dropped; malformed
    entries reset to a fresh state. A rolled-back brain loses the key and the
    pools re-bootstrap on roll-forward — dwell resets, same accepted cost as
    the v2→v3 classes key.
    """
    legacy = {"account": d.get("account"), "since": d.get("since", 0.0),
              "reason": d.get("reason", "init"), "last_switch": d.get("last_switch", 0.0),
              "pinned": d.get("pinned")}
    out = dict(legacy)
    out["lanes"] = {}
    old_lanes = d.get("lanes") if isinstance(d.get("lanes"), dict) else {}
    for ln in lane_names:
        prev = old_lanes.get(ln)
        if isinstance(prev, dict):
            out["lanes"][ln] = {"account": prev.get("account"),
                                "since": prev.get("since", 0.0),
                                "reason": prev.get("reason", "init"),
                                "last_switch": prev.get("last_switch", 0.0),
                                "pinned": prev.get("pinned")}
        else:
            out["lanes"][ln] = dict(legacy)
    out["classes"] = {}
    old_classes = d.get("classes") if isinstance(d.get("classes"), dict) else {}
    for cn in class_names:
        prev = old_classes.get(cn)
        st = _new_class_state()
        if isinstance(prev, dict):
            if isinstance(prev.get("pinned"), str):
                st["pinned"] = prev["pinned"]
            sessions = prev.get("sessions")
            for sid, s in (sessions.items() if isinstance(sessions, dict) else ()):
                if isinstance(s, dict) and isinstance(s.get("member"), str):
                    st["sessions"][sid] = {
                        "member": s["member"], "since": s.get("since", 0.0),
                        "last_seen": s.get("last_seen", 0.0),
                        "last_switch": s.get("last_switch", 0.0),
                        "reason": s.get("reason", "restored"),
                        "marks": s.get("marks") if isinstance(s.get("marks"), dict) else {},
                        "pay_settings": (s["pay_settings"] if isinstance(s.get("pay_settings"), str)
                                         else None)}
        out["classes"][cn] = st
    out["providers"] = {}
    old_prov = d.get("providers") if isinstance(d.get("providers"), dict) else {}
    for pid in provider_ids:
        prev = old_prov.get(pid)
        if isinstance(prev, dict):
            out["providers"][pid] = {"account": prev.get("account"),
                                     "since": prev.get("since", 0.0),
                                     "reason": prev.get("reason", "init"),
                                     "last_switch": prev.get("last_switch", 0.0),
                                     "pinned": prev.get("pinned")}
        else:
            out["providers"][pid] = _new_lane_state()
    return out


def _load_state(t):
    d = None
    try:
        d = _store.read_json(t.state_file)
    except Exception as e:  # noqa: BLE001
        log(f"WARN could not load switch state: {e}")
    with t.plock:
        if isinstance(d, dict):
            t.preferred.update(_migrate_state(d, _lane_names, [c["class"] for c in t.classes],
                                             _pollable_provider_ids()))
        else:
            t.preferred["lanes"] = {ln: _new_lane_state() for ln in _lane_names}
            t.preferred["classes"] = {}
            t.preferred["providers"] = {}
    if isinstance(d, dict):
        lanes_now = {ln: s.get("account") for ln, s in t.preferred["lanes"].items()}
        log(f"restored preferred account = {t.preferred.get('account')!r} lanes={lanes_now} "
            f"readings={_restore_readings(t, d.get('readings'))}")


def _weekly(res):
    """The 7-day utilization % for an account's poll result, or None if unknown."""
    w = _reading_windows(res.get("reading")).get("seven_day") or {}
    return w.get("used_pct")


def _scoped_window(res, scope_model):
    """The scoped window of this model family in an account's reading (a
    model-scoped limit, matched case-blind by _scope_binds), or None."""
    for w in _reading_windows(res.get("reading")).values():
        if w.get("scope") is not None and _scope_binds(w["scope"], scope_model):
            return w
    return None


def _lane_effective(res, lane_cfg):
    """A lane's effective utilization on one account: the binding constraint for that
    model class = the MAX of every window that would 429 a request for the lane's
    model — the account-wide ones (5-hour session, 7-day, any limits[] kind that
    names no model) and the windows scoped to the lane's scope_model. A request
    succeeds only when ALL of these are under 100%, so the one nearest 100% is
    what actually binds. Recomputed statelessly from the reading each poll, so a
    cap appearing or clearing takes effect next cycle. None when any of them is
    unknown, or there is none (§1.5: unknown is never 0)."""
    return _reading_effective(res, lane_cfg.get("scope_model"))


# ---- the target contract (#24) ------------------------------------------------
# A *target* is anything the router can send a request to: an Anthropic account
# (kind="anthropic_account") or a provider (kind="provider", e.g. Kimi). Every
# target exposes the SAME contract, and the policy consumes targets ONLY
# through these functions (what /preferred publishes of each is its standing,
# accounts{}) — adding a provider means making its poller
# emit the normalized reading (§1.5), never touching the policy:
#
#   _target_kind(res)             how the proxy actuates it. Actuation is #27's
#                                 job: an account target = inject a brain-dispensed
#                                 OAuth bearer via /token; a provider target = the
#                                 proxy's own static key + model rewrite. /token
#                                 stays Anthropic-only.
#   _target_effective(res, lane)  0-100, "% until this target refuses the next
#                                 request" = max over every binding limit; None if
#                                 unknown (never route on a partial picture).
#                                 Anthropic: (target, lane)-dependent via scoped
#                                 caps; provider: target-global (lane ignored).
#   _serves(res, lane)            can this target serve this lane at all? The
#                                 policy filter. Providers serve NO Anthropic lane;
#                                 cross-provider membership arrives with the class
#                                 config (#25) and changes only this function.
#   good_ts / stale               freshness. Staleness exclusion applies to ALL
#                                 targets uniformly (compute_preferred).
#
# A target's windows are its standing's (_account_standing): the drift-up keys a
# window's identity off its resets_at (a reset mints a new one).

def _target_kind(res):
    return "provider" if res.get("kind") == "provider" else "anthropic_account"


def _target_effective(res, lane_cfg):
    """The contract's `effective(target, class)` — sole dispatch point."""
    if res.get("kind") == "provider":
        return _provider_effective(res)
    return _lane_effective(res, lane_cfg)


def _serves(res, lane_cfg):  # noqa: ARG001 — lane_cfg kept for the contract signature
    """Whether this target can serve this LANE at all. Anthropic accounts serve
    every lane (any account can serve any Claude model); providers serve none —
    they never compete inside the Anthropic account switchover. Cross-provider
    membership is the CLASS layer (#25): _class_member_targets is this function's
    sibling for class-routed lookups — lanes and classes stay separate axes."""
    return res.get("kind") != "provider"


# ---- the class layer (#25) ----------------------------------------------------

def _lane_cfg(name):
    return next((lc for lc in _lanes if lc["lane"] == name), None)


def _class_member_targets(member, snap):
    """The polled target result-dicts that could back one class member — the #25
    membership half of the target contract (_serves' sibling for the class axis).
    NO policy here — #26's pointer computation ranks and picks. An `anthropic`
    member is backed by EVERY account (each can serve its lane; which one is the
    lane switchover's call), a `provider` member by the single polled target
    whose provider id matches, [] when that provider isn't polled yet (e.g. gpt
    before #31 — visible degradation, not an error)."""
    if member.get("kind") == "provider":
        return [r for r in snap
                if _target_kind(r) == "provider" and r.get("provider") == member.get("provider")]
    return [r for r in snap if _target_kind(r) == "anthropic_account"]


def _fresh_for(st, scope, now):
    """The dial check's freshness (§1.4, _route_stale) for a route of this
    scope on one account: measured, and no binding limit read more than
    STALE_MAX ago, each window by its own as_of (a reported reading's, 64
    §5). The account's good_ts is for display only."""
    return st is not None and st.get("as_of") is not None and \
        not _route_stale(st, _binding_windows(st, scope), now, STALE_MAX)


def _unmetered(res):
    """Whether a target's reading says it has no quota at all (an allowance
    marked unmetered: today only a local model server, #135), so a fresh poll is the
    whole answer and no number is missing."""
    pot = _reading_pot((res or {}).get("reading"), ALLOWANCE_POT) or {}
    return pot.get("unmetered") is True


def _class_member_entry(member, snap, now, standings, scope):
    """One member's live view for the /preferred `classes` key: the config fields
    plus per-target effective (via _target_effective, per #24) and a usable/status
    rollup. Freshness is the dial check's for the member's route scope
    (_fresh_for, on the context's standings).
    status: ok            = some fresh target has a known effective, or reads
                            unmetered (#135: no quota to report) (usable=True)
            no_quota_data = fresh target(s) but none reports a number
            stale         = target(s) exist but all data is older than STALE_MAX
            no_target     = nothing polled backs this member"""
    entry = dict(member)
    lane_cfg = _lane_cfg(member["lane"]) if member.get("kind") == "anthropic" else None
    backing = [] if (member.get("kind") == "anthropic" and lane_cfg is None) \
        else _class_member_targets(member, snap)
    targets, fresh_any, fresh_known = {}, False, False
    for r in backing:
        v = _target_effective(r, lane_cfg)
        targets[r["name"]] = v
        if _fresh_for(standings.get(r["name"]), scope, now):
            fresh_any = True
            if v is not None or _unmetered(r):
                fresh_known = True
    if not targets:
        status = "no_target"
    elif fresh_known:
        status = "ok"
    elif fresh_any:
        status = "no_quota_data"
    else:
        status = "stale"
    entry["targets"] = targets
    entry["usable"] = status == "ok"
    entry["status"] = status
    return entry


# ---- the dial check (quota-model 50 §1.6, C2) --------------------------------
#
# One function answers, for one route on one account: `included` (the allowance
# serves), `paid` (a money pot the route's `pay` lists serves), or `no` with a
# reason and an `until`. It is evaluated twice: here per (route, account), with
# the class's request_shape standing in for the request, to drive the pointer,
# the bind, the pin and session moves; and in the proxy per request, on these
# same facts plus its overlay (P/dialcheck.go). The two are kept in agreement by
# one fixture set, services/claude-switch-proxy/testdata/dialcheck_golden.json,
# which both test suites run (62 m2: the proxy never ANDs with this answer).
#
# The inputs are plain data so both sides read the same thing:
#   route    {id, pay: [pot ids], stop_at: {window, level} | None, scope}
#   standing one /preferred accounts{} entry (_account_standing)
#   pots     the money document's pots{} ({pot id: {topup, budget?, route_budgets?}})
#   ledger   the /preferred ledger block (_ledger_payload): this period's spend
#            per pot and per route, and when it was read (as_of)
# A check is {check, pot, reason, until, basis}: until is an epoch, "until_paid"
# or None; basis is fresh | stale | unknown.

_CHECK_RANK = {"included": 0, "paid": 1, "no": 2}
_BASIS_RANK = {"fresh": 0, "unknown": 1, "stale": 2}


def _check(check, pot=None, reason=None, until=None, basis="fresh"):
    return {"check": check, "pot": pot, "reason": reason, "until": until, "basis": basis}


def _binding_windows(standing, scope):
    """The windows that bind a route: every unscoped window plus the scoped
    windows of the route's own scope (_scope_binds)."""
    return {name: w for name, w in ((standing.get("allowance") or {}).get("windows") or {}).items()
            if _scope_binds(w.get("scope"), scope)}


def _window_closed(w, now):
    """At its limit, or shut by its own gate (an additional limit's
    allowed), and its reset (when known) still ahead: a reset that has passed
    opens the window whatever the last reading said (§1.4)."""
    used, resets = w.get("used_pct"), w.get("resets_at")
    shut = w.get("open") is False or (used is not None and used >= 100)
    return shut and not (resets is not None and resets <= now)


def _stop_window(stop_at, windows):
    """The window a stop_at names among the binding ones: a period ("7d"), a
    scope ("Fable"), or "effective" (the highest). None = not reported here,
    and then the stop_at has no effect on this account (§1.9)."""
    name = str(stop_at.get("window"))
    if name == "effective":
        # By window name, the first highest winning: the proxy's twin walks
        # the same sorted order, so a tie resolves to the same window.
        best = None
        for _, w in sorted(windows.items()):
            if w.get("used_pct") is not None and (best is None or w["used_pct"] > best["used_pct"]):
                best = w
        return best
    for w in windows.values():
        if w.get("scope") is None and w.get("period") == name:
            return w
    for w in windows.values():
        if w.get("scope") is not None and str(w["scope"]).lower() == name.lower():
            return w
    return None


def _allowance_state(standing, windows, now):
    """(open, reopen): open is True, False or None (unknown); reopen is when a
    closed allowance opens again, None when no reading says. The provider's own
    gate (an OpenAI plan's rate_limit.allowed) closes it below 100 too, until every
    binding window it could mean has reset."""
    gate = (standing.get("allowance") or {}).get("open")
    closed = [w for w in windows.values() if _window_closed(w, now)]
    if closed:
        resets = [w.get("resets_at") for w in closed]
        return False, (None if None in resets else max(resets))
    if gate is False:
        resets = [w.get("resets_at") for w in windows.values()]
        if resets and None not in resets and max(resets) <= now:
            return True, None
        return False, (None if not resets or None in resets else max(resets))
    if gate is None and (not windows or any(w.get("used_pct") is None for w in windows.values())):
        return None, None
    return True, None


def _route_stale(standing, windows, now, stale_max):
    """Staleness is per limit (§1.4): stale when a limit that binds the route
    was read more than stale_max ago. A window carries its own as_of (a
    response refreshes the windows it carries); a provider's own gate (an OpenAI plan's)
    and an account with no binding window are as old as the poll that read
    them, the standing's as_of."""
    times = [w.get("as_of") for w in windows.values()]
    if not times or (standing.get("allowance") or {}).get("open") is not None:
        times.append(standing["as_of"])
    return any(t is None or now - t > stale_max for t in times)


def _budget_period(now):
    """The budget period holding `now` (§1.9): the calendar month in UTC,
    [00:00Z on the 1st, 00:00Z on the next 1st), as epoch seconds."""
    d = datetime.fromtimestamp(now, timezone.utc)
    start = datetime(d.year, d.month, 1, tzinfo=timezone.utc)
    end = datetime(d.year + (d.month == 12), d.month % 12 + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def _budgets_reached(route, pot_id, settings, ledger, now, request=None):
    """Which of the owner's budgets on the pot this route has reached this
    period: a subset of ["pot", "route"], in that order. A ledger of an
    earlier period has spent nothing of this one. A request whose most it can
    charge is known (a Runway task's max_cost, §1.4) is admitted only if spend
    + reserved + max_cost <= budget (§1.7 overrun); reserved is what dials in
    flight hold against the pot and the route (the proxy's; the brain holds
    none). With the cost unknown the budget is reached at spend + reserved >=
    budget: a chat request's cost is learned afterwards."""
    budget = settings.get("budget")
    rbudget = (settings.get("route_budgets") or {}).get(route.get("id"))
    start, _ = _budget_period(now)
    lp = ((ledger or {}).get("pots") or {}).get(pot_id) or {}
    this = ((ledger or {}).get("period") or {}).get("start") == start
    spend = (lp.get("period_spend") or 0) if this else 0
    rspend = ((lp.get("routes") or {}).get(route.get("id")) or 0) if this else 0
    spend += lp.get("reserved") or 0
    rspend += (lp.get("routes_reserved") or {}).get(route.get("id")) or 0
    cost = (request or {}).get("max_cost")

    def reached(limit, spent):
        if limit is None:
            return False
        return spent >= limit if cost is None else spent + cost > limit
    return (["pot"] if reached(budget, spend) else []) + \
        (["route"] if reached(rbudget, rspend) else [])


def _budget_gate(route, pot_id, settings, ledger, now, stale_max, reopen, basis, next_poll_at,
                 request=None):
    """The owner's budget on the drawn money pot (§1.7, §1.9): None when no
    budget binds this route, else its `no`. A pot budget and the route's own
    budget both apply; the first reached closes, until the allowance ahead
    reopens and no later than the period's end. A reached budget stays
    reached whatever else the ledger lacks (spend only grows), so it is
    decided first. Otherwise a ledger that may undercount cannot say the
    budget has room: no (unknown), its basis saying why:
      unknown            the ledger is older than stale_max (the proxy's,
                         after pulls fail): until the account's next poll;
      ledger_unreadable  the ledger file has a line it cannot read, which may
                         hide spend, until it is mended (no time stated);
      no_rates           the ledger lists no unit for the pot, or its rates
                         lack a table its charges need (the ledger's
                         rate_tables), so the proxy would enter no charge, or
                         some charges uncounted (no time stated);
      unpriced           a lower_bound entry this period on the pot (a pot
                         budget) or on this route (its route budget): part of
                         its cost has no price, so the spend may be higher
                         (no time stated; the next period starts clean)."""
    if settings.get("budget") is None and (settings.get("route_budgets") or {}).get(route.get("id")) is None:
        return None
    if ledger and _budgets_reached(route, pot_id, settings, ledger, now, request):
        end = _budget_period(now)[1]
        return _check("no", reason="budget", until=end if reopen is None else min(reopen, end),
                      basis=basis)
    if not ledger or ledger.get("as_of") is None or now - ledger["as_of"] > stale_max:
        return _check("no", reason="unknown", until=next_poll_at, basis="unknown")
    if ledger.get("unreadable_lines"):
        return _check("no", reason="unknown", basis="ledger_unreadable")
    unit = ((ledger.get("pots") or {}).get(pot_id) or {}).get("unit")
    rates = (ledger.get("rates") or {}).get(unit) if unit else None
    if not unit or not _rates_complete(unit, rates, ledger.get("rate_tables") or {}):
        return _check("no", reason="unknown", basis="no_rates")
    lp = (ledger.get("pots") or {}).get(pot_id) or {}
    unpriced = (lp.get("unpriced") or {}) if (ledger.get("period") or {}).get("start") == \
        _budget_period(now)[0] else {}
    if (settings.get("budget") is not None and any(unpriced.values())) or \
            ((settings.get("route_budgets") or {}).get(route.get("id")) is not None
             and unpriced.get(route.get("id"))):
        return _check("no", reason="unknown", basis="unpriced")
    return None


# An empty Runway balance under topup on (§1.9): autobilling checks hourly
# (13 §5), so the refill is due by the empty reading's as_of plus this.
TOPUP_REFILL_S = 3600


def _api_check(route, account, standing, pots, now, stale_max, ledger, request):
    """§1.6 for an api account (§1.2: no allowance, a money pot drawn first,
    Runway's balance): every dial spends money. In order:
      pay       the route may not draw the pot: no (pay, until_paid);
      a closed binding window (only a refusal record makes one here, the
                proxy's): no (spent, until it reopens);
      balance   below the request's max_cost, or nothing left when the cost
                is unknown (the brain's own check has no request), or the pot
                shut: no (balance), until a reading shows money (until_paid)
                with topup off, and with on until the hourly refill: the
                empty reading's as_of + TOPUP_REFILL_S, else the next poll
                once that has passed (§1.9);
      rolling_count  used + what the request counts > limit, for a count that
                binds the route's model: no (spent) until the next poll (a
                rolling count names no reset);
      budget    the budget gate, the request's max_cost reserved (§1.7);
      paid      otherwise, basis unknown while the balance is.
    A pot whose limits are unknown does not block: the route may pay it, so
    the provider's refusal is the worst it costs (§1.4), and that walks."""
    pot_name, pot = sorted((standing.get("money") or {}).items())[0] \
        if standing.get("money") else (None, None)
    pot_id = f"{account}/{pot_name}" if pot_name else None
    windows = _binding_windows(standing, route.get("scope"))
    stale = _route_stale(standing, windows, now, stale_max)
    basis = "stale" if stale else "fresh"
    if pot_id is None or pot_id not in (route.get("pay") or []):
        return _check("no", reason="pay", until="until_paid", basis=basis)
    allow, reopen = _allowance_state(standing, windows, now) if windows else (True, None)
    if allow is False:
        return _check("no", reason="spent",
                      until=reopen if reopen is not None else standing.get("next_poll_at"),
                      basis=basis)
    req = request or {}
    cost = req.get("max_cost")
    bal = pot.get("balance")
    if pot.get("open") is False or (bal is not None and (bal < cost if cost is not None else bal <= 0)):
        until = "until_paid"
        if ((pots or {}).get(pot_id) or {}).get("topup") == "on":
            until = standing["as_of"] + TOPUP_REFILL_S
            if until <= now:
                until = standing.get("next_poll_at")
        return _check("no", reason="balance", until=until, basis=basis)
    for lim in (pot.get("limits") or {}).values():
        if lim.get("kind") != "rolling_count" or not _scope_binds(lim.get("scope"), route.get("scope")):
            continue
        used, limit = lim.get("used"), lim.get("limit")
        counted = max(int(req.get("outputs") or 1), 1) if lim.get("counts") == "output" else 1
        if used is not None and limit is not None and used + counted > limit:
            return _check("no", reason="spent", until=standing.get("next_poll_at"), basis=basis)
    gate = _budget_gate(route, pot_id, (pots or {}).get(pot_id) or {}, ledger, now, stale_max,
                        None, basis, standing.get("next_poll_at"), request)
    if gate is not None:
        return gate
    return _check("paid", pot=pot_id, basis="unknown" if bal is None else basis)


def dial_check(route, account, standing, pots, now, stale_max=None, ledger=None, request=None):
    """§1.6 steps 1-4 for one (route, account) and one request. Entitlement
    (step 5) has no reading before C9, and every request is admitted by the
    allowance until a reading says otherwise, so neither appears here yet.
    request is {max_cost, outputs}: the most the request can charge in the
    pot's unit (None: unknown in advance) and how many outputs a count counts;
    None is the brain's own check, which has no request: unknown cost, one
    output. An api account has its own walk (_api_check), unless its reading
    opens an allowance (an api key with no quota to read): then the allowance
    walk below is the whole answer, as for any open allowance."""
    if not standing or standing.get("as_of") is None:
        return _check("no", reason="unknown", until=(standing or {}).get("next_poll_at"),
                      basis="unknown")
    stale_max = STALE_MAX if stale_max is None else stale_max
    if standing.get("kind") == "api" and (standing.get("allowance") or {}).get("open") is not True:
        return _api_check(route, account, standing, pots, now, stale_max, ledger, request)
    windows = _binding_windows(standing, route.get("scope"))
    stale = _route_stale(standing, windows, now, stale_max)
    allow, reopen = _allowance_state(standing, windows, now)
    # One money pot per account (a subscription's credits, an api account's
    # balance); sorted so both evaluators take the same one regardless.
    money = sorted((standing.get("money") or {}).items())
    pot_name, pot = money[0] if money else (None, None)
    pot_id = f"{account}/{pot_name}" if pot_name else None
    may_pay = pot_id is not None and pot_id in (route.get("pay") or [])
    money_may_open = pot is not None and pot.get("open") is not False
    basis = "stale" if stale else ("unknown" if allow is None else "fresh")
    # Step 4: stale or unknown blocks only where it could hide money the route
    # may not pay (62 B1). Elsewhere the last value is used, and ranks after fresh.
    if (stale or allow is None) and money_may_open and not may_pay:
        return _check("no", reason="unknown", until=standing.get("next_poll_at"), basis=basis)
    if allow is not False:
        stop = route.get("stop_at")
        if stop and stop.get("level") is not None and stop["level"] < 100:
            # A window whose reset has passed since the reading has reopened:
            # its last value no longer counts against the level (§1.4). Level 0
            # never uses that allowance (§1.9), so it holds across the reset,
            # until the reading after it.
            live = windows if stop["level"] <= 0 else {
                n: w for n, w in windows.items()
                if not (w.get("resets_at") is not None and w["resets_at"] <= now)}
            w = _stop_window(stop, live)
            if w is not None and (stop["level"] <= 0 or
                                  (w.get("used_pct") is not None and w["used_pct"] >= stop["level"])):
                until = w.get("resets_at")
                if until is not None and until <= now:
                    until = standing.get("next_poll_at")
                return _check("no", reason="stop_at", until=until, basis=basis)
        return _check("included", basis=basis)
    if money_may_open:
        if may_pay:
            gate = _budget_gate(route, pot_id, (pots or {}).get(pot_id) or {}, ledger, now,
                                stale_max, reopen, basis, standing.get("next_poll_at"), request)
            if gate is not None:
                return gate
            return _check("paid", pot=pot_id, basis="unknown" if pot.get("open") is None else basis)
        return _check("no", reason="pay", until=reopen if reopen is not None else "until_paid",
                      basis=basis)
    refill = None
    if pot is not None and ((pots or {}).get(pot_id) or {}).get("topup") == "on":
        refill = standing.get("next_poll_at")
    times = [t for t in (reopen, refill) if t is not None]
    # No reading says when the allowance reopens: retry at the next poll,
    # which will (no money is involved: the pot is closed or absent).
    until = min(times) if times else standing.get("next_poll_at")
    return _check("no", reason="spent", until=until, basis=basis)


def _until_key(until):
    if isinstance(until, (int, float)):
        return (0, until)
    return (1, 0) if until == "until_paid" else (2, 0)


def check_key(c):
    """Sort key of a check: included, then paid, then no; fresh before unknown
    before stale inside a tier; the earliest `until` first among `no`."""
    if c["check"] == "no":
        return (2, _until_key(c.get("until")))
    return (_CHECK_RANK[c["check"]], (_BASIS_RANK.get(c.get("basis"), 2), 0))


def best_check(checks):
    """Index of a member's verdict among its accounts' checks (the first wins a
    tie), or None for none."""
    best = None
    for i, c in enumerate(checks):
        if best is None or check_key(c) < check_key(checks[best]):
            best = i
    return best


def pay_order_key(pot, priority, pay_order):
    """The paid tier's key (§1.9): the drawn pot's rank in pay_order (every
    unlisted pot after every listed one), then member priority, then pot id."""
    rank = pay_order.index(pot) if pot in pay_order else len(pay_order)
    return (rank, priority, pot)


# ---- the unavailable reply (quota-model 50 §2.8, C3) ----------------------------
#
# What a client is told when no route of a class has a check that is included
# or paid: every member's route and each account's check, the earliest moment
# any of them could change (available_at, and its basis), the Retry-After that
# follows from it, and the owner actions that would serve sooner (options). The
# brain builds it for /class-bind's 409; the proxy builds the same body from its
# own checks when a walk runs out (P/unavailable.go) and renders both by wire.
# The golden file's `unavailable` section holds the two builders together.
#
# Only a `no` whose until is a time contributes one: until_paid and an unknown
# next poll say money or an owner must act, and invent no time. With no time
# at all the reply is the non-retryable one. A `no (blocked)` (a model the
# plan refuses) gives no time and no option, and is not retryable.


def _is_time(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _check_label(c):
    """included | paid:<pot> | no: the check as one word, as the reply and the
    proxy's log print it."""
    if c["check"] == "paid":
        return f"paid:{c['pot']}"
    return c["check"]


def _until_window(standing, scope, until):
    """The binding window whose reset is this until (the first by name), or
    None: the limit the reply says comes back then."""
    if not _is_time(until):
        return None
    for name, w in sorted(_binding_windows(standing, scope).items()):
        if w.get("resets_at") == until:
            return name
    return None


def _until_basis(standing, window, until, why, now):
    """How firm a contributing time is: the window's own reset_basis (exact,
    or early_possible for a provider which has ended periods early); exact for a
    budget's period end (§1.9: 00:00Z on the next 1st); next_poll when it is
    the account's next poll (only a reading can tell); derived for an empty
    balance's hourly refill under topup on (§1.9); record for any other time
    (the proxy's refusal records, never the brain's)."""
    if window is not None:
        return standing["allowance"]["windows"][window].get("reset_basis") or "exact"
    if why == "budget" and until == _budget_period(now)[1]:
        return "exact"
    if standing.get("next_poll_at") == until:
        return "next_poll"
    if why == "balance" and standing.get("as_of") is not None and \
            until == standing["as_of"] + TOPUP_REFILL_S:
        return "derived"
    return "record"


def _pot_of(standing, account):
    """(pot id, pot) of the account's money pot, or (None, None): the one pot
    the dial check reads, sorted so both evaluators take the same."""
    money = sorted((standing.get("money") or {}).items())
    if not money:
        return None, None
    return f"{account}/{money[0][0]}", money[0][1]


def _reply_options(route, account, standing, c, pots, ledger, now, request=None):
    """The owner actions that would make this (route, account) serve sooner
    (§2.8 item 1): its money pot, where permitting, funding or enabling it
    would serve, raising each budget the route has reached on it (§1.9), and
    the allowance's events while the allowance is what holds it
    (_event_options). Each names its action in words."""
    if c["check"] != "no":
        return []
    out = []
    rid = route["id"]
    pid, pot = _pot_of(standing, account)
    listed = pid is not None and pid in (route.get("pay") or [])
    if pot is not None and c["reason"] in ("pay", "unknown") and pot.get("open") is True and not listed:
        out.append({"kind": "permit", "route": rid, "account": account, "pot": pid,
                    "action": f"add {pid} to {rid}'s pay"})
    if pot is not None and (c["reason"] == "balance" or
                            (c["reason"] in ("spent", "pay") and pot.get("open") is False)):
        # An empty pot, or a balance below this request's cost: money would
        # serve, and where the route may not draw the pot, the permit too.
        then = "" if listed else f", and add {pid} to {rid}'s pay"
        if standing.get("provider") == "anthropic":
            out.append({"kind": "enable", "route": rid, "account": account, "pot": pid,
                        "action": f"enable usage credits on {account}{then}"})
        else:
            out.append({"kind": "fund", "route": rid, "account": account, "pot": pid,
                        "action": f"add money to {pid}{then}"})
    if c["reason"] == "budget":
        for which in _budgets_reached(route, pid, (pots or {}).get(pid) or {}, ledger, now, request):
            out.append({"kind": "budget", "route": rid, "account": account, "pot": pid,
                        "action": f"raise {pid}'s budget" if which == "pot"
                        else f"raise {rid}'s route budget on {pid}"})
    if c["reason"] in ("pay", "spent", "budget"):
        out.extend(_event_options(route, account, standing, now))
    return out


def _event_gain(e, windows):
    """Does this effect give use back? A refill of a window the standing
    carries with use on it (or use unknown), or a give_back of more than 0
    points. A give_back with no points stated (no share read) or 0 gives
    nothing the reply can promise."""
    w = windows.get(e.get("window"))
    if e.get("effect") == "refill":
        return w is not None and (w.get("used_pct") is None or w["used_pct"] > 0)
    if e.get("effect") == "give_back":
        pts = _num(e.get("points"))
        return w is not None and pts is not None and pts > 0
    return False


def _event_options(route, account, standing, now):
    """The allowance's events the owner could use now that would let this
    route serve (69 §6.2): each with trigger owner or owner_pays, usable now
    by the provider's word, with an effect that gives use back (_event_gain)
    to a window that refused the route (a binding window closed now), or to
    any window when the account's own gate is closed. A bought one says so;
    none is valued here, which is the fleet's."""
    allowance = standing.get("allowance") or {}
    windows = allowance.get("windows") or {}
    closed = allowance.get("open") is False
    refused = {name for name, w in _binding_windows(standing, route.get("scope")).items()
               if _window_closed(w, now)}
    out = []
    for ev in allowance.get("events") or []:
        if ev.get("trigger") not in ("owner", "owner_pays") or \
                (ev.get("usable") or {}).get("now") is not True:
            continue
        if not any((closed or e.get("window") in refused) and _event_gain(e, windows)
                   for e in ev.get("effects") or []):
            continue
        fee = ", for a fee" if ev["trigger"] == "owner_pays" else ""
        out.append({"kind": "event", "route": route["id"], "account": account, "pot": None,
                    "event": ev["id"], "action": f"{ev.get('label') or ev['id']} on {account}{fee}"})
    return out


def unavailable_body(routes, checks, standings, now, pots, ledger, request=None):
    """The §2.8 body for a class's member routes: {members, available_at,
    available_basis, retry_after_s, retryable, options}. routes are in member
    order ({id, accounts, pay, scope}); checks is {route id: {account:
    check}}; standings {account: standing}; pots and ledger are what the checks
    read (the money settings' pots, the /preferred ledger block), and request
    the one they were checked for (None: the brain's own). available_at
    is the earliest `no`
    until that is a time still to come, and retry_after_s the whole seconds to
    it; both null when nothing gives one. An until already past is stated as
    null (unknown), never as a time.

    retryable is by meaning, not by whether a time is known: false only when
    every account is in a known state that no time ends (`until_paid`: money
    or the owner's permit must come first), which is the non-retryable money
    reply. Any other account, a `no (unknown)` above all, may serve again with
    no one acting, so the reply is retryable even when it can state no time.

    None when no route has an account: nothing has a check, so no time or
    owner action would serve, and that is a configuration error, answered with
    the typed 503 (§2.8 item 6). Golden-tested against the proxy's
    unavailableBody."""
    if not any(rt["accounts"] for rt in routes):
        return None
    members, options = [], []
    at, at_basis, retryable = None, None, False
    for rt in routes:
        accounts = []
        for a in rt["accounts"]:
            c = checks[rt["id"]][a]
            st = standings.get(a) or {}
            until = c.get("until") if c["check"] == "no" else None
            if _is_time(until) and until <= now:
                # A time already past (the next poll of a brain that answered
                # long ago) is no time the reply can state: unknown.
                until = None
            window = _until_window(st, rt.get("scope"), until)
            accounts.append({"account": a, "check": _check_label(c), "why": c.get("reason"),
                             "until": until, "basis": c["basis"], "window": window})
            # A blocked account (the plan refuses the model: the proxy's
            # not_entitled record) waits on the owner as until_paid does: no
            # time, no option, not retryable.
            retryable = retryable or (until != "until_paid" and c.get("reason") != "blocked")
            if _is_time(until) and (at is None or until < at):
                at, at_basis = until, _until_basis(st, window, until, c.get("reason"), now)
            options.extend(_reply_options(rt, a, st, c, pots, ledger, now, request))
        members.append({"route": rt["id"], "accounts": accounts})
    retry = None if at is None else math.ceil(at - now)  # at > now, so at least 1
    return {"members": members, "available_at": at, "available_basis": at_basis,
            "retry_after_s": retry, "retryable": retryable, "options": options}


# ---- account standing (quota-model 50 §1.5, §1.6) ------------------------------

def _standing_pot(pot):
    """A reading's pot in the standing's shape: its facts, its limits keyed by
    id, and for a money pot the balance said outright (None = unknown)."""
    out = {k: v for k, v in pot.items() if k not in ("id", "limits", "draw")}
    out["limits"] = {lim["id"]: {k: v for k, v in lim.items() if k != "id"}
                     for lim in pot.get("limits") or [] if lim.get("kind") != "window"}
    if pot.get("type") == "money":
        out["balance"] = (out["limits"].get("balance") or {}).get("amount")
    return out


def _account_standing(t, name, res, provider, now):
    """One account's standing (the /preferred accounts{} entry the dial check
    reads, golden shape in dialcheck_golden.json), from its latest good
    reading (§1.5): the allowance's gate and windows keyed by id, its other
    limits, each money pot keyed by id, as_of that reading (null = never
    measured), and when the brain will next ask (next_poll_at, backoff
    included: the schedule itself, in the past while a due poll runs, so it
    and every check's until taken from it stay put between renders, as
    PREFERRED_CLOCK_ONLY needs), and the owner's last re-check (recheck_epoch, null for none:
    the proxy drops the account's refusal records older than it, §2.6). A
    failed poll keeps the last good reading and good_ts (poller),
    so a failure ages the standing rather than blanking it. Each window
    carries its own as_of (quota-model 64 §5): the poll's, or that of the
    reported reading that won it or the hold that closes it (the result's
    reported_as_of, _reported_result and _hold_snap), so a
    report keeps its window fresh while polls fail. The standing's as_of
    stays the poll's."""
    with t.poll_health_lock:
        sched = t.poll_schedule.get(name)
        epoch = t.recheck_epochs.get(name)
    rd = (res or {}).get("reading")
    standing = {"provider": provider, "kind": _account_kind(provider),
                "as_of": (res or {}).get("good_ts") if rd else None,
                "next_poll_at": round(sched) if sched else None,
                "recheck_epoch": epoch,
                "source": None, "plan": None,
                "allowance": {"open": None, "open_basis": None, "windows": {}, "limits": {},
                              # Not read yet, which is not "offers none" (69 §2.6).
                              "events": [],
                              "events_basis": {"state": "unread", "why": "no reading yet"}},
                "money": {_money_pot(provider): {"open": None, "balance": None, "unit": None}},
                "beside": [], "models": {}}
    if standing["as_of"] is None:
        return standing
    for k in ("kind", "source", "plan", "beside", "models"):
        standing[k] = rd.get(k, standing[k])
    # A reading with no allowance pot (an api account's money alone) was
    # read: it offers no reset, as its adapter's allowance would say.
    standing["allowance"]["events_basis"] = {"state": "none", "why": "no reset mechanism read"}
    reported = res.get("reported_as_of") or {}
    for pot in rd.get("pots") or []:
        sp = _standing_pot(pot)
        if pot.get("type") == "allowance":
            sp["windows"] = {w["id"]: dict({k: v for k, v in w.items() if k != "id"},
                                           as_of=reported.get(w["id"], standing["as_of"]))
                             for w in pot.get("limits") or [] if w.get("kind") == "window"}
            standing["allowance"] = sp
        else:
            standing["money"][pot["id"]] = sp
    return standing


def _account_roster(t, snap):
    """{account: provider} for every account the brain knows: Anthropic accounts
    in the creds cache, every enabled provider poll target, and anything with a
    reading. An account listed but not yet read is "never measured"."""
    roster = {n: "anthropic" for n in _account_loop_names(t)}
    for name, (pid, _) in _provider_poll_targets(t).items():
        roster[name] = pid
    for r in snap:
        roster[r["name"]] = (r.get("provider") or r["name"]) if _target_kind(r) == "provider" \
            else "anthropic"
    return roster


def api_key(t, account):
    """(key, "") for a key-signed account whose provider's reader finds a
    key: an api-kind account (§1.2), or a provider whose static key the proxy
    signs with (Kimi). ("", "not_api") for an account whose provider has no
    reader here, ("", "no_key") for one with no key placed. The account is
    named in the roster, else by its provider's id (a single-account
    provider's one account), so one whose key is absent (its poll not
    enabled) is still known."""
    with t.lock:
        roster = _account_roster(t, list(t.state.values()))
    provider = roster.get(account) or account
    reader = (PROVIDERS.get(provider) or {}).get("api_key")
    if reader is None:
        return "", "not_api"
    key = (reader(t, account) or "").strip()
    return (key, "") if key else ("", "no_key")


def recheck(t, account, now):
    """POST /recheck?account= (quota-model 50 §2.6, C7): poll the account at
    once and bump its recheck_epoch to now, so the proxy drops every refusal
    record on it older than that at its next pull. For a purchase no reading
    can see; an owner action, never triggered by anything here. Returns the
    epoch, or None for an account the brain does not know."""
    with t.lock:
        roster = _account_roster(t, list(t.state.values()))
    if account not in roster:
        return None
    epoch = _advance_epoch(t, account, now)
    _poll_wakeup.set()
    return epoch


def _advance_epoch(t, account, now):
    """Bump account's recheck_epoch to now (never back), persist it and make
    the account due at once. A /recheck and every enrolment (C11) do: the
    proxy reads an advanced epoch as a new identity, dropping its refusal
    records and its cached credential, so a remove and a re-add between two
    of its pulls never leaves it holding the removed one."""
    with t.poll_health_lock:
        epoch = max(now, t.recheck_epochs.get(account) or 0)
        t.recheck_epochs[account] = epoch
        _save_rechecks_locked(t)
        t.poll_schedule.pop(account, None)  # due at once
    return epoch


def _save_rechecks_locked(t):
    """Write _recheck_epochs to RECHECK_FILE, atomically (not fsynced, as
    _save_state). Caller holds _poll_health_lock."""
    try:
        _store.write_json(t.recheck_file, t.recheck_epochs, sort_keys=True)
    except OSError as e:
        log(f"WARN could not persist the re-checks to {t.recheck_file}: {e}")


def _load_rechecks(t):
    """Read RECHECK_FILE into _recheck_epochs (main()): each account's newer
    epoch wins. -> how many were read."""
    try:
        doc = _store.read_json(t.recheck_file)
    except (OSError, ValueError, credstore.CredStoreError) as e:
        log(f"WARN cannot read {t.recheck_file} ({e}); no re-check carried over")
        return 0
    if doc is None:
        return 0
    if not isinstance(doc, dict):
        log(f"WARN {t.recheck_file} is not a re-check file; no re-check carried over")
        return 0
    n = 0
    with t.poll_health_lock:
        for acct, epoch in doc.items():
            if isinstance(epoch, (int, float)) and not isinstance(epoch, bool) and math.isfinite(epoch):
                t.recheck_epochs[acct] = max(epoch, t.recheck_epochs.get(acct) or 0)
                n += 1
    return n


def _is_money_pot_id(pid, roster):
    """Is pid a money pot's id: the account's own pot when the account is
    known, else a name some account's money pot has (a dangling pot)."""
    acct, pot = _split_pot(pid)
    if acct in roster:
        return pot == _money_pot(roster[acct])
    return pot in {MONEY_POT, *_API_POTS.values()}


def _money_pots(roster):
    """Every money pot id of every account: a subscription's credits, an api
    account's balance (_money_pot)."""
    return sorted(f"{a}/{_money_pot(pv)}" for a, pv in roster.items())


# ---- the route registry (quota-model 50 §1.1, §1.9) ----------------------------
# A route is what owner settings attach to and what the dial check is asked
# about: each class member ("<class>/<member id>") and each bare lane
# ("lane:<lane>"). Its accounts are the ones that can serve it: every Anthropic
# account for a lane or a lane member, the provider's pool for a provider member.

def _pay_pots(pay, roster):
    """A route's pay as exact pot ids, in its order: "<provider>/*" becomes
    the money pot of each of the roster's accounts of that provider (sorted;
    none when it has none), and a pot listed twice is kept once."""
    out = []
    for p in pay:
        prov, pot = _split_pot(p)
        each = ([f"{a}/{_money_pot(pv)}" for a, pv in sorted(roster.items()) if pv == prov]
                if pot == PAY_ALL else [p])
        out += [x for x in each if x not in out]
    return out


def _routes(classes, lane_settings, roster):
    """{route id: route} for every class member and every bare lane. A route's
    pay is exact pot ids (a "<provider>/*" expanded against this roster);
    pay_declared is what the document says, which _route_setting_issues
    checks."""
    anth = sorted(n for n, pv in roster.items()
                  if pv == "anthropic" or (PROVIDERS.get(pv) or {}).get("lanes"))
    out = {}
    for c in classes:
        for m in c["members"]:
            mid = _member_id(m)
            if m["kind"] == "anthropic":
                lc = _lane_cfg(m["lane"])
                lane, prov, scope, accts = m["lane"], "anthropic", (lc or {}).get("scope_model"), anth
            else:
                # A provider member's scope is its model: a window the
                # provider scopes to that model binds it (O-D7), no other.
                lane, prov, scope = None, m["provider"], m["model"]
                accts = sorted(n for n, pv in roster.items() if pv == prov)
            rid = f"{c['class']}/{mid}"
            out[rid] = {"id": rid, "kind": "member", "class": c["class"], "member": mid,
                        "lane": lane, "provider": prov, "scope": scope, "accounts": accts,
                        "pay": _pay_pots(m["pay"], roster), "pay_declared": list(m["pay"]),
                        "stop_at": m["stop_at"],
                        "priority": m["priority"], "request_kind": c["request_kind"]}
    for lc in _lanes:
        st = lane_settings.get(lc["lane"]) or {}
        rid = f"lane:{lc['lane']}"
        out[rid] = {"id": rid, "kind": "lane", "class": None, "member": None,
                    "lane": lc["lane"], "provider": "anthropic", "scope": lc["scope_model"],
                    "accounts": anth, "pay": _pay_pots(st.get("pay") or [], roster),
                    "pay_declared": list(st.get("pay") or []),
                    "stop_at": st.get("stop_at"), "priority": None, "request_kind": "chat"}
    return out


def _route_setting_issues(route, roster, standings):
    """[(severity, message)] for one route's settings against the accounts:
    "error" for what a PUT refuses, which is structure alone (a pay pot that is
    no money pot; a known account's pot that can never back the route, being
    another provider's; a <provider>/* of no provider, or of one whose
    accounts can never back the route); "dangling" for what depends on the accounts and their
    readings (an account not in the pool; a stop_at window no measured account
    carries), which a PUT accepts with a warning,
    since the pool and the readings move under the document; and "warning"
    (stop_at >= 100; no account measured yet). At runtime an error or a
    dangling setting is shown and binds nothing (§1.9)."""
    out = []
    for pid in route["pay_declared"]:
        acct, pot = _split_pot(pid)
        if pot == PAY_ALL:
            # Every account of provider `acct`: one that can back the route is
            # its own provider's, or for a lane's an Anthropic or lane provider's.
            if acct != "anthropic" and acct not in PROVIDERS:
                out.append(("error", f"pay {pid!r}: no provider {acct!r}"))
            elif acct != route["provider"] and not (
                    route["provider"] == "anthropic" and (PROVIDERS.get(acct) or {}).get("lanes")):
                out.append(("error", f"pay {pid!r}: {acct}'s accounts can never back a route of "
                                     f"{route['provider']}"))
            continue
        if pot != _money_pot(roster.get(acct, route["provider"])):
            out.append(("error", f"pay {pid!r}: not a money pot (an account's money pot is "
                                 f"{acct}/{_money_pot(roster.get(acct, route['provider']))})"))
        elif acct not in roster:
            out.append(("dangling", f"pay {pid!r}: unknown pot (no account {acct!r})"))
        elif roster[acct] != route["provider"]:
            out.append(("error", f"pay {pid!r}: {acct!r} is an account of {roster[acct]}; it can "
                                 f"never back a route of {route['provider']}"))
    stop = route["stop_at"]
    if stop:
        if stop["level"] >= 100:
            out.append(("warning", f"stop_at level {stop['level']:g} has no effect: at 100 the "
                                   "window is closed for every route and money answers to pay"))
        measured = [a for a in route["accounts"]
                    if (standings.get(a) or {}).get("as_of") is not None]
        if stop["window"] != "effective":
            if not measured:
                out.append(("warning", f"stop_at window {stop['window']!r} unchecked: no account "
                                       "of this route has been measured yet"))
            elif not any(_stop_window(stop, _binding_windows(standings[a], route["scope"]))
                         for a in measured):
                out.append(("dangling", f"stop_at window {stop['window']!r}: no reading of this "
                                     "route's accounts carries it (a period like 5h or 7d, "
                                     "a scope, or 'effective')"))
    return out


def _settings_problems(t, classes, lane_settings):
    """(errors, warnings) of a candidate classes document's settings against
    the accounts the brain knows now (the PUT-time check, §1.9). Only
    structure is an error; a setting the accounts leave dangling is a
    warning, so an unrelated save never fails on a reading."""
    with t.lock:
        snap = list(t.state.values())
    now = time.time()
    roster, _, _, standings = _decision_view(t, snap, now)  # what the document's warnings read
    errors, warnings = [], []
    routes = _routes(classes, lane_settings, roster)
    for rid, route in routes.items():
        for sev, msg in _route_setting_issues(route, roster, standings):
            if sev == "error":
                errors.append(f"{rid}: {msg}")
            else:
                warnings.append(f"{rid}: {'dangling: ' if sev == 'dangling' else ''}{msg}")
    # A route budget this document orphans (its route gone, or the pot out of
    # its pay) is accepted and binds nothing, said as dangling (§1.9).
    with t.plock:
        money = t.money
    warnings += [f"money: dangling: {e}" for e in _money_problems(money, roster, routes)[0]
                 if ".route_budgets[" in e]
    return errors, warnings


def _money_problems(money, roster, routes):
    """(errors, warnings) of a money document against the accounts and the
    routes. Errors are structure: pay_order may name only money pots, a pot is
    an account's credits or allowance, a budget sits only on a pot whose
    charges the ledger counts (§1.7), and a route budget names a route whose
    pay lists that pot. A pot whose account is not in the pool is a
    "dangling" warning (the pool moves under the document), as is a topup
    "on" where the provider documents no top-up (Kimi's Extra Usage, 12 §5).
    At runtime the errors read as dangling too: a classes PUT that takes the
    pot out of a route's pay leaves that route budget binding nothing."""
    errors, warnings = [], []
    for pid in money["pay_order"]:
        acct, pot = _split_pot(pid)
        if not _is_money_pot_id(pid, roster):
            errors.append(f"pay_order {pid!r}: not a money pot")
        elif acct not in roster:
            warnings.append(f"dangling: pay_order {pid!r}: unknown pot (no account {acct!r})")
    for pid, e in money["pots"].items():
        acct, pot = _split_pot(pid)
        if (pot not in (_money_pot(roster[acct]), ALLOWANCE_POT) if acct in roster else
                pot not in (MONEY_POT, ALLOWANCE_POT, *_API_POTS.values())):
            api = ", ".join(f"<{prov} account>/{p}" for prov, p in sorted(_API_POTS.items()))
            errors.append(f"pots[{pid!r}]: unknown pot (a pot is <account>/{MONEY_POT}, "
                          f"<account>/{ALLOWANCE_POT}, or an api account's own: {api})")
        elif acct not in roster:
            warnings.append(f"dangling: pots[{pid!r}]: unknown pot (no account {acct!r})")
        else:
            if e["topup"] == "on" and roster[acct] == "kimi":
                warnings.append(f"pots[{pid!r}]: topup on, but Kimi documents no top-up for "
                                "Extra Usage — an empty pot may stay empty")
            if ("budget" in e or "route_budgets" in e) and \
                    (pot != _money_pot(roster[acct]) or roster[acct] not in _COUNTABLE_UNITS):
                errors.append(f"pots[{pid!r}]: a budget needs charges the ledger can count, and "
                              f"{roster[acct]}'s {pot} has none (§1.7); its provider's own "
                              "spend cap is the bound")
            elif ("budget" in e or "route_budgets" in e) and \
                    not _rates_complete(_COUNTABLE_UNITS[roster[acct]],
                                        _LEDGER_RATES.get(_COUNTABLE_UNITS[roster[acct]])):
                errors.append(f"pots[{pid!r}]: a budget needs the rates its charges are counted "
                              f"by, and the ledger has none for {_COUNTABLE_UNITS[roster[acct]]}")
        for rid in sorted(e.get("route_budgets") or {}):
            rt = routes.get(rid)
            if rt is None:
                errors.append(f"pots[{pid!r}].route_budgets[{rid!r}]: unknown route")
            elif pid not in rt["pay"]:
                errors.append(f"pots[{pid!r}].route_budgets[{rid!r}]: the route's pay does not "
                              f"list {pid}, so it never draws this pot")
    return errors, warnings


# ---- the class policy (#26): pointer + session-sticky + reset-boundary drift-up

def _member_backing(t, member, snap):
    """The single poll result that carries a member's numbers right now, or None.
    anthropic member -> the LANE'S INCUMBENT account (the nested account-switchover
    layer keeps deciding which account that is); provider member -> its provider
    POOL's incumbent (#71 — the same nesting, keyed by provider id), falling back
    to the coolest polled target before the pool has bootstrapped. Caller must
    hold _plock (reads lane + pool state)."""
    if member.get("kind") == "anthropic":
        ls = t.preferred["lanes"].get(member["lane"])
        cur = ls.get("account") if ls else None
        if not cur:
            return None
        return next((r for r in snap if r["name"] == cur), None)
    ps = t.preferred.get("providers", {}).get(member.get("provider"))
    return _pool_pick(_class_member_targets(member, snap),
                      ps.get("account") if ps else None)


def _member_incumbent(t, member, ctx, rid):
    """The account this member sits on now. It wins a tie among equal checks,
    so the verdict's account is the one the proxy will dial. A provider member
    takes its pool's incumbent. An anthropic member takes its lane's incumbent
    while that account is among the ones its OWN route's checks admit at their
    best (_usable_by_check); otherwise the coolest of those. A bare lane's pay
    and stop_at bind lane:<lane> alone (§1.9, §2.2 row 16), so they never
    steer a member: the lane's choice is kept only where the member's own
    checks agree with it. Where no passing account has a known effective, the
    ones at the best check still pass and the proxy dials the incumbent, so
    the member names one of them (the lane's while it is one, else the first
    by name): an unknown use is never 0, but it is no reason to name nobody.
    Caller holds _plock."""
    if member.get("kind") != "anthropic":
        return (t.preferred.get("providers", {}).get(member.get("provider")) or {}).get("account")
    cur = (t.preferred["lanes"].get(member["lane"]) or {}).get("account")
    lc = _lane_cfg(member["lane"])
    checks = ctx["checks"][rid]
    usable = _usable_by_check(checks,
                              lambda a: _account_effective(ctx["by_name"].get(a), lc, None))
    if not usable:
        usable = _usable_by_check(checks, lambda a: 0)
    if not usable or cur in usable:
        return cur
    return min(sorted(usable), key=usable.get)


def _verdict(route, checks, incumbent):
    """(account, check) of a route: its best check across its accounts, the
    incumbent first so it wins a tie (best_check keeps the first). (None, None)
    when no account backs the route."""
    order = ([incumbent] if incumbent in checks else []) + \
        [a for a in route["accounts"] if a != incumbent and a in checks]
    i = best_check([checks[a] for a in order])
    return (None, None) if i is None else (order[i], checks[order[i]])


def _account_effective(res, lane_cfg, model):
    """The §1.5 effective of one account for a member or a lane: the windows
    that bind the lane's scope on an Anthropic account, the ones that bind
    the member's model on a provider (`model` set). None for an Anthropic
    member whose lane is not configured."""
    if res is None:
        return None
    if model is not None:
        return _reading_effective(res, model)
    return _lane_effective(res, lane_cfg) if lane_cfg else None


def _policy_ctx(t, snap, now):
    """_settings_ctx under the tenant's live settings. Caller holds t.plock
    (reads t.classes, t.lane_settings, t.money)."""
    return _settings_ctx(t, snap, now, t.classes, t.lane_settings, t.money)


def _settings_ctx(t, snap, now, classes, lane_settings, money):
    """Everything the dial check reads this cycle, computed once: the roster,
    each account's standing, the route registry and every (route, account)
    check, under the given owner settings: the live ones, or a candidate
    document's (_session_preview). Caller holds _plock.

    The proxy's reports count here (quota-model 64 §5): `snap` is the poll
    results, and the context's own `snap` and `by_name` are those results
    with the reported readings merged in and the holds applied
    (_decision_view), so every check, lane, class decision and the document
    read them: a held window reads 100 everywhere, as the proxy's records
    make it in its own standing. _state,
    and everything rendered from it alone (the window gauges,
    account_health), stays the poll's."""
    roster, snap, by_name, standings = _decision_view(t, snap, now)
    routes = _routes(classes, lane_settings, roster)
    ledger = _ledger_payload(t, now, roster, money)
    checks = {rid: {a: dial_check(rt, a, standings[a], money["pots"], now, ledger=ledger)
                    for a in rt["accounts"]}
              for rid, rt in routes.items()}
    return {"now": now, "roster": roster, "snap": snap, "by_name": by_name,
            "standings": standings, "routes": routes, "checks": checks, "money": money,
            "ledger": ledger, "classes": classes, "limits": _limit_snap(t, standings, now)}


def _class_verdicts(t, cls, ctx):
    """{member id: {route, checks, account, check, effective}} for one class.
    `check` is the member's verdict (§1.6), `account` the one it came from, and
    `effective` that account's #24 number: the included tiers rank on it, and
    it is shown for the others. Caller holds _plock."""
    out = {}
    for m in cls["members"]:
        mid = _member_id(m)
        rid = f"{cls['class']}/{mid}"
        route, checks = ctx["routes"][rid], ctx["checks"][rid]
        inc = _member_incumbent(t, m, ctx, rid)
        acct, verdict = _verdict(route, checks, inc)
        prov = m.get("kind") == "provider"
        lc = None if prov else _lane_cfg(m["lane"])
        # `effective` is null when the reading behind it is stale by the
        # dial check's rule (_fresh_for: each binding window's own age), so a
        # frozen number never reads as live; the check's basis says stale,
        # and ranks it after fresh.
        res = ctx["by_name"].get(acct) if acct else None
        eff = (_account_effective(res, lc, m["model"] if prov else None)
               if _fresh_for(ctx["standings"].get(acct), route["scope"], ctx["now"]) else None)
        out[mid] = {"route": rid, "checks": checks, "account": acct, "check": verdict,
                    "effective": eff, "incumbent": inc}
    return out


def _check_text(c):
    """One check for a log line: included(fresh), paid:gpt/credits(fresh),
    no:spent(until 1800000000)."""
    if not c:
        return "none"
    if c["check"] == "no":
        return f"no:{c['reason']}(until {c['until']}, {c['basis']})"
    pot = f":{c['pot']}" if c.get("pot") else ""
    return f"{c['check']}{pot}({c['basis']})"


# ---- reserve ceilings (#33)
#
# A member's optional `reserve_ceiling` is a PREFERENCE TIER, never a filter.
# It can change WHICH member the policy chooses; it can never shrink the set of
# routable outcomes. Every rule below is therefore two-pass: prefer candidates
# still inside their reserve, and if that yields nothing, apply the pre-ceiling
# predicate verbatim. That gives a property worth stating outright — /class-bind
# 409s in EXACTLY the cases it would without ceilings (since C2 that set is
# "no member's dial check is included or paid", see _class_pointer), and
# forced-down finds a target in exactly the cases it would without them. A
# reserve that refused service would be strictly worse than spending the
# reserve. An owner limit that must STOP a route is a stop_at, which lives in
# the dial check (quota-model 50 §2.4).
#
# Why the feature exists (#33 item 4): gpt's only window is 7 days wide with no
# short brake, so "P2 with no ceiling" means draining an entire weekly window
# before kimi is ever touched, and the mistake takes up to 7 days to undo. The
# ceiling is what makes P2 meaningfully different from P3 in a bounded way.

def _member_ceiling(m):
    """A member's reserve ceiling, or None for no ceiling. A ceiling >= SW_HIGH
    is inert (the SW_HIGH gate binds first) and 0 means emergency-reserve-only
    (never a first-pass preference, still the last-resort fallback). Both are
    legal, useful configs rather than errors."""
    return m.get("reserve_ceiling")


def _under_ceiling(m, eff):
    """Is this member still inside its reserve? An unknown effective is not a
    ceiling question — callers gate None separately, and this must not turn a
    missing number into a ceiling violation."""
    c = _member_ceiling(m)
    return c is None or eff is None or eff < c


_BASES = ("fresh", "unknown", "stale")


def _class_pointer(cls, verdicts, pay_order):
    """The per-class pointer: what a NEW session binds to. Stateless —
    recomputed every cycle. Only NEW sessions consume it (priority is
    re-honored at every session boundary), and a session on money once when
    the owner changes the class's pay settings (repay), so pointer movement is
    not a flap.

    Only a member whose verdict is included or paid is eligible (quota-model
    50 §2.3); capacity is the dial check's, this ranks.

    The included tier, one basis at a time (fresh, then unknown, then stale:
    a stale reading ranks after a fresh one in the same tier, §1.6 step 4):
      1. highest-priority member with known effective < SW_HIGH AND inside its
         reserve ceiling;
      2. same predicate, ceilings relaxed (the reserve-ceiling contract above:
         a ceiling is a preference tier, never a filter);
      3. the coolest included member, priority breaking ties, an unknown
         effective last. SW_HIGH is a preference, "the % at which we consider
         leaving", not an admission filter: a member at 95-99 % still holds
         real allowance, and refusing to seat a session there is a router
         worse than naming the model directly (#88).
    Then the paid tier: the first paid member by pay_order_key (the drawn
    pot's rank in pay_order, then priority, then pot id, §1.9).

    None when no member is included or paid: the class is unavailable."""
    for basis in _BASES:
        tier = [m for m in cls["members"]
                if (verdicts[_member_id(m)]["check"] or {}).get("check") == "included"
                and verdicts[_member_id(m)]["check"]["basis"] == basis]
        if not tier:
            continue
        for reserve_holds in (True, False):
            for m in tier:
                v = verdicts[_member_id(m)]["effective"]
                if v is None or v >= SW_HIGH:
                    continue
                if reserve_holds and not _under_ceiling(m, v):
                    continue
                return _member_id(m)
        coolest = min(tier, key=lambda m: (verdicts[_member_id(m)]["effective"] is None,
                                           verdicts[_member_id(m)]["effective"] or 0.0,
                                           m["priority"]))
        return _member_id(coolest)
    paid = [(pay_order_key(verdicts[_member_id(m)]["check"]["pot"], m["priority"], pay_order),
             _member_id(m))
            for m in cls["members"]
            if (verdicts[_member_id(m)]["check"] or {}).get("check") == "paid"]
    return min(paid)[1] if paid else None


def _member_window_resets(t, member, snap):
    """{'<backing-name>:<window>': resets_at} for a member's current backing
    target. Keys carry the backing target's name so an anthropic member changing
    incumbent produces NEW keys (unknown-comparable -> re-baseline) rather than
    changed values (a fake reset boundary). Caller holds _plock."""
    res = _member_backing(t, member, snap)
    if not res:
        return {}
    return {f"{res['name']}:{w}": v.get("resets_at")
            for w, v in _reading_windows(res.get("reading")).items()}


def _class_marks(t, cls, member_id, snap):
    """Reset-boundary marks for every member STRICTLY higher priority than
    member_id (members are priority-sorted at load). A marked resets_at value
    changing later IS the quota-window reset the #26 drift-up fires on — the
    window's identity is its resets_at value (#24 contract). Caller holds _plock."""
    marks = {}
    for m in cls["members"]:
        mid = _member_id(m)
        if mid == member_id:
            break
        marks[mid] = _member_window_resets(t, m, snap)
    return marks


def _eval_drift_up(t, cls, sess, effs, snap):
    """The single sanctioned mid-session up-move (#26). Walks members higher
    priority than the session's binding in priority order; a changed resets_at
    value on the SAME key = a crossed window boundary. Every crossed boundary is
    CONSUMED (mark updated) whether or not it fires — that is what enforces "at
    most one drift-up per window, only AT the reset": a member that was still
    warm (> SW_HIGH - SW_MARGIN) at its boundary cannot fire later mid-window
    when it cools. Returns (member id to move up to or None, marks-changed bool —
    consumption must persist even when nothing fires). New keys (e.g. an
    anthropic member's incumbent changed) re-baseline without firing; None
    resets never fire (documented #24 degradation). Caller holds _plock."""
    fired = None
    changed = False
    marks = sess.setdefault("marks", {})
    for m in cls["members"]:
        mid = _member_id(m)
        if mid == sess["member"]:
            break
        cur = _member_window_resets(t, m, snap)
        prev = marks.get(mid) or {}
        crossed = any(k in prev and prev[k] is not None and cur[k] is not None
                      and cur[k] != prev[k] for k in cur)
        if cur != prev:
            marks[mid] = cur
            changed = True
        if crossed and fired is None:
            v = effs.get(mid)
            # A member over its reserve ceiling is not somewhere to drift UP to.
            # Consumption above is deliberately left unconditional, so a boundary
            # skipped for the ceiling is still spent — same one-per-window rule
            # that applies when a member is merely too warm.
            if v is not None and v <= SW_HIGH - SW_MARGIN and _under_ceiling(m, v):
                fired = mid
    return fired, changed


def _pick_forced(member_ids, ceilings, effs, ok):
    """First acceptable forced-down target under the two-pass reserve contract:
    prefer a member still inside its own ceiling, else fall back to `ok` alone —
    the pre-ceiling predicate, unchanged. `ok(mid, eff)` carries the rule
    (hard-vs-hot); members with unknown effective are never candidates."""
    for reserve_holds in (True, False):
        for mid in member_ids:
            v = effs.get(mid)
            if v is None or not ok(mid, v):
                continue
            c = ceilings.get(mid)
            if reserve_holds and c is not None and v >= c:
                continue
            return mid
    return None


def _class_pay_settings(cls, ctx):
    """A digest of the owner's settings that decide where one class spends
    money (quota-model 50 §2.3): the pots each member route may pay from, the
    order pay_order gives them, and their budget and route budgets. A session
    keeps the digest it was seated under, so a change re-seats a session on
    money once. A route that may pay from nothing adds nothing, so adding or
    removing a member that may not pay changes nothing. Member priority is not
    among them (a priority change reaches new sessions only, as everywhere),
    nor is topup: it describes the provider, and only moves a refusal's
    `until`. Caller holds _plock."""
    pots = ctx["money"]["pots"]
    routes = {}
    for m in cls["members"]:
        rid = f"{cls['class']}/{_member_id(m)}"
        pays = [[p, (pots.get(p) or {}).get("budget"),
                 ((pots.get(p) or {}).get("route_budgets") or {}).get(rid)]
                for p in sorted(ctx["routes"][rid]["pay"])]
        if pays:
            routes[rid] = pays
    payable = {p for pays in routes.values() for p, _, _ in pays}
    order = [p for p in ctx["money"]["pay_order"] if p in payable]
    return hashlib.sha256(json.dumps([routes, order], sort_keys=True).encode()).hexdigest()[:16]


def _set_session(t, cls, sid, sess, member_id, reason, now, snap, switch, check, record):
    """Move one session's binding and re-baseline its drift marks, and hand
    the move, with the destination's dial check, to `record`. Caller holds
    _plock. Returns True (dirty)."""
    record("move", {"class": cls["class"], "session": sid, "from": sess.get("member"),
                    "to": member_id, "reason": reason, "check": check})
    sess["member"] = member_id
    sess["reason"] = reason
    sess["since"] = now
    if switch:
        sess["last_switch"] = now
    sess["marks"] = _class_marks(t, cls, member_id, snap)
    return True


def _live_session_event(t, what, ev):
    """The live pass's record of a class-session event: its log line, whose
    check shows whether a move went to allowance or to money, and a move
    joins _session_journal. A hold is the preview's alone: the pass finds it
    again every cycle. Caller holds _plock."""
    if what == "move":
        log(f"class[{ev['class']}] session {ev['session'][:16]} -> {ev['to']} ({ev['reason']}) "
            f"check={_check_text(ev['check'])}")
        t.journal_seq += 1
        t.session_journal.append((t.journal_seq, ev))
    elif what == "expired":
        log(f"class[{ev['class']}] session {ev['session'][:16]} expired (idle > {SESSION_IDLE}s)")


def _check_of(verdicts, mid):
    return (verdicts.get(mid) or {}).get("check")


def _repays(check, seated, pay_now):
    """Does repay re-seat a session on a member with this check, seated under
    pay settings `seated`, now its class's are `pay_now` (quota-model 50
    §2.3)? Only one on money, and only on a change: a session seated before
    the brain kept pay settings has none, and takes today's."""
    return (check or {}).get("check") == "paid" and seated not in (None, pay_now)


def _compute_class_sessions(t, snap, now, ctx, classes, states, record):
    """#26 movement policy for bound class sessions, on the dial check
    (quota-model 50 §2.3). Caller must hold _plock and must have computed this
    cycle's LANE and pool decisions first (a verdict prefers the incumbent's
    account). With no classes or no sessions this is a no-op.

    Per session, in order:
      pin      a pin whose member's verdict is not `no` holds every session
               (a pin never grants pay: the verdict is the route's own);
      rebind   the bound member's verdict is `no` (or it is gone): to the
               pointer, with bootstrap semantics (no dwell, no switch stamp);
      repay    the session is on a paid member and the class's pay settings
               (_class_pay_settings) changed since it was seated: to the
               pointer, once, as a rebind moves (no dwell, no switch stamp,
               so allowance can take it back at once). A change is the
               owner's and is made once, so it cannot flap. It applies at the
               pass that sees it: a route that cannot serve then does not pull
               the session when it later can. A pin holds a session through a
               change, and the change applies when the pin ends;
      dwell    nothing below moves a session inside SW_DWELL;
      paid     a session on a paid member moves to the pointer once the
               pointer is included, and never elsewhere: money ends as soon as
               allowance returns, and within the paid tier only repay moves it;
      included forced-down (hard / high+margin / ceiling) and the
               reset-boundary drift-up, comparing effective between included
               members only. An included member whose effective is unknown is
               never forced down: no number says it is hot.
    Idle sessions are pruned first (SESSION_IDLE). `classes` and `states` are
    the class config and its runtime state: the live ones, or a candidate's
    and a copy (_session_preview). record(what, event) is told each move,
    each expiry and each session a pin holds from a repay: the live pass logs
    and journals them (_live_session_event), the preview collects them.
    Returns True if state changed (caller persists once per cycle)."""
    dirty = False
    for cls in classes:
        cname = cls["class"]
        cs = states.setdefault(cname, _new_class_state())
        member_ids = [_member_id(m) for m in cls["members"]]
        ceilings = {_member_id(m): _member_ceiling(m) for m in cls["members"]}
        for sid in [s for s, v in cs["sessions"].items()
                    if (now - v.get("last_seen", 0)) > SESSION_IDLE]:
            del cs["sessions"][sid]
            record("expired", {"class": cname, "session": sid})
            dirty = True
        if not cs["sessions"]:
            continue
        verdicts = _class_verdicts(t, cls, ctx)
        pointer = _class_pointer(cls, verdicts, ctx["money"]["pay_order"])
        pay_now = _class_pay_settings(cls, ctx)
        # The forced-down and drift-up rules see included members only: a
        # paid member's effective sits at or past 100 by construction, and
        # that number means nothing for money.
        effs = {mid: v["effective"] for mid, v in verdicts.items()
                if (v["check"] or {}).get("check") == "included"}
        pin = cs.get("pinned")
        pin_ok = pin in member_ids and (_check_of(verdicts, pin) or {}).get("check") not in (None, "no")
        for sid, sess in cs["sessions"].items():
            cur = sess.get("member")
            if pin_ok:
                if cur != pin:
                    dirty |= _set_session(t, cls, sid, sess, pin, f"pinned to {pin}",
                                          now, snap, switch=True, check=_check_of(verdicts, pin),
                                          record=record)
                elif (_repays(_check_of(verdicts, cur), sess.get("pay_settings"), pay_now)
                      and pointer and pointer != cur):
                    record("held", {"class": cname, "session": sid, "member": cur,
                                    "pin": pin, "to": pointer})
                continue
            # The pay settings this session was last seated under. A session
            # restored from before they were kept has none and takes today's:
            # no change is known to act on.
            seated = sess.get("pay_settings")
            if seated != pay_now:
                sess["pay_settings"] = pay_now
                dirty = True
            cur_check = _check_of(verdicts, cur) if cur in member_ids else None
            if cur_check is None or cur_check["check"] == "no":
                if pointer and pointer != cur:
                    why = "gone" if cur not in member_ids else _check_text(cur_check)
                    dirty |= _set_session(t, cls, sid, sess, pointer, f"rebind: {cur} {why}",
                                          now, snap, switch=False,
                                          check=_check_of(verdicts, pointer), record=record)
                continue
            if _repays(cur_check, seated, pay_now):
                if pointer and pointer != cur:
                    dirty |= _set_session(t, cls, sid, sess, pointer,
                                          f"repay: pay settings changed, leaving paid {cur}",
                                          now, snap, switch=False,
                                          check=_check_of(verdicts, pointer), record=record)
                continue
            if (now - sess.get("last_switch", 0)) < SW_DWELL:
                continue
            if cur_check["check"] == "paid":
                if pointer and pointer != cur and pointer in effs:
                    dirty |= _set_session(t, cls, sid, sess, pointer,
                                          f"allowance back: {pointer} included, leaving paid {cur}",
                                          now, snap, switch=True,
                                          check=_check_of(verdicts, pointer), record=record)
                continue
            eff_cur = effs.get(cur)
            target = reason = None
            if eff_cur is None:
                pass
            elif eff_cur >= SW_HARD:
                target = _pick_forced(member_ids, ceilings, effs,
                                      lambda mid, v: mid != cur and v < eff_cur)
                if target:
                    reason = (f"forced: {cur} {eff_cur:.0f}% >= hard {SW_HARD:.0f}%, "
                              f"{target} at {effs[target]:.0f}%")
            elif eff_cur >= SW_HIGH:
                target = _pick_forced(member_ids, ceilings, effs,
                                      lambda mid, v: mid != cur and (eff_cur - v) >= SW_MARGIN)
                if target:
                    reason = (f"forced: {cur} {eff_cur:.0f}% >= {SW_HIGH:.0f}%, "
                              f"{target} has {eff_cur - effs[target]:.0f}pts more headroom")
            elif ceilings.get(cur) is not None and eff_cur >= ceilings[cur]:
                # Ceiling eviction (#33). Without this branch the reserve is
                # theater: bindings are sticky and the two rules above only fire
                # at SW_HIGH, so a session that lands on a reserved member at 0%
                # would ride it straight through its ceiling to 80.
                #
                # Deliberately a DOWN-move only (member_ids is priority-sorted).
                # The rules above may select a higher-priority member — the
                # documented #26 deviation — but they only fire when the
                # incumbent is genuinely hot. This one trips far below SW_HIGH,
                # so allowing up-moves here would make routine exactly what #26
                # rations to one drift-up per window, at the window boundary.
                #
                # No SW_MARGIN test: a ceiling is an absolute policy line, not a
                # headroom comparison. Flap safety is free — effective is
                # monotone within a window, so this cannot un-fire until a reset,
                # and SW_DWELL still gates the rate.
                below = member_ids[member_ids.index(cur) + 1:]
                target = next((mid for mid in below
                               if effs.get(mid) is not None and effs[mid] < SW_HIGH
                               and (ceilings.get(mid) is None or effs[mid] < ceilings[mid])), None)
                if target:
                    reason = (f"reserve: {cur} {eff_cur:.0f}% >= ceiling {ceilings[cur]:.0f}%, "
                              f"{target} at {effs[target]:.0f}%")
                # No acceptable target => the session stays put. Never evict to
                # nowhere; holding a reserve is not worth dropping a session.
            if target:
                dirty |= _set_session(t, cls, sid, sess, target, reason, now, snap, switch=True,
                                      check=_check_of(verdicts, target), record=record)
                continue
            up, marks_changed = _eval_drift_up(t, cls, sess, effs, snap)
            dirty |= marks_changed
            if up:
                dirty |= _set_session(t, cls, sid, sess, up,
                                      f"drift-up: {up} window reset, at {effs[up]:.0f}%"
                                      f" <= {SW_HIGH - SW_MARGIN:.0f}%",
                                      now, snap, switch=True, check=_check_of(verdicts, up),
                                      record=record)
    return dirty


def _bind_class_session(t, cname, sid, now):
    """Create-or-touch one session binding (the /class-bind endpoint's core).
    An existing binding is returned untouched (session-sticky — only last_seen
    updates); a new session binds to the pin if its verdict is not `no`, else
    the pointer (included or paid). Returns (http_code, payload); the payload
    names the bound route and its check now. With neither, nothing is bound:
    409 class_unavailable with the §2.8 body, or 503 when no member's route
    has an account (configuration, §2.8 item 6). Takes the locks itself.

    The class lookup happens INSIDE _plock (§6 resurrection race): a hot-swap
    landing between an outside-the-lock lookup and the setdefault below would
    resurrect zombie state for a deleted class — invisible in the UI but
    persisted by _save_state()."""
    with t.lock:
        snap = list(t.state.values())
    with t.plock:
        # The tenant's own classes: another tenant's are unknown here.
        visible = t.classes
        cls = next((c for c in visible if c["class"] == cname), None)
        if not cls:
            return 404, {"error": f"unknown class {cname!r}",
                         "classes": [c["class"] for c in visible]}
        cs = t.preferred["classes"].setdefault(cname, _new_class_state())
        member_ids = [_member_id(m) for m in cls["members"]]
        ctx = _policy_ctx(t, snap, now)
        verdicts = _class_verdicts(t, cls, ctx)
        sess = cs["sessions"].get(sid)
        created = False
        if not sess or sess.get("member") not in member_ids:
            pin = cs.get("pinned")
            if pin in member_ids and (_check_of(verdicts, pin) or {}).get("check") not in (None, "no"):
                target, reason = pin, f"bind: pinned to {pin}"
            else:
                target, reason = (_class_pointer(cls, verdicts, ctx["money"]["pay_order"]),
                                  "bind: pointer")
            if not target:
                # §2.8: no route of the class is included or paid. Nothing is
                # bound; the reply says when a check could change and what
                # the owner could do sooner.
                routes = [ctx["routes"][verdicts[mid]["route"]] for mid in member_ids]
                body = unavailable_body(routes, ctx["checks"], ctx["standings"], now,
                                        ctx["money"]["pots"], ctx["ledger"])
                if body is None:
                    return 503, {"error": f"no member of class {cname!r} has an account",
                                 "class": cname,
                                 "routes": [rt["id"] for rt in routes]}
                log(f"class[{cname}] session {sid[:16]} unavailable: available_at="
                    f"{body['available_at']} ({body['available_basis']}) "
                    f"retryable={body['retryable']} "
                    f"options={len(body['options'])}")
                return 409, {"error": "class_unavailable", "class": cname, **body}
            sess = {"member": target, "since": now, "last_seen": now,
                    "last_switch": 0.0, "reason": reason,
                    "marks": _class_marks(t, cls, target, snap),
                    "pay_settings": _class_pay_settings(cls, ctx)}
            cs["sessions"][sid] = sess
            created = True
            log(f"class[{cname}] session {sid[:16]} bound -> {target} ({reason}) "
                f"check={_check_text(_check_of(verdicts, target))}")
            _save_state(t)
        sess["last_seen"] = now
        member = next(m for m in cls["members"] if _member_id(m) == sess["member"])
        entry = dict(member)
        entry["id"] = sess["member"]
        return 200, {"class": cname, "session": sid, "member": entry,
                     "route": verdicts[sess["member"]]["route"],
                     "check": _check_of(verdicts, sess["member"]),
                     "reason": sess["reason"], "since": sess["since"],
                     "created": created, "pinned": bool(cs.get("pinned") == sess["member"])}


def _classes_payload(t, snap, now, ctx):
    """The /preferred `classes` key: class config + per-member live view (#25)
    plus the #26 policy view — per class `pointer` (what a NEW session would
    bind to; null = nothing included or paid), `pinned`, and `sessions` (live
    bindings, marks and pay_settings omitted — internal bookkeeping). Per
    member adds `id` (the stable binding identity), `route` (its route id),
    `checks` ({account: dial check}), `check` (its verdict, null with no
    account), `scope`, and `effective` (the verdict account's #24 number — the
    included tiers rank on it; distinct from the per-target `targets` map). {}
    when no classes are configured. Takes _plock itself (callers of
    _preferred_payload build outside the lock)."""
    out = {}
    with t.plock:
        claims = _consumer_claims(t.consumers)
        for c in t.classes:
            verdicts = _class_verdicts(t, c, ctx)
            members = []
            for m in c["members"]:
                mid = _member_id(m)
                v = verdicts[mid]
                entry = _class_member_entry(m, snap, now, ctx["standings"],
                                            ctx["routes"][v["route"]]["scope"])
                entry["id"] = mid
                entry["route"] = v["route"]
                entry["scope"] = ctx["routes"][v["route"]]["scope"]
                entry["checks"] = v["checks"]
                entry["check"] = v["check"]
                # The account currently speaking for this member (#71): the
                # lane's incumbent for anthropic, the provider POOL's for
                # providers. The proxy pairs it with `targets` to steer its
                # local 429 detours to a pool sibling before crossing members.
                entry["incumbent"] = v["incumbent"]
                ev = v["effective"]
                entry["effective"] = ev
                # `reserved` = past its ceiling, so the policy will pass it over
                # while anything else is bindable. Deliberately NOT folded into
                # `usable`/`status`, which are freshness/data facts the proxy
                # reads to build its 429 detour ladder — a member held back as a
                # preference must still be reachable in an emergency.
                entry["reserved"] = ev is not None and not _under_ceiling(m, ev)
                members.append(entry)
            cs = t.preferred["classes"].get(c["class"]) or _new_class_state()
            out[c["class"]] = {
                "display": c["display"],
                "guidance": c.get("guidance"),
                # §2.9 (C4): "image" is the image door's class; the proxy
                # serves it there and nowhere else, so it is no picker row.
                "request_kind": c["request_kind"],
                "context_window": c["context_window"],
                # #44: which member's effective window governs, whose tokenizer
                # units the number is in, and when it last moved (§10).
                "floor_member": c.get("floor_member"),
                "units": c.get("units"),
                "previous_context_window": c.get("previous_context_window"),
                "window_changed_at": c.get("window_changed_at"),
                "pointer": _class_pointer(c, verdicts, ctx["money"]["pay_order"]),
                "pinned": cs.get("pinned"),
                # The derived consumer field (#95): which consumer's tank this
                # class drains into, per the consumers document; null when no
                # consumer claims it. Additive — the proxy decoder ignores it.
                "consumer": claims.get(("class", c["class"])),
                "sessions": {sid: {k: v for k, v in s.items()
                                   if k not in ("marks", "pay_settings")}
                             for sid, s in cs["sessions"].items()},
                "members": members,
            }
    return out


def _set_lane(ls, lane, account, reason, now, switch):
    """Update one lane's decision. Caller must hold _plock. Returns True (dirty)."""
    ls["account"] = account
    ls["reason"] = reason
    ls["since"] = now
    if switch:
        ls["last_switch"] = now
    log(f"preferred[{lane}] -> {account} ({reason})")
    return True


def _compute_lane(ls, lane, usable, now):
    """Incumbent-wins sticky-hysteresis choice for ONE lane. Caller must hold _plock;
    returns True if the lane's state changed (caller persists once per cycle).

    All accounts are equal — there is no "home"/"native". `usable` holds LANE-EFFECTIVE
    utilization (max of overall 7d, the 5-hour session, and the lane's scoped cap). The
    policy minimizes switches so a lane burns one account's quota down before moving on:

      - The account the lane is currently on WINS by default (incumbent-wins). We never
        move toward a merely-cooler account — only away from a forced-out one — so a lane
        makes at most N-1 switches as you exhaust N accounts, with no drift/flap.
      - We leave ONLY when the incumbent is forced out, and we leave to the COOLEST
        remaining account (the eviction target-selector):
          1) HARD: incumbent >= SW_HARD (basically maxed) and some account is lower.
          2) HOT + margin: incumbent >= SW_HIGH and the coolest account has >= SW_MARGIN
             points more headroom (a proactive buffer so we jump before a mid-request
             429, not after).
      - The dwell gate (anti-flap) applies before either rule.

    On bootstrap (no incumbent, or its data went stale) we seed the lane onto the
    coolest usable account — no account is preferred.
    """
    pin = ls.get("pinned")
    if pin:
        if pin in usable and ls["account"] != pin:
            return _set_lane(ls, lane, pin, f"pinned to {pin}", now, switch=True)
        return False
    cur = ls["account"]
    if cur is None or cur not in usable:
        pick = min(sorted(usable), key=usable.get)   # coolest, ties by name — no account is privileged
        return _set_lane(ls, lane, pick, "bootstrap", now, switch=False)
    cur_w = usable[cur]
    alts = {n: v for n, v in usable.items() if n != cur}
    if not alts:
        return False
    best = min(alts, key=alts.get)
    best_w = alts[best]
    dwell_ok = (now - ls["last_switch"]) >= SW_DWELL
    if not dwell_ok:
        return False
    # 1) incumbent is basically maxed for this lane -> evict to the coolest account now
    if cur_w >= SW_HARD and best_w < cur_w:
        return _set_lane(ls, lane, best,
                         f"{cur} {lane}-effective {cur_w:.0f}% >= hard {SW_HARD:.0f}%, "
                         f"{best} at {best_w:.0f}%", now, switch=True)
    # 2) incumbent is hot for this lane and the coolest account has clearly more headroom
    if cur_w >= SW_HIGH and (cur_w - best_w) >= SW_MARGIN:
        return _set_lane(ls, lane, best,
                         f"{cur} {lane}-effective {cur_w:.0f}% >= {SW_HIGH:.0f}%, {best} has "
                         f"{cur_w - best_w:.0f}pts more headroom", now, switch=True)
    # incumbent wins — stay put until forced out (no drift toward cooler accounts)
    return False


def _usable_by_check(checks, effective_of):
    """{account: effective} of the accounts a nested switchover may sit on: not
    `no`, a known effective (never route on a partial picture), and only the
    best (check, basis) present — never a paid account while an included one
    serves, never a stale reading while a fresh one does (quota-model 50 §1.6).
    This is what replaced "fresh only, else every account": a stale account is
    used at its last value only where no money could hide behind it."""
    cands = {}
    for a, c in checks.items():
        if c["check"] == "no":
            continue
        v = effective_of(a)
        if v is not None:
            cands[a] = (check_key(c), v)
    if not cands:
        return {}
    best = min(k for k, _ in cands.values())
    return {a: v for a, (k, v) in cands.items() if k == best}


def _pool_route(pid):
    """The route a provider pool's incumbent is chosen for: no pay and no stop,
    so the pool sits on an account whose allowance serves. A member that may
    pay still gets its paid verdict across every pool account (_verdict)."""
    return {"id": f"pool:{pid}", "pay": [], "stop_at": None, "scope": None}


def _compute_provider_pools(t, ctx, now):
    """Incumbent-wins account choice for each provider's target pool (#71).

    A provider with several polled accounts behaves exactly like a lane over
    Anthropic accounts: the incumbent wins until forced out (hard / hot+margin),
    eviction goes to the coolest, dwell gates flips — literally _compute_lane,
    on state keyed "provider:<id>", over the accounts _usable_by_check admits.
    Pool state is keyed by PROVIDER ID, not by class member: every member of
    one provider shares the same pool and the same target-global utilization,
    and the /openai-token no-param dispense (the direct picker path) resolves
    through it too. A pool of one bootstraps onto its sole account and then
    never moves — the pre-pool behavior by construction. A provider whose
    accounts serve the lanes (`lanes`) has no pool. Caller must hold _plock;
    returns True if any state changed."""
    dirty = False
    by_prov = {}
    for r in ctx["by_name"].values():
        if _target_kind(r) != "provider" or (PROVIDERS.get(r.get("provider")) or {}).get("lanes"):
            continue
        by_prov.setdefault(r.get("provider") or r["name"], []).append(r["name"])
    for pid in sorted(by_prov):
        route = _pool_route(pid)
        checks = {a: dial_check(route, a, ctx["standings"][a], ctx["money"]["pots"], now)
                  for a in by_prov[pid]}
        usable = (_usable_by_check(checks, lambda a: _provider_effective(ctx["by_name"][a]))
                  or _usable_by_check(checks, lambda a: 0))
        if not usable:
            continue
        ps = t.preferred.setdefault("providers", {}).setdefault(pid, _new_lane_state())
        if _compute_lane(ps, f"provider:{pid}", usable, now):
            dirty = True
    return dirty


def _held_accounts(t, snap):
    """(lane accounts, {provider id: pool accounts}) the tenant holds now: the
    ones configured (the account loop's names, a provider's poll targets) and
    the ones it is polling. A removed account is in neither: its removal drops
    its credentials and its reading, and a restart restores readings only for
    configured accounts. The account loop's cache keeps its names through an
    unreadable file, so a passing read error removes nothing here."""
    lanes = set(_account_loop_names(t))
    pools = {}
    for name, (pid, _) in _provider_poll_targets(t).items():
        pools.setdefault(pid, set()).add(name)
    for r in snap:
        if _target_kind(r) == "provider":
            pools.setdefault(r.get("provider") or r["name"], set()).add(r["name"])
        else:
            lanes.add(r["name"])
    return lanes, pools


def _drop_unheld(ls, lane, held, now):
    """Clear a lane's or pool's pin and account when its tenant no longer holds
    them. _compute_lane moves a lane off a removed account whenever another one
    serves; with none left, the lane (and a pin on it) would name the removed
    account for good, and the proxy would ask for a token the brain no longer
    has. Caller holds _plock; True when anything changed."""
    dirty = False
    if ls.get("pinned") and ls["pinned"] not in held:
        log(f"preferred[{lane}] pin {ls['pinned']!r} invalidated (account removed)")
        ls["pinned"] = None
        dirty = True
    if ls.get("account") and ls["account"] not in held:
        dirty = _set_lane(ls, lane, None, f"{ls['account']} is no longer connected", now,
                          switch=False) or dirty
    return dirty


def compute_preferred(t):
    """Run the sticky-hysteresis policy for every lane, then refresh the legacy
    mirror (flat top-level keys := the default lane) and persist if anything moved.

    Each lane chooses among the accounts its bare-lane route's dial check
    admits (_usable_by_check): an account whose reading is stale is used at its
    last value only when no fresh one serves and no money it may not pay could
    hide behind it; otherwise its check is `no (unknown)` and the lane keeps
    its incumbent until a reading returns. A lane or pool where no passing
    account has a known effective still names one (the incumbent while it
    passes, else the first by name), as _member_incumbent does for a member:
    the proxy dials a passing account there, and an unknown use is never 0,
    but it is no reason to name nobody.
    """
    with t.lock:
        snap = list(t.state.values())
    now = time.time()
    dirty = False
    lane_held, pool_held = _held_accounts(t, snap)
    with t.plock:
        _prune_reports(t, snap, now)
        ctx = _policy_ctx(t, snap, now)
        for ln, ls in t.preferred["lanes"].items():
            if _drop_unheld(ls, ln, lane_held, now):
                dirty = True
        for pid, ps in (t.preferred.get("providers") or {}).items():
            if _drop_unheld(ps, f"provider:{pid}", pool_held.get(pid, set()), now):
                dirty = True
        for lc in _lanes:
            # The contract filter: providers serve no Anthropic lane today, so
            # they never compete in the account switchover (see _serves) — the
            # lane route's accounts are Anthropic accounts only.
            checks = {a: c for a, c in ctx["checks"][f"lane:{lc['lane']}"].items()
                      if a in ctx["by_name"] and _serves(ctx["by_name"][a], lc)}
            usable = (_usable_by_check(checks,
                                       lambda a, lc=lc: _lane_effective(ctx["by_name"][a], lc))
                      or _usable_by_check(checks, lambda a: 0))
            if not usable:
                continue
            ls = t.preferred["lanes"].setdefault(lc["lane"], _new_lane_state())
            if _compute_lane(ls, lc["lane"], usable, now):
                dirty = True
        dl = t.preferred["lanes"].get("default")
        if dl:
            for k in ("account", "since", "reason", "last_switch", "pinned"):
                if t.preferred[k] != dl[k]:
                    t.preferred[k] = dl[k]
                    dirty = True
        # Provider pools (#71) run after the lane loop and BEFORE the class
        # policy, so provider members resolve to this cycle's pool incumbents —
        # the same ordering contract the lanes give anthropic members.
        if _compute_provider_pools(t, ctx, now):
            dirty = True
        # Class-session policy (#26) runs AFTER the lane loop: a member's
        # verdict prefers this cycle's lane and pool incumbents. It reads the
        # reports through ctx; `snap` (the polls) only marks reset boundaries
        # for drift-up, since a response's reset differs from the poll's by
        # seconds and would read as a crossed boundary twice.
        if _compute_class_sessions(t, snap, now, ctx, t.classes, t.preferred["classes"],
                                   lambda what, ev: _live_session_event(t, what, ev)):
            dirty = True
        if dirty:
            _save_state(t)
    # The prepaid notices (notices.py) read this pass's decisions; the tick
    # only decides, and the worker delivers after the mark below.
    try:
        t.notices.tick(_notices_views(t, ctx, now), now)
    except Exception as e:  # noqa: BLE001 — a notice never stops the policy
        log(f"WARN notices: tick failed: {type(e).__name__}: {e}")
        traceback.print_exc()
    # Every pass of the poll loop ends here, as do the handlers that recompute:
    # a moment the document may have changed (quota-model 64 §2).
    _events_mark(t)
    t.notices.kick()


# ---- the prepaid notices (notices.py) -------------------------------------------

# A tenant's notices are its own (t.notices, _tenant_fresh): the bridge
# delivers the LAN tenant's to the owner; every tenant's are in its /preferred.


def _prepaid_accounts(ctx):
    """{account: pot id} of every prepaid account: an api account whose
    money pot has topup off in the money document, unless its reading says
    the pot is not there (entitlement none: a pot no dial draws). An account
    not measured yet (no reading) is not listed: nothing is known of it."""
    out = {}
    for acct, pv in sorted(ctx["roster"].items()):
        if _account_kind(pv) != "api":
            continue
        pot = _money_pot(pv)
        pid = f"{acct}/{pot}"
        if ((ctx["money"]["pots"].get(pid) or {}).get("topup", "off")) == "on":
            continue
        st = ctx["standings"].get(acct) or {}
        if st.get("as_of") is None:
            continue
        mp = (st.get("money") or {}).get(pot) or {}
        if (mp.get("entitlement") or {}).get("state") == "none":
            continue
        out[acct] = pid
    return out


def _notices_coverage(acct, ctx):
    """[{class, covered_by}] for every class a member of which the account
    serves (the member's route lists it): covered_by is, in member order, the
    first member with an account other than this one whose check is included
    or paid now, else None. On a member of the account's own, that is
    another account of it (a second key of one provider), named by its
    account; on any other member, the member by its id."""
    out = []
    for c in ctx["classes"]:
        rids = [(m, f"{c['class']}/{_member_id(m)}") for m in c["members"]]
        mine = {rid for _, rid in rids if acct in ((ctx["routes"].get(rid) or {}).get("accounts") or [])}
        if not mine:
            continue
        by = None
        for m, rid in rids:
            checks = ctx["checks"].get(rid) or {}
            order = (ctx["routes"].get(rid) or {}).get("accounts") or []
            other = next((a for a in order + sorted(set(checks) - set(order))
                          if a != acct and (checks.get(a) or {}).get("check") in ("included", "paid")), None)
            if other is not None:
                by = other if rid in mine else _member_id(m)
                break
        out.append({"class": c["class"], "covered_by": by})
    return out


def _notices_views(t, ctx, now):
    """{account: view} of every prepaid account for notices.tick and
    payload: its driver's name, serves and billing, whether its pot's own
    check reads no (balance, until_paid) (dry), its polled balance and when
    it was read, the pot's unit and reset, and its classes' coverage; and
    what ends a dry: the pot's newest counted ledger entry (charged_at) and
    the owner's last re-check (recheck_at). What starts one besides the
    check: the proxy's last accepted report of the pot shut (shut_at); and
    dry_at, the held report's time when the check reads no by it, so a
    resent report older than the last dry's end starts nothing."""
    views = {}
    with t.ledger_lock:
        counted = dict(t.ledger.get("counted_at") or {})
    with t.poll_health_lock:
        rechecks = dict(t.recheck_epochs)
    with t.reports_lock:
        shuts = dict(t.shut_reports)
        held = {a: ((rep.get("money") or {}).get("open") or {}).get("at")
                for a, rep in t.reported.items()}
    for acct, pid in _prepaid_accounts(ctx).items():
        pv = ctx["roster"][acct]
        row = PROVIDERS.get(pv) or {}
        st = ctx["standings"][acct]
        pot = _split_pot(pid)[1]
        mp = (st.get("money") or {}).get(pot) or {}
        chk = _api_check({"id": None, "pay": [pid], "scope": None}, acct, st, ctx["money"]["pots"],
                         now, STALE_MAX, ctx["ledger"], None)
        bal = (mp.get("limits") or {}).get("balance") or {}
        views[acct] = {
            "name": row.get("name"), "serves": row.get("serves"), "billing_url": row.get("billing"),
            "dry": chk["check"] == "no" and chk["reason"] == "balance" and chk["until"] == "until_paid",
            "balance": mp.get("balance") if st.get("as_of") is not None else None,
            "read_at": st.get("as_of"), "unit": mp.get("unit"), "resets_at": bal.get("resets_at"),
            "classes": _notices_coverage(acct, ctx),
            "charged_at": counted.get(pid), "recheck_at": rechecks.get(acct),
            "shut_at": shuts.get(acct),
            # The check's evidence when a held shut report makes it read no.
            "dry_at": held.get(acct)}
    return views


def _routes_payload(ctx, incumbents):
    """The /preferred `routes` key and the route-level warnings: per route its
    settings, every account's check and its verdict. Settings that no longer
    bind (a pay pot whose account left; a stop_at window a reading no longer
    carries) are shown as warnings and bind nothing (§1.9)."""
    out, all_warnings = {}, []
    for rid, rt in ctx["routes"].items():
        checks = ctx["checks"][rid]
        inc = incumbents.get(rt["lane"]) if rt["provider"] == "anthropic" \
            else incumbents.get(f"provider:{rt['provider']}")
        _, verdict = _verdict(rt, checks, inc)
        warnings = [msg if sev == "warning" else f"{sev}: {msg}"
                    for sev, msg in _route_setting_issues(rt, ctx["roster"], ctx["standings"])]
        stop = rt["stop_at"]
        if stop and stop["window"] != "effective" and stop["level"] < 100:
            for a in rt["accounts"]:
                st = ctx["standings"].get(a) or {}
                if st.get("as_of") is not None and \
                        _stop_window(stop, _binding_windows(st, rt["scope"])) is None:
                    warnings.append(f"stop_at: window not reported on {a} — no effect there")
        entry = {k: rt[k] for k in ("kind", "class", "member", "lane", "provider", "scope",
                                    "accounts", "pay", "stop_at", "priority", "request_kind")}
        entry.update(checks=checks, check=verdict, warnings=warnings)
        out[rid] = entry
        all_warnings += [f"{rid}: {w}" for w in warnings]
    return out, all_warnings


def _pots_payload(ctx):
    """The /preferred `pots` key: every money pot of every account with its
    declared topup, its budgets (null / {} = none), whether the ledger counts
    its charges (a budget is legal only then), and its rank in pay_order
    (null = unlisted, ranked after every listed pot by member priority), plus
    the money document's problems
    against today's accounts, which read as dangling."""
    money = ctx["money"]
    errors, warnings = _money_problems(money, ctx["roster"], ctx["routes"])
    out = {}
    for pid in _money_pots(ctx["roster"]):
        acct, pot = _split_pot(pid)
        st = money["pots"].get(pid) or {}
        out[pid] = {"account": acct, "pot": pot,
                    "topup": st.get("topup", "off"),
                    "topup_basis": "declared",
                    "budget": st.get("budget"), "route_budgets": st.get("route_budgets") or {},
                    "countable": ctx["roster"].get(acct) in _COUNTABLE_UNITS,
                    "pay_rank": money["pay_order"].index(pid) if pid in money["pay_order"] else None,
                    "warnings": [f"dangling: {e}" for e in errors if e.startswith(f"pots[{pid!r}]")]
                    + [w for w in warnings if w.startswith(f"pots[{pid!r}]")]}
    return out, [f"money: dangling: {e}" for e in errors] + [f"money: {w}" for w in warnings]


def _preferred_payload(t):
    """The /preferred response. CONTRACT: the flat legacy keys (account/reason/weekly/
    since/last_switch/pinned/thresholds) keep their exact v1 types — the proxy decodes
    {account: string, reason: string, weekly: map[string]float64} and fails the WHOLE
    decode on any shape change. All lane data is additive, under NEW keys the decoder
    ignores:
      lane_rules: ordered [{lane, match, scope}] for the proxy's first-match model
                  walk; scope is the lane's scope_model (None for none), so the
                  proxy binds window "scoped:<scope>" to that lane exactly (C5)
      lanes:      {lane: {account, reason, pinned, since, last_switch,
                          effective: {account: pct}}}   (drives per-lane alert tiering)
      targets:    {id: {kind, good_ts, age_s, stale, serves, effective,
                        effective_global?, windows}} — the FULL target contract
                  (#24) for every polled target incl. providers; this is what the
                  Phase 2 class routing (#26/#27) consumes. Additive; the legacy
                  keys above still describe Anthropic accounts only.
      classes:    {name: {display, context_window, pointer, pinned, sessions,
                          members: [{priority, kind, lane|provider+model,
                          context_window, pay, stop_at, route, scope, checks,
                          check, targets, usable, status, effective,
                          incumbent, reserved}]}} — the class config (#25) +
                  each member's live view and verdict; {} when no classes.
    The dial check's inputs and answers (quota-model 50 §2.7), so the proxy
    evaluates the same function on the same facts:
      accounts:   {account: standing} — _account_standing, every known account:
                  its §1.5 reading keyed for lookup (windows with their aliases,
                  money pots, plan, beside, models)
      routes:     {route id: {kind, class, member, lane, provider, scope,
                  accounts, pay, stop_at, priority, request_kind, checks,
                  check, warnings}} — every class member and every bare lane
      pots:       {pot id: {account, pot, topup, topup_basis, pay_rank,
                  warnings}} — every money pot of every account
      pay_order:  the money document's ranking of pots
      ledger:     {period {start, end}, as_of, seq, unreadable_lines, rates,
                  rate_tables {unit: [the tables its charges need]},
                  pots {pot id: {unit, period_spend, routes, uncounted, budget,
                  route_budgets, reconciliation}}} — _ledger_payload, the
                  budget gate's input (§1.7)
      settings:   {classes: revision, money: revision} — which owner settings
                  these answers were computed from
      settings_warnings: settings accepted with a warning, and dangling ones
      fleet:      {unit, proxy, selections, accounts} — the room of every
                  selection, the whole proxy and each account (quota-model 67
                  §8, _fleet_figure)
      prices:     {models, long_context} — the API list prices a `served`
                  event is priced by (_prices_payload)
      notices:    {enabled, why, accounts {account: {state, since, name,
                  serves, billing_url, pace, classes, sent}}} — the prepaid
                  "needs payment" notices (notices.py), every prepaid account
    The proxy joins a scoped window to a lane only through lane_rules[].scope, and
    a header token to a window only through the window's aliases: by meaning,
    never by a name substring."""
    with t.lock:
        snap = list(t.state.values())
    view = _sizing_fleet_view(t)  # under _sizing_lock, never inside _plock (67 §8)
    now = time.time()
    # account_health takes _lock, so it is read before _plock too; the fleet's
    # asks read it (68 §3.2).
    account_health = _unified_account_health(t, now)
    with t.plock:
        # Every number below reads the polls with the reports merged in
        # (ctx["snap"], quota-model 64 §5); account_health reads _state alone.
        ctx = _policy_ctx(t, snap, now)
        fleet = _fleet_payload(ctx, view, _fleet_health(account_health), _fleet_preferred(t, ctx))
        snap = ctx["snap"]
        accounts = [r for r in snap if _target_kind(r) == "anthropic_account"]
        weekly = {r["name"]: _weekly(r) for r in accounts}
        effective = {lc["lane"]: {r["name"]: _lane_effective(r, lc) for r in accounts}
                     for lc in _lanes}
        p = {k: t.preferred[k] for k in ("account", "since", "reason", "last_switch", "pinned")}
        claims = _consumer_claims(t.consumers)
        consumers_snap = t.consumers
        classes_snap = t.classes
        lanes_out = {}
        for lc in _lanes:
            ls = t.preferred["lanes"].get(lc["lane"])
            if not ls:
                continue
            lanes_out[lc["lane"]] = dict(ls, effective=effective.get(lc["lane"]) or {},
                                         consumer=claims.get(("lane", lc["lane"])))
        prov_states = {pid: dict(ps) for pid, ps in t.preferred.get("providers", {}).items()}
        incumbents = {lc["lane"]: (t.preferred["lanes"].get(lc["lane"]) or {}).get("account")
                      for lc in _lanes}
        incumbents.update({f"provider:{pid}": ps.get("account") for pid, ps in prov_states.items()})
        # stale_max: the age past which a limit's reading is stale; the proxy
        # judges its own dial checks by the same number.
        # Each settings document's revision (the GET body's `revision`), so a subscriber re-reads
        # a document only when its own revision moves (quota-model plan 64 §4).
        settings = {"classes": _doc_revision(t.classes_doc),
                    "consumers": _doc_revision(t.consumers_doc),
                    "money": _doc_revision(t.money_doc), "stale_max": STALE_MAX}
        money_warnings = list(t.money_warnings)
    p["weekly"] = weekly
    p["thresholds"] = {"high": SW_HIGH, "margin": SW_MARGIN, "hard": SW_HARD, "dwell_s": SW_DWELL}
    p["lane_rules"] = [{"lane": lc["lane"], "match": lc["match"], "scope": lc["scope_model"]}
                       for lc in _lanes]
    p["lanes"] = lanes_out
    p["classes"] = _classes_payload(t, snap, now, ctx)
    p["accounts"] = ctx["standings"]
    p["routes"], route_warnings = _routes_payload(ctx, incumbents)
    p["pay_order"] = list(ctx["money"]["pay_order"])
    p["pots"], money_issues = _pots_payload(ctx)
    p["ledger"] = ctx["ledger"]
    p["settings"] = settings
    p["settings_warnings"] = route_warnings + money_issues + money_warnings
    # Consumers (#95, additive): the runtime view — per-member dangling status
    # is computed against the LIVE classes/lanes/providers on every call, so a
    # class hot-swap is reflected immediately. Tanks + editor read one truth.
    p["consumers"] = _consumers_view(consumers_snap, classes_snap)
    p["account_health"] = account_health
    # Provider pool state (#71, additive): per provider the lane-shaped
    # incumbent state plus each pool account's target-global effective.
    prov_pool = {}
    for r in snap:
        if _target_kind(r) == "provider":
            prov_pool.setdefault(r.get("provider") or r["name"], {})[r["name"]] = \
                _provider_effective(r)
    p["providers"] = {pid: dict(ps, pool=prov_pool.get(pid, {}))
                      for pid, ps in prov_states.items()}
    p["fleet"] = fleet
    _burn_stamp(t, fleet, now)  # a render of the figure (69 §4.2), outside _plock
    p["prices"] = _prices_payload()
    # The prepaid notices (notices.py): every prepaid account's state, what it
    # serves, where it is paid, and each notice sent.
    p["notices"] = t.notices.payload(_notices_views(t, ctx, now))
    return p


# ---- POST /reports: what the proxy saw (quota-model 64 §5) -----------------------
# The proxy reports what its responses showed between the brain's polls: one
# window's reading, a refusal that holds a window at 100 (and the 2xx that
# ends one), and its ledger entries. Readings and holds are kept beside the
# polls, never written into _state: _settings_ctx merges the readings into
# its copy of the poll results (_reported_snap) and closes each held window
# there (_hold_snap), before anything is derived, so every check, lane and class decision
# and the document read them, while _state, account_health and the window
# gauges stay the poll's. Nothing here is persisted: on every stream connect
# the proxy sends its live records and latest readings again.

REPORTS_POST_MAX = 500      # events per POST
REPORT_AHEAD_S = 60         # a read_at further ahead of the brain's clock is refused
REPORT_RESET_SLACK_S = 600  # the proxy's recResetSlack: one period's reset, read to two precisions
_REPORT_KINDS = ("reading", "refusal", "ledger", "served")
_REPORT_WINDOW_KEYS = frozenset(("id", "kind", "account", "read_at", "window", "used_pct",
                                 "resets_at"))
_REPORT_CREDITS_KEYS = frozenset(("id", "kind", "account", "read_at", "pot", "open", "balance"))
_REPORT_REFUSAL_KEYS = frozenset(("id", "kind", "account", "read_at", "window", "limit", "class",
                                  "until", "resets_at", "by_reading", "seen", "ended"))
# A refusal's class (refusal.go): a window's is spent; a no-window limit's is
# spent or not_entitled (67 §9). Required on every refusal since 67 push 3c.
_REPORT_REFUSAL_CLASSES = ("spent", "not_entitled")
# A no-window limit as a refusal names it: model:<model id as the request
# named it>, or pool:<name> (67 §9).
_REPORT_LIMIT_RE = re.compile(r"(model|pool):(.+)")  # fullmatch

# The reports stores, a tenant's each (t.reported, t.shut_reports, t.holds,
# t.limit_holds), guarded by t.reports_lock: taken inside plock and lock,
# never around them.
# {account: {"windows": {window id: {field: {"v", "at"}}}, "money": {field: {"v", "at"}}}}:
# per window (and the account's money pot: a subscription's credits, an api
# account's own pot) and per field, the newest reported value and the read_at
# it was read at. A field leaves once a poll that started after it lands; on a
# pot no poll reads (_pot_unread), once a counted ledger entry on it or the
# owner's re-check is newer (_prune_reports).
#   reported
# {account: read_at}: the newest accepted report of an api account's money pot
# shut, which starts a prepaid account's dry (notices.py) and outlives the
# report itself: on a pot the poll reads, the next poll replaces the report,
# and only money showing ends the dry.
#   shut_reports
# {(account, window id): {refused_at, until, resets, by_reading, seen {used_pct, resets_at}}}:
# the windows a refusal holds at 100. until is the record's end (learned from a
# reading when it came null), resets the reset the refusal itself named (null
# for none): the proxy record's until and resets.
#   holds
# {(account, limit): {refused_at, until, class}}: the refusals of limits the
# brain has no window for (67 §9), a model:<m> or a pool:<name>. They feed the
# fleet figure only, never the standings or the dial check (§13 Q6), and are
# no head for sizing. Each ends at its until, at the owner's re-check, or by
# an `ended` newer than it (_limit_ended, _prune_reports).
#   limit_holds


def _report_window(wid):
    """A window a reading may add where the poll carries none, by its id, in
    the reading's shape: five_hour, seven_day and scoped:<scope>, with their
    period and scope (the proxy's windowFor). None for any other id."""
    if wid == "five_hour":
        return _window(wid, 18000, None, None, None, refill="at_once", anchor="first_use")
    if wid == "seven_day":
        return _window(wid, 604800, None, None, None, refill="at_once", anchor="fixed")
    if wid.startswith("scoped:") and len(wid) > len("scoped:"):
        sc = wid[len("scoped:"):]
        return _window(wid, 604800, None, None, None, scope=sc, aliases=SCOPE_ALIASES.get(sc),
                       refill="at_once", anchor="fixed")
    return None


def _report_window_problem(acct, provider, rd, wid):
    """Why a reported window is refused on this account, or None: a window
    the account's reading carries, or one a reading may add."""
    if not (isinstance(wid, str) and wid):
        return "window must be the brain's window id"
    if _account_kind(provider) == "api":
        return f"{acct} is an api account: it has no allowance window"
    if wid in _reading_windows(rd) or (_report_window(wid) is not None and
                                       (provider == "anthropic" or not wid.startswith("scoped:"))):
        return None
    return f"unknown window {wid!r} on {acct}"


def _report_time(v):
    """Epoch seconds, or None."""
    t = _num(v)
    return t if t is not None and t > 0 else None


def _reports_keep(store, path, field, value, at, floor):
    """Keep value as store[path...][field] when at is newer than floor (the
    poll's start) and than the value held. True when kept. Caller holds
    _reports_lock."""
    node = store
    for p in path:
        node = node.setdefault(p, {})
    held = node.get(field)
    if at <= floor or (held is not None and at <= held["at"]):
        return False
    node[field] = {"v": value, "at": at}
    return True


def _reports_tidy(t, acct):
    """Drop what is left empty under acct. Caller holds _reports_lock."""
    rep = t.reported.get(acct)
    if rep is None:
        return
    wins = rep.get("windows") or {}
    for wid in [w for w, f in wins.items() if not f]:
        del wins[wid]
    if not wins:
        rep.pop("windows", None)
    if not rep.get("money"):
        rep.pop("money", None)
    if not rep:
        del t.reported[acct]


def _report_event(t, ev, roster, by_name, now, reporter=None):
    """Apply one reading or refusal event -> (result, why); ("ledger", None)
    for a ledger entry, which reports_post appends with the batch's others."""
    if not isinstance(ev, dict):
        return "refused", "an event is an object"
    kind = ev.get("kind")
    if kind not in _REPORT_KINDS:
        return "refused", f"unknown kind {kind!r}"
    if kind == "ledger":
        # The entry's own fields; its `at` is its time, so no read_at rule.
        # ledger_accept validates it, each entry on its own.
        return "ledger", None
    if not (isinstance(ev.get("id"), str) and 0 < len(ev["id"]) <= 200):
        return "refused", "id must be a string of 1-200 chars"
    acct = ev.get("account")
    if not isinstance(acct, str) or acct not in roster:
        return "refused", f"unknown account {acct!r}"
    at = _report_time(ev.get("read_at"))
    if at is None:
        return "refused", "read_at must be epoch seconds"
    if at > now + REPORT_AHEAD_S:
        return "refused", (f"read_at is {at - now:.0f}s ahead of the brain's clock "
                           f"(more than {REPORT_AHEAD_S}s)")
    if kind == "served":
        return _report_served(t, ev, acct, roster[acct], reporter)
    res = by_name.get(acct) or {}
    rd, good = res.get("reading"), res.get("good_ts")
    measured = rd is not None and good is not None
    if kind == "reading" and "pot" in ev:
        return _report_credits(t, ev, acct, roster[acct], rd, measured, good, at)
    keys = _REPORT_WINDOW_KEYS if kind == "reading" else _REPORT_REFUSAL_KEYS
    bad = sorted(set(ev) - keys)
    if bad:
        return "refused", f"unknown key(s) {bad}"
    if kind == "refusal":
        why = _report_refusal_problem(ev)
        if why:
            return "refused", why
        if ev.get("limit") is not None:
            return _report_limit(t, ev, acct, roster[acct], measured, at, now)
    wid = ev.get("window")
    why = _report_window_problem(acct, roster[acct], rd, wid)
    if why:
        return "refused", why
    if kind == "reading":
        return _report_reading(t, ev, acct, wid, rd, measured, good, at)
    # A refusal is a head (66 §2): once the brain is shutting down it is
    # answered retry before anything of it is kept, as a served event is.
    if not _sizing_enter(t):
        return "retry", "the brain is shutting down"
    try:
        out = _report_refusal(t, ev, acct, wid, measured, at, now)
        if out[0] == "applied" and not ev.get("ended"):
            _sizing_refusal(t, acct, roster[acct], wid, at, ev.get("seen"))
        return out
    finally:
        _sizing_leave(t)


def _use_agrees(held_used, held_reset, used, reset):
    """The proxy's useAgrees: a reported use at most a point below the use
    the brain holds for the window, in the same period (resets within
    REPORT_RESET_SLACK_S, or either unknown), is the held reading again. The
    poll and a response's headers state one counter rounded half a point
    apart (quota-model 10, "Rounding"), so for the upper half of every point a
    response reads a point below the poll; use does not fall within a period."""
    if held_used is None or used is None:
        return False
    if held_reset is not None and reset is not None and abs(held_reset - reset) > REPORT_RESET_SLACK_S:
        return False
    return 0 < held_used - used <= 1


def _report_reading(t, ev, acct, wid, rd, measured, good, at):
    used, resets = ev.get("used_pct"), ev.get("resets_at")
    if used is not None and (_num(used) is None or used < 0):
        return "refused", "used_pct must be a percent >= 0, or null"
    if resets is not None and _report_time(resets) is None:
        return "refused", "resets_at must be epoch seconds, or null"
    if not measured:
        return "ignored", "an account the brain has never measured"
    took = False
    with t.reports_lock:
        held = ((t.reported.get(acct) or {}).get("windows") or {}).get(wid) or {}
        if used is None and wid not in _reading_windows(rd) and "used_pct" not in held:
            return "ignored", "a reading that carries no use never adds a window"
        polled = _reading_windows(rd).get(wid) or {}

        def holding(field):
            f = held.get(field)
            return f["v"] if f is not None and f["at"] > good else polled.get(field)

        if used is not None and _use_agrees(holding("used_pct"), holding("resets_at"), float(used),
                                            None if resets is None else float(resets)):
            # The held use stands; the reading moves only its age.
            used = holding("used_pct")
        for field, v in (("used_pct", used), ("resets_at", resets)):
            if v is not None:
                took |= _reports_keep(t.reported, (acct, "windows", wid), field, float(v), at, good)
        _reports_tidy(t, acct)
    return ("applied", None) if took else ("ignored", "no newer than what the brain holds")


def _pot_unread(rd, pot):
    """Does the poll read nothing of the money pot: its open and its balance
    both unknown (a provider that publishes no balance)? Never measured
    counts as read: nothing is kept for it."""
    if not rd:
        return False
    p = _reading_pot(rd, pot)
    if p is None:
        return False
    return p.get("open") is None and (_reading_limit(p, "balance") or {}).get("amount") is None


def _report_money_floor(t, acct, provider, good, unread):
    """What a report on the account's money pot must be newer than: the poll's
    start, or, on a pot no poll reads, the newest counted ledger entry on it
    and the owner's last re-check, the two things that end a shut there."""
    if not unread:
        return good
    pid = f"{acct}/{_money_pot(provider)}"
    with t.ledger_lock:
        counted = (t.ledger.get("counted_at") or {}).get(pid) or 0
    with t.poll_health_lock:
        epoch = t.recheck_epochs.get(acct) or 0
    return max(counted, epoch)


def _report_credits(t, ev, acct, provider, rd, measured, good, at):
    """A reading of the account's money pot: a subscription's credits (a
    codex response's credits header), or an api account's own pot shut (the
    proxy's door refused for money, e.g. a 402). Any other pot is
    refused."""
    bad = sorted(set(ev) - _REPORT_CREDITS_KEYS)
    if bad:
        return "refused", f"unknown key(s) {bad}"
    pot = _money_pot(provider)
    if ev.get("pot") != pot:
        return "refused", (f"unknown pot {ev.get('pot')!r} on {acct}: a reading carries only "
                           f"its money pot, {pot!r}")
    open_, bal = ev.get("open"), ev.get("balance")
    if open_ is not None and open_ is not False:
        return "refused", "open is false or null: a response never says a pot is open"
    if bal is not None and _num(bal) is None:
        return "refused", "balance must be a number, or null"
    if not measured:
        return "ignored", "an account the brain has never measured"
    floor = _report_money_floor(t, acct, provider, good, _pot_unread(rd, pot))
    took = False
    with t.reports_lock:
        if open_ is False:
            shut = _reports_keep(t.reported, (acct, "money"), "open", False, at, floor)
            if shut and provider in _API_POTS:
                t.shut_reports[acct] = max(at, t.shut_reports.get(acct) or 0)
            took |= shut
        if bal is not None:
            took |= _reports_keep(t.reported, (acct, "money"), "balance", float(bal), at, floor)
        _reports_tidy(t, acct)
    return ("applied", None) if took else ("ignored", "no newer than what the brain holds")


def _report_refusal_problem(ev):
    """Why a refusal event's window, limit and class are refused, or None:
    exactly one of window and limit is set (67 §9); a limit is model:<m> or
    pool:<name>; class is spent or not_entitled, and spent on a window."""
    if "class" not in ev:
        return "class is required: spent, or not_entitled on a limit"
    wid, lim = ev.get("window"), ev.get("limit")
    if (wid is None) == (lim is None):
        return "a refusal names exactly one of window and limit"
    if lim is not None and not (isinstance(lim, str) and _REPORT_LIMIT_RE.fullmatch(lim)):
        return f"unknown limit {lim!r}: a limit is model:<model> or pool:<name>"
    cls = ev["class"]
    if cls not in _REPORT_REFUSAL_CLASSES:
        return f"class must be one of {list(_REPORT_REFUSAL_CLASSES)}"
    if wid is not None and cls != "spent":
        return "a window's refusal is spent"
    return None


def _report_limit(t, ev, acct, provider, measured, at, now):
    """A refusal of a limit the brain has no window for (67 §9): held in
    _limit_holds until its until, the owner's re-check or a newer `ended`.
    Not a head for sizing, and nothing of the standings moves."""
    ended = ev.get("ended", False)
    if not isinstance(ended, bool):
        return "refused", "ended must be true or false"
    lim = ev["limit"]
    if _account_kind(provider) == "api":
        return "refused", f"{acct} is an api account: it has no allowance limit"
    key = (acct, lim)
    if ended:
        with t.reports_lock:
            h = t.limit_holds.get(key)
            if h is not None and h["refused_at"] < at:
                del t.limit_holds[key]
                return "applied", None
        return "ignored", "no hold on the limit older than the end"
    until, named = ev.get("until"), ev.get("resets_at")
    if until is None:
        return "refused", "a limit's refusal carries its until: no reading can end it"
    if _report_time(until) is None:
        return "refused", "until must be epoch seconds"
    if named is not None and _report_time(named) is None:
        return "refused", "resets_at must be epoch seconds, or null"
    if ev.get("by_reading") is not False:
        return "refused", "by_reading is false: no reading can end a limit's refusal"
    if ev.get("seen") is not None:
        return "refused", "seen is null on a limit's refusal: no reading carries it"
    if not measured:
        return "ignored", "an account the brain has never measured"
    if until <= now:
        return "ignored", "its until has passed"
    with t.poll_health_lock:
        epoch = t.recheck_epochs.get(acct)
    if epoch is not None and at < epoch:
        return "ignored", "older than the owner's re-check"
    with t.reports_lock:
        h = t.limit_holds.get(key)
        if h is not None and h["refused_at"] >= at:
            return "ignored", "no newer than the hold the brain holds"
        t.limit_holds[key] = {"refused_at": at, "until": float(until), "class": ev["class"]}
    return "applied", None


def _limit_ended(h, st, now):
    """Why a limit's hold has ended, or None while it holds (67 §9): expired
    at its until, or ended by the owner's re-check (recheck_epoch newer than
    the refusal). An `ended` event deletes it where it arrives."""
    if now >= h["until"]:
        return "expired"
    epoch = st.get("recheck_epoch")
    if epoch is not None and h["refused_at"] < epoch:
        return "re-check"
    return None


def _limit_snap(t, standings, now):
    """{account: [{limit, class, until}]}: the live limit holds of each
    measured account, by limit."""
    with t.reports_lock:
        holds = [(k, dict(h)) for k, h in t.limit_holds.items()]
    out = {}
    for (acct, lim), h in sorted(holds, key=lambda kh: kh[0]):
        st = standings.get(acct)
        if not st or st.get("as_of") is None or _limit_ended(h, st, now):
            continue
        out.setdefault(acct, []).append({"limit": lim, "class": h["class"], "until": h["until"]})
    return out


def _report_refusal(t, ev, acct, wid, measured, at, now):
    ended = ev.get("ended", False)
    if not isinstance(ended, bool):
        return "refused", "ended must be true or false"
    named = ev.get("resets_at")
    if named is not None and _report_time(named) is None:
        return "refused", "resets_at must be epoch seconds, or null"
    key = (acct, wid)
    if ended:
        with t.reports_lock:
            h = t.holds.get(key)
            if h is not None and h["refused_at"] < at:
                del t.holds[key]
                return "applied", None
        return "ignored", "no hold on the window older than the end"
    until, by_reading, seen = ev.get("until"), ev.get("by_reading"), ev.get("seen")
    if until is not None and _report_time(until) is None:
        return "refused", "until must be epoch seconds, or null"
    if not isinstance(by_reading, bool):
        return "refused", "by_reading must be true or false"
    if until is None and not by_reading:
        return "refused", "a refusal no reading can end carries its until"
    seen = {} if seen is None else seen
    if not isinstance(seen, dict) or set(seen) - {"used_pct", "resets_at"}:
        return "refused", "seen must be {used_pct, resets_at}, or null"
    if any(v is not None and _num(v) is None for v in seen.values()):
        return "refused", "seen.used_pct and seen.resets_at must be numbers, or null"
    if not measured:
        return "ignored", "an account the brain has never measured"
    if until is not None and until <= now:
        return "ignored", "its until has passed"
    with t.poll_health_lock:
        epoch = t.recheck_epochs.get(acct)
    if epoch is not None and at < epoch:
        return "ignored", "older than the owner's re-check"
    with t.reports_lock:
        h = t.holds.get(key)
        if h is not None and h["refused_at"] >= at:
            return "ignored", "no newer than the hold the brain holds"
        t.holds[key] = {"refused_at": at, "until": None if until is None else float(until),
                       "resets": None if named is None else float(named),
                       "by_reading": by_reading,
                       "seen": {k: _num(seen.get(k)) for k in ("used_pct", "resets_at")}}
    return "applied", None


# `served` (quota-model 65 §7): one event per request an account answered,
# priced here at list prices (_served_price) and counted on /metrics. Step 1
# keeps nothing else from it: no decision reads it, so it never recomputes or
# publishes (64 §2: documents follow decisions). In memory only; a restart
# zeroes the counters, which Prometheus reads as a counter reset.
SERVED_SPEEDS = ("standard", "fast", "ultrafast")
SERVED_TOKEN_KINDS = ("input", "cache_write_5m", "cache_write_1h", "cache_write_30m",
                      "cache_write_12h", "cache_write", "cache_read", "output")
# head_at and readings are step 2's (quota-model 66 §1), required since 66 §11's push 3;
# output_max is the bound's (66 §1, §4), required since push 6.
_SERVED_KEYS = frozenset(("id", "kind", "account", "read_at", "head_at", "readings", "boot",
                          "seq", "consumer", "selection", "member", "model", "speed", "paid",
                          "tokens", "complete", "why", "output_max"))
_SERVED_READING_FIELDS = frozenset(("used_pct", "resets_at"))
# A window id as a reading report names it (64 §5).
_SERVED_WINDOW_ID_RE = re.compile(r"five_hour|seven_day|scoped:.+|window:.+")  # fullmatch
# Past this a time is in milliseconds, not epoch seconds (the year 5138).
EPOCH_S_MAX = 1e11
# The ids taken, oldest first, up to SERVED_IDS_MAX: a resend follows a lost
# reply within the proxy's 30 s retry ceiling, so 20 full posts' worth of ids
# covers it many times over. An id older than that, or one sent again across a
# brain restart, is counted again.
SERVED_IDS_MAX = 10000
# The highest seq taken per proxy boot, for the SERVED_BOOTS_MAX boots heard
# from most recently: an older boot's process is long gone.
SERVED_BOOTS_MAX = 8


def _served_fresh():
    # head_at: account -> the latest head_at of a served event taken for it, when
    # the proxy last served the account (the accounts page sets a proxy refusal
    # against it).
    return {"ids": OrderedDict(), "boots": OrderedDict(), "events": {}, "usd": {},
            "unpriced": {}, "incomplete": {}, "seq_missing": 0, "duplicates": 0,
            "head_at": {}}




def _served_price(model, speed, tokens, complete):
    """A served event's API dollars at list prices -> (usd, None), or
    (None, reason) when unpriced (65 §2). tokens holds every kind, an int or
    None; a None count is a kind the response didn't report, priced as 0."""
    usd, why, _, _ = _served_priced(model, speed, tokens, complete)
    return usd, why


def _served_priced(model, speed, tokens, complete):
    """_served_price, with the price row it used -> (usd, reason, speed,
    tier): the speed priced (a model with one row prices a null speed as
    standard) and the tier, "standard" or "long" (the model's long-context
    row past its own threshold, API_LONG_CONTEXT), which sizing keeps its
    token totals by (quota-model 66 §4)."""
    if not complete:
        return None, "incomplete", None, None
    speeds = API_LIST_PRICES.get(model)
    if speeds is None:
        return None, "no list price", None, None
    if speed is None:
        if len(speeds) > 1:
            return None, "speed unknown", None, None
        speed = "standard"
    row = speeds.get(speed)
    if row is None:
        return None, f"no {speed} price", None, None
    tier = "standard"
    prompt = sum(tokens.get(k) or 0 for k in SERVED_TOKEN_KINDS if k != "output")
    over, long_speeds = API_LONG_CONTEXT.get(model, (None, None))
    if over is not None and prompt > over:
        row, tier = long_speeds[speed], "long"
    usd = 0.0
    for kind in SERVED_TOKEN_KINDS:
        n = tokens.get(kind) or 0
        if not n:
            continue
        price = row.get(kind)
        if price is None:
            return None, ("cache write split unknown" if kind == "cache_write"
                          else f"no {kind} price"), None, None
        usd += n * price
    return usd / 1e6, None, speed, tier


def _served_bound(model, speed, tokens, output_max):
    """A hung-up request's range, for sizing only (quota-model 66 §4) ->
    (usd, span, speed, tier, tokens), or None when the table can't price it:
    its known tokens and half its output_max as output, priced at its
    model's row as a complete event is; its span, the whole output_max at
    that row's output price."""
    mid = dict(tokens, output=output_max / 2)
    usd, why, speed, tier = _served_priced(model, speed, mid, True)
    if why is not None:
        return None
    out = _price_row(model, speed, tier).get("output") or 0.0
    return usd, output_max * out / 1e6, speed, tier, mid


def _served_problem(ev):
    """Why a served event's fields are refused, or None. Every key is sent,
    null where unknown (the proxy's encoder, served_golden.json); head_at and
    readings are checked by _served_readings_problem, output_max by
    _served_bound_problem."""
    bad = sorted(set(ev) - _SERVED_KEYS)
    if bad:
        return f"unknown key(s) {bad}"
    missing = sorted(_SERVED_KEYS - set(ev))
    if missing:
        return f"missing key(s) {missing}"
    boot, seq = ev["boot"], ev["seq"]
    if not (isinstance(boot, str) and 0 < len(boot) <= 200):
        return "boot must be a string of 1-200 chars"
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        return "seq must be an integer >= 1"
    if not (isinstance(ev["model"], str) and 0 < len(ev["model"]) <= 200):
        return "model must be a string of 1-200 chars"
    if ev["speed"] is not None and ev["speed"] not in SERVED_SPEEDS:
        return f"speed must be one of {list(SERVED_SPEEDS)}, or null"
    for k in ("consumer", "selection", "member"):
        if ev[k] is not None and not isinstance(ev[k], str):
            return f"{k} must be a string, or null"
    if ev["paid"] is not None and not isinstance(ev["paid"], bool):
        return "paid must be true or false, or null"
    tokens = ev["tokens"]
    if not isinstance(tokens, dict) or set(tokens) != set(SERVED_TOKEN_KINDS):
        return f"tokens must carry exactly {list(SERVED_TOKEN_KINDS)}"
    for k, n in tokens.items():
        if n is not None and (not isinstance(n, int) or isinstance(n, bool) or n < 0):
            return f"tokens.{k} must be an integer >= 0, or null"
    if not isinstance(ev["complete"], bool):
        return "complete must be true or false"
    why = ev["why"]
    if why is not None and not isinstance(why, str):
        return "why must be a string, or null"
    if not ev["complete"] and not why:
        return "an incomplete event carries its why"
    return _served_readings_problem(ev) or _served_bound_problem(ev)


def _served_bound_problem(ev):
    """Why a served event's output_max is refused, or None (quota-model 66
    §1): an integer >= 0, set only on an incomplete event that has its
    input count and no output count."""
    m = ev["output_max"]
    if m is None:
        return None
    if not isinstance(m, int) or isinstance(m, bool) or m < 0:
        return "output_max must be an integer >= 0, or null"
    if ev["complete"]:
        return "output_max on a complete event"
    if ev["tokens"]["output"] is not None:
        return "output_max with an output count"
    if ev["tokens"]["input"] is None:
        return "output_max without an input count"
    return None


def _served_readings_problem(ev):
    """Why a served event's head_at and readings are refused, or None
    (quota-model 66 §1); both are present (_served_problem): head_at, epoch
    seconds no later than read_at; readings, {window id: {used_pct,
    resets_at}} as a reading report names and carries them, a window with
    neither field left out."""
    h = _num(ev["head_at"])
    if h is None or not 0 < h < EPOCH_S_MAX:
        return "head_at must be epoch seconds"
    if h > ev["read_at"]:
        return "head_at must not be after read_at"
    rds = ev["readings"]
    if not isinstance(rds, dict):
        return "readings must be an object of window id: {used_pct, resets_at}"
    for wid, rd in rds.items():
        if not _SERVED_WINDOW_ID_RE.fullmatch(wid):
            return f"readings: {wid!r} is not a window id"
        if not isinstance(rd, dict) or set(rd) - _SERVED_READING_FIELDS:
            return f"readings.{wid} must be {{used_pct, resets_at}}"
        used, resets = rd.get("used_pct"), rd.get("resets_at")
        if used is None and resets is None:
            return f"readings.{wid} carries neither used_pct nor resets_at"
        if used is not None and (_num(used) is None or used < 0):
            return f"readings.{wid}.used_pct must be a percent >= 0, or null"
        if resets is not None and (_num(resets) is None or not 0 < resets < EPOCH_S_MAX):
            return f"readings.{wid}.resets_at must be epoch seconds, or null"
    return None


def _report_served(t, ev, acct, provider=None, reporter=None):
    """Take one served event (65 §7): an id taken before is ignored; else its
    boot's seq gap is counted, it is priced and counted, and it goes to
    sizing (quota-model 66, _sizing_take). Once the brain is shutting down
    (_sizing_shutdown) it is answered retry before anything of it is kept,
    so the proxy's resend is taken whole by the next brain."""
    why = _served_problem(ev)
    if why:
        return "refused", why
    usd, unpriced, speed, tier = _served_priced(ev["model"], ev["speed"], ev["tokens"],
                                                ev["complete"])
    bound = None
    if not ev["complete"] and ev["output_max"] is not None:
        bound = _served_bound(ev["model"], ev["speed"], ev["tokens"], ev["output_max"])
    if not _sizing_enter(t):
        return "retry", "the brain is shutting down"
    try:
        return _report_served_counted(t, ev, acct, provider, reporter, usd, unpriced, speed, tier,
                                      bound)
    finally:
        _sizing_leave(t)


def _report_served_counted(t, ev, acct, provider, reporter, usd, unpriced, speed, tier, bound):
    with t.served_lock:
        ids = t.served["ids"]
        if ev["id"] in ids:
            t.served["duplicates"] += 1
            return "ignored", "an id the brain has taken"
        ids[ev["id"]] = None
        while len(ids) > SERVED_IDS_MAX:
            ids.popitem(last=False)
        # A boot first heard from counts no gap: what it sent before is
        # indistinguishable from what it sent before this brain started.
        boots, boot, seq = t.served["boots"], ev["boot"], ev["seq"]
        last = boots.pop(boot, None)
        if last is not None and seq > last + 1:
            t.served["seq_missing"] += seq - last - 1
        boots[boot] = seq if last is None else max(last, seq)
        while len(boots) > SERVED_BOOTS_MAX:
            boots.popitem(last=False)
        t.served["events"][acct] = t.served["events"].get(acct, 0) + 1
        t.served["head_at"][acct] = max(t.served["head_at"].get(acct, 0.0), float(ev["head_at"]))
        if usd is not None:
            t.served["usd"][acct] = t.served["usd"].get(acct, 0.0) + usd
        else:
            key = (acct, ev["model"], unpriced)
            t.served["unpriced"][key] = t.served["unpriced"].get(key, 0) + 1
        if not ev["complete"]:
            t.served["incomplete"][acct] = t.served["incomplete"].get(acct, 0) + 1
    _sizing_take(t, acct, provider, ev, usd, unpriced, speed, tier, reporter, bound)
    return "applied", None


# ---- sizing and outside use (quota-model 66, 65 §8 step 2) -----------------------
#
# Per (account, window) the brain learns a size: the API dollars (65 §2) the
# window takes per point of its used percentage, x 100. Heads (a served
# event's readings at its head_at, a refusal's seen use at its read_at, or a
# Kimi poll) give the boundaries T_q where the meter first read q (66 §2);
# the work of the events that ended between two boundaries is the step's
# (66 §3); a clean step is one sample at its per-point value (66 §4), and the
# size is 100 x the median of the kept samples (66 §5). The outside share is
# the signed sum of points over work across the period's measured range
# (66 §6). Nothing here decides anything: it goes to /metrics and GET /sizing
# only, never to /preferred or /events (66 §9).

SIZING_SETTLE_S = 600        # data time a boundary waits for requests in flight (66 §2, §15.1)
SIZING_FUZZ = 0.25           # straddling work past this share of a step's: not a sample (66 §4)
SIZING_UNSEEN = 0.05         # hung-up requests' spans past this share of a step's work: incomplete
SIZING_CHEAP = 0.25         # a sample this far under the learned size is flagged cheap (66 §4)
SIZING_SAMPLES_KEEP = 24     # the newest samples kept per window (66 §5)
SIZING_SAMPLE_AGE_S = 30 * 86400
SIZING_SAMPLES_MIN = 3       # learned at three steps ... (66 §5, §15.2)
SIZING_AGREE = 0.25          # ... whose spread is within this
SIZING_SHIFT_N = 3           # this many newest samples ...
SIZING_SHIFT = 0.35          # ... each this far above the older median retire the older ones
SIZING_DOMINANT = 0.8        # a model's share of a step's dollars that makes the step its own
SIZING_WEIGHT_AGREE = 0.25   # a model is weighed when its weight's standard error is within this (67 §3)
SIZING_WEIGHT_SHARE = 0.01   # a weight's candidate holds this share of a sample's dollars ...
SIZING_WEIGHT_STEPS_MIN = 3  # ... in this many samples of windows that hold more than one model
SIZING_WEIGHT_ITERS = 200    # Gauss-Newton steps per fit, at most
SIZING_WEIGHT_Z = 2.0        # a window's own weight is apart past this many of its standard errors too
SIZING_EVENTS_MAX = 5000     # events held per account (66 §8)
SIZING_GAPS_MAX = 100        # seq-gap intervals held (66 §8)
# The file is the tenant's sizing.json (t.sizing_file), through the store.
SIZING_FILE_VERSION = 1
SIZING_SHUTDOWN_WAIT_S = 5   # the shutdown save waits this long for reports in flight
SIZING_RANGE_STEPS_MAX = 200  # a range's newest steps kept whole; older ones are final in it
# sizing_steps_total's outcomes, counted once per step at its close: §4's,
# with `cheap` and `bounded` flags each counted beside the `sample` it also
# is, and §2's two ways an open step is lost.
SIZING_OUTCOMES = ("sample", "cheap", "bounded", "unpriced", "incomplete", "paid", "seq_gap",
                   "proxy_restart", "brain_restart", "overflow", "silent", "fuzz",
                   "over_limit", "reordered", "period_change")
# sizing_samples_retracted_total's: a sample taken back after its step
# closed, because an event of the step arrived late (a reporter more than
# SIZING_SETTLE_S behind another) or a reporter's restart is found in it.
SIZING_RETRACTED = ("late", "proxy_restart")
# The outcomes whose work is known, so the step counts in the outside sum (66 §6).
_SIZING_MEASURED = ("sample", "silent", "fuzz")

# t.sizing_lock is never held around t.lock, t.plock or t.served_lock;
# t.sizing_cv is a condition on it.
# closing: set by the shutdown save, after which no save writes the file and
# no served event is taken (it is answered retry); inflight: served events
# past that gate, which the shutdown save waits for; frozen: why nothing may
# write the file (it could not be read, or an unusable one could not be set
# aside), or None; set_aside: where an unusable file was moved, or None.
#   sizing_gate = {"closing": False, "inflight": 0, "frozen": None, "set_aside": None}


def _sizing_enter(t):
    """Let a served or refusal report in, or False once the brain is
    shutting down."""
    with t.sizing_lock:
        if t.sizing_gate["closing"]:
            return False
        t.sizing_gate["inflight"] += 1
        return True


def _sizing_leave(t):
    with t.sizing_lock:
        t.sizing_gate["inflight"] -= 1
        t.sizing_cv.notify_all()


def _sizing_fresh():
    """accounts: {acct: {provider, data_t (the newest served read_at
    applied), buf (events by read_at), keys {(boot, seq)}, dropped, late}};
    windows: {(acct, wid): window}; boots: {boot: {first, last, seq, seq_at,
    reporter}} in the order first heard (seq is proxy-wide, so boots and
    gaps are too); gaps: [[lo, hi]] read_at intervals a seq jump left;
    reporters: {reporter: {boot, last}}, each POST's reporter's current boot
    and its last read_at; restarts: [[old boot's last read_at, new boot's
    first]] per reporter; restarted: the windows a start without the open
    state found mid-step (66 §8); weights: {provider: its fit (67 §3,
    _sizing_refit)}, derived from the samples and never saved; stale: the
    providers whose samples changed since their fit, fitted again as the
    batch ends (_sizing_after). While a batch runs, "dirty" says the file
    needs a save and "resized" that a window's samples or weights changed,
    so the document's fleet may have (67 §8). burn: the pace's hourly
    buckets (_burn_fresh, quota-model 69 §4.2), saved under the file's
    `burn`."""
    return {"accounts": {}, "windows": {}, "boots": OrderedDict(), "gaps": [],
            "reporters": {}, "restarts": [], "restarted": set(), "weights": {},
            "stale": set(), "burn": _burn_fresh()}




def _api_prices_hash():
    """The price table's identity, kept with each sample (66 §9)."""
    blob = json.dumps({"list": API_LIST_PRICES,
                       "long": {m: [t, rows] for m, (t, rows) in API_LONG_CONTEXT.items()}},
                      sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:12]


def _sizing_mode(provider, wid, kind="window"):
    """How a window is sized, fixed per provider and window (66 §7): "header"
    from the readings every response carries, "poll" from the brain's polls,
    or None, not sized: Anthropic's five_hour (a 2xx never carries 5h), money
    and api accounts."""
    if provider == "anthropic":
        return "header" if kind == "window" and (wid == "seven_day" or wid.startswith("scoped:")) \
            else None
    sizing = (PROVIDERS.get(provider) or {}).get("sizing")
    return sizing(wid, kind) if sizing else None


def _sizing_window_scope(wid):
    return wid[len("scoped:"):] if wid.startswith("scoped:") else None


def _sizing_lane_cfg(model):
    """The first lane_rules entry whose match is in the model id (the proxy's
    first-match walk, 66 §3), the default lane when none matches."""
    m = (model or "").lower()
    for lc in _lanes:
        if lc["lane"] == "default" or any(s in m for s in lc["match"]):
            return lc
    return None


def _sizing_lane_scope(model):
    """The scope of the model's lane (_sizing_lane_cfg); the default lane's is None."""
    return (_sizing_lane_cfg(model) or {}).get("scope_model")


def _sizing_acct(t, acct, provider):
    """The account's record. When it learns its provider (a start without
    the open state keeps samples but no provider), the provider's weights
    are fitted again with its samples in them (67 §3). Caller holds
    _sizing_lock."""
    a = t.sizing["accounts"].get(acct)
    if a is None:
        a = t.sizing["accounts"][acct] = {"provider": provider, "data_t": 0.0, "buf": [],
                                         "keys": set(), "dropped": 0, "late": 0}
    elif provider and not a.get("provider"):
        a["provider"] = provider
    else:
        return a
    if provider and any(w["samples"] for (x, _), w in t.sizing["windows"].items() if x == acct):
        _sizing_refit(t, provider)
        t.sizing["resized"] = True
    return a


def _sizing_win(t, acct, wid, mode):
    w = t.sizing["windows"].get((acct, wid))
    if w is None:
        w = t.sizing["windows"][(acct, wid)] = {
            "mode": mode, "scope": _sizing_window_scope(wid), "anchor": None, "pending": [],
            "overflow_to": None, "last_head": None, "samples": [], "range": None,
            "counts": {}, "retracted": {}, "basis": "none"}
    return w


def _sizing_count(w, outcome):
    w["counts"][outcome] = w["counts"].get(outcome, 0) + 1


def _sizing_hold_from(w):
    """The read_at past which the window still needs an event, or None when
    it needs none: no anchor, or the meter at or past its allowance (66 §8).
    A poll-sized window keeps the poll interval before its anchor too, the
    poll fuzz's (66 §7)."""
    a = w["anchor"]
    if a is None or a["q"] >= 100:
        return None
    if w["mode"] == "poll":
        return a["lo"] if a.get("lo") is not None else a["t"] - POLL_INTERVAL
    return a["t"]


def _sizing_acct_windows(t, acct):
    return [(wid, w) for (a, wid), w in t.sizing["windows"].items() if a == acct]


def _sizing_needs(t, acct, e):
    """(needed, anchored): does a window of the account that the event draws
    still need it, and does the event draw any window holding events."""
    anchored = False
    for _, w in _sizing_acct_windows(t, acct):
        if not _scope_binds(w["scope"], e["scope"]):
            continue
        hold = _sizing_hold_from(w)
        if hold is None:
            continue
        anchored = True
        if e["r"] > hold:
            return True, True
    return False, anchored


def _sizing_prune(t, acct):
    """Keep only the events a window still needs; one that draws no window
    holding events stays for SIZING_SETTLE_S of data time, so a first head
    that arrives late still finds the events after it (66 §8)."""
    a = t.sizing["accounts"].get(acct)
    if a is None:
        return
    keep = []
    for e in a["buf"]:
        needed, anchored = _sizing_needs(t, acct, e)
        if needed or (not anchored and e["r"] >= a["data_t"] - SIZING_SETTLE_S):
            keep.append(e)
        else:
            a["keys"].discard((e["boot"], e["seq"]))
    a["buf"] = keep


def _sizing_prune_gaps(t):
    """A gap or restart interval leaves once every sized window's anchor is
    past it."""
    holds = [h for w in t.sizing["windows"].values() for h in (_sizing_hold_from(w),) if h is not None]
    if holds:
        floor = min(holds)
        t.sizing["gaps"] = [g for g in t.sizing["gaps"] if g[1] > floor]
        t.sizing["restarts"] = [g for g in t.sizing["restarts"] if max(g) > floor]


def _sizing_boot(t, boot, seq, at, reporter=""):
    """Note a served event's boot and seq: boots by first heard, each with its
    first and last read_at; a seq jump within a boot leaves a gap interval
    (the last seq's read_at, this one's), where the missing events ended. A
    reporter's new boot is its restart (66 §4, per reporter: a boot is one
    reporter's, its POSTs' `reporter`, "" when they name none); its first
    boot ever heard is not. -> (new, restart): is the event past every seq
    its boot sent before (not a resend), and the restart interval it found."""
    boots = t.sizing["boots"]
    b = boots.get(boot)
    restart = None
    if b is None:
        rep = t.sizing["reporters"].get(reporter)
        if rep is not None and rep["boot"] != boot:
            restart = [rep["last"], at]
            t.sizing["restarts"].append(restart)
            del t.sizing["restarts"][:-SIZING_GAPS_MAX]
        t.sizing["reporters"][reporter] = {"boot": boot, "last": at}
        boots[boot] = {"first": at, "last": at, "seq": seq, "seq_at": at, "reporter": reporter}
        while len(boots) > SERVED_BOOTS_MAX:
            boots.popitem(last=False)
        return True, restart
    new = seq > b["seq"]
    if seq > b["seq"] + 1:
        t.sizing["gaps"].append([b["seq_at"], at])
        del t.sizing["gaps"][:-SIZING_GAPS_MAX]
    if new:
        b["seq"], b["seq_at"] = seq, at
    b["first"], b["last"] = min(b["first"], at), max(b["last"], at)
    rep = t.sizing["reporters"].get(b.get("reporter", reporter))
    if rep is not None and rep["boot"] == boot:
        rep["last"] = max(rep["last"], at)
    return new, None


def _sizing_restart_in(t, lo, hi):
    """Did a proxy restart fall in (lo, hi]: a reporter's old boot's last
    event, or its new boot's first, inside the step (66 §4). Two reporters
    serving at once are not a restart of either."""
    return any(lo < old <= hi or lo < new <= hi for old, new in t.sizing["restarts"])


def _sizing_anchor(w, q, t, reset, fresh, lo=None):
    w["anchor"] = {"q": q, "t": t, "reset": reset, "fresh": fresh, "lo": lo}


def _sizing_range_for(w, reset, q, new=False):
    """The window's measured range for the period of a head reading q with
    this reset: the current one when it is the same period, else (or when
    new) a fresh one (66 §6: a period change starts a new range)."""
    rg = w["range"]
    same = not new and rg is not None and not (
        (reset is not None and rg["reset"] is not None and abs(reset - rg["reset"]) > REPORT_RESET_SLACK_S)
        or (rg["q"] is not None and q < rg["q"] - 1))
    if not same:
        w["range"] = {"reset": reset, "first": None, "last": None, "q": None,
                      "points": 0.0, "work": 0.0, "silent": 0.0, "by": {}, "steps": []}
    elif reset is not None and rg["reset"] is None:
        rg["reset"] = reset
    return w["range"]


def _sizing_head(t, acct, wid, w, h, q, reset, at):
    """Apply one head (h, q, reset) to a window (66 §2). at is the data time
    it arrived with (its event's read_at, or the poll's time). -> True when
    the anchor moved. Caller holds _sizing_lock."""
    prev = w["last_head"]
    w["last_head"] = h if prev is None else max(prev, h)
    # A poll-sized window's settle is one poll: the next poll closes the step.
    moved = _sizing_close(t, acct, wid, w, at) if w["mode"] == "poll" else False
    a = w["anchor"]
    if a is None:
        _sizing_range_for(w, reset, q)
        _sizing_anchor(w, q, h, reset, True, lo=prev)
        return True
    if h < a["t"]:
        return moved  # the meter before what the anchor knows
    level = a["q"]
    for b in w["pending"]:
        if b["t"] <= h:
            level = max(level, b["q"])
    if ((reset is not None and a["reset"] is not None
         and abs(reset - a["reset"]) > REPORT_RESET_SLACK_S) or q < level - 1):
        # A new period: the open step closes if this head is late enough,
        # else it is lost; then the window anchors afresh.
        _sizing_close(t, acct, wid, w, at)
        _sizing_lose_pending(acct, wid, w)
        _sizing_range_for(w, reset, q, new=True)
        _sizing_anchor(w, q, h, reset, True, lo=prev)
        return True
    if reset is not None and a["reset"] is None:
        a["reset"] = reset
    if q <= a["q"]:
        return moved  # the anchor's reading, or a point below it
    if any(b["q"] < q and h < b["t"] for b in w["pending"]):
        # A boundary below a lower point's would make a negative step.
        _sizing_count(w, "reordered")
        _sizing_log_step(acct, wid, a["q"], q, h, None, 0, "reordered")
        w["pending"] = []
        _sizing_anchor(w, q, h, a["reset"], True, lo=prev)
        return True
    if any(b["q"] > q and b["t"] <= h for b in w["pending"]):
        return moved  # a higher point was read before it: a point below, again
    for b in w["pending"]:
        if b["q"] == q:
            if h < b["t"]:
                b["t"] = h  # a later-ending event with an earlier head
            return moved
    # A poll's boundary keeps the poll before it: the meter crossed q between.
    w["pending"].append({"q": q, "t": h, "lo": prev if w["mode"] == "poll" else None})
    w["pending"].sort(key=lambda b: b["q"])
    return moved


def _sizing_lose_pending(acct, wid, w):
    """The window's open steps are no steps (66 §2, period_change): a new
    period's head came too early for them, or no head came past the reset."""
    lower = w["anchor"]
    for b in w["pending"]:
        if not lower["fresh"]:  # a fresh anchor's first transition is no step
            _sizing_count(w, "period_change")
            _sizing_log_step(acct, wid, lower["q"], b["q"], b["t"], None, 0, "period_change")
        lower = dict(b, fresh=False)
    w["pending"] = []


def _sizing_expire(t, acct, wid, w, t_data):
    """A window whose heads stopped (a header that is no longer sent):
    once data time is SIZING_SETTLE_S past its period's reset with no head
    of the next period, the steps due by then close as usual, the rest are
    lost (period_change), and the window lets go of its anchor and what it
    held. Without a reset only the buffer's bound limits it. -> True when
    it let go."""
    a = w["anchor"]
    if a is None or a["reset"] is None or t_data < a["reset"] + SIZING_SETTLE_S:
        return False
    _sizing_close(t, acct, wid, w, t_data)
    _sizing_lose_pending(acct, wid, w)
    w["anchor"] = None
    t.sizing["restarted"].discard((acct, wid))
    log(f"sizing: {acct} {wid} no head of a new period {SIZING_SETTLE_S}s past its reset: "
        "the window lets go of its open step")
    return True


def _sizing_add_by(by, e):
    k = by.setdefault(e["key"], {"usd": 0.0, "tokens": {}})
    k["usd"] += e["usd"]
    for kind, n in e["tokens"].items():
        k["tokens"][kind] = k["tokens"].get(kind, 0) + n


def _sizing_sub_by(by, part):
    """Take one step's token totals back out of a range's."""
    for key, p in part.items():
        tot = by.get(key)
        if tot is None:
            continue
        tot["usd"] -= p["usd"]
        for kind, n in p["tokens"].items():
            left = tot["tokens"].get(kind, 0) - n
            if left > 0:
                tot["tokens"][kind] = left
            else:
                tot["tokens"].pop(kind, None)
        if not tot["tokens"]:
            del by[key]


def _sizing_range_step(w, t):
    """The measured range's step holding read_at t, while it is kept whole."""
    for st in (w["range"] or {}).get("steps") or []:
        if st["lo"] < t <= st["hi"]:
            return st
    return None


def _sizing_unmeasure(t, w, st):
    """A step whose work became unknown leaves the measured range (66 §6):
    its points, its work and its silent points."""
    rg = w["range"]
    rg["steps"].remove(st)
    rg["points"] -= st["points"]
    rg["work"] = max(0.0, rg["work"] - st["work"])
    _sizing_sub_by(rg["by"], st["by"])
    if st["silent"]:
        rg["silent"] -= st["points"]
    t.sizing["dirty"] = True


def _sizing_retract(t, acct, wid, w, at, outcome):
    """A kept sample whose step holds t, found unclean after it closed, is
    taken back and counted on sizing_samples_retracted_total. -> True when
    one was."""
    for i, smp in enumerate(w["samples"]):
        if smp["from"] < at <= smp["to"]:
            del w["samples"][i]
            w["retracted"][outcome] = w["retracted"].get(outcome, 0) + 1
            _sizing_log_step(acct, wid, smp["p"], smp["q"], smp["to"], smp["usd"], smp["events"],
                             f"{outcome} (sample retracted)")
            _sizing_rejudge(t, acct, wid, w)
            t.sizing["dirty"] = True
            return True
    return False


def _sizing_unknown(t, acct, wid, w, at, outcome):
    """The step holding t, already closed, has work the brain can't know (a
    restart in it, or a late event without priced work): its sample is taken
    back and it leaves the measured range. -> True when either was so."""
    done = _sizing_retract(t, acct, wid, w, at, outcome)
    st = _sizing_range_step(w, at)
    if st is not None:
        _sizing_unmeasure(t, w, st)
    return done or st is not None


def _sizing_late(t, acct, e):
    """A served event whose read_at is in a step already closed (66 §8: a
    reporter behind another by more than SIZING_SETTLE_S): the step's
    sample, if kept, is taken back (`late`). With priced work, the work
    joins the step's in the measured range (a silent step is silent no
    more); without (unpriced, incomplete, paid), the step leaves the range.
    A hung-up request counts as incomplete here, bound or not: whether the
    closed step's spans stay within SIZING_UNSEEN is not kept (66 §4).
    -> True when it was of use."""
    used = False
    known = e["complete"] and e["usd"] is not None and not e["paid"]
    for wid, w in _sizing_acct_windows(t, acct):
        if not _scope_binds(w["scope"], e["scope"]):
            continue
        if not known:
            used |= _sizing_unknown(t, acct, wid, w, e["r"], "late")
            continue
        used |= _sizing_retract(t, acct, wid, w, e["r"], "late")
        st = _sizing_range_step(w, e["r"])
        if st is not None:
            rg = w["range"]
            rg["work"] += e["usd"]
            st["work"] += e["usd"]
            _sizing_add_by(rg["by"], e)
            _sizing_add_by(st["by"], e)
            if st["silent"]:  # a silent step had work after all
                st["silent"] = False
                rg["silent"] -= st["points"]
            t.sizing["dirty"] = used = True
    return used


def _sizing_close(t, acct, wid, w, t_data):
    """Close every step of the window that is final at data time t_data:
    header-sized, SIZING_SETTLE_S after its boundary; poll-sized, at the
    next poll. -> True when any closed. Caller holds _sizing_lock."""
    closed = False
    while w["pending"] and w["anchor"] is not None:
        b = w["pending"][0]
        due = (b["t"] + SIZING_SETTLE_S <= t_data) if w["mode"] == "header" else (b["t"] < t_data)
        if not due:
            break
        _sizing_step(t, acct, wid, w, b)
        closed = True
    return closed


def _sizing_events_in(t, acct, lo, hi):
    """The account's buffered events with read_at in (lo, hi]."""
    buf = (t.sizing["accounts"].get(acct) or {}).get("buf") or []
    i = bisect.bisect_right(buf, lo, key=lambda e: e["r"])
    j = bisect.bisect_right(buf, hi, key=lambda e: e["r"])
    return buf[i:j]


def _sizing_straddle(t, acct, w, lo, hi, b):
    """The work of the events whose side of a boundary depends on when the
    provider counts them (66 §4): header-sized, those with head_at <= T <
    read_at at either boundary; poll-sized, those that ended in the poll
    interval before either boundary (66 §7)."""
    a = w["anchor"]
    if w["mode"] == "poll":
        evs = []
        for edge_lo, edge in ((a.get("lo"), lo), (b.get("lo"), hi)):
            if edge_lo is None:
                edge_lo = edge - POLL_INTERVAL
            evs += _sizing_events_in(t, acct, edge_lo, edge)
    else:
        buf = (t.sizing["accounts"].get(acct) or {}).get("buf") or []
        i = bisect.bisect_right(buf, lo, key=lambda e: e["r"])
        evs = [e for e in buf[i:] if e["h"] is not None
               and any(e["h"] <= edge < e["r"] for edge in (lo, hi))]
    return sum(e["usd"] or 0.0 for e in evs
               if _scope_binds(w["scope"], e["scope"]) and not e["paid"])


def _sizing_estimate(samples, rho=None):
    """-> (size, basis, spread, median per point) over the kept samples (66
    §5), their dollars weighted at rho (67 §3: reference-model dollars; with
    no weighed model, list dollars)."""
    vals = sorted(_sizing_weighted(s["usd"], s.get("by"), rho) / s["points"] for s in samples)
    if not vals:
        return None, "none", None, None
    med = _median(vals)
    spread = None
    if len(vals) >= 2 and med > 0:
        spread = _median([abs(v - med) for v in vals]) / med
    if len(vals) < SIZING_SAMPLES_MIN:
        basis = "few_steps"
    elif spread is not None and spread <= SIZING_AGREE:
        basis = "learned"
    else:
        basis = "disagree"
    return 100 * med, basis, spread, med


def _median(vals):
    v = sorted(vals)
    n = len(v)
    if not n:
        return None
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2


def _sizing_kept(samples, now):
    """The newest SIZING_SAMPLES_KEEP samples, none older than
    SIZING_SAMPLE_AGE_S of data time."""
    fresh = [s for s in samples if s["to"] >= now - SIZING_SAMPLE_AGE_S]
    return fresh[-SIZING_SAMPLES_KEEP:]


def _sizing_learned(w, now, rho=None):
    """The window's learned per-point dollars, weighted at rho, or None while
    not learned."""
    size, basis, _, med = _sizing_estimate(_sizing_kept(w["samples"], now), rho)
    return med if basis == "learned" else None


def _sizing_now(t, acct, w):
    """The data time a window's kept samples are taken at: its account's, or
    its newest sample's end before the account has any."""
    return ((t.sizing["accounts"].get(acct) or {}).get("data_t")
            or max([s["to"] for s in w["samples"]] or [0]))


# ---- sizing: per-model weights (quota-model 67 §3) ----
#
# A window's meter charges each model's list dollar at its own rate: a step
# moves points = sum over models of usd_m * rho_m / pi_W, rho_m the model's
# weight against its provider's reference model (rho = 1: the model with the
# most dollars in the provider's kept samples), pi_W the window's size per
# point in reference dollars. The weights are fitted per provider over all
# its windows' kept samples, with a size per window; they are derived, never
# saved, and a start fits them again from the re-priced samples.

def _sizing_model(key):
    """The model of a "model|speed|tier" key."""
    return key.rsplit("|", 2)[0]


def _sizing_model_usd(by):
    """List dollars by model, from token totals by "model|speed|tier"."""
    out = {}
    for key, part in (by or {}).items():
        m = _sizing_model(key)
        out[m] = out.get(m, 0.0) + part["usd"]
    return out


def _sizing_weighted(usd, by, rho):
    """Dollars weighted at rho ({model: weight} of the weighed models; any
    other counts 1): reference-model dollars. With no weighed model, the
    list dollars as they are."""
    if not rho:
        return usd
    return sum(v * rho.get(m, 1.0) for m, v in _sizing_model_usd(by).items())


def _sizing_weights_of(t, acct):
    """The account's provider's fit, or None."""
    prov = (t.sizing["accounts"].get(acct) or {}).get("provider")
    return t.sizing["weights"].get(prov) if prov else None


def _sizing_rho(t, acct):
    """{model: weight} of the account's provider's weighed models."""
    return (_sizing_weights_of(t, acct) or {}).get("ratios") or {}


def _sizing_unweighed_share(mu, wt):
    """The share of list dollars by model `mu` in models neither the
    reference nor weighed, or None with no dollars."""
    total = sum(mu.values())
    if not total > 0:
        return None
    wt = wt or {}
    return sum(v for m, v in mu.items()
               if m != wt.get("reference") and m not in (wt.get("ratios") or {})) / total


def _sizing_gauss(A, B):
    """Solve A X = B for a symmetric positive semi-definite A (normal
    equations), eliminating in order -> (X, None), or (None, i) at the first
    column i whose pivot is no more than 1e-10 of its own diagonal: that
    unknown is not determined apart from the earlier ones."""
    n, k = len(A), len(B[0])
    M = [list(A[i]) + list(B[i]) for i in range(n)]
    for i in range(n):
        if not (A[i][i] > 0 and M[i][i] > 1e-10 * A[i][i]):
            return None, i
        for r in range(n):
            if r != i and M[r][i]:
                f = M[r][i] / M[i][i]
                M[r] = [M[r][c] - f * M[i][c] for c in range(n + k)]
    return [[M[i][n + j] / M[i][i] for j in range(k)] for i in range(n)], None


def _sizing_fit_sizes(rows, idx, rho):
    """Each window's 1/size: the one-parameter least-squares fit of its rows'
    points on their weighted dollars, over the rows idx."""
    num, den = {}, {}
    for i in idx:
        k, pts, u = rows[i]
        x = sum(v * rho.get(m, 1.0) for m, v in u.items())
        num[k] = num.get(k, 0.0) + pts * x
        den[k] = den.get(k, 0.0) + x * x
    return {k: num[k] / den[k] if den[k] > 0 else 0.0 for k in num}


def _sizing_fit_system(rows, groups, weighed, rho):
    """The weights' least squares over the rows of `groups` ({window: its
    row indices}) at rho, each window's size at its best for rho (variable
    projection) -> (1/size by window, the weighed models' reduced normal
    matrix S with the sizes taken out, its diagonal before they were, the
    gradient g, the residual sum of squares). S d = g is a Gauss-Newton
    step on the weights; S's inverse times the residual variance is their
    covariance with the sizes fitted jointly."""
    p = len(weighed)
    S = [[0.0] * p for _ in range(p)]
    C, g, rss, inv_pi = [0.0] * p, [0.0] * p, 0.0, {}
    for W, mine in groups.items():
        pts = [rows[i][1] for i in mine]
        x = [sum(v * rho.get(m, 1.0) for m, v in rows[i][2].items()) for i in mine]
        d = sum(v * v for v in x)
        w = inv_pi[W] = sum(a * b for a, b in zip(pts, x)) / d if d > 0 else 0.0
        r = [a - w * b for a, b in zip(pts, x)]
        cols = [[w * rows[i][2].get(m, 0.0) for i in mine] for m in weighed]
        bx = [sum(a * b for a, b in zip(x, col)) for col in cols]
        rss += sum(v * v for v in r)
        for a in range(p):
            C[a] += sum(v * v for v in cols[a])
            g[a] += sum(u * v for u, v in zip(cols[a], r))
            for b in range(a, p):
                s = (sum(u * v for u, v in zip(cols[a], cols[b]))
                     - (bx[a] * bx[b] / d if d > 0 else 0.0))
                S[a][b] += s
                if b != a:
                    S[b][a] += s
    return inv_pi, S, C, g, rss


def _sizing_fit_weights(rows, use, mixed, weighed, rho):
    """67 §3 step 2: the weighed models' weights over the rows `use` of the
    mixed windows (the reference's and the unweighed models' dollars at
    their fixed weights), each window's size at its best for them:
    Gauss-Newton steps from rho, a step halved while it does not lower the
    squares, until the step moves no weight past 1e-9 or SIZING_WEIGHT_ITERS
    steps -> (rho, errors, steps). errors: each weighed model's standard error over its
    weight, from the residuals with the sizes fitted jointly (67 §3 step
    4); inf for a weight the rows do not determine (a model at one share
    of every step of its window, which the size takes up, or in a fixed
    proportion to a model ahead of it in `weighed`), None for one not
    judged then. Errors conditional on the sizes would call such a weight
    exact."""
    rho = dict(rho)
    if not weighed:
        return rho, {}, 0
    groups = {}
    for i in use:
        if rows[i][0] in mixed:
            groups.setdefault(rows[i][0], []).append(i)
    p, steps = len(weighed), 0

    def undetermined(at):
        return {m: (math.inf if j == at else None) for j, m in enumerate(weighed)}

    # At list prices a model at one share of every step of its windows is
    # the window's own dollars: whatever its weight, the size takes it up.
    # Tested apart, as at fitted weights it is undetermined only together
    # with another weighed model, which would then share the blame.
    ones = dict(rho)
    ones.update((m, 1.0) for m in weighed)
    _, S, C, _, _ = _sizing_fit_system(rows, groups, weighed, ones)
    for a in range(p):
        if not (C[a] > 0 and S[a][a] > 1e-10 * C[a]):
            return rho, undetermined(a), steps
    system = _sizing_fit_system(rows, groups, weighed, rho)
    while True:
        _, S, C, g, rss = system
        for a in range(p):
            if not (C[a] > 0 and S[a][a] > 1e-10 * C[a]):
                return rho, undetermined(a), steps
        d, bad = _sizing_gauss(S, [[v] for v in g])
        if d is None:
            return rho, undetermined(bad), steps
        # Settled when the step would move no weight: at the floor a step
        # only trades rounding, and halving it would never lower the squares.
        if steps >= SIZING_WEIGHT_ITERS or max(abs(v[0]) for v in d) <= 1e-9:
            break
        steps += 1
        t = 1.0
        for _ in range(60):
            trial = dict(rho)
            trial.update((m, rho[m] + t * d[a][0]) for a, m in enumerate(weighed))
            new = _sizing_fit_system(rows, groups, weighed, trial)
            if new[4] <= rss:
                break
            t /= 2
        else:
            break  # no step lowers the squares: the weights are at the floor
        moved = max(abs(trial[m] - rho[m]) for m in weighed)
        rho, system = trial, new
        if moved <= 1e-9:
            break
    inv_pi, S, _, _, rss = system
    dof = sum(len(v) for v in groups.values()) - p - len(inv_pi)
    if dof <= 0:
        return rho, {m: math.inf for m in weighed}, steps
    inv, bad = _sizing_gauss(S, [[float(i == j) for j in range(p)] for i in range(p)])
    if inv is None:
        return rho, undetermined(bad), steps
    s2 = rss / dof
    return rho, {m: (math.sqrt(max(s2 * inv[a][a], 0.0)) / rho[m] if rho[m] > 0 else math.inf)
                 for a, m in enumerate(weighed)}, steps


def _sizing_fit(rows, ref=None, whole=None):
    """67 §3's fit over rows [(window, points, {model: list dollars})], taken
    in their order -> {"reference", "ratios" {weighed model: weight},
    "models" {model: {ratio, rel_error, basis, dollars, steps}}, "fit"
    {samples, trimmed, windows, mixed, rounds, max_residual_points}}.

    The reference is the model with the most dollars, or `ref` (the
    per-window check passes its provider's; absent from the rows, nothing
    is weighed). Dollars are summed over the windows in `whole`, those that
    bind every model, when they hold any rows: a request is in every window
    that binds it, so a model with a scoped window of its own would count
    twice over all of them. A candidate holds SIZING_WEIGHT_SHARE of a sample's dollars
    in SIZING_WEIGHT_STEPS_MIN samples of the mixed windows, those holding
    more than one model: a window of one model pins its own size and nothing
    else. The candidates' weights are fitted (_sizing_fit_weights); the
    mixed windows' rows cheap at those weights against their window's median
    (66 §4's rule; never at list prices, where a dear model's steps look
    cheap) leave the fit, which runs again, until no more are cheap. A row
    left out stays out, and every row stays in the medians (66 §5). Only
    then are the weights judged: the worst candidate whose weight is not
    positive or whose error is past SIZING_WEIGHT_AGREE goes back to 1, and
    the rest are fitted and trimmed again from every row, until all pass.
    Judged before the trim, a few steps of outside use can fail a good
    weight and leave the trim at list prices."""
    rows = [r for r in rows if r[1] > 0]
    models = sorted({m for _, _, u in rows for m, v in u.items() if v > 0})
    counted = [r for r in rows if r[0] in (whole or ())] or rows
    dollars = {m: sum(u.get(m, 0.0) for _, _, u in counted) for m in models}
    if ref is None and models:
        ref = max(models, key=lambda m: (dollars[m], m))
    wins = sorted({k for k, _, _ in rows})
    held = {}
    for k, _, u in rows:
        held.setdefault(k, set()).update(m for m, v in u.items() if v > 0)
    mixed = {k for k in wins if len(held[k]) > 1}

    def steps_of(m, rs):
        return sum(1 for _, _, u in rs
                   if u.get(m, 0.0) > 0 and u[m] >= SIZING_WEIGHT_SHARE * sum(u.values()))

    steps = {m: steps_of(m, rows) for m in models}
    mixed_rows = [r for r in rows if r[0] in mixed]
    # Most dollars first: of two weights the rows can't tell apart, the one
    # with fewer dollars is judged undetermined (_sizing_fit_weights).
    weighed = (sorted((m for m in models
                       if m != ref and steps_of(m, mixed_rows) >= SIZING_WEIGHT_STEPS_MIN),
                      key=lambda m: (-dollars[m], m))
               if ref in dollars else [])
    rho = {m: 1.0 for m in models}
    failed, rounds = {}, 0
    while True:
        left = set()
        while True:
            use = [i for i in range(len(rows)) if i not in left]
            rho, err, n = _sizing_fit_weights(rows, use, mixed, weighed, rho)
            rounds += n
            if not weighed:
                break
            x = [sum(v * rho.get(m, 1.0) for m, v in u.items()) for _, _, u in rows]
            med = {k: _median([x[i] / pts for i, (kk, pts, _) in enumerate(rows) if kk == k])
                   for k in mixed}
            cheap = {i for i, (k, pts, _) in enumerate(rows)
                     if k in mixed and x[i] < (1 - SIZING_CHEAP) * med[k] * pts}
            if cheap <= left:
                break
            left |= cheap
        bad = [m for m in weighed if err.get(m) is not None
               and not (rho[m] > 0 and err[m] <= SIZING_WEIGHT_AGREE)]
        if not bad:
            break
        worst = max(bad, key=lambda m: (err[m], m))
        weighed.remove(worst)
        failed[worst] = err[worst]
        rho[worst] = 1.0
    # What the weights leave unexplained: the mixed windows' rows they were
    # fitted over (a window of one model has its own noise and no weight).
    x = [sum(v * rho.get(m, 1.0) for m, v in u.items()) for _, _, u in rows]
    idx = [i for i in use if rows[i][0] in mixed]
    inv_pi = _sizing_fit_sizes(rows, idx, rho)
    out = {}
    for m in models:
        if m == ref:
            basis, ratio, rel = "reference", 1.0, None
        elif m in weighed:
            basis, ratio, rel = "weighed", rho[m], err[m]
        else:
            basis, ratio, rel = "unweighed", 1.0, failed.get(m)
        out[m] = {"ratio": ratio, "rel_error": rel if rel is None or math.isfinite(rel) else None,
                  "basis": basis, "dollars": dollars[m], "steps": steps[m]}
    return {"reference": ref, "ratios": {m: rho[m] for m in sorted(weighed)}, "models": out,
            "fit": {"samples": len(rows), "trimmed": len(left), "windows": len(wins),
                    "mixed": len(mixed), "rounds": rounds,
                    "max_residual_points": max([abs(rows[i][1] - inv_pi[rows[i][0]] * x[i])
                                                for i in idx], default=None)}}


def _sizing_refit(t, provider):
    """Fit the provider's weights over its windows' kept samples (67 §3) with
    the per-window check beside it, then judge each of its windows again at
    them. Logged: a change of reference or of which models are weighed, and
    a window's disagreement as it starts or ends; a weight that only moves
    is not (/sizing and sizing_weight show it). Caller holds _sizing_lock."""
    keys = sorted(k for k in t.sizing["windows"]
                  if (t.sizing["accounts"].get(k[0]) or {}).get("provider") == provider)
    rows = []
    for k in keys:
        w = t.sizing["windows"][k]
        rows += [(k, s["points"], _sizing_model_usd(s.get("by")))
                 for s in _sizing_kept(w["samples"], _sizing_now(t, k[0], w))]
    fit = _sizing_fit(rows, whole={k for k in keys if t.sizing["windows"][k]["scope"] is None})
    own, off = {}, {}
    for k in keys:
        mine = [r for r in rows if r[0] == k]
        if fit["reference"] is None or len(mine) < SIZING_WEIGHT_STEPS_MIN:
            continue
        ow = {m: {"ratio": v["ratio"], "rel_error": v["rel_error"]}
              for m, v in _sizing_fit(mine, ref=fit["reference"])["models"].items()
              if v["basis"] == "weighed"}
        if ow:
            own[k] = ow
            # Only a weight the shared fit has: an unweighed model's 1 is no
            # weight to be apart from.
            apart = sorted(m for m in ow
                           if m in fit["ratios"] and _sizing_apart(ow[m], fit["ratios"][m]))
            if apart:
                off[k] = apart
    fit.update(own=own, disagree=off)
    old = t.sizing["weights"].get(provider)
    t.sizing["weights"][provider] = fit

    def shape(f):
        return (f["reference"], sorted(f["ratios"])) if f else (None, [])

    if shape(old) != shape(fit):
        log(f"sizing: {provider} weights: reference {fit['reference'] or '-'}; "
            + ("; ".join(f"{m} weighed {fit['ratios'][m]:.3f} (error {fit['models'][m]['rel_error']:.1%})"
                         for m in sorted(fit["ratios"])) or "none weighed"))
    was_off = (old or {}).get("disagree") or {}
    for k in sorted(set(was_off) | set(off)):
        if was_off.get(k, []) != off.get(k, []):
            log(f"sizing: {k[0]} {k[1]} weights "
                + ("disagree: " + ", ".join(f"{m} {own[k][m]['ratio']:.3f} here, "
                                           f"{fit['models'][m]['ratio']:.3f} shared" for m in off[k])
                   if k in off else "agree again"))
    for k in keys:
        w = t.sizing["windows"][k]
        _sizing_basis(t, k[0], k[1], w, _sizing_now(t, k[0], w), fit["ratios"])


def _sizing_apart(own, shared):
    """67 §3's per-window check: a window's own weight ({ratio, rel_error})
    is apart from the shared one when they differ by more than
    SIZING_WEIGHT_AGREE of the shared weight and by more than
    SIZING_WEIGHT_Z of the own weight's standard error, so a window's noise
    alone does not flag it. The shared fit holds the window's own rows, so
    the two vary together: the own error alone is the cautious bound on
    their difference's."""
    gap = abs(own["ratio"] - shared)
    return (gap > SIZING_WEIGHT_AGREE * shared
            and gap > SIZING_WEIGHT_Z * own["rel_error"] * own["ratio"])


def _sizing_rejudge(t, acct, wid, w):
    """The window's samples changed: its provider's weights are fitted again
    as the batch ends (_sizing_after), which judges every window of the
    provider; before its account knows its provider, judge this window
    alone, now."""
    t.sizing["resized"] = True
    prov = (t.sizing["accounts"].get(acct) or {}).get("provider")
    if prov:
        t.sizing["stale"].add(prov)
    else:
        _sizing_basis(t, acct, wid, w, _sizing_now(t, acct, w))


def _sizing_log_step(acct, wid, p, q, t, usd, n, outcome, weighted=None):
    """The step line; `weighted` is (the step's weighted dollars, its
    unweighed share) while the provider weighs a model (67 §10)."""
    when = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    money = "$-" if usd is None else f"${usd:.4f}"
    if usd is not None and weighted is not None:
        money += (f" (weighted ${weighted[0]:.4f}, unweighed "
                  + ("-" if weighted[1] is None else f"{weighted[1]:.0%}") + ")")
    log(f"sizing: {acct} {wid} {_pct_text(p)}→{_pct_text(q)} at {when} {money} over {n} events: {outcome}")


def _pct_text(v):
    return f"{v:g}"


def _sizing_step(t, acct, wid, w, b):
    """Close the step (T_p, T_q] from the anchor to its first pending
    boundary: cut its work (66 §3), judge it (§4), sample it (§5), add it
    to the measured range (§6), and anchor at (q, T_q). Caller holds
    _sizing_lock."""
    a = w["anchor"]
    p, q, lo, hi = a["q"], b["q"], a["t"], b["t"]
    w["pending"].pop(0)
    key = (acct, wid)
    evs = [e for e in _sizing_events_in(t, acct, lo, hi) if _scope_binds(w["scope"], e["scope"])]
    usd = sum(e["usd"] or 0.0 for e in evs if not e["paid"])
    # Hung-up requests counted at the middle of their range, and how far
    # either way their unseen output could take the step's work (66 §4).
    hung = [e for e in evs if _sizing_bounded(e) and not e["paid"]]
    spans = sum(e["span"] for e in hung)
    if a["fresh"]:
        # The first transition after a fresh anchor: nothing said when the
        # meter reached p, so the span is no step; the window anchors here.
        if key in t.sizing["restarted"]:
            _sizing_count(w, "brain_restart")
            _sizing_log_step(acct, wid, p, q, hi, usd, len(evs), "brain_restart")
        t.sizing["restarted"].discard(key)
        _sizing_anchor(w, q, hi, a["reset"], False, lo=b.get("lo"))
        return
    if q > 100 or p >= 100:
        outcome = "over_limit"
    elif any(e["why"] is not None and e["complete"] for e in evs):
        outcome = "unpriced"
    elif any(not e["complete"] and e["usd"] is None for e in evs) or spans > SIZING_UNSEEN * usd:
        outcome = "incomplete"
    elif any(e["paid"] for e in evs):
        outcome = "paid"
    elif any(g[0] < hi and g[1] > lo for g in t.sizing["gaps"]):
        outcome = "seq_gap"
    elif _sizing_restart_in(t, lo, hi):
        outcome = "proxy_restart"
    elif w["overflow_to"] is not None and lo < w["overflow_to"]:
        outcome = "overflow"
    elif not evs:
        outcome = "silent"
    elif _sizing_straddle(t, acct, w, lo, hi, b) > SIZING_FUZZ * usd:
        outcome = "fuzz"
    else:
        outcome = "sample"
    points = q - p
    data_t = (t.sizing["accounts"].get(acct) or {}).get("data_t") or hi
    if outcome in _SIZING_MEASURED:
        rg = _sizing_range_for(w, a["reset"], p)
        if rg["first"] is None:
            rg["first"] = lo
        rg["last"], rg["q"] = hi, q
        rg["points"] += points
        st = {"lo": lo, "hi": hi, "points": points, "silent": outcome == "silent",
              "work": 0.0, "by": {}}
        if outcome == "silent":
            rg["silent"] += points
        else:
            rg["work"] += usd
            st["work"] = usd
            for e in evs:
                _sizing_add_by(rg["by"], e)
                _sizing_add_by(st["by"], e)
        rg["steps"].append(st)
        del rg["steps"][:-SIZING_RANGE_STEPS_MAX]  # older steps are final in the range
        t.sizing["dirty"] = True
    _sizing_count(w, outcome)
    cheap = False
    # The step's dollars weighted at its provider's weights as they stand
    # before it joins them (67 §3, §4): what `cheap` judges, and the line says.
    rho = _sizing_rho(t, acct)
    mu = {}
    for e in evs:
        if e["usd"] is not None and not e["paid"]:
            mu[e["model"]] = mu.get(e["model"], 0.0) + e["usd"]
    wusd = sum(v * rho.get(m, 1.0) for m, v in mu.items()) if rho else usd
    if outcome == "sample":
        s_pt = _sizing_learned(w, data_t, rho)
        cheap = s_pt is not None and wusd < (1 - SIZING_CHEAP) * s_pt * points
        by = {}
        for e in evs:
            _sizing_add_by(by, e)
        w["samples"].append({"from": lo, "to": hi, "p": p, "q": q, "points": points,
                             "events": len(evs), "usd": usd, "by": by, "cheap": cheap,
                             "bounded": bool(hung), "prices": _api_prices_hash()})
        w["samples"] = _sizing_kept(w["samples"], data_t)
        if cheap:
            _sizing_count(w, "cheap")
        if hung:
            _sizing_count(w, "bounded")
        # The growth retire judges at the weights as they stand, as `cheap`
        # does; the weights take the sample in as the batch ends, and that
        # judges the provider's windows again (67 §3).
        _sizing_shift(t, acct, wid, w)
        _sizing_rejudge(t, acct, wid, w)
        t.sizing["dirty"] = True
    note = "sample (cheap)" if cheap else outcome
    if hung:
        note += (f"; {len(hung)} hung-up, spans ${spans:.4f} against "
                 f"{SIZING_UNSEEN:.0%} of the work, ${SIZING_UNSEEN * usd:.4f}")
    _sizing_log_step(acct, wid, p, q, hi, usd, len(evs), note,
                     (wusd, _sizing_unweighed_share(mu, _sizing_weights_of(t, acct))) if rho else None)
    _sizing_anchor(w, q, hi, a["reset"], False, lo=b.get("lo"))
    if w["overflow_to"] is not None and w["overflow_to"] <= hi:
        w["overflow_to"] = None


def _sizing_shift(t, acct, wid, w):
    """A provider's limit grew (66 §5): the newest SIZING_SHIFT_N samples
    each more than SIZING_SHIFT above the median of those before them
    retire the older ones. Never downward: a shrink flows through the
    median (66 §15.3). Per point in weighted dollars (67 §4). -> True when
    it retired any."""
    s = w["samples"]
    if len(s) <= SIZING_SHIFT_N:
        return False
    rho = _sizing_rho(t, acct)
    pp = [_sizing_weighted(x["usd"], x.get("by"), rho) / x["points"] for x in s]
    older = _median(pp[:-SIZING_SHIFT_N])
    if older > 0 and all(v > (1 + SIZING_SHIFT) * older for v in pp[-SIZING_SHIFT_N:]):
        w["samples"] = s[-SIZING_SHIFT_N:]
        log(f"sizing: {acct} {wid} samples retired: the window grew")
        return True
    return False


def _sizing_basis(t, acct, wid, w, now, rho=None):
    """The window's basis at its provider's weights (or rho), logged as it
    turns to disagree."""
    _, basis, spread, _ = _sizing_estimate(_sizing_kept(w["samples"], now),
                                           _sizing_rho(t, acct) if rho is None else rho)
    if basis == "disagree" and w["basis"] != "disagree":
        log(f"sizing: {acct} {wid} samples disagree: spread "
            f"{'unknown' if spread is None else f'{spread:.2f}'} over "
            f"{len(_sizing_kept(w['samples'], now))} steps")
    w["basis"] = basis


def _sizing_settle_account(t, acct):
    """Close the account's header-sized steps its data time has settled, then
    let go of what no window needs. Caller holds _sizing_lock."""
    a = t.sizing["accounts"].get(acct)
    if a is None:
        return
    moved = False
    for wid, w in _sizing_acct_windows(t, acct):
        if w["mode"] == "header":
            moved |= _sizing_close(t, acct, wid, w, a["data_t"])
            moved |= _sizing_expire(t, acct, wid, w, a["data_t"])
    return moved


def _sizing_after(t, acct, moved):
    """End a batch (one served event, refusal or poll, under _sizing_lock):
    fit the weights again of each provider whose samples changed in it (67
    §3), once, so a proxy restart that takes back a sample of every window
    fits once; then tidy after anchors moved, and save what changed (66
    §8); then, when sizes may have moved, mark the document (67 §8)."""
    for prov in sorted(t.sizing["stale"]):
        _sizing_refit(t, prov)
    t.sizing["stale"].clear()
    if moved:
        _sizing_prune(t, acct)
        _sizing_prune_gaps(t)
    # The pace's rings ride on the batch's save; with none due, they wait at
    # most BURN_SAVE_S since the last one (69 §4.2), never a save per event.
    if t.sizing.pop("dirty", False) or (
            t.sizing.get("burn_unsaved")
            and time.time() - t.sizing.get("saved_at", 0.0) >= BURN_SAVE_S):
        _sizing_save(t)
    if t.sizing.pop("resized", False):
        # A sample closed, retired or was taken back, or the weights moved:
        # the document's fleet reads the sizes (67 §8), so it may have changed.
        _events_mark(t)


def _sizing_bounded(e):
    """A buffered hung-up request counted at the middle of its range (66 §4,
    §8): incomplete, with its dollars and its span."""
    return not e["complete"] and e["usd"] is not None


def _sizing_take(t, acct, provider, ev, usd, why, speed, tier, reporter="", bound=None):
    """A served event, applied (66 §10): note its boot (a reporter's restart
    takes back the samples it falls in), take back a closed step it is late
    for, buffer it, apply its readings as heads, then close the steps its
    read_at settles. Only served events move data time. A hung-up request
    with a bound (_served_bound) is buffered at its middle, its token totals
    carrying the half-limit output, with its span beside it (66 §4)."""
    rt = ev["read_at"]
    model_speed = speed if speed is not None else ev["speed"]
    tokens, span = ev["tokens"], None
    priced = usd  # the pace counts list dollars only, never a bound's middle (69 §4.2)
    if bound is not None:
        usd, span, model_speed, tier, tokens = bound
    e = {"r": float(rt), "h": float(ev["head_at"]),
         "usd": usd, "why": why, "complete": ev["complete"], "paid": ev["paid"] is True,
         "model": ev["model"], "scope": _sizing_lane_scope(ev["model"]),
         "key": f"{ev['model']}|{model_speed}|{tier}" if usd is not None else None,
         "tokens": {k: n for k, n in tokens.items() if n},
         "boot": ev["boot"], "seq": ev["seq"]}
    if span is not None:
        e["span"] = span
    with t.sizing_lock:
        a = _sizing_acct(t, acct, provider)
        if (e["boot"], e["seq"]) in a["keys"]:
            return  # a resend of an event the loaded buffer holds
        _burn_take(t, ev["selection"], ev["member"], acct, priced, rt)
        new, restart = _sizing_boot(t, e["boot"], e["seq"], e["r"], reporter or "")
        if restart is not None:
            for (racct, wid), w in list(t.sizing["windows"].items()):
                for rt in restart:
                    _sizing_unknown(t, racct, wid, w, rt, "proxy_restart")
        used = new and _sizing_late(t, acct, e)
        a["data_t"] = max(a["data_t"], e["r"])
        moved = False
        for wid, rd in sorted(ev["readings"].items()):
            mode = _sizing_mode(provider, wid)
            if mode != "header" or rd.get("used_pct") is None:
                continue
            w = _sizing_win(t, acct, wid, mode)
            moved |= _sizing_head(t, acct, wid, w, e["h"], float(rd["used_pct"]),
                                  None if rd.get("resets_at") is None else float(rd["resets_at"]),
                                  e["r"])
        needed, anchored = _sizing_needs(t, acct, e)
        if needed or not anchored:
            bisect.insort(a["buf"], e, key=lambda x: x["r"])
            a["keys"].add((e["boot"], e["seq"]))
        elif not used:
            a["late"] += 1
        while len(a["buf"]) > SIZING_EVENTS_MAX:
            gone = a["buf"].pop(0)
            a["keys"].discard((gone["boot"], gone["seq"]))
            a["dropped"] += 1
            for _, w in _sizing_acct_windows(t, acct):
                hold = _sizing_hold_from(w)
                if hold is not None and gone["r"] > hold and _scope_binds(w["scope"], gone["scope"]):
                    w["overflow_to"] = max(w["overflow_to"] or gone["r"], gone["r"])
        moved |= bool(_sizing_settle_account(t, acct))
        if not moved:
            # An event no window needs leaves once its grace has passed.
            while a["buf"] and not _sizing_needs(t, acct, a["buf"][0])[1] \
                    and a["buf"][0]["r"] < a["data_t"] - SIZING_SETTLE_S:
                gone = a["buf"].pop(0)
                a["keys"].discard((gone["boot"], gone["seq"]))
        _sizing_after(t, acct, moved)


def _sizing_refusal(t, acct, provider, wid, at, seen):
    """An applied refusal's seen use is a head at its read_at (66 §2): on
    Anthropic the 429 is the only response that reads 100."""
    used = (seen or {}).get("used_pct")
    if used is None or _sizing_mode(provider, wid) != "header":
        return
    reset = (seen or {}).get("resets_at")
    with t.sizing_lock:
        _sizing_acct(t, acct, provider)  # data time is the served events' alone
        w = _sizing_win(t, acct, wid, "header")
        moved = _sizing_head(t, acct, wid, w, float(at), float(used),
                             None if reset is None else float(reset), float(at))
        moved |= bool(_sizing_settle_account(t, acct))
        _sizing_after(t, acct, moved)


def _sizing_poll(t, acct, res):
    """A landed poll's heads for the account's poll-sized limits (66 §7):
    any allowance limit with a used_pct, a window or a cycle pool."""
    rd = res.get("reading")
    provider = res.get("provider") if _target_kind(res) == "provider" else "anthropic"
    if not rd or provider is None:
        return
    heads = []
    at = _num(res.get("good_ts"))
    for lim in (_reading_pot(rd, ALLOWANCE_POT) or {}).get("limits") or []:
        if lim.get("used_pct") is None or not isinstance(lim.get("id"), str):
            continue
        if _sizing_mode(provider, lim["id"], lim.get("kind")) != "poll":
            continue
        h = _num(lim.get("as_of")) or _num(rd.get("as_of")) or _num(res.get("good_ts"))
        if h is None:
            continue
        heads.append((lim["id"], h, float(lim["used_pct"]), _num(lim.get("resets_at"))))
    if at is None and heads:
        at = max(h for _, h, _, _ in heads)
    with t.sizing_lock:
        if t.sizing_gate["closing"] or (not heads and acct not in t.sizing["accounts"]):
            return
        _sizing_acct(t, acct, provider)
        moved = False
        for wid, h, q, reset in heads:
            w = _sizing_win(t, acct, wid, "poll")
            if w["last_head"] is not None and h <= w["last_head"]:
                continue  # the same poll again
            moved |= _sizing_head(t, acct, wid, w, h, q, reset, h)
        if at is not None:
            polled = {wid for wid, _, _, _ in heads}
            for wid, w in _sizing_acct_windows(t, acct):
                if w["mode"] == "poll" and wid not in polled:
                    moved |= _sizing_expire(t, acct, wid, w, at)
        _sizing_after(t, acct, moved)


# ---- sizing: persistence (66 §8) ----

def _sizing_window_doc(w):
    return {"mode": w["mode"], "samples": w["samples"], "range": w["range"]}


def _sizing_open_doc(t):
    """The open state: anchors, pending boundaries, the event buffers, the
    boots and the gap intervals."""
    accts = {}
    for acct, a in t.sizing["accounts"].items():
        accts[acct] = {"provider": a["provider"], "data_t": a["data_t"], "buf": a["buf"]}
    wins = {}
    for (acct, wid), w in t.sizing["windows"].items():
        wins.setdefault(acct, {})[wid] = {k: w.get(k) for k in
                                          ("anchor", "pending", "overflow_to", "last_head")}
    return {"accounts": accts, "windows": wins,
            "boots": [[b, v] for b, v in t.sizing["boots"].items()], "gaps": t.sizing["gaps"],
            "reporters": t.sizing["reporters"], "restarts": t.sizing["restarts"]}


def _sizing_save(t, open_state=False, final=False):
    """Write SWITCH_SIZING_FILE atomically: the samples and the measured
    ranges always; the open state at a graceful shutdown only (final), after
    which nothing writes it again. Caller holds _sizing_lock. -> True when
    written."""
    if (t.sizing_gate["closing"] and not final) or t.sizing_gate["frozen"]:
        return False
    wins = {}
    for (acct, wid), w in t.sizing["windows"].items():
        wins.setdefault(acct, {})[wid] = _sizing_window_doc(w)
    doc = {"v": SIZING_FILE_VERSION, "prices": _api_prices_hash(), "windows": wins,
           "burn": _burn_doc(t.sizing["burn"])}
    if open_state:
        doc["open"] = _sizing_open_doc(t)
    try:
        _store.write_json(t.sizing_file, doc)
        t.sizing["burn_unsaved"] = False
        t.sizing["saved_at"] = time.time()
        return True
    except Exception as e:  # noqa: BLE001
        log(f"WARN sizing: could not write {t.sizing_file}: {e}")
        return False


def _sizing_shutdown(t):
    """The graceful shutdown's save: the open state goes too, so a deploy
    keeps the open step (66 §8, §15.4). From here no served event is taken
    (answered retry, so the proxy resends it to the next brain), no refusal
    or poll changes sizing, and no other save writes the file; served
    events already past the gate are waited for, so the saved state holds
    them."""
    with t.sizing_lock:
        t.sizing_gate["closing"] = True
        if not t.sizing_cv.wait_for(lambda: t.sizing_gate["inflight"] == 0,
                                   timeout=SIZING_SHUTDOWN_WAIT_S):
            log(f"WARN sizing: {t.sizing_gate['inflight']} report(s) still in flight "
                f"after {SIZING_SHUTDOWN_WAIT_S}s; saving without them")
        saved = _sizing_save(t, open_state=True, final=True)
        frozen = t.sizing_gate["frozen"]
    if saved:
        log(f"sizing: open state saved to {t.sizing_file}")
    elif frozen:
        log(f"sizing: open state not saved: sizing is frozen ({frozen})")
    else:
        log(f"WARN sizing: open state not saved: {t.sizing_file} could not be written")


def _price_row(model, speed, tier):
    """The price row a (model, speed, tier) was priced at, or None."""
    if tier == "long":
        over, rows = API_LONG_CONTEXT.get(model, (None, {}))
        return rows.get(speed)
    return (API_LIST_PRICES.get(model) or {}).get(speed)


def _sizing_reprice_by(by):
    """Token totals by "model|speed|tier" re-priced at the current table, in
    place -> their dollars, or None when the table no longer prices a
    model, speed, tier or kind in them."""
    total = 0.0
    for key, part in by.items():
        model, speed, tier = key.rsplit("|", 2)
        row = _price_row(model, speed, tier)
        if row is None:
            return None
        usd = 0.0
        for kind, n in part["tokens"].items():
            if n and row.get(kind) is None:
                return None
            usd += n * (row.get(kind) or 0.0)
        part["usd"] = usd / 1e6
        total += part["usd"]
    return total


def _sizing_reprice(sample):
    """A sample's dollars at the current table from its token totals (66 §5),
    or None when the table no longer prices it."""
    total = _sizing_reprice_by(sample["by"])
    if total is None:
        return None
    sample["usd"] = total
    sample["prices"] = _api_prices_hash()
    return sample


def _sizing_reprice_range(rg):
    """The measured range's work at the current table, as its samples are,
    so outside points never mix two tables; None (the range starts again)
    when the table no longer prices it."""
    if rg is None:
        return None
    total = _sizing_reprice_by(rg["by"])
    if total is None:
        return None
    for st in rg["steps"]:
        work = _sizing_reprice_by(st["by"])
        if work is None:
            return None
        st["work"] = work
    rg["work"] = total
    return rg


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _sizing_by_ok(by):
    return isinstance(by, dict) and all(
        isinstance(k, str) and k.count("|") >= 2 and isinstance(v, dict) and _is_num(v.get("usd"))
        and isinstance(v.get("tokens"), dict)
        and all(isinstance(kk, str) and _is_num(n) for kk, n in v["tokens"].items())
        for k, v in by.items())


def _sizing_doc_problem(doc):
    """What makes a version-1 SWITCH_SIZING_FILE unusable, or None: every
    field the engine reads, of its type, so a file that loads never fails
    later."""
    def nums(d, *ks, null=()):
        return isinstance(d, dict) and all(k in d and (_is_num(d[k]) or (k in null and d[k] is None))
                                           for k in ks)
    if not isinstance(doc, dict) or doc.get("v") != SIZING_FILE_VERSION:
        return f"not version {SIZING_FILE_VERSION}"
    if not isinstance(doc.get("windows"), dict):
        return "no windows"
    for acct, ws in doc["windows"].items():
        if not isinstance(ws, dict):
            return f"windows.{acct}"
        for wid, wd in ws.items():
            where = f"windows.{acct}.{wid}"
            if not isinstance(wd, dict) or wd.get("mode") not in ("header", "poll") \
                    or not isinstance(wd.get("samples"), list):
                return where
            for smp in wd["samples"]:
                if not nums(smp, "from", "to", "p", "q", "points", "events", "usd") \
                        or smp["points"] <= 0 or not _sizing_by_ok(smp.get("by")):
                    return f"{where}.samples"
            rg = wd.get("range")
            if rg is not None and not (
                    nums(rg, "reset", "first", "last", "q", null=("reset", "first", "last", "q"))
                    and nums(rg, "points", "work", "silent") and _sizing_by_ok(rg.get("by"))
                    and isinstance(rg.get("steps"), list)
                    and all(nums(st, "lo", "hi", "points", "work") and isinstance(st.get("silent"), bool)
                            and _sizing_by_ok(st.get("by")) for st in rg["steps"])):
                return f"{where}.range"
    op = doc.get("open")
    if op is None:
        return None
    if not isinstance(op, dict):
        return "open"
    for acct, ad in (op.get("accounts") or {}).items():
        if not nums(ad, "data_t") or not isinstance(ad.get("buf"), list):
            return f"open.accounts.{acct}"
        for e in ad["buf"]:
            if not (nums(e, "r", "h", "usd", null=("h", "usd")) and isinstance(e.get("tokens"), dict)
                    and all(_is_num(n) for n in e["tokens"].values())
                    and isinstance(e.get("boot"), str) and _is_num(e.get("seq"))
                    and all(k in e for k in ("why", "complete", "paid", "model", "scope", "key"))
                    and (e["usd"] is None or isinstance(e["key"], str))
                    # a hung-up request at its middle carries its span (66 §8)
                    and isinstance(e["complete"], bool)
                    and (e["complete"] or e["usd"] is None or _is_num(e.get("span")))):
                return f"open.accounts.{acct}.buf"
    for acct, ws in (op.get("windows") or {}).items():
        for wid, od in (ws or {}).items():
            where = f"open.windows.{acct}.{wid}"
            a = (od or {}).get("anchor")
            if a is not None and not (nums(a, "q", "t", "reset", "lo", null=("reset", "lo"))
                                      and isinstance(a.get("fresh"), bool)):
                return where
            if not isinstance((od or {}).get("pending"), list) or not all(
                    nums(b, "q", "t", "lo", null=("lo",)) for b in od["pending"]):
                return where
            if not all(od.get(k) is None or _is_num(od.get(k)) for k in ("overflow_to", "last_head")):
                return where
    if not all(isinstance(x, list) and len(x) == 2 and isinstance(x[0], str)
               and nums(x[1], "first", "last", "seq", "seq_at") for x in op.get("boots") or []):
        return "open.boots"
    for k in ("gaps", "restarts"):
        if not all(isinstance(g, list) and len(g) == 2 and all(map(_is_num, g)) for g in op.get(k) or []):
            return f"open.{k}"
    if not all(isinstance(r, str) and isinstance(v, dict) and isinstance(v.get("boot"), str)
               and _is_num(v.get("last")) for r, v in (op.get("reporters") or {}).items()):
        return "open.reporters"
    return None


def _sizing_freeze(t, why):
    """Sizing starts empty and writes nothing over SWITCH_SIZING_FILE; /sizing
    says why."""
    t.sizing_gate["frozen"] = why
    log(f"WARN sizing: {why}; sizing starts empty and writes nothing over {t.sizing_file}")


def _sizing_set_aside(t, why):
    """Move an unusable SWITCH_SIZING_FILE (it does not parse or does not
    check) to <file>.bad-<UTC time>, never over an earlier one, before
    anything can write over it; sizing starts empty."""
    base = f"{t.sizing_file}.bad-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    bad, n = base, 1
    while os.path.exists(bad):
        bad, n = f"{base}-{n}", n + 1
    try:
        os.replace(t.sizing_file, bad)
    except OSError as e:
        _sizing_freeze(t, f"{t.sizing_file} unusable ({why}) and not set aside ({e})")
        return
    t.sizing_gate["set_aside"] = bad
    log(f"WARN sizing: {t.sizing_file} unusable ({why}); set aside as {bad}, sizing starts empty")


def _sizing_load(t):
    """Read SWITCH_SIZING_FILE before /reports is served (66 §8): the samples
    and the measured ranges, re-priced at the current table; the open state
    when a graceful shutdown left one, which is then taken off the file so a
    later crash never resumes from it. Without it, each window found is
    marked: its first step from here is brain_restart. A file that does not
    parse or check (not JSON, another version, a field missing or of the
    wrong type) is set aside as <file>.bad-<UTC time> before anything is
    written; one that can't be read is left where it is and nothing writes
    over it. Either way sizing starts empty: a sizing file never keeps the
    brain from serving."""
    try:
        raw = _store.read_bytes(t.sizing_file)
    except credstore.CredStoreError as e:  # the wrong form for the store's mode
        _sizing_set_aside(t, str(e))
        return
    except OSError as e:  # the file may be good: leave it, and write nothing over it
        _sizing_freeze(t, f"cannot read {t.sizing_file}: {e}")
        return
    if raw is None:
        return
    try:
        doc = json.loads(raw)
    except (ValueError, RecursionError) as e:
        _sizing_set_aside(t, f"not JSON: {e}")
        return
    why = _sizing_doc_problem(doc)
    if why is None:
        try:
            st, dropped, ranges_dropped = _sizing_state_from(doc)
        except Exception as e:  # noqa: BLE001
            why = f"{type(e).__name__}: {e}"
    if why is not None:
        _sizing_set_aside(t, why)
        return
    opened = isinstance(doc.get("open"), dict)
    # The pace's buckets (69 §4.2): a malformed `burn` is dropped with a log
    # line and the rest of the file kept; a file without one (an older
    # brain's) starts the pace afresh.
    burn_dropped = None
    if "burn" in doc:
        burn, burn_dropped = _burn_from_doc(doc["burn"])
        if burn is not None:
            st["burn"] = burn
        else:
            log(f"WARN sizing: the pace's buckets in {t.sizing_file} were dropped: "
                f"{burn_dropped} does not check")
    with t.sizing_lock:
        t.sizing = st
        # The weights from the re-priced samples (67 §3); an account the open
        # state didn't name joins its provider's fit at its first event.
        for prov in sorted({a["provider"] for a in st["accounts"].values() if a.get("provider")}):
            _sizing_refit(t, prov)
        if opened or dropped or ranges_dropped or burn_dropped:
            _sizing_save(t)  # the open state is good for this start only
    _events_mark(t)  # the fleet reads the sizes just read (67 §8)
    n = sum(len(w["samples"]) for w in st["windows"].values())
    log(f"sizing: {n} samples of {len(st['windows'])} windows read from {t.sizing_file}"
        + (f", {dropped} dropped: the price table no longer prices them" if dropped else "")
        + (f", {ranges_dropped} measured range(s) started again: the price table no longer "
           "prices them" if ranges_dropped else "")
        + ("; open state resumed" if opened
           else "; no open state (the last stop was not graceful): open steps are brain_restart"))


def _sizing_state_from(doc):
    """The engine's state from a checked file (_sizing_doc_problem) -> (state,
    samples dropped, ranges dropped)."""
    st = _sizing_fresh()
    dropped = ranges_dropped = 0
    for acct, ws in doc["windows"].items():
        for wid, wd in ws.items():
            rg = wd.get("range")
            w = {"mode": wd["mode"], "scope": _sizing_window_scope(wid), "anchor": None,
                 "pending": [], "overflow_to": None, "last_head": None, "samples": [],
                 "range": _sizing_reprice_range(rg), "counts": {}, "retracted": {},
                 "basis": "none"}
            ranges_dropped += rg is not None and w["range"] is None
            for smp in wd["samples"]:
                smp = _sizing_reprice(smp)
                if smp is None:
                    dropped += 1
                else:
                    w["samples"].append(smp)
            st["windows"][(acct, wid)] = w
    opened = doc.get("open")
    if isinstance(opened, dict):
        for acct, ad in (opened.get("accounts") or {}).items():
            buf = sorted(ad["buf"], key=lambda e: e["r"])
            st["accounts"][acct] = {"provider": ad.get("provider"), "data_t": ad["data_t"],
                                    "buf": buf, "keys": {(e["boot"], e["seq"]) for e in buf},
                                    "dropped": 0, "late": 0}
        for acct, ws in (opened.get("windows") or {}).items():
            for wid, od in (ws or {}).items():
                w = st["windows"].get((acct, wid))
                if w is None:
                    continue
                w.update({k: od.get(k) for k in ("anchor", "pending", "overflow_to", "last_head")})
        st["boots"] = OrderedDict((b, v) for b, v in opened.get("boots") or [])
        st["gaps"] = [list(g) for g in opened.get("gaps") or []]
        st["restarts"] = [list(g) for g in opened.get("restarts") or []]
        st["reporters"] = {r: dict(v) for r, v in (opened.get("reporters") or {}).items()}
    else:
        st["restarted"] = set(st["windows"])
    for (acct, wid), w in st["windows"].items():
        _, w["basis"], _, _ = _sizing_estimate(
            _sizing_kept(w["samples"], (st["accounts"].get(acct) or {}).get("data_t")
                         or max([smp["to"] for smp in w["samples"]] or [0])))
    return st, dropped, ranges_dropped


# ---- sizing: publication (66 §9) ----

def _sizing_sized_windows(t):
    """{acct: {wid: mode}} for every measured account's sized windows, from
    its reading: what /sizing and the gauges list, sized or not yet."""
    with t.lock:
        snap = list(t.state.values())
    out = {}
    for r in snap:
        rd = r.get("reading")
        if not rd or r.get("good_ts") is None:
            continue
        provider = r.get("provider") if _target_kind(r) == "provider" else "anthropic"
        for lim in (_reading_pot(rd, ALLOWANCE_POT) or {}).get("limits") or []:
            mode = _sizing_mode(provider, lim.get("id") or "", lim.get("kind"))
            if mode is not None:
                out.setdefault(r["name"], {})[lim["id"]] = mode
    return out


def _sizing_window_view(t, acct, wid, w, now, full):
    """A window's sizes (67 §2): `size` the mix size, list dollars per point
    of what has run on it lately (× 100), at its provider's weights;
    size_ref the same in reference-model dollars; `sizes` per model for the
    reference and the weighed models its scope binds. The per-point values,
    the spread and the outside sum are in weighted dollars; with no weighed
    model all of it reads as 66 §9 did."""
    wt = _sizing_weights_of(t, acct) or {}
    rho = wt.get("ratios") or {}
    kept = _sizing_kept(w["samples"], now) if w else []
    size_ref, basis, spread, med = _sizing_estimate(kept, rho)
    mu = {}
    for s in kept:
        for m, v in _sizing_model_usd(s.get("by")).items():
            mu[m] = mu.get(m, 0.0) + v
    total = sum(mu.values())
    mix = sum(v * rho.get(m, 1.0) for m, v in mu.items()) / total if rho and total > 0 else 1.0
    scope = w["scope"] if w else _sizing_window_scope(wid)
    ref = wt.get("reference")
    sizes = {m: size_ref / rho.get(m, 1.0) for m in ([ref] if ref else []) + sorted(rho)
             if size_ref is not None and _scope_binds(scope, _sizing_lane_scope(m))}
    out = {"size": None if size_ref is None else size_ref / mix, "size_ref": size_ref,
           "sizes": sizes, "size_basis": basis, "size_mode": w["mode"] if w else None,
           "spread": spread, "median_per_point": med, "samples_n": len(kept),
           "newest_per_point": (_sizing_weighted(kept[-1]["usd"], kept[-1].get("by"), rho)
                                / kept[-1]["points"] if kept else None),
           "unweighed_share": _sizing_unweighed_share(mu, wt) if wt else None,
           "own_weights": (wt.get("own") or {}).get((acct, wid)) or {},
           "weights_disagree": (wt.get("disagree") or {}).get((acct, wid)) or []}
    rg = (w or {}).get("range")
    s_pt = med if basis == "learned" else None
    if rg and rg["points"] > 0:
        work = _sizing_weighted(rg["work"], rg["by"], rho)
        outside = (max(0.0, rg["points"] - work / s_pt) if s_pt else rg["silent"])
        out["outside"] = {"period": rg["reset"], "first": rg["first"], "last": rg["last"],
                          "measured_points": rg["points"], "work_usd": rg["work"],
                          "work_weighted": work, "outside_points": outside,
                          "outside_pct": 100 * outside / rg["points"]}
    else:
        out["outside"] = None
    if not full:
        return out
    by_model = {}
    for s in kept:
        models = {}
        for key, part in s["by"].items():
            m = key.rsplit("|", 2)[0]
            models[m] = models.get(m, 0.0) + part["usd"]
        for m, v in models.items():
            if s["usd"] > 0 and v >= SIZING_DOMINANT * s["usd"]:
                by_model.setdefault(m, []).append(s["usd"] / s["points"])
    out["per_model"] = {m: {"median_per_point": _median(v), "samples": len(v)}
                        for m, v in sorted(by_model.items())}
    out["samples"] = [dict(s, weighted=_sizing_weighted(s["usd"], s.get("by"), rho)) for s in kept]
    out["steps"] = dict(sorted((w or {}).get("counts", {}).items()))
    out["retracted"] = dict(sorted((w or {}).get("retracted", {}).items()))
    a = (w or {}).get("anchor")
    out["open"] = {"anchor": a, "pending": (w or {}).get("pending") or [],
                   "events_held": sum(1 for e in ((t.sizing["accounts"].get(acct) or {}).get("buf") or [])
                                      if w and _scope_binds(w["scope"], e["scope"])
                                      and _sizing_hold_from(w) is not None
                                      and e["r"] > _sizing_hold_from(w))}
    return out


def _sizing_snapshot(t, full):
    """{acct: {"windows": {wid: view}, ...}}: every measured account's sized
    windows and every window the engine holds. Pure: marks nothing."""
    listed = _sizing_sized_windows(t)
    with t.sizing_lock:
        accts = {}
        keys = {(a, wid) for a, ws in listed.items() for wid in ws} | set(t.sizing["windows"])
        for acct, wid in sorted(keys):
            w = t.sizing["windows"].get((acct, wid))
            a = t.sizing["accounts"].get(acct) or {}
            now = a.get("data_t") or max([s["to"] for s in (w or {}).get("samples") or []] or [0])
            view = copy.deepcopy(_sizing_window_view(t, acct, wid, w, now, full))
            if view["size_mode"] is None:
                view["size_mode"] = listed.get(acct, {}).get(wid)
            ad = accts.setdefault(acct, {"windows": {}})
            ad["windows"][wid] = view
        for acct, ad in accts.items():
            a = t.sizing["accounts"].get(acct) or {}
            ad.update({"events_held": len(a.get("buf") or []), "data_t": a.get("data_t") or None,
                       "events_dropped": a.get("dropped", 0), "events_late": a.get("late", 0)})
        # Per provider (67 §3): its reference, each model's weight, error,
        # basis, dollars and steps, and the fit's counts.
        extra = {"weights": copy.deepcopy({p: {k: f[k] for k in ("reference", "models", "fit")}
                                           for p, f in sorted(t.sizing["weights"].items())})}
        if full:
            extra.update({"boots": [dict(v, boot=b) for b, v in t.sizing["boots"].items()],
                          "gaps": [list(g) for g in t.sizing["gaps"]]})
    return accts, extra


def sizing_payload(t):
    """GET /sizing (66 §9): the live check's instrument, never the document."""
    accts, extra = _sizing_snapshot(t, full=True)
    with t.sizing_lock:
        fd = {"path": t.sizing_file, "frozen": t.sizing_gate["frozen"] is not None,
              "frozen_why": t.sizing_gate["frozen"], "set_aside": t.sizing_gate["set_aside"]}
        burn = _burn_payload(t, time.time())
    return dict({"as_of": time.time(), "unit": "api_usd", "prices": _api_prices_hash(),
                 "file": fd,
                 "constants": {"settle_s": SIZING_SETTLE_S, "fuzz": SIZING_FUZZ,
                               "unseen": SIZING_UNSEEN, "cheap": SIZING_CHEAP,
                               "samples_keep": SIZING_SAMPLES_KEEP,
                               "sample_age_s": SIZING_SAMPLE_AGE_S,
                               "samples_min": SIZING_SAMPLES_MIN, "agree": SIZING_AGREE,
                               "shift_n": SIZING_SHIFT_N, "shift": SIZING_SHIFT,
                               "dominant": SIZING_DOMINANT, "events_max": SIZING_EVENTS_MAX,
                               "weight_agree": SIZING_WEIGHT_AGREE,
                               "weight_share": SIZING_WEIGHT_SHARE,
                               "weight_steps_min": SIZING_WEIGHT_STEPS_MIN,
                               "weight_iters": SIZING_WEIGHT_ITERS,
                               "weight_z": SIZING_WEIGHT_Z,
                               "burn_hours": BURN_HOURS,
                               "burn_min_servable_h": BURN_MIN_SERVABLE_H,
                               "forecast_h": FORECAST_H},
                 "accounts": accts, "burn": burn}, **extra)


def _sizing_metric_lines(t):
    _line = _tenant_line(t.id)
    accts, extra = _sizing_snapshot(t, full=False)
    with t.sizing_lock:
        counts = {k: dict(w["counts"]) for k, w in t.sizing["windows"].items()}
        retracted = {k: dict(w.get("retracted") or {}) for k, w in t.sizing["windows"].items()}
    lines = []
    for acct, ad in sorted(accts.items()):
        for wid, v in sorted(ad["windows"].items()):
            lab = {"account": acct, "window": wid}
            if v["size_basis"] == "learned":
                lines.append(_line("sizing_size_api_usd", lab, round(v["size"], 6)))
                for m, s in sorted(v["sizes"].items()):
                    lines.append(_line("sizing_model_size_api_usd", dict(lab, model=m), round(s, 6)))
            for m in sorted(v["own_weights"]):
                lines.append(_line("sizing_weights_disagree", dict(lab, model=m),
                                   int(m in v["weights_disagree"])))
            if v["spread"] is not None:
                lines.append(_line("sizing_spread", lab, round(v["spread"], 6)))
            lines.append(_line("sizing_samples", lab, v["samples_n"]))
            if v["newest_per_point"] is not None:
                lines.append(_line("sizing_step_api_usd", lab, round(v["newest_per_point"], 6)))
            for outcome, n in sorted(counts.get((acct, wid), {}).items()):
                lines.append(_line("sizing_steps_total", dict(lab, outcome=outcome), n))
            for outcome, n in sorted(retracted.get((acct, wid), {}).items()):
                lines.append(_line("sizing_samples_retracted_total", dict(lab, outcome=outcome), n))
            o = v["outside"]
            if o is not None:
                lines.append(_line("sizing_outside_points", lab, round(o["outside_points"], 6)))
                lines.append(_line("sizing_measured_points", lab, round(o["measured_points"], 6)))
                lines.append(_line("sizing_outside_pct", lab, round(o["outside_pct"], 6)))
        lines.append(_line("sizing_events_dropped_total", {"account": acct}, ad["events_dropped"]))
        lines.append(_line("sizing_events_late_total", {"account": acct}, ad["events_late"]))
    for prov, f in extra["weights"].items():
        for m, d in sorted(f["models"].items()):
            if d["basis"] != "unweighed":
                lines.append(_line("sizing_weight", {"provider": prov, "model": m}, round(d["ratio"], 6)))
    return lines


# ---- pace: served dollars over the hours a selection could serve (quota-model 69 §4.2) ----
#
# Per tenant, hourly buckets of priced served dollars for the last
# BURN_HOURS, one ring per selection (_burn_selection: a served event's
# `selection` when it names a class, else its member's bare lane), per
# account, and one for the whole proxy (every served event, whatever its
# selection or none). The pace
# is the dollars over the hours the selection could serve, not over the
# clock: behind a wall nothing is served, and an average over every hour
# would fall exactly while a way through is worth most. A bucket is servable
# when a served event of the ring landed in it, or when a render of the
# figure in that hour counted at least one of the ring's accounts with room
# that was neither resting nor refused (_burn_stamp). An hour with neither
# keeps the last render's stamp, since the standings do not change without a
# reading. The pace reads complete hours only: the current hour joins it
# when it ends, so a hand sum over the same hours matches. Unpriced and
# incomplete events add no dollars and are counted beside. The rings live in
# t.sizing["burn"] and are saved with the sizing file under its own key,
# `burn`: every take marks it unsaved, and the batch's save carries it,
# with at most BURN_SAVE_S between saves while it waits (_sizing_after).

BURN_HOURS = 168             # the pace's hours (69 §4.2)
BURN_MIN_SERVABLE_H = 24     # servable hours before a pace is said; under it, no pace yet
FORECAST_H = 7 * 86400       # the forecast's horizon after now (69 §4.3)
BURN_SAVE_S = 60             # the longest a taken event's dollars wait for a save
_BURN_LEVELS = ("selections", "accounts")


def _burn_fresh():
    """{"selections": {key: ring}, "accounts": {acct: ring}, "proxy": ring}.
    A ring is {"hours": {hour: bucket}, "carry": the last render stamp of the
    hours that rolled off (None for none)}; an hour is int(epoch // 3600); a
    bucket is {usd, n (priced events), un (unpriced events), s (servable at
    any stamp or event of the hour), l (the hour's last render stamp, None
    for none)}."""
    return {"selections": {}, "accounts": {}, "proxy": _burn_ring()}


def _burn_ring():
    return {"hours": {}, "carry": None}


def _burn_bucket(ring, hour):
    """The ring's bucket for the hour -> (bucket, created)."""
    b = ring["hours"].get(hour)
    if b is not None:
        return b, False
    b = ring["hours"][hour] = {"usd": 0.0, "n": 0, "un": 0, "s": False, "l": None}
    return b, True


def _burn_roll(ring, now):
    """Let go of the hours before the last BURN_HOURS complete ones, keeping
    the last render stamp among them as the carry the next hours inherit."""
    first = int(now // 3600) - BURN_HOURS
    for h in sorted(h for h in ring["hours"] if h < first):
        b = ring["hours"].pop(h)
        if b["l"] is not None:
            ring["carry"] = b["l"]


def _burn_selection(t, selection, member):
    """The selection a served event counts for: its `selection` when that
    names one of the tenant's classes; else the bare lane its member names
    (lane:<x>); else none. A class's member id is lane:<x> for an Anthropic
    lane and <provider>/<model> otherwise, so the member alone never names
    the class, and a request for a bare model through a lane has a model
    name, not a class, as its selection."""
    if isinstance(selection, str) and any(c["class"] == selection for c in t.classes):
        return selection
    if isinstance(member, str) and member.startswith("lane:") and len(member) > 5:
        return member
    return None


def _burn_take(t, selection, member, acct, usd, at, now=None):
    """A served event into its rings: the proxy's, its account's and, when it
    counts for one (_burn_selection), its selection's, in the hour of its
    read_at (`at`). Each bucket it lands in is servable. Every take marks
    the rings unsaved (_sizing_after). Caller holds _sizing_lock."""
    now = time.time() if now is None else now
    hour = int(float(at) // 3600)
    if hour < int(now // 3600) - BURN_HOURS:
        return  # older than the pace's hours: it counts in none of them
    burn = t.sizing["burn"]
    t.sizing["burn_unsaved"] = True
    rings = [burn["proxy"], burn["accounts"].setdefault(acct, _burn_ring())]
    sel = _burn_selection(t, selection, member)
    if sel is not None:
        rings.append(burn["selections"].setdefault(sel, _burn_ring()))
    for ring in rings:
        _burn_roll(ring, now)
        b, _ = _burn_bucket(ring, hour)
        if usd is None:
            b["un"] += 1
        else:
            b["usd"] += usd
            b["n"] += 1
        b["s"] = True


def _burn_servable_now(fleet):
    """{"selections": {key: bool}, "accounts": {acct: bool}, "proxy": bool}
    from a rendered figure: could the proxy serve each now, i.e. does it count
    an account with room that is neither resting nor refused?"""
    def open_(acct, e, resting):
        return (e.get("room") or 0) > 0 and e.get("refused") is None and acct not in resting
    sels = {k: any(open_(a, e, s.get("resting") or ()) for a, e in (s.get("accounts") or {}).items())
            for k, s in (fleet.get("selections") or {}).items()}
    resting = fleet["proxy"].get("resting") or ()
    accts = {a: open_(a, e, resting) for a, e in (fleet.get("accounts") or {}).items()}
    return {"selections": sels, "accounts": accts, "proxy": any(accts.values())}


def _burn_stamp(t, fleet, now):
    """Stamp the current hour's buckets from a rendered figure (69 §4.2).
    Takes _sizing_lock; never call it under _plock. Marks no save: the stamps
    go to the file with the next batch's save or at shutdown."""
    flags = _burn_servable_now(fleet)
    hour = int(now // 3600)
    with t.sizing_lock:
        burn = t.sizing["burn"]
        pairs = [(burn["proxy"], flags["proxy"])]
        for level in _BURN_LEVELS:
            for key, v in flags[level].items():
                pairs.append((burn[level].setdefault(key, _burn_ring()), v))
        for ring, v in pairs:
            b, _ = _burn_bucket(ring, hour)
            b["s"] = b["s"] or v
            b["l"] = v
        # Every ring rolls, and one with no hour left (a selection gone a
        # week) goes.
        _burn_roll(burn["proxy"], now)
        for level in _BURN_LEVELS:
            for ring in burn[level].values():
                _burn_roll(ring, now)
            for key in [k for k, r in burn[level].items() if not r["hours"]]:
                del burn[level][key]


def _burn_hours(ring, now, hours):
    """(usd, servable hours, priced events, unpriced events) over the ring's
    last `hours` complete hours, the current one left out until it ends. An
    hour with no bucket keeps the last render stamp before it."""
    cur = int(now // 3600)
    first = cur - BURN_HOURS
    lo = cur - hours
    stamp = ring.get("carry")
    usd = 0.0
    servable = n = un = 0
    for h in range(first, cur):
        b = ring["hours"].get(h)
        if b is not None and b["l"] is not None:
            stamp = b["l"]
        if h < lo:
            continue
        if b is None:
            servable += bool(stamp)
            continue
        usd += b["usd"]
        n += b["n"]
        un += b["un"]
        servable += bool(b["s"])
    return usd, servable, n, un


def _burn_pace(ring, now):
    """A ring's pace (69 §4.2, §6.1's `pace`): rate, the dollars over the
    servable hours of the last BURN_HOURS complete hours, null under
    BURN_MIN_SERVABLE_H of them (no pace yet, never 0); rate_24h the same
    over the last 24 complete hours, for show; servable_h; unpriced, the share of the events that carried no
    price (null with none)."""
    if ring is None:
        ring = _burn_ring()
    usd, sh, n, un = _burn_hours(ring, now, BURN_HOURS)
    usd24, sh24, _, _ = _burn_hours(ring, now, 24)
    learned = sh >= BURN_MIN_SERVABLE_H
    return {"rate": round(usd / sh, 4) if learned else None,
            "rate_24h": round(usd24 / sh24, 4) if learned and sh24 else None,
            "unit": "api_usd/h", "hours": BURN_HOURS, "servable_h": sh,
            "unpriced": round(un / (n + un), 4) if n + un else None,
            "basis": "learned" if learned else "no pace yet"}


def _burn_paces(t, now):
    """{"selections": {key: pace}, "accounts": {acct: pace}, "proxy": pace}.
    Caller holds _sizing_lock."""
    burn = t.sizing["burn"]
    return {"selections": {k: _burn_pace(r, now) for k, r in sorted(burn["selections"].items())},
            "accounts": {k: _burn_pace(r, now) for k, r in sorted(burn["accounts"].items())},
            "proxy": _burn_pace(burn["proxy"], now)}


def _burn_payload(t, now):
    """/sizing's `burn`: per selection, per account and for the proxy, each
    bucket {at (the hour's start), usd, events, unpriced, servable} and the
    ring's pace. Caller holds _sizing_lock."""
    burn = t.sizing["burn"]

    def one(ring):
        _burn_roll(ring, now)
        buckets = [{"at": h * 3600, "usd": round(b["usd"], 6), "events": b["n"],
                    "unpriced": b["un"], "servable": b["s"]}
                   for h, b in sorted(ring["hours"].items())]
        return dict({"buckets": buckets}, **_burn_pace(ring, now))
    return {"selections": {k: one(r) for k, r in sorted(burn["selections"].items())},
            "accounts": {k: one(r) for k, r in sorted(burn["accounts"].items())},
            "proxy": one(burn["proxy"])}


def _burn_doc(burn):
    """The rings as sizing.json's `burn`: each {carry, hours: [[hour, usd, n,
    un, s, l], ...]}."""
    def ring(r):
        return {"carry": r["carry"],
                "hours": [[h, round(b["usd"], 6), b["n"], b["un"], b["s"], b["l"]]
                          for h, b in sorted(r["hours"].items())]}
    return {"selections": {k: ring(r) for k, r in sorted(burn["selections"].items())},
            "accounts": {k: ring(r) for k, r in sorted(burn["accounts"].items())},
            "proxy": ring(burn["proxy"])}


def _burn_from_doc(raw):
    """sizing.json's `burn` -> (rings, None), or (None, why) when it does not
    check: the caller drops it and keeps the rest of the file."""
    def ring(r, where):
        if not isinstance(r, dict) or not isinstance(r.get("hours"), list) \
                or r.get("carry") not in (None, True, False):
            return None, where
        out = _burn_ring()
        out["carry"] = r["carry"]
        for row in r["hours"]:
            if not (isinstance(row, list) and len(row) == 6
                    and isinstance(row[0], int) and not isinstance(row[0], bool)
                    and _is_num(row[1]) and row[1] >= 0
                    and all(isinstance(x, int) and not isinstance(x, bool) and x >= 0
                            for x in row[2:4])
                    and isinstance(row[4], bool) and row[5] in (None, True, False)):
                return None, f"{where}.hours"
            out["hours"][row[0]] = {"usd": float(row[1]), "n": row[2], "un": row[3],
                                    "s": row[4], "l": row[5]}
        return out, None
    if not isinstance(raw, dict):
        return None, "burn"
    burn = _burn_fresh()
    got, why = ring(raw.get("proxy"), "burn.proxy")
    if why:
        return None, why
    burn["proxy"] = got
    for level in _BURN_LEVELS:
        rings = raw.get(level)
        if not isinstance(rings, dict):
            return None, f"burn.{level}"
        for key, r in rings.items():
            got, why = ring(r, f"burn.{level}.{key}")
            if why:
                return None, why
            burn[level][key] = got
    return burn, None


# ---- fleet: room across a selection, the proxy and each account (quota-model 65 §1, 67 §5-§8) ----
#
# The figure of 65 §1, per selection (every class and bare lane), for the
# whole proxy and per account: capacity = cap x size and room = (cap - used)
# x size, summed over the accounts that are sized, in dollars of the work
# each account would do: the model the selection's valuing member runs there
# (its size per point on the window), or the window's mix size where that
# model is not weighed (`valued: "mix"`, 67 §13 Q2). The whole proxy values
# every account at its own mix and its highest cap (Q3, Q4). It is built in
# two steps, since _sizing_lock is never held around _plock: the sizing view
# (_sizing_fleet_view, under _sizing_lock alone), then the figure under _plock
# from ctx and that view (_fleet_payload). Nothing in it follows the clock
# alone (64 §2): a reset that passes, or a reading that goes stale, flips it
# by the dial check's own rules.

_FLEET_PERIOD_UNITS = {"mo": 30 * 86400, "d": 86400, "h": 3600, "m": 60, "s": 1}


def _sizing_fleet_view(t):
    """The sizing view the figure reads (67 §8), from _sizing_snapshot's
    numbers: {"windows": {acct: {wid: {basis, mix, sizes, samples}}},
    "providers": {provider: {reference, models: {model:
    {basis, dollars}}}}}. `mix` and `sizes` are whole-window dollars (x 100
    the per-point ones) as /sizing publishes them: the mix size and the size
    in each model the window's sizes name. Nothing a served event moves
    without closing a sample (the outside sum) is in it, so such an event
    publishes no document (64 §2, 67 §8). `pace` is each selection's and the
    proxy's (_burn_paces, 69 §4.2): it moves with every served event and the
    clock, and the document's fields made from it are clock-only
    (PREFERRED_CLOCK_ONLY). Takes _lock, then _sizing_lock; never call it
    under _plock."""
    accts, extra = _sizing_snapshot(t, full=False)
    windows = {acct: {wid: {"basis": v["size_basis"], "mix": v["size"], "sizes": dict(v["sizes"]),
                            "samples": v["samples_n"]}
                      for wid, v in ad["windows"].items()}
               for acct, ad in accts.items()}
    providers = {p: {"reference": f["reference"],
                     "models": {m: {"basis": d["basis"], "dollars": d["dollars"]}
                                for m, d in f["models"].items()}}
                 for p, f in extra["weights"].items()}
    with t.sizing_lock:
        pace = _burn_paces(t, time.time())
    return {"windows": windows, "providers": providers, "pace": pace}


def _fleet_period_s(lim):
    """A limit's length in seconds, for ordering only: its period_s, else its
    period label ("1mo" a 30-day month, "7d", "5h"); None when neither says."""
    p = _num(lim.get("period_s"))
    if p:
        return p
    m = re.fullmatch(r"(\d+)(mo|d|h|m|s)", str(lim.get("period") or ""))
    return int(m.group(1)) * _FLEET_PERIOD_UNITS[m.group(2)] if m else None


def _fleet_limits(st):
    """{id: limit} of the account's allowance limits that have a length:
    its windows and its cycle pools (Kimi's monthly pool, 66 §7)."""
    allow = st.get("allowance") or {}
    out = {k: v for k, v in (allow.get("limits") or {}).items() if v.get("kind") == "cycle_pool"}
    out.update(allow.get("windows") or {})
    return out


def _fleet_longest(lims):
    """(id, limit) of the longest limit with a used_pct and a length, the
    first by id among equals; (None, None) for none."""
    best = None
    for wid, w in sorted(lims.items()):
        p = _fleet_period_s(w)
        if w.get("used_pct") is not None and p is not None and (best is None or p > best[0]):
            best = (p, wid, w)
    return (best[1], best[2]) if best else (None, None)


def _fleet_main(st):
    """The account's main window (67 §6): its longest unscoped allowance limit
    with a used_pct, a window or a cycle pool."""
    return _fleet_longest({k: w for k, w in _fleet_limits(st).items() if w.get("scope") is None})


def _fleet_used(w, now):
    """The use the figure counts on a window, as the dial check reads it: 0
    once a reset has passed since the reading (the window has reopened,
    §1.4), 100 while it is closed (at its limit or shut by its own gate), its
    used_pct otherwise. A hold already reads 100 (_hold_window)."""
    if w.get("resets_at") is not None and w["resets_at"] <= now:
        return 0.0
    if _window_closed(w, now):
        return 100.0
    return float(w["used_pct"])


def _fleet_stopped(w, cap, now):
    """Is the window at the route's cap there (the dial check's no: stop_at)?
    A cap of 0 holds across a reset (§1.9); otherwise a window whose reset has
    passed has reopened."""
    if cap >= 100:
        return False
    if cap <= 0:
        return True
    passed = w.get("resets_at") is not None and w["resets_at"] <= now
    return not passed and w.get("used_pct") is not None and w["used_pct"] >= cap


def _fleet_cap(route, st, wid):
    """The cap a route allows on one window of an account (67 §6): its
    stop_at's level when the stop_at names the window (a period or a scope,
    as _stop_window finds it; `effective` names every binding window), else
    100. A level at or above 100 is 100; below 0 is 0."""
    stop = route.get("stop_at")
    if not stop or stop.get("level") is None or stop["level"] >= 100:
        return 100.0
    windows = _binding_windows(st, route.get("scope"))
    if wid not in windows:
        return 100.0
    if str(stop.get("window")) != "effective" and _stop_window(stop, windows) is not windows[wid]:
        return 100.0
    return max(0.0, float(stop["level"]))


def _fleet_lane_model(view, provider, lane):
    """The model a bare lane is valued at (67 §5): the most-served of its
    provider's reference and weighed models that the lane would serve
    (most dollars, then by name); None, the mix, for none."""
    models = ((view.get("providers") or {}).get(provider) or {}).get("models") or {}
    cands = sorted((-d["dollars"], m) for m, d in models.items()
                   if d["basis"] in ("reference", "weighed")
                   and (_sizing_lane_cfg(m) or {}).get("lane") == lane)
    return cands[0][1] if cands else None


def _fleet_limit_binds(limit, scope, model, lane=None):
    """Does a no-window limit bind a selection's account (67 §9)? A pool
    binds every selection and the whole proxy. A model:<m> binds the member
    that runs m there (by its model id, exactly), and a route whose scope is
    m's lane scope (_sizing_lane_scope, case-blind): a limit on one Sonnet
    model binds the Sonnet lane. A model in no scoped lane binds by its id
    alone, and the whole proxy (no scope, no model) never. `lane` is a bare
    lane's (None for a class member, which dials its own model): the bare
    default lane is never bound by a model, for it serves every model no
    other lane matches, and one model's limit stops only that one, even when
    it is the model the lane is valued at. A class member on the default
    lane is bound by its model's id, as the proxy refuses it."""
    kind, _, name = limit.partition(":")
    if kind == "pool":
        return True
    if lane == "default":
        return False
    if model is not None and name == model:
        return True
    ls = _sizing_lane_scope(name)
    return ls is not None and scope is not None and str(ls).lower() == str(scope).lower()


def _fleet_limit_hold(limits, members):
    """The live limit that holds a selection off an account (67 §9), from
    the account's limits (_limit_snap) and the selection's members there,
    each (scope, model, bare lane): None while any member no limit binds is
    left, for the proxy falls through to it. Else the account is back when
    its first member frees, and a member frees when the last limit binding
    it ends: of each member's binding limits the one that ends last, and of
    those the one that ends first (the higher-priority member on a tie)."""
    held = None
    for scope, model, lane in members:
        bound = [lm for lm in limits if _fleet_limit_binds(lm["limit"], scope, model, lane)]
        if not bound:
            return None
        last = max(bound, key=lambda x: (x["until"], x["limit"]))
        if held is None or last["until"] < held["until"]:
            held = last
    return held


def _fleet_pct(room, capacity):
    return None if capacity <= 0 else round(100 * room / capacity, 1)


def _fleet_kind(why):
    """A listed account's kind (68 §2) from its why: unread (no reading has
    ever applied), not_quota (an api account, or one with no main window),
    else learning (a counted window not sized, or the qualifying scoped
    window not read)."""
    if why == "unmeasured":
        return "unread"
    if why in ("api account", "no main window"):
        return "not_quota"
    return "learning"


def _fleet_earliest(times):
    """The earliest of times; None for none, and None while any is unknown:
    a time is not said while the thing might come sooner (68 §2)."""
    return None if not times or None in times else min(times)


def _fleet_counted(binding, mid, main):
    """(counted, why) for one route's binding windows on an account (67 §6):
    the main window and the longest scoped window that binds the route and
    is no shorter than the main (a shorter one is a speed limit, as an
    unscoped one is: it rests or refuses the account, and is not counted).
    why is "<window> not read" when such a scoped window has no reading."""
    main_p = _fleet_period_s(main)
    scopes = {k: w for k, w in binding.items()
              if w.get("scope") is not None and (_fleet_period_s(w) or main_p) >= main_p}
    sid, scoped = _fleet_longest(scopes)
    if sid is None and scopes:
        return {mid: main}, f"{min(scopes)} not read"
    return ({mid: main} if sid is None else {mid: main, sid: scoped}), None


def _fleet_secs(t):
    """A time 68 adds to `fleet` as integer seconds, a fractional one rounded
    up (never said sooner than it is); None stays None."""
    return None if t is None else int(math.ceil(t))


def _fleet_closed(st, binding, counted, cap_of, mid, main, held, now):
    """One member's route on an account as the dial check closes it (67 §6,
    68 §3.1), judged before any size: {serves, resting, resting_until,
    refused, block}. A binding window is closed for the route when it is at
    its limit or shut by its own gate (spent), or at the route's cap there
    (no: stop_at); the provider's own gate shuts them all. The route rests
    the account when every closed window is shorter than the main and the
    main is open (room kept), and is refused otherwise, but for a stop_at
    reached on a window in `counted`, which is room 0 by the arithmetic
    alone (a listed account passes no counted windows: it has no arithmetic
    to carry it). `held`, a limit the brain has no window for that binds the
    member there (_fleet_limit_hold, 67 §9), refuses it too: the refusal
    shown is the one that ends later, a window's with no known end counting
    as later. A window the route caps at 0 holds across its reset (§1.9),
    spent or not, and ends only when the owner lifts the stop_at: its end is
    unknown, null. `block` is the refusal the route would show were it
    refused, set whenever it cannot serve; `serves` is the dial check's
    included."""
    main_p = _fleet_period_s(main)
    allow, reopen = _allowance_state(st, binding, now)
    gate = allow is False and (st.get("allowance") or {}).get("open") is False
    spent = {wid: w for wid, w in binding.items() if _window_closed(w, now)}
    stopped = {wid: w for wid, w in binding.items()
               if wid not in spent and _fleet_stopped(w, cap_of(wid), now)}
    closed = dict(spent, **stopped)
    # A main window that binds is in `closed` when the route's cap stops it; a
    # cycle pool binds no route, so its cap is 100 and only its limit closes it.
    main_open = mid not in closed and not _window_closed(main, now)

    def end(wid, w):
        return None if cap_of(wid) <= 0 else w.get("resets_at")
    why = "spent" if gate or spent else "stop_at"
    refused, resting, resting_until, block = None, False, None, None
    if closed and not gate and main_open and \
            all((_fleet_period_s(w) or main_p) < main_p for w in closed.values()):
        # Every closed window is shorter than the main: a speed limit (65 §5).
        resting = True
        ends = [end(wid, w) for wid, w in closed.items()]
        resting_until = None if None in ends else max(ends)
        block = {"why": why, "until": resting_until}
    elif gate or closed:
        # The spent windows' resets (the allowance's reopen), or, for a gate
        # with none at its limit, when the gate's windows have all reset.
        times = ([end(wid, w) for wid, w in spent.items()] if spent else
                 [reopen] if allow is False else []) + [end(wid, w) for wid, w in stopped.items()]
        block = {"why": why, "until": None if None in times else max(times)}
        if gate or spent or any(wid not in counted for wid in stopped):
            refused = block
        # Else a stop_at reached on a counted window only: room 0 by the
        # arithmetic, not refused.
    if held is not None and (refused is None or (
            refused["until"] is not None and held["until"] > refused["until"])):
        refused = block = {"why": held["class"], "limit": held["limit"], "until": held["until"]}
        resting, resting_until = False, None
    return {"serves": not closed and not gate and held is None, "resting": resting,
            "resting_until": resting_until, "refused": refused, "block": block}


def _fleet_judge(closures):
    """(valuing member's index, refused, resting, resting_until) of an
    account over the members of a selection that reach it, in priority
    order (68 §3.4): the highest-priority member whose route can serve it
    now values it, open. When none can, the account rests if any member
    rests, valued at the highest-priority one that rests, room kept, until
    the first member is back: the earliest of each resting member's rest end
    and each other member's refusal end, null while any of those is unknown.
    When none rests, the highest-priority member values it: not refused
    when each is held only by a stop_at on a window it counts (room 0 by the
    arithmetic), and otherwise refused, back when the first member frees:
    that member's refusal, its `until` null while any member's end is
    unknown."""
    for i, c in enumerate(closures):
        if c["serves"]:
            return i, None, False, None
    ends = [c["block"]["until"] for c in closures]
    first = None if None in ends else min(ends)
    for i, c in enumerate(closures):
        if c["resting"]:
            return i, None, True, first
    if all(c["refused"] is None for c in closures):
        return 0, None, False, None
    j = min(range(len(closures)), key=lambda i: (ends[i] is None, ends[i] or 0, i))
    refused = dict(closures[j]["block"])
    refused["until"] = first
    return 0, refused, False, None


def _fleet_account(acct, st, members, view, now, stale_max):
    """One account's room for a selection (67 §6, 68 §3): -> (got, listed,
    v). `members` are the selection's members that reach the account, in
    priority order, each {scope, model, cap_of, held}; v is the index of the
    one that values it (_fleet_judge). got is None and listed the listed
    entry's fields ({why, kind, window, left_pct, refused, resting,
    resting_until}) when the account is listed, not added; else got =
    (entry, capacity, room, more) with the two unrounded and `more` the
    moment the entry's figure rises (68 §2's more_at) in a list, [None] for
    one unknown, [] for none, and listed None. The windows counted are the valuing member's
    (_fleet_counted); for the whole proxy, whose scope is None, the main
    alone. Each must be learned, and the account takes the smaller capacity
    and the smaller room of them (65 §5). Whether it is closed is judged
    before the size (68 §3.1), so a listed account is refused by exactly
    the rules that refuse a counted one."""
    def listed(why, **kw):
        out = {"why": why, "kind": _fleet_kind(why), "window": None, "left_pct": None,
               "refused": None, "resting": False, "resting_until": None}
        out.update(kw)
        return None, out, 0
    if not st or st.get("as_of") is None:
        return listed("unmeasured")
    if st.get("kind") == "api":
        return listed("api account")
    mid, main = _fleet_main(st)
    if mid is None:
        return listed("no main window")
    sized = (view.get("windows") or {}).get(acct) or {}
    views = []
    for m in members:
        binding = _binding_windows(st, m["scope"])
        counted, why = _fleet_counted(binding, mid, main)
        for wid in counted if why is None else ():
            basis = (sized.get(wid) or {}).get("basis") or "none"
            if basis != "learned":
                why = f"{wid} not sized yet ({basis})"
                break
        views.append((m, binding, counted, why))
    v, refused, resting, resting_until = _fleet_judge(
        [_fleet_closed(st, b, {}, m["cap_of"], mid, main, m["held"], now) for m, b, _, _ in views])
    m, binding, counted, why = views[v]
    if why is not None:
        # Listed: its own left % on the counted window with the least left,
        # at the route's cap (the min rule, unit-free: it needs no size).
        window = left = None
        for wid, w in sorted(counted.items()):
            if w.get("used_pct") is None:
                continue
            cap = m["cap_of"](wid)
            pct = _fleet_pct(max(0.0, cap - _fleet_used(w, now)), cap)
            if pct is not None and (left is None or pct < left):
                window, left = wid, pct
        if refused is not None:
            refused = dict(refused, until=_fleet_secs(refused["until"]))
        return listed(why, window=window, left_pct=left, refused=refused, resting=resting,
                      resting_until=_fleet_secs(resting_until))
    # Counted: a stop_at reached on a window the member counts is room 0 by
    # the arithmetic, not a refusal (as built, 67 §6).
    _, refused, resting, resting_until = _fleet_judge(
        [_fleet_closed(st, b, c, mm["cap_of"], mid, main, mm["held"], now)
         for mm, b, c, _ in views])
    model, cap_of = m["model"], m["cap_of"]
    windows, caps, rooms, valued, binds = {}, [], [], set(), []
    for wid, w in sorted(counted.items()):
        sz = sized[wid]
        if model is not None and model in sz["sizes"]:
            pp, val = sz["sizes"][model] / 100, model
        else:
            pp, val = sz["mix"] / 100, "mix"
        cap, used = cap_of(wid), _fleet_used(w, now)
        c, r = cap * pp, max(0.0, cap - used) * pp
        caps.append(c)
        rooms.append(r)
        valued.add(val)
        windows[wid] = {"cap": cap, "used": used, "per_point": round(pp, 4), "valued": val,
                        "capacity": round(c, 2), "room": round(r, 2)}
        # When the window gives room back: at its reset when it has use; a
        # cap of 0 never, until the owner lifts the stop_at.
        binds.append((round(r, 2), None if cap <= 0 else w.get("resets_at") if used > 0 else 0))
    capacity, room = min(caps), min(rooms)
    # The figure rises when its binding window, the one with the least room,
    # gives room back; among tied ones the last to reset (68 §2, as built).
    # A tied window with no use (0 here) never gives room back by a reset.
    least = min(r for r, _ in binds)
    tied = [t for r, t in binds if r == least]
    if None in tied:
        more = [None]
    elif 0 in tied:
        more = []
    else:
        more = [max(tied)]
    if refused is not None:
        # Refused, the figure rises when the refusal ends, not before; a
        # counted window capped at 0 holds it at 0 past that.
        room = 0.0
        more = [None] if any(cap_of(wid) <= 0 for wid in counted) else [refused["until"]]
    entry = {"valued": model if valued == {model} else "mix",
             "capacity": round(capacity, 2), "room": round(room, 2),
             "refused": refused, "resting": resting,
             "resting_until": resting_until, "more_at": _fleet_secs(_fleet_earliest(more)),
             "stale": _route_stale(st, dict(binding, **{mid: main}), now, stale_max),
             "windows": windows}
    return (entry, capacity, room, more), None, v


def _fleet_sum(results):
    """A level's totals from {acct: ((entry, capacity, room, more) | None,
    listed)}: the figure, its counted accounts' states, the listed accounts,
    and 68 §3.2's coverage, more_at and back_at."""
    capacity = room = 0.0
    resting, stale, listed, more, back = [], [], [], [], []
    cov = {"counted": 0, "learning": 0, "empty": 0, "unread": 0, "stale": 0, "not_quota": 0}
    for acct, (got, lst) in sorted(results.items()):
        if got is None:
            listed.append(dict({"account": acct}, **lst))
            cov[lst["kind"]] += 1
            if lst["kind"] == "learning" and lst["refused"] is not None:
                cov["empty"] += 1
                back.append(lst["refused"]["until"])
            continue
        entry, c, r, m = got
        cov["counted"] += 1
        capacity += c
        room += r
        more += m
        if entry["resting"]:
            resting.append(acct)
        if entry["stale"]:
            stale.append(acct)
    cov["stale"] = len(stale)
    return {"capacity": round(capacity, 2), "room": round(room, 2),
            "left_pct": _fleet_pct(room, capacity), "resting": resting, "stale": stale,
            "listed": listed, "coverage": cov, "more_at": _fleet_secs(_fleet_earliest(more)),
            "back_at": _fleet_secs(_fleet_earliest(back))}


def _fleet_asks(accts, health, limits, bound=None):
    """What only the owner can fix on these accounts (68 §3.2): an `auth`
    ask for each whose health row says its sign-in was refused, and a
    `not_entitled` ask for each live not_entitled limit (_limit_snap), with
    its model (null for a pool) and until, on a selection only where the
    limit binds a member that reaches the account (`bound(account, limit)`,
    67 §9's rule; None, the whole proxy, lists every one). Sorted auth
    first, then by account. `sign_in` is the provider's sign-in kind, null
    when the account has no health row."""
    out = []
    for a in sorted(set(accts)):
        h = health.get(a) or {}
        if h.get("kind") == "auth":
            out.append({"account": a, "kind": "auth", "sign_in": h.get("sign_in"),
                        "model": None, "until": None})
        for lm in limits.get(a, ()):
            if lm["class"] == "not_entitled" and (bound is None or bound(a, lm["limit"])):
                kind, _, name = lm["limit"].partition(":")
                out.append({"account": a, "kind": "not_entitled", "sign_in": h.get("sign_in"),
                            "model": name if kind == "model" else None,
                            "until": _fleet_secs(lm["until"])})
    return sorted(out, key=lambda x: (x["kind"] != "auth", x["account"], x["model"] or ""))


# ---- the forecast: when a selection runs dry, for how long, and when room returns (69 §4.3) ----
#
# The walk takes the figure's counted accounts as they are now, each window
# counted with its room and its capacity in the selection's dollars, and walks
# forward to the horizon (now + FORECAST_H). The selection spends at its pace
# from one account at a time: the one the brain prefers for it now, then the
# others in the figure's listing order; when the account in turn runs dry it
# moves to the next with room and does not move back when an earlier one
# refills. A resting account waits for its rest to end, a refused one for its
# refusal to end (never, when that end is unknown). A counted window refills
# to its capacity at its reset: a refill sets room, it never adds to what is
# left. A fixed window's next period follows at once, every period after
# within the horizon. A first-use window's next period starts at the first
# draw after a refill, so its following refill is that draw plus its period;
# one whose reset is past or unknown is not running, and refills a period
# after the walk first draws from it. In a drought the first draw is at once.
# Spending a dollar takes a dollar from each counted window of the account,
# since each window's room is in the same dollars, so the account's room
# stays the least of them, as in the figure. Listed accounts add nothing.
# Nothing here is a fact: the pace is learned, and the forecast is the
# walk's estimate.

_WALK_EPS = 1e-9


def _fleet_walk_accounts(entries, standings, now, horizon):
    """{acct: {"wins": {wid: {room, capacity, period, first_use, next (the
    next refill, None for none known yet)}}, "from": the first instant it may
    serve (inf for an unknown end)}} for the walk, from the counted accounts'
    figure entries (with their `resting`) and their standings."""
    out = {}
    for acct, e in entries.items():
        st = standings.get(acct) or {}
        lims = _fleet_limits(st)
        wins = {}
        for wid, wv in sorted((e.get("windows") or {}).items()):
            w = lims.get(wid) or {}
            r, period = w.get("resets_at"), _fleet_period_s(w)
            first_use = w.get("anchor") == "first_use"
            if r is not None and r <= now:
                if first_use or not period:
                    r = None  # not running: a first-use window waits for its draw
                else:
                    r += period * math.ceil((now - r) / period)
                    if r <= now:
                        r += period
            wins[wid] = {"room": float(wv["room"]), "capacity": float(wv["capacity"]),
                         "period": period, "first_use": first_use,
                         "next": float(r) if r is not None else None}
        start = now
        if e.get("refused") is not None:
            until = e["refused"].get("until")
            start = math.inf if until is None else max(now, float(until))
        elif e.get("resting"):
            until = e.get("resting_until")
            start = math.inf if until is None else max(now, float(until))
        out[acct] = {"wins": wins, "from": start}
    return out


def _fleet_walk(accts, order, rate, now, horizon, walls):
    """The walk itself (69 §4.3) over _fleet_walk_accounts' accounts, in draw
    order, at `rate` dollars an hour (> 0) -> spells [(from, to)], each dry
    spell within the horizon. `walls` ({acct: the instant it ran dry, None
    for not yet}, those dry now already set) gains each account's first
    instant dry in the walk."""
    b = rate / 3600.0

    def room(a):
        ws = accts[a]["wins"]
        return min(w["room"] for w in ws.values()) if ws else 0.0

    def can(a, t):
        return accts[a]["from"] <= t and room(a) > _WALK_EPS

    def pick(t, after):
        if not order:
            return None
        i = 0 if after is None else (order.index(after) + 1) % len(order)
        ring = order[i:] + order[:i]
        return next((a for a in ring if can(a, t)), None)

    def draw(a, t):
        # Drawing from an account starts each of its idle first-use windows.
        if a is None:
            return
        for w in accts[a]["wins"].values():
            if w["first_use"] and w["next"] is None and w["period"]:
                w["next"] = t + w["period"]

    def spend(a, usd):
        for w in accts[a]["wins"].values():
            w["room"] = max(0.0, w["room"] - usd)

    def upcoming(t):
        ts = [w["next"] for a in accts.values() for w in a["wins"].values()
              if w["next"] is not None and w["next"] > t]
        ts += [a["from"] for a in accts.values() if t < a["from"] < horizon]
        return min(ts + [horizon])

    spells, dry_from = [], None
    t, cur, last = now, pick(now, None), None
    draw(cur, t)
    while t < horizon:
        if cur is None and dry_from is None:
            dry_from = t
        nxt = upcoming(t)
        if cur is not None:
            r = room(cur)
            t_dry = t + r / b
            if t_dry < nxt:
                spend(cur, r)
                t = t_dry
                if walls[cur] is None:
                    walls[cur] = t
                last, cur = cur, pick(t, cur)
                draw(cur, t)
                continue
            spend(cur, b * (nxt - t))
        t = nxt
        if t >= horizon:
            break
        for a in accts.values():
            for w in a["wins"].values():
                if w["next"] is not None and w["next"] <= t:
                    w["room"] = w["capacity"]
                    if w["first_use"] or not w["period"]:
                        w["next"] = None
                    else:
                        while w["next"] <= t:
                            w["next"] += w["period"]
        if cur is None:
            cur = pick(t, last)
            if cur is not None and dry_from is not None:
                spells.append((dry_from, t))
                dry_from = None
        draw(cur, t)
    if dry_from is not None:
        spells.append((dry_from, horizon))
    return spells


def _fleet_forecast(entries, standings, preferred, pace, now):
    """A level's `forecast` (69 §4.3) from its counted accounts' figure
    entries ({acct: entry with `resting`}, in listing order), the account the
    brain prefers for it now (None for the proxy, which draws in listing
    order), its pace and now."""
    horizon = now + FORECAST_H
    order = sorted(entries)
    if preferred in entries:
        order = [preferred] + [a for a in order if a != preferred]
    rate = pace.get("rate")
    out = {"dry_at": None, "back_at": None, "dry_s": None, "unmet": None, "spells": None,
           "walls": {}, "horizon_at": _fleet_secs(horizon), "rate": rate,
           "rate_basis": "learned" if rate is not None else "no pace yet",
           "basis": "learned" if rate is not None else "no pace"}
    accts = _fleet_walk_accounts(entries, standings, now, horizon)
    if rate is None or not order:
        # No projection: only the accounts dry now (69 §4.3). With nothing
        # counted there is nothing to walk.
        out["walls"] = {a: (_fleet_secs(now) if entries[a]["room"] <= 0 else None) for a in order}
        if rate is not None:
            out["basis"] = "nothing counted"
        return out
    walls = {a: (now if entries[a]["room"] <= 0 else None) for a in order}
    spells = _fleet_walk(accts, order, rate, now, horizon, walls) if rate > 0 else []
    dry_s = sum(b - a for a, b in spells)
    out.update(
        dry_at=_fleet_secs(spells[0][0]) if spells else None,
        back_at=_fleet_secs(spells[0][1]) if spells and spells[0][1] < horizon else None,
        dry_s=int(round(dry_s)), unmet=round(rate * dry_s / 3600.0, 2),
        spells=[{"from": _fleet_secs(a), "to": _fleet_secs(b)} for a, b in spells],
        walls={a: _fleet_secs(w) for a, w in walls.items()})
    return out


def _fleet_figure(standings, routes, models, view, limits, now, stale_max, health,
                  preferred=None):
    """The `fleet` section (67 §8, 68 §3) from plain data: standings and
    routes as the dial check reads them (ctx), models {route id: its
    member's model} for class members, the sizing view (_sizing_fleet_view,
    with its `pace`), the live limits the brain has no window for
    (_limit_snap, 67 §9), each account's sign-in health {account: {kind,
    sign_in}} (_fleet_health), and the account the brain prefers for each
    selection now ({selection: account}, _fleet_preferred), which the
    forecast draws from first (69 §4.3)."""
    paces = view.get("pace") or {}
    preferred = preferred or {}

    def pace_of(p):
        return dict(p) if p is not None else _burn_pace(None, now)
    # The selections (67 §7): each class, and each bare lane. An account is
    # valued by the highest-priority member whose route can serve it now,
    # windows and limits alike, and is refused only when every member that
    # reaches it is refused (68 §3.4); that member's scope picks its scoped
    # window.
    groups = {}
    for rid, rt in routes.items():
        groups.setdefault(rt["class"] if rt["kind"] == "member" else rid, []).append(rt)
    selections = {}
    for key, rts in sorted(groups.items()):
        reach = {}
        for rt in sorted(rts, key=lambda r: (r["priority"] is None, r["priority"] or 0, r["id"])):
            for a in rt["accounts"]:
                reach.setdefault(a, []).append(rt)

        def model_of(rt):
            return models.get(rt["id"]) if rt["kind"] == "member" else \
                _fleet_lane_model(view, rt["provider"], rt["lane"])

        def held_by(rt):
            # What a limit binds a member by: its scope, its model, and its
            # lane only when it is a bare lane.
            return rt["scope"], model_of(rt), rt["lane"] if rt["kind"] == "lane" else None
        results, members = {}, {}
        for a, rts_a in reach.items():
            lims, st = limits.get(a, ()), standings.get(a)
            mem = [{"scope": rt["scope"], "model": model_of(rt),
                    "cap_of": lambda wid, rt=rt, st=st: _fleet_cap(rt, st, wid),
                    "held": _fleet_limit_hold(lims, [held_by(rt)])} for rt in rts_a]
            got, lst, v = _fleet_account(a, st, mem, view, now, stale_max)
            results[a] = (got, lst)
            members[a] = (rts_a[v]["member"] or rts_a[v]["id"], mem[v]["model"])
        sel = _fleet_sum(results)
        sel["asks"] = _fleet_asks(reach, health, limits, lambda a, lim, reach=reach: any(
            _fleet_limit_binds(lim, *held_by(rt)) for rt in reach[a]))
        sel["pace"] = pace_of((paces.get("selections") or {}).get(key))
        sel["forecast"] = _fleet_forecast(
            {a: got[0] for a, (got, _) in results.items() if got is not None},
            standings, preferred.get(key), sel["pace"], now)
        sel["accounts"] = {}
        for a, (got, _) in sorted(results.items()):
            if got is not None:
                e = dict(got[0])
                del e["resting"]
                sel["accounts"][a] = dict({"member": members[a][0], "model": members[a][1]}, **e)
        selections[key] = sel
    # The whole proxy and the accounts: every account once, at its mix size
    # and the highest cap any route that reaches it allows (Q3, Q4).
    reach = {}
    for rt in routes.values():
        for a in rt["accounts"]:
            reach.setdefault(a, []).append(rt)

    def highest(a, st, wid):
        return max([_fleet_cap(rt, st, wid) for rt in reach.get(a, ())
                    if wid in _binding_windows(st, rt["scope"])] or [100.0])

    results, accounts = {}, {}
    providers = view.get("providers") or {}
    for a, st in sorted(standings.items()):
        got, lst, _ = _fleet_account(
            a, st, [{"scope": None, "model": None,
                     "cap_of": lambda wid, a=a, st=st: highest(a, st, wid),
                     "held": _fleet_limit_hold(limits.get(a, ()), [(None, None, None)])}],
            view, now, stale_max)
        results[a] = (got, lst)
        why = None if lst is None else lst["why"]
        held = [dict(lm) for lm in limits.get(a, ())]
        mid, main = _fleet_main(st) if st and st.get("as_of") is not None and \
            st.get("kind") != "api" else (None, None)
        if mid is None:
            accounts[a] = {"main": None, "listed": why, "kind": lst["kind"], "refused": None,
                           "resting_until": None, "limits": held}
            continue
        sized = (view.get("windows") or {}).get(a) or {}

        def own(wid, w, a=a, st=st, sized=sized):
            cap, used = highest(a, st, wid), _fleet_used(w, now)
            sz = sized.get(wid) or {}
            basis = sz.get("basis") or "none"
            sizes = {} if basis != "learned" else dict(
                {"mix": round(sz["mix"], 2)},
                **{m: round(v, 2) for m, v in sorted(sz["sizes"].items())})
            return {"used": used, "cap": cap, "left_pct": _fleet_pct(max(0.0, cap - used), cap),
                    "size_basis": basis, "sizes": sizes}, sz

        m, sz = own(mid, main)
        entry = {"main": mid, "used": m["used"], "cap": m["cap"], "left_pct": m["left_pct"],
                 "stale": _route_stale(st, dict(_binding_windows(st, None), **{mid: main}),
                                       now, stale_max),
                 "size_basis": m["size_basis"], "steps": sz.get("samples", 0),
                 "sizes": m["sizes"],
                 "reference": (providers.get(st.get("provider")) or {}).get("reference"),
                 "scoped": {wid: own(wid, w)[0] for wid, w in
                            sorted(((st.get("allowance") or {}).get("windows") or {}).items())
                            if w.get("scope") is not None and w.get("used_pct") is not None},
                 "capacity": None, "room": None, "refused": None, "resting_until": None,
                 "listed": why, "kind": None, "limits": held}
        if got is not None:
            e = got[0]
            entry.update(capacity=e["capacity"], room=e["room"], refused=e["refused"],
                         resting_until=e["resting_until"], stale=e["stale"])
        else:
            entry.update(kind=lst["kind"], refused=lst["refused"],
                         resting_until=lst["resting_until"])
        accounts[a] = entry
    proxy = dict(_fleet_sum(results), valued="mix")
    proxy["asks"] = _fleet_asks(set(standings) | set(health), health, limits)
    proxy["pace"] = pace_of(paces.get("proxy"))
    proxy["forecast"] = _fleet_forecast(
        {a: got[0] for a, (got, _) in results.items() if got is not None},
        standings, None, proxy["pace"], now)
    return {"unit": "api_usd", "proxy": proxy, "selections": selections, "accounts": accounts}


# The sign-in a provider's auth authority takes (68 §3.2): the consumers word
# an `auth` ask's fix on it, never on the account's kind.
_FLEET_SIGN_IN = {"oauth-pkce": "oauth", "oauth-rotating": "oauth", "static-key": "key"}


def _fleet_health(account_health):
    """{account: {kind: last_error_kind, sign_in}} from _unified_account_health's
    rows, for _fleet_figure: a provider with no auth authority has no row."""
    out = {}
    for row in account_health.get("accounts") or ():
        auth = (PROVIDERS.get(row.get("provider")) or {}).get("auth") or {}
        out[row["name"]] = {"kind": row.get("last_error_kind"),
                            "sign_in": _FLEET_SIGN_IN.get(auth.get("kind"))}
    return out


def _fleet_preferred(t, ctx):
    """{selection: account} the brain prefers for each selection now, as the
    document names it: a bare lane's verdict account (its incumbent first,
    as routes[] has it), a class's pointer member's verdict account (None
    while the class has no pointer). The forecast draws from it first (69
    §4.3). Caller holds _plock."""
    out = {}
    for rid, rt in ctx["routes"].items():
        if rt["kind"] != "lane":
            continue
        inc = (t.preferred["lanes"].get(rt["lane"]) or {}).get("account") \
            if rt["provider"] == "anthropic" else \
            (t.preferred.get("providers", {}).get(rt["provider"]) or {}).get("account")
        out[rid], _ = _verdict(rt, ctx["checks"][rid], inc)
    for c in ctx["classes"]:
        verdicts = _class_verdicts(t, c, ctx)
        ptr = _class_pointer(c, verdicts, ctx["money"]["pay_order"])
        out[c["class"]] = verdicts[ptr]["account"] if ptr is not None else None
    return out


def _fleet_payload(ctx, view, health, preferred=None):
    """The `fleet` section of the document from the decision context, the
    sizing view and the sign-in health (_fleet_health), both taken before
    _plock, and the account the brain prefers for each selection
    (_fleet_preferred). Caller holds _plock."""
    models = {f"{c['class']}/{_member_id(m)}": m.get("model")
              for c in ctx["classes"] for m in c["members"]}
    return _fleet_figure(ctx["standings"], ctx["routes"], models, view, ctx["limits"], ctx["now"],
                         STALE_MAX, health, preferred)


def _fleet_metric_lines(fleet, tid):
    """fleet_capacity_api_usd, fleet_room_api_usd and fleet_left_pct, by
    {level, name}: the proxy, each selection and each account at the proxy
    level; a listed account's own left_pct only (67 §8, §13 Q7). For the
    proxy and each selection, fleet_pace_api_usd_per_h while a pace is
    learned, and fleet_forecast_dry_s and fleet_forecast_unmet_api_usd while
    the walk projects (69 §6.1). No rule reads them."""
    _line = _tenant_line(tid)
    rows = [("proxy", "proxy", fleet["proxy"])]
    rows += [("selection", k, s) for k, s in sorted(fleet["selections"].items())]
    rows += [("account", k, a) for k, a in sorted(fleet["accounts"].items())]
    lines = []
    for level, name, v in rows:
        lab = {"level": level, "name": name}
        if v.get("capacity") is not None:
            lines.append(_line("fleet_capacity_api_usd", lab, v["capacity"]))
            lines.append(_line("fleet_room_api_usd", lab, v["room"]))
        if v.get("left_pct") is not None:
            lines.append(_line("fleet_left_pct", lab, v["left_pct"]))
        if (v.get("pace") or {}).get("rate") is not None:
            lines.append(_line("fleet_pace_api_usd_per_h", lab, v["pace"]["rate"]))
        fc = v.get("forecast") or {}
        if fc.get("dry_s") is not None:
            lines.append(_line("fleet_forecast_dry_s", lab, fc["dry_s"]))
            lines.append(_line("fleet_forecast_unmet_api_usd", lab, fc["unmet"]))
    return lines


def reports_body_problem(body):
    """Why a POST /reports body is a 400, or None."""
    shape = (f'body must be {{"reporter": "<host>", "events": [0-{REPORTS_POST_MAX} events]}} '
             f"(JSON, <= {DOC_BODY_MAX} bytes)")
    if not isinstance(body, dict) or "events" not in body or set(body) - {"reporter", "events"}:
        return shape
    if not isinstance(body["events"], list) or len(body["events"]) > REPORTS_POST_MAX:
        return shape
    if body.get("reporter") is not None and not isinstance(body["reporter"], str):
        return "reporter must be a string"
    return None


def reports_post(t, events, reporter, now):
    """POST /reports (quota-model 64 §5): each event on its own, in order.
    -> ({results: [{id, result, why}], seq}, decisive count): the events
    applied that a decision reads, so served events (65 §7, counted only)
    leave it 0. Ledger entries go to the ledger in one append
    (ledger_accept); seq is the ledger's."""
    with t.lock:
        snap = list(t.state.values())
    roster = _account_roster(t, snap)
    by_name = {r["name"]: r for r in snap}
    outcomes, ledger_at = [], []
    for i, ev in enumerate(events):
        res, why = _report_event(t, ev, roster, by_name, now, reporter or "")
        if res == "ledger":
            ledger_at.append(i)
        outcomes.append((res, why))
    if ledger_at:
        entries = [{k: v for k, v in events[i].items() if k not in ("kind", "read_at")}
                   for i in ledger_at]
        accepted, seq = ledger_accept(t, entries, roster, now)
        for i, out in zip(ledger_at, accepted):
            outcomes[i] = out
    else:
        with t.ledger_lock:
            seq = t.ledger["seq"]
    results = [{"id": ev.get("id") if isinstance(ev, dict) else None, "result": res, "why": why}
               for ev, (res, why) in zip(events, outcomes)]
    counts = {k: sum(1 for r in results if r["result"] == k)
              for k in ("applied", "ignored", "refused", "retry")}
    if events:
        trouble = [f"{r['id']}: {r['result']} ({r['why']})" for r in results
                   if r["result"] in ("refused", "retry")]
        log(f"reports from {reporter or '(unnamed)'}: {len(events)} event(s), "
            + ", ".join(f"{n} {k}" for k, n in counts.items() if n)
            + (f"; {'; '.join(trouble[:5])}" + (f"; +{len(trouble) - 5} more" if len(trouble) > 5 else "")
               if trouble else ""))
    decisive = sum(1 for ev, r in zip(events, results)
                   if r["result"] == "applied" and ev.get("kind") != "served")
    return {"results": results, "seq": seq}, decisive


def _reported_result(res, rep):
    """res with the reported readings that are newer than its poll merged
    into a copy of its reading, per window and per field, as the proxy's
    overlay does (standingRead): a used_pct newer than the poll's start
    replaces the poll's and moves the window's as_of (reported_as_of), a
    newer resets_at replaces the poll's, and a window the poll does not
    carry is added by a reading that carries its use (a reset alone adds
    nothing, and leaves a window's as_of as it was). The credits pot's open
    (only ever false) and balance are
    merged the same way. res itself when nothing reported is newer, or the
    account was never measured."""
    rd, good = res.get("reading"), res.get("good_ts")
    if not rd or good is None or not rep:
        return res

    def newer(f):
        return f is not None and f["at"] > good

    wins = {wid: f for wid, f in (rep.get("windows") or {}).items()
            if newer(f.get("used_pct")) or newer(f.get("resets_at"))}
    pot_id = _money_pot(res.get("provider"))
    unread = _pot_unread(rd, pot_id)
    # On a pot no poll reads, a report stands until _prune_reports ends it.
    credits = {k: f for k, f in (rep.get("money") or {}).items() if unread or newer(f)}
    if not wins and not credits:
        return res
    rd = copy.deepcopy(rd)
    pots = rd.setdefault("pots", [])
    reported_as_of = {}
    if wins:
        allow = _reading_pot(rd, ALLOWANCE_POT)
        if allow is None:
            allow = _allowance_pot([])
            pots.insert(0, allow)
        windows = _reading_windows(rd)
        for wid, f in wins.items():
            used, resets = f.get("used_pct"), f.get("resets_at")
            w = windows.get(wid)
            if w is None:
                w = _report_window(wid) if newer(used) else None
                if w is None:
                    continue  # a reading with no use never adds a window
                allow["limits"].append(w)
            if newer(used):
                w.update(used_pct=used["v"], basis="exact", as_of=used["at"])
                reported_as_of[wid] = used["at"]
            if newer(resets):
                w.update(resets_at=resets["v"], reset_basis=w.get("reset_basis") or "exact")
    if credits:
        pot = _reading_pot(rd, pot_id)
        if pot is None:
            pot = {"id": pot_id, "type": "money", "draw": 2, "unit": None, "unit_usd": None,
                   "open": None, "open_basis": None, "limits": []}
            pots.append(pot)
        if "open" in credits:
            pot.update(open=False, open_basis=pot.get("open_basis") or "exact")
        if "balance" in credits:
            lim = _reading_limit(pot, "balance")
            if lim is None:
                lim = {"id": "balance", "kind": "balance", "amount": None, "basis": "unknown"}
                pot.setdefault("limits", []).append(lim)
            lim["amount"] = credits["balance"]["v"]
            if lim.get("basis") in (None, "unknown"):
                lim["basis"] = "exact"
    return dict(res, reading=rd, reported_as_of=reported_as_of)


def _reported_snap(t, snap):
    """The poll results with the reported readings merged in (a new list;
    _state is never touched)."""
    with t.reports_lock:
        if not t.reported:
            return list(snap)
        rep = copy.deepcopy(t.reported)
    return [_reported_result(r, rep[r["name"]]) if r.get("name") in rep else r for r in snap]


def _hold_window_changed(seen, w, now):
    """The proxy's windowChanged: the reading shows a different period (a
    reset still ahead, more than REPORT_RESET_SLACK_S from the one seen at
    the refusal) or a lower use than the refusal's reading. The same period
    at the same or a higher use is no change; a null or passed reset is no
    new period. A use at most a point below the one seen, in the same period
    (_use_agrees), is the seen reading again and no change: the 99 a 2xx
    answered after a refusal at 100 reads (quota-model 10, "Rounding"), which
    a brain restarted during the hold gets with no 100 held (64 §8)."""
    resets, seen_resets = w.get("resets_at"), seen.get("resets_at")
    if resets is not None and resets > now and \
            (seen_resets is None or abs(seen_resets - resets) > REPORT_RESET_SLACK_S):
        return True
    used, seen_used = w.get("used_pct"), seen.get("used_pct")
    return seen_used is not None and used is not None and used < seen_used and \
        not _use_agrees(seen_used, seen_resets, used, resets)


def _hold_judge(h, wid, st, now):
    """Does the hold end, and until when does it hold -> (end, why, until).
    The proxy's rule for a window record (pruneRecords, recordEnds), against
    the account's standing with the reports merged and no hold applied:
    expired at its until; ended by the owner's re-check (recheck_epoch newer
    than the refusal); and, for a refusal a reading can end, by the first
    reading of the window newer than the refusal that shows it open (under
    100, its own gate and the account's not shut) and changed. Until then a
    hold with no until learns the reading's reset while that is ahead."""
    until = h["until"]
    if until is not None and now >= until:
        return True, "expired", None
    epoch = st.get("recheck_epoch")
    if epoch is not None and h["refused_at"] < epoch:
        return True, "re-check", None
    if not h["by_reading"]:
        return False, None, until
    allow = st.get("allowance") or {}
    w = (allow.get("windows") or {}).get(wid)
    if w is None or w.get("as_of") is None or not w["as_of"] > h["refused_at"]:
        return False, None, until
    if not _window_closed(w, now) and allow.get("open") is not False and \
            _hold_window_changed(h["seen"], w, now):
        return True, "reading", None
    if until is None and w.get("resets_at") is not None and w["resets_at"] > now:
        return False, None, w["resets_at"]
    return False, None, until


def _hold_window(h, until, w, now):
    """The window a hold makes of its window (the proxy's recordWindow and
    recordBasis): at 100, read no earlier than the refusal. A hold that ends
    only by reading resets at the reading's own reset while that is ahead,
    and else at no known time (basis record): a passed reset never shortens
    it. Otherwise it resets at until, or at the reset the refusal named when
    that is later; the basis is exact when the named reset is at or after
    until, else record, and a window's own basis stands unless it is
    record."""
    w = dict(w, used_pct=100.0, basis="exact")
    if w.get("as_of") is None or w["as_of"] < h["refused_at"]:
        w["as_of"] = h["refused_at"]
    if until is None:
        if w.get("resets_at") is not None and w["resets_at"] <= now:
            w.update(resets_at=None, reset_basis="record")
        return w
    named = h.get("resets")
    basis = "exact" if named is not None and named >= until else "record"
    if basis == "record" or not w.get("reset_basis"):
        w["reset_basis"] = basis
    w["resets_at"] = named if named is not None and named > until else until
    return w


def _hold_snap(t, snap, standings, now):
    """-> (snap, held accounts): the results with each live hold applied to
    a copy of its reading as the proxy's records close its standing
    (standingFor): the held window at 100 (_hold_window), added where the
    reading lacks it. Judged against the standings without holds; an
    account never measured is left as it is. Holds go into the readings, so
    every number derived from them (the standings, weekly, the lanes'
    effective, the class targets) reads the window at 100."""
    with t.reports_lock:
        holds = [(k, dict(h)) for k, h in t.holds.items()]
    live = {}
    for (acct, wid), h in holds:
        st = standings.get(acct)
        if not st or st.get("as_of") is None:
            continue
        end, _, until = _hold_judge(h, wid, st, now)
        if not end:
            live.setdefault(acct, []).append((wid, h, until))
    if not live:
        return snap, set()
    out = []
    for r in snap:
        items = live.get(r.get("name"))
        if not items or not r.get("reading"):
            out.append(r)
            continue
        rd = copy.deepcopy(r["reading"])
        own = dict(r.get("reported_as_of") or {})
        allow = _reading_pot(rd, ALLOWANCE_POT)
        if allow is None:
            allow = _allowance_pot([])
            rd.setdefault("pots", []).insert(0, allow)
        windows = _reading_windows(rd)
        for wid, h, until in items:
            w = windows.get(wid)
            if w is None:
                w = _report_window(wid) or _window(wid, None, None, None, None)
                w["as_of"] = None
                allow["limits"].append(w)
            else:
                w["as_of"] = own.get(wid, r.get("good_ts"))
            held = _hold_window(h, until, w, now)
            w.clear()
            w.update(held)
            own[wid] = held["as_of"]
        out.append(dict(r, reading=rd, reported_as_of=own))
    return out, set(live)


def _decision_view(t, snap, now):
    """-> (roster, snap, by_name, standings): what every decision reads. The
    poll results with the reported readings merged in (_reported_snap) and
    the holds applied (_hold_snap), and the standings from them."""
    roster = _account_roster(t, snap)
    snap = _reported_snap(t, snap)
    by_name = {r["name"]: r for r in snap}
    standings = {a: _account_standing(t, a, by_name.get(a), pv, now) for a, pv in roster.items()}
    snap, held = _hold_snap(t, snap, standings, now)
    if held:
        by_name = {r["name"]: r for r in snap}
        for a in held:
            standings[a] = _account_standing(t, a, by_name.get(a), roster[a], now)
    return roster, snap, by_name, standings


def _prune_reports(t, snap, now):
    """At the start of every compute_preferred: forget each reported field a
    poll that started after it has replaced (also by leaving its window
    out), and every report on an account no longer polled or never
    measured; end each hold the proxy's rule ends (_hold_judge), and keep
    the until a hold learns from a reading; end each limit's hold at its
    until or the owner's re-check (_limit_ended). Caller holds _plock."""
    by_name = {r["name"]: r for r in snap}
    # The floor of each money report on a pot no poll reads, taken before
    # _reports_lock: it reads the ledger and the re-checks under their locks.
    with t.reports_lock:
        with_money = [a for a, rep in t.reported.items() if rep.get("money")]
    floors = {}
    for acct in with_money:
        res = by_name.get(acct) or {}
        good = res.get("good_ts") if res.get("reading") else None
        if good is None:
            continue
        provider = res.get("provider")
        floors[acct] = _report_money_floor(t, acct, provider, good,
                                           _pot_unread(res.get("reading"), _money_pot(provider)))
    with t.reports_lock:
        for acct in list(t.reported):
            res = by_name.get(acct) or {}
            good = res.get("good_ts") if res.get("reading") else None
            rep = t.reported[acct]
            for node in (rep.get("windows") or {}).values():
                for field in [f for f, v in node.items() if good is None or v["at"] <= good]:
                    del node[field]
            # A pot no poll reads keeps a shut until a counted ledger entry on
            # it or the owner's re-check is newer (_report_money_floor); a
            # report that arrived since the floors were taken waits a pass.
            money = rep.get("money") or {}
            floor = floors.get(acct, good) if acct in with_money else good
            for field in [f for f, v in money.items() if floor is None or v["at"] <= floor]:
                del money[field]
            _reports_tidy(t, acct)
        if not t.holds and not t.limit_holds:
            return
        holds = dict(t.holds)
        limits = dict(t.limit_holds)
    roster = _account_roster(t, snap)
    merged = {r["name"]: r for r in _reported_snap(t, snap)}
    standings, ended = {}, []

    def standing(acct):
        if acct not in standings:
            standings[acct] = _account_standing(t, acct, merged.get(acct), roster[acct], now)
        return standings[acct]

    # A limit's hold (67 §9) ends at its until or at the owner's re-check.
    for (acct, lim), h in limits.items():
        why = "account gone" if acct not in roster else _limit_ended(h, standing(acct), now)
        if why is None:
            continue
        with t.reports_lock:
            if t.limit_holds.get((acct, lim)) is not h:
                continue  # a newer report replaced it meanwhile
            del t.limit_holds[(acct, lim)]
        if why != "expired":
            ended.append(f"{acct}/{lim} ({why})")
    for (acct, wid), h in holds.items():
        if acct not in roster:
            end, why, until = True, "account gone", None
        else:
            end, why, until = _hold_judge(h, wid, standing(acct), now)
        with t.reports_lock:
            if t.holds.get((acct, wid)) is not h:
                continue  # a newer report replaced it meanwhile
            if end:
                del t.holds[(acct, wid)]
                if why != "expired":
                    ended.append(f"{acct}/{wid} ({why})")
            elif until != h["until"]:
                h["until"] = until
    if ended:
        log(f"🔋 hold ended on {', '.join(sorted(ended))}")


# ---- GET /events: the brain publishes /preferred (quota-model 64 §2) -------------
# Subscribers (the proxy, a console) hold one server-sent-events stream instead of
# pulling /preferred. Anything that can change the document marks it dirty
# (_events_mark: the end of compute_preferred, and every handled request but
# the pure reads); one publisher thread renders it, compares it with the last
# one published and, when it differs, publishes it as the next event. Each
# event is the whole document, so a subscriber only ever needs the newest:
# nothing is queued or replayed, and a reconnect receives the current one.

EVENTS_KEEPALIVE_S = 15.0  # silence after which a stream sends ": keepalive"
EVENTS_MIN_INTERVAL_S = 1.0  # at most one document a second; a change inside it goes in the next
EVENTS_RETRY_MS = 1000     # the reconnect delay the stream asks its client for

# The /preferred fields that are a function of the clock ALONE, ignored when
# the publisher compares documents (each path's "*" is every element). Each is
# recomputed from `now` on every render, so it differs between two renders of
# the same state:
#   account_health.accounts[].age_s           now - good_ts; good_ts is in the row
#   account_health.accounts[].next_retry_in_s retry_at - now, a countdown to the
#                                             next poll (that poll's result is
#                                             itself a change)
#   ledger.as_of                              round(now, 3): the time of the render
#   fleet.proxy.{pace, forecast},             the pace and the walk forward from now
#   fleet.selections[].{pace, forecast}       (quota-model 69 §4.2, §4.3): they move
#                                             with the clock and with served events,
#                                             and are recomputed at each render; a
#                                             reading change that moves them moves
#                                             the figure too, which is a change
# A field the clock flips by a rule is NOT here: `stale`, a check whose `until`
# has passed, the budget period. Those flip by a decision and are a change.
# Nor may a field follow the clock some of the time: accounts[].next_poll_at is
# the poll schedule as it stands, not max(schedule, now), which would follow the
# clock while a due poll runs and carry every check's `until` taken from it.
# ledger.as_of has one more duty: a reader's dial check holds a ledger older
# than STALE_MAX as unknown, so the publisher also republishes once the
# published ledger.as_of is STALE_MAX / 2 old (_events_publish).
PREFERRED_CLOCK_ONLY = (
    ("account_health", "accounts", "*", "age_s"),
    ("account_health", "accounts", "*", "next_retry_in_s"),
    ("ledger", "as_of"),
    ("fleet", "proxy", "pace"),
    ("fleet", "proxy", "forecast"),
    ("fleet", "selections", "*", "pace"),
    ("fleet", "selections", "*", "forecast"),
)

_EVENTS_BOOT = str(int(_start_time))  # the id's boot part, fixed at process start
_events_dirty = threading.Event()
# Per tenant (tenancy.Tenant, _tenant_fresh): events_cond guards events_pub
# and events_subs (one token per open /events stream); events_publish_lock
# serializes one render-and-compare at a time, taken before events_cond.
_events_thread = None
_events_thread_lock = threading.Lock()


def _events_mark(t=None):
    """The tenant's document may have changed (every tenant's, with None: the
    classes document is theirs all): wake the publisher. Never blocks."""
    for tt in ([t] if t is not None else (_registry.all() if _registry is not None else ())):
        tt.events_dirty = True
    _events_dirty.set()


def _strip_clock_only(doc):
    """Remove PREFERRED_CLOCK_ONLY from doc, in place."""
    def strip(node, path):
        head, rest = path[0], path[1:]
        if head == "*":
            kids = node.values() if isinstance(node, dict) else node if isinstance(node, list) else ()
            for kid in kids:
                strip(kid, rest)
        elif isinstance(node, dict):
            if rest:
                if head in node:
                    strip(node[head], rest)
            else:
                node.pop(head, None)
    for path in PREFERRED_CLOCK_ONLY:
        strip(doc, path)
    return doc


def _preferred_digest(doc):
    """The digest the publisher compares: doc without its clock-only fields."""
    clean = _strip_clock_only(json.loads(json.dumps(doc)))
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":"))
                          .encode("utf-8")).hexdigest()


def _events_publish(t):
    """Render /preferred and publish it when it differs from the last one
    published (or its ledger.as_of is STALE_MAX / 2 old). True when it
    published. Holds no state lock while subscribers are woken; never touches
    a socket."""
    with t.events_publish_lock:
        doc = _preferred_payload(t)
        try:
            data = json.dumps(doc, allow_nan=False, separators=(",", ":"))
        except ValueError as e:
            log(f"ERROR /events: /preferred carries a non-finite number ({e}); not published")
            return False
        digest = _preferred_digest(json.loads(data))
        as_of = (doc.get("ledger") or {}).get("as_of")
        with t.events_cond:
            last = t.events_pub["ledger_as_of"]
            ledger_aging = last is not None and as_of is not None and as_of - last >= STALE_MAX / 2
            if digest == t.events_pub["digest"] and not ledger_aging:
                return False
            t.events_pub.update(seq=t.events_pub["seq"] + 1, data=data, digest=digest,
                               ledger_as_of=as_of)
            t.events_cond.notify_all()
        return True


def _events_publisher(stop=None):
    """The publisher loop, one for every tenant: it renders each tenant whose
    flag is set and publishes to that tenant's subscribers. `stop` (an Event;
    set it, then mark) ends it, for tests. At most one pass each
    EVENTS_MIN_INTERVAL_S (64 §2, reports raise the rate): a mark inside that
    second waits for it to pass with the dirty flag still set, so the next
    render carries every change and none is dropped."""
    pause = stop if stop is not None else threading.Event()
    last = None
    while True:
        _events_dirty.wait()
        if stop is not None and stop.is_set():
            return
        if last is not None:
            left = last + EVENTS_MIN_INTERVAL_S - time.monotonic()
            if left > 0 and pause.wait(left):
                return
        _events_dirty.clear()  # before the render: a mark during it looks again
        for t in _registry.all():
            if not t.events_dirty:
                continue
            t.events_dirty = False
            try:
                if _events_publish(t):
                    last = time.monotonic()
            except Exception:  # noqa: BLE001 — one bad render must not end publishing
                log(f"ERROR /events publisher ({t.id}):\n" + traceback.format_exc())


def _events_start():
    """Start the publisher thread (once) and have it publish the first document."""
    global _events_thread
    with _events_thread_lock:
        if _events_thread is None:
            _events_dirty.set()
            _events_thread = threading.Thread(target=_events_publisher, name="events-publisher",
                                              daemon=True)
            _events_thread.start()


# The PROVIDERS registry is filled by the driver modules (providers/,
# register()) after this module is imported; the poller and state loaders
# resolve it at call time.

def _account_loop():
    """The account loop's source: the one registered provider whose accounts
    are polled by name in the poller's account pass and serve every lane (its
    row's `account_loop`), or None when no driver brings one. Keys: start()
    at boot, load() -> names each loop, names() -> the names known now,
    poll(name, known_meta) -> a result, generation(name) -> an int a
    credential replacement bumps, expires_at(name) -> its token's expiry."""
    for p in PROVIDERS.values():
        if p.get("account_loop"):
            return p["account_loop"]
    return None


def _account_loop_names(t):
    """The account loop's accounts as known now, [] without a loop."""
    loop = _account_loop()
    return list(loop["names"](t)) if loop else []


def _provider_poll_targets(t):
    """target name -> (provider id, account arg) for every enabled POLLABLE
    provider — the poll loop's sole dispatch source. A provider with an
    "accounts" enumerator (#71) contributes one target per pool account;
    without one (or when it returns None) it stays a single target named like
    the provider id — the pre-pool behavior. Rows with poll=None (an auth
    facade whose accounts the account loop polls, #72) never dispatch:
    they'd be a None(...) call inside the poller thread."""
    dispatch = {}
    for pname, p in PROVIDERS.items():
        if not p.get("poll") or not p["enabled"](t):
            continue
        accounts_fn = p.get("accounts")
        accounts = accounts_fn(t) if accounts_fn else None
        if accounts is None:
            dispatch[pname] = (pname, None)
        else:
            for acct in accounts:
                dispatch[acct] = (pname, acct)
    return dispatch


def _pollable_provider_ids():
    """Provider ids that dispatch polls — the key set for pool state and the
    /preferred `providers` payload. A facade row (poll=None) must NOT appear
    there: the proxy decodes providers.* as pool-steering data (#71), and a
    lane provider's entry would read as an unknown pool. A provider whose
    accounts serve the lanes (`lanes`) is no pool either."""
    return [pid for pid, p in PROVIDERS.items() if p.get("poll") and not p.get("lanes")]


def _poll_backoff(res, fails):
    """Seconds until a failed poll is retried (quota-model 50 §2.10). No HTTP
    response (DNS, connect, timeout: http 0) retries at the normal cadence,
    since no provider asked us to wait — a DNS fault can push every
    Anthropic account past STALE_MAX at once. An HTTP error honours its
    Retry-After, else backs off exponentially, each wait capped at
    POLL_MAX_BACKOFF. The cap is per wait: a run of failures still ages the
    last reading past STALE_MAX, and the stale rule then decides."""
    if not res.get("http"):
        return POLL_INTERVAL
    return res.get("retry_after") or min(POLL_INTERVAL * (2 ** fails), POLL_MAX_BACKOFF)


def poller(once=False):
    """The one poll thread: each pass walks every tenant (_poll_pass), then
    waits for the interval or a wakeup. `once` runs one pass (tests)."""
    while True:
        # Clear before doing work: a signal raised during the loop remains set and
        # makes the wait below return immediately. A signal between loops is safe
        # because this loop is already about to inspect the cleared schedule.
        _poll_wakeup.clear()
        for t in _registry.all():
            if t.stopped:
                continue
            try:
                _poll_pass(t)
            except Exception:  # noqa: BLE001 — one tenant's fault is never the thread's
                log(f"ERROR {t.id}: poll pass raised\n" + traceback.format_exc().rstrip())
        if once:
            return
        _poll_wakeup.wait(min(30, POLL_INTERVAL))


def _poll_pass(t):
    """One tenant's pass: its account loop's names and its enabled provider
    targets, each polled when due, then its decision recomputed."""
    # Backoff bookkeeping lives on the tenant (poll_fails/poll_schedule) so the
    # HTTP handlers can report it — the poller thread is their only writer.
    schedule = t.poll_schedule
    fails = t.poll_fails
    now = time.time()
    loop = _account_loop()
    names = loop["load"](t) if loop else []
    # Provider poll targets are keyed separately so they never collide with account
    # names and are not pruned by the account creds file logic. Enablement comes
    # from the PROVIDERS registry, re-checked every loop (see
    # _provider_poll_targets for the pool/facade dispatch rules).
    provider_dispatch = _provider_poll_targets(t)
    provider_names = set(provider_dispatch)
    # Prune provider targets that disappeared (provider disabled, account left
    # its file): a provider result in the state no target dispatches now.
    with t.lock:
        gone_providers = [n for n, r in t.state.items()
                          if _target_kind(r) == "provider" and n not in provider_names]
        for gone in gone_providers:
            t.state.pop(gone, None)
    with t.poll_health_lock:
        for gone in gone_providers:
            schedule.pop(gone, None)
            fails.pop(gone, None)

    targets = list(names) + sorted(provider_names)
    polled = False
    for name in targets:
        if t.stopped:
            return
        # Skip if not due yet.
        with t.poll_health_lock:
            retry_at = schedule.get(name, 0)
        if retry_at > now:
            continue

        is_provider = name in provider_names
        with t.lock:
            prev = t.state.get(name)

        # One target's fault is that target's failed poll, never the thread's:
        # a body of the wrong types raised in an adapter once, and no account
        # was polled again. The line and the balance note are read before the
        # result is published, so a fault there fails the poll too.
        tb = None
        try:
            if is_provider:
                pid, acct = provider_dispatch[name]
                res = PROVIDERS[pid]["poll"](t, prev, account=acct)
                if name not in _provider_poll_targets(t):
                    # Removed while it was polled (POST /auth/<pid>/remove): its
                    # result would list it again until the next pass pruned it.
                    log(f"{name}: discarded poll of a removed account")
                    continue
            else:
                # Re-authorization can finish while this network poll is in flight.
                # Never publish/schedule a result produced by the superseded token.
                generation = loop["generation"](t, name)
                known_meta = {"email": prev.get("email"), "tier": prev.get("tier")} if prev else None
                res = loop["poll"](t, name, known_meta)
                if generation != loop["generation"](t, name):
                    log(f"{name}: discarded poll from replaced credential")
                    continue
                if name not in loop["names"](t):
                    log(f"{name}: discarded poll of a removed account")
                    continue
            if res["ok"]:
                _checked_events(res["reading"], name)
                line = _reading_line(res)
                pot = _money_pot(res.get("provider"))
                credits = _reading_pot(res["reading"], pot)
                if is_provider and res.get("provider") in _COUNTABLE_UNITS and credits:
                    bal = (_reading_limit(credits, "balance") or {}).get("amount")
                    _ledger_note_balance(t, f"{name}/{pot}", bal, res["ts"])
        except Exception as e:  # noqa: BLE001
            tb = traceback.format_exc().rstrip()
            res = {"name": name, "ok": 0, "ts": time.time(), "http": 0,
                   "err": f"raised {type(e).__name__}: {e}", "retry_after": None, "reading": None}
            res.update({"kind": "provider", "provider": provider_dispatch[name][0]} if is_provider
                       else {"email": name, "tier": ""})

        with t.lock:
            if not res["ok"] and prev and prev.get("reading"):
                # transient failure: retain the last good reading so the gauges persist.
                res["reading"] = prev["reading"]
                if not is_provider:
                    res["tier"] = prev.get("tier") or res["tier"]
                    res["email"] = prev.get("email") or res["email"]
                res["good_ts"] = prev.get("good_ts")
                res["stale"] = 1
            else:
                res["good_ts"] = res["ts"] if res["ok"] else (prev or {}).get("good_ts")
                res["stale"] = 0 if res["ok"] else 1
            t.state[name] = res

        if res["ok"]:
            polled = True
            with t.poll_health_lock:
                fails[name] = 0
                schedule[name] = now + POLL_INTERVAL
            log(f"{t.id}: {res.get('email') or name}: {line}")
            try:
                _sizing_poll(t, name, res)  # poll-sized limits (quota-model 66 §7)
            except Exception:  # noqa: BLE001 — a sizing fault is never the poll's
                log(f"{name}: sizing raised on the poll's heads\n" + traceback.format_exc().rstrip())
        else:
            with t.poll_health_lock:
                fails[name] = fails.get(name, 0) + 1
                backoff = _poll_backoff(res, fails[name])
                schedule[name] = now + backoff
            stale = res.get("stale") and "(serving last-good)" or ""
            log(f"{t.id}: {name}: POLL FAILED — {res['err']} — backing off {int(backoff)}s {stale}")
            if tb:
                log(tb)
    # prune schedule/fails/state for accounts no longer configured (only when
    # the creds file actually listed some — never wipe on a transient read miss).
    if names:
        with t.poll_health_lock:
            gone_names = set(schedule) - set(names) - provider_names
            for gone in gone_names:
                schedule.pop(gone, None)
                fails.pop(gone, None)
        for gone in gone_names:
            with t.lock:
                t.state.pop(gone, None)
    compute_preferred(t)
    if polled:
        with t.plock:
            _save_state(t)  # the readings, so a restart serves them with their age


def _b64url(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


# state -> {verifier, account, minted_at, provider, mode, state}; one-shot,
# TTL-bounded. One flat store for every provider's pending grants (#72) — the
# provider tag is checked at take time, so a grant minted for one provider can
# never complete another's flow.
#   t.reauth_pending
REAUTH_TTL = 600      # seconds a minted authorize URL stays exchangeable


def _reauth_prune(t, now):
    for s, g in list(t.reauth_pending.items()):
        if now - g["minted_at"] > REAUTH_TTL:
            del t.reauth_pending[s]


def _auth_grant_mint(t, provider, mode, account=None, verifier=None):
    """Mint a one-shot pending grant and return its state token. mode is
    "reauth" (repair a named account) or "add" (enroll a new one) — persistence
    policy at complete time keys off it."""
    state = _b64url(secrets.token_bytes(24))
    with t.reauth_lock:
        _reauth_prune(t, time.time())
        t.reauth_pending[state] = {"verifier": verifier, "account": account,
                                  "minted_at": time.time(), "provider": provider,
                                  "mode": mode, "state": state}
    return state


def _auth_grant_take(t, state, provider):
    """Consume a pending grant (one-shot; consumed on the attempt whether or not
    the rest of the flow succeeds). Raises ValueError on unknown/expired state
    or a cross-provider state paste."""
    with t.reauth_lock:
        _reauth_prune(t, time.time())
        grant = t.reauth_pending.pop(state, None)
    if not grant:
        raise ValueError("unknown or expired state — begin again")
    if grant.get("provider", "anthropic") != provider:
        raise ValueError(f"this attempt was started for {grant.get('provider')} "
                         "— begin again on the right provider")
    return grant


def _remove_credentials(path):
    """Delete a driver's credentials file and any write left half-done beside
    it (a remove's last account). ValueError, naming the file, on failure."""
    try:
        _store.remove(path)
    except OSError as e:
        raise ValueError(f"the credentials could not be removed ({type(e).__name__})") from e


def _forget_account(t, name):
    """Drop everything the core keeps on account `name` once its driver has
    removed its credentials (POST /auth/<pid>/remove): its reading, its poll
    health and schedule, its re-check epoch and its served mark. Then the
    decision is recomputed, the state written and the document republished,
    so the next /preferred and /events no longer list it. A poll of it already
    in flight is discarded (the driver's remove bumps its generation) or pruned
    at the next pass (a provider target its driver no longer enumerates)."""
    with t.lock:
        t.state.pop(name, None)
    with t.poll_health_lock:
        t.poll_fails.pop(name, None)
        t.poll_schedule.pop(name, None)
        if t.recheck_epochs.pop(name, None) is not None:
            _save_rechecks_locked(t)
    with t.served_lock:
        t.served["head_at"].pop(name, None)
    compute_preferred(t)
    with t.plock:
        _save_state(t)
    _events_mark(t)


def _auth_providers_payload(t, now):
    """GET /auth/providers: per provider the DECLARED auth data (kind, custody,
    enrollment descriptor) plus live per-account health. Secrets never enter
    this payload — status rows carry names, health, and fingerprints only."""
    out = {}
    for pid, p in PROVIDERS.items():
        a = p.get("auth")
        if not a:
            continue
        out[pid] = {"kind": a["kind"], "label": a["label"],
                    "custody": a["custody"], "enroll": a["enroll"],
                    "accounts": a["status"](t, now)}
    return {"providers": out}


# ---- metric rendering -------------------------------------------------------

def _esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _line(name, labels, value):
    if labels:
        lbl = ",".join(f'{k}="{_esc(v)}"' for k, v in labels.items())
        return f"{name}{{{lbl}}} {value}"
    return f"{name} {value}"


def _tenant_line(tid):
    """A _line that stamps every series with its tenant: /metrics serves every
    tenant's series in one exposition, told apart by the `tenant` label."""
    def line(name, labels, value):
        return _line(name, {"tenant": tid, **labels}, value)
    return line


def render(tenants):
    """The exposition: the exporter's own series, then each tenant's, every
    one of those with a `tenant` label. GET /metrics renders every tenant;
    GET /t/<t>/metrics that one."""
    lines = []

    def helptype(metric, typ, help_):
        lines.append(f"# HELP {metric} {help_}")
        lines.append(f"# TYPE {metric} {typ}")

    lines.append("# HELP claude_usage_exporter_up Exporter liveness (1 while serving).")
    lines.append("# TYPE claude_usage_exporter_up gauge")
    lines.append(_line("claude_usage_exporter_up", {}, 1))
    lines.append(_line("claude_usage_exporter_uptime_seconds", {}, round(time.time() - _start_time, 1)))

    helptype("claude_usage_poll_success", "gauge", "1 if the last poll for this account succeeded.")
    helptype("claude_usage_last_poll_timestamp_seconds", "gauge", "Unix time of the last poll attempt.")
    helptype("claude_usage_data_age_seconds", "gauge", "Age of the usage numbers being served (0 on a fresh poll; climbs while an account is backed off / failing, since the last-good snapshot is retained).")
    helptype("claude_usage_poll_http_code", "gauge", "HTTP status of the last failed call (0 on success).")
    helptype("claude_usage_token_expires_at_seconds", "gauge", "Unix time the cached access token expires.")
    helptype("claude_usage_window_utilization_percent", "gauge", "Percent of the rolling window used (0-100).")
    helptype("claude_usage_window_resets_at_seconds", "gauge", "Unix time when the window resets.")
    helptype("claude_usage_limit_percent", "gauge", "Percent used for a specific limit entry.")
    helptype("claude_usage_limit_severity", "gauge", "Anthropic severity: 0 normal, 1 warning, 2 critical.")
    helptype("claude_usage_limit_is_active", "gauge", "1 if this limit is the currently binding constraint.")
    helptype("claude_usage_limit_resets_at_seconds", "gauge", "Unix time when this limit entry resets.")
    helptype("claude_usage_extra_credits_enabled", "gauge", "1 if extra-usage (overage) is enabled.")
    helptype("claude_usage_extra_credits_utilization_percent", "gauge", "Percent of extra-usage credits used.")
    helptype("claude_usage_extra_credits_used_dollars", "gauge", "Extra-usage credits spent, in dollars.")
    helptype("claude_usage_preferred", "gauge", "1 for the account the switchover policy currently prefers (the default lane), else 0.")
    helptype("claude_usage_switch_last_timestamp_seconds", "gauge", "Unix time of the last preferred-account switch (max over lanes).")
    helptype("claude_usage_lane_preferred", "gauge", "1 for the account this model lane currently routes to, else 0.")
    helptype("claude_usage_lane_effective_percent", "gauge", "Lane-effective utilization: max(overall 7d, 5-hour session, the lane's scoped model cap) — the binding constraint that 429s a request for this lane's model, and the switchover policy input.")
    helptype("claude_usage_lane_switch_last_timestamp_seconds", "gauge", "Unix time this lane last switched accounts.")
    helptype("claude_usage_lane_scoped_cap_found", "gauge", "1 if the lane's configured scope_model matched a weekly_scoped limit in the account's usage payload (join health; only for lanes that declare a scope_model).")

    helptype("provider_poll_success", "gauge", "1 if the last poll for this provider succeeded.")
    helptype("provider_last_poll_timestamp_seconds", "gauge", "Unix time of the last poll attempt for this provider.")
    helptype("provider_data_age_seconds", "gauge", "Age of the provider quota numbers being served (0 on a fresh poll; climbs while the provider is backed off / failing, since the last-good snapshot is retained).")
    helptype("provider_poll_http_code", "gauge", "HTTP status of the last failed provider call (0 when it returned 2xx).")
    helptype("provider_quota_utilization_percent", "gauge", "Percent of the provider quota window used (0-100).")
    helptype("provider_quota_used", "gauge", "Provider quota used value for the window.")
    helptype("provider_quota_limit", "gauge", "Provider quota limit value for the window.")
    helptype("provider_quota_remaining", "gauge", "Provider quota remaining value for the window.")
    helptype("provider_quota_window_resets_at_seconds", "gauge", "Unix time when the provider quota window resets.")
    helptype("provider_effective_percent", "gauge", "Provider effective utilization: max(five_hour, weekly), floored at 100 while the provider's own allowance gate is closed (codex rate_limit.allowed) — the Phase 2 routing input.")
    helptype("provider_quota_total_limit", "gauge", "Provider totalQuota.limit (informational).")
    helptype("provider_quota_total_remaining", "gauge", "Provider totalQuota.remaining (informational).")
    helptype("provider_parallel_limit", "gauge", "Provider parallel request limit (informational).")
    helptype("provider_membership_level", "gauge", "Provider membership level, value 1, level in label.")

    helptype("provider_pool_size", "gauge", "Polled accounts in this provider's credential pool (#71).")
    helptype("provider_account_poll_success", "gauge", "1 if the last poll for this provider account succeeded.")
    helptype("provider_account_data_age_seconds", "gauge", "Age of this provider account's quota numbers (0 on a fresh poll).")
    helptype("provider_account_poll_http_code", "gauge", "HTTP status of this provider account's last failed call (0 when it returned 2xx).")
    helptype("provider_account_effective_percent", "gauge", "This provider account's effective utilization: max(five_hour, weekly), floored at 100 while its allowance gate is closed — the pool policy input.")
    helptype("provider_account_quota_utilization_percent", "gauge", "Percent of this provider account's quota window used (0-100).")
    helptype("provider_account_quota_window_resets_at_seconds", "gauge", "Unix time when this provider account's quota window resets.")
    helptype("provider_account_preferred", "gauge", "1 for the account this provider's pool currently routes to, else 0.")
    helptype("money_balance", "gauge", "A money pot's balance in its provider's own unit, as last read (codex credits). Advisory: shown, never a routing gate.")
    helptype("quota_pot_open", "gauge", "1 while a pot is open, 0 while closed, by the basis of that answer (exact: the provider's own gate; derived: from its windows or flags). Absent while unknown.")
    helptype("entitlement_active", "gauge", "1 while the account is entitled to the pot, 0 when a reading says it is not. Absent while unknown.")
    helptype("route_check", "gauge", "1 for each route's current dial check on each account: included, paid:<pot> or no:<reason>.")
    helptype("money_spent_total", "counter", "Our own charges on a money pot by route, in the pot's unit, as the ledger counted them (uncounted hops excluded).")
    helptype("served_events_total", "counter", "Served requests the proxy reported, per account (quota-model 65 §7).")
    helptype("served_api_dollars_total", "counter", "The served requests' cost at the models' API list prices, in USD, per account: a yardstick, not money spent (65 §2).")
    helptype("served_unpriced_total", "counter", "Served requests left unpriced, per account, model and reason (no list price, incomplete, speed unknown, ...).")
    helptype("served_incomplete_total", "counter", "Served requests whose response ended without its final usage, per account.")
    helptype("served_seq_missing_total", "counter", "Served events lost on the way: the seqs missing where a proxy boot's seq jumped.")
    helptype("served_duplicates_total", "counter", "Served events sent again with an id already taken, and ignored.")
    helptype("sizing_size_api_usd", "gauge", "A window's learned size in API dollars at the mix of what has run on it lately (100 x the median per-point dollars of its kept samples, at its provider's model weights), present only while learned (quota-model 66 §5, 67 §2).")
    helptype("sizing_model_size_api_usd", "gauge", "A window's learned size in one model's API dollars, for its provider's reference model and each weighed model the window's scope binds, present only while learned (quota-model 67 §2).")
    helptype("sizing_weight", "gauge", "How many reference-model API dollars one of a model's counts as on its provider's meters: 1 for the reference, a learned weight for each weighed model (quota-model 67 §3).")
    helptype("sizing_weights_disagree", "gauge", "1 when a window's own samples weigh the model apart from its provider's shared weight by more than the weight bound, else 0; for the models a window's own samples determine (quota-model 67 §3).")
    helptype("fleet_capacity_api_usd", "gauge", "Room with nothing used, in API dollars of the work each account would do: per level (proxy, selection, account) and name; sized accounts only (quota-model 65 §1, 67 §5).")
    helptype("fleet_room_api_usd", "gauge", "Room left now, in the same dollars as fleet_capacity_api_usd: (cap - used) x size, 0 while refused, summed over the sized accounts (quota-model 65 §1, 67 §5).")
    helptype("fleet_left_pct", "gauge", "Room left as a percent of the room with nothing used, for the proxy and each selection. For an account, its main window's own (cap - used) / cap, the provider's number: shown whether the account is counted, refused or listed, so it is not room / capacity (quota-model 67 §8, §13 Q7).")
    helptype("fleet_pace_api_usd_per_h", "gauge", "A selection's or the whole proxy's pace: its priced served API dollars over the hours of the last week it could serve, present once 24 such hours are counted (quota-model 69 §4.2).")
    helptype("fleet_forecast_dry_s", "gauge", "Seconds in the coming week a selection or the whole proxy is forecast to have no room, walking its accounts' room and resets forward at its pace; an estimate, present only with a pace (quota-model 69 §4.3).")
    helptype("fleet_forecast_unmet_api_usd", "gauge", "The pace times the forecast's dry seconds: the API dollars of work the selection could not be served in the coming week, by the same estimate (quota-model 69 §4.3).")
    helptype("sizing_spread", "gauge", "The spread of a window's kept samples: their median absolute deviation over their median, with two or more samples.")
    helptype("sizing_samples", "gauge", "Samples kept for a window's size, one per clean step.")
    helptype("sizing_step_api_usd", "gauge", "The newest sample's API dollars per point of the window, at its provider's model weights (reference-model dollars; list dollars while no model is weighed).")
    helptype("sizing_steps_total", "counter", "A window's closed steps by outcome: sample (cheap and bounded counted beside it), or why it is no sample (66 §4).")
    helptype("sizing_outside_points", "gauge", "Points of the window's measured range this period that came from outside the proxy (66 §6).")
    helptype("sizing_measured_points", "gauge", "Points of the window's measured range this period: the closed steps whose work is known.")
    helptype("sizing_outside_pct", "gauge", "Outside points as a percent of the measured points.")
    helptype("sizing_events_dropped_total", "counter", "Served events dropped from an account's sizing buffer, past its bound.")
    helptype("sizing_samples_retracted_total", "counter", "Samples taken back after their step closed: an event of the step arrived late (late), or a reporter's restart was found in it (proxy_restart).")
    helptype("sizing_events_late_total", "counter", "Served events that arrived after every window they draw had closed past them.")

    for t in tenants:
        lines.extend(_render_tenant(t))
    return "\n".join(lines) + "\n"


def _render_tenant(t):
    """One tenant's series, each with its `tenant` label."""
    lines = []
    _line = _tenant_line(t.id)
    loop = _account_loop()
    with t.plock:
        pref_acct = t.preferred.get("account")
        pref_last_switch = t.preferred.get("last_switch", 0)
        lanes_pref = {ln: dict(s) for ln, s in t.preferred.get("lanes", {}).items()}
    switch_ts = max([pref_last_switch] + [s.get("last_switch", 0) for s in lanes_pref.values()])
    lines.append(_line("claude_usage_switch_last_timestamp_seconds", {}, round(switch_ts)))
    for lc in _lanes:
        s = lanes_pref.get(lc["lane"])
        if s:
            lines.append(_line("claude_usage_lane_switch_last_timestamp_seconds",
                               {"lane": lc["lane"]}, round(s.get("last_switch", 0))))

    with t.lock:
        snapshot = list(t.state.values())

    for res in snapshot:
        # Label by the STABLE config name (not the API-discovered email): a profile
        # 429 must never change an account's identity mid-flight, or it spawns an
        # orphan series (e.g. "user" vs "user@example.com") that lingers on the
        # dashboard. Set the config `name` to the email you want shown.
        acct = res["name"]
        base = {"account": acct}
        lines.append(_line("claude_usage_poll_success", base, res.get("ok", 0)))
        lines.append(_line("claude_usage_last_poll_timestamp_seconds", base, round(res.get("ts", 0), 1)))
        good_ts = res.get("good_ts")
        age = round(time.time() - good_ts, 1) if good_ts else 0
        lines.append(_line("claude_usage_data_age_seconds", base, age))
        lines.append(_line("claude_usage_poll_http_code", base, res.get("http", 0)))
        lines.append(_line("claude_usage_preferred", base, 1 if res["name"] == pref_acct else 0))
        for lc in _lanes:
            lbase = dict(base, lane=lc["lane"])
            s = lanes_pref.get(lc["lane"])
            if s:
                lines.append(_line("claude_usage_lane_preferred", lbase,
                                   1 if s.get("account") == acct else 0))
            v = _lane_effective(res, lc)
            if v is not None:
                lines.append(_line("claude_usage_lane_effective_percent", lbase, v))
            if lc.get("scope_model") and res.get("reading"):
                w = _scoped_window(res, lc["scope_model"])
                found = w is not None and w.get("used_pct") is not None
                lines.append(_line("claude_usage_lane_scoped_cap_found", lbase, 1 if found else 0))
        exp = loop["expires_at"](t, res["name"]) if loop else None
        if exp:
            lines.append(_line("claude_usage_token_expires_at_seconds", base, round(exp)))
        rd = res.get("reading")
        if not rd:
            continue
        provider = res.get("kind") == "provider"
        tier = res.get("tier", "")
        wl = dict(base, tier=tier)
        windows = _reading_windows(rd)
        for window in ("five_hour", "seven_day"):
            w = windows.get(window)
            if w is None:
                continue
            if w.get("used_pct") is not None:
                lines.append(_line("claude_usage_window_utilization_percent", dict(wl, window=window),
                                   w["used_pct"]))
            # A provider's window reset rides provider_quota_* alone, as it
            # always has: this family keeps its samples.
            if w.get("resets_at") is not None and not provider:
                lines.append(_line("claude_usage_window_resets_at_seconds", dict(wl, window=window),
                                   round(w["resets_at"])))
        for w in windows.values():
            for lim in w.get("sources") or []:
                ll = dict(wl, kind=lim["kind"], group=lim["group"], scope_model=lim["scope_model"])
                if lim["percent"] is not None:
                    lines.append(_line("claude_usage_limit_percent", ll, lim["percent"]))
                # A severity Anthropic gave none of, or one we do not know, has no
                # sample: unknown is never 0 (normal).
                if lim["severity"] in SEVERITY:
                    lines.append(_line("claude_usage_limit_severity", ll, SEVERITY[lim["severity"]]))
                lines.append(_line("claude_usage_limit_is_active", ll, 1 if lim["is_active"] else 0))
                if lim["resets_at"] is not None:
                    lines.append(_line("claude_usage_limit_resets_at_seconds", ll, round(lim["resets_at"])))
        credits = None if provider else _reading_pot(rd, MONEY_POT)
        if credits is not None:
            cap = _reading_limit(credits, "monthly_limit") or {}
            lines.append(_line("claude_usage_extra_credits_enabled", wl, 1 if credits["open"] else 0))
            if cap.get("used_pct") is not None:
                lines.append(_line("claude_usage_extra_credits_utilization_percent", wl, cap["used_pct"]))
            if cap.get("used") is not None:
                # used_credits is in cents-like minor units per the API (1234 == $12.34)
                lines.append(_line("claude_usage_extra_credits_used_dollars", wl,
                                   round(cap["used"] / 100.0, 2)))

    # ---- provider quota metrics (Phase 1; additive, does not touch claude_usage_*)
    # With account pools (#71) one provider can have several polled targets. The
    # legacy provider_* series are labeled only {provider}, so they are emitted
    # ONCE per provider, from the pool INCUMBENT's result — duplicate samples of
    # one series are invalid exposition and would kill the whole scrape. A pool
    # of one renders byte-identically to the pre-pool exporter. Per-account
    # detail rides the additive provider_account_* family below.
    with t.plock:
        prov_incumbents = {pid: ps.get("account")
                           for pid, ps in t.preferred.get("providers", {}).items()}
    prov_results = {}
    for res in snapshot:
        if res.get("kind") == "provider":
            prov_results.setdefault(res.get("provider") or res["name"], []).append(res)
    for prov in sorted(prov_results):
        res = _pool_pick(prov_results[prov], prov_incumbents.get(prov))
        base = {"provider": prov}
        lines.append(_line("provider_pool_size", base, len(prov_results[prov])))
        lines.append(_line("provider_poll_success", base, res.get("ok", 0)))
        lines.append(_line("provider_last_poll_timestamp_seconds", base, round(res.get("ts", 0), 1)))
        good_ts = res.get("good_ts")
        age = round(time.time() - good_ts, 1) if good_ts else 0
        lines.append(_line("provider_data_age_seconds", base, age))
        lines.append(_line("provider_poll_http_code", base, res.get("http", 0)))
        eff = _provider_effective(res)  # the reading's account-wide windows, any provider
        if eff is not None:
            lines.append(_line("provider_effective_percent", base, eff))
        rd = res.get("reading")
        if not rd:
            continue
        # Every window of the reading: five_hour and seven_day as always, and
        # any other (a provider's own length, an additional limit) as a new label value.
        for wid, w in _reading_windows(rd).items():
            wl = dict(base, window=wid)
            if w.get("used_pct") is not None:
                lines.append(_line("provider_quota_utilization_percent", wl, w["used_pct"]))
            for k in ("used", "limit", "remaining"):
                if w.get(k) is not None:
                    lines.append(_line(f"provider_quota_{k}", wl, w[k]))
            if w.get("resets_at") is not None:
                lines.append(_line("provider_quota_window_resets_at_seconds", wl, round(w["resets_at"])))
        tq = _reading_limit(_reading_pot(rd, ALLOWANCE_POT), "total_quota") or {}
        if tq.get("limit") is not None:
            lines.append(_line("provider_quota_total_limit", base, tq["limit"]))
        if tq.get("remaining") is not None:
            lines.append(_line("provider_quota_total_remaining", base, tq["remaining"]))
        par = next((b for b in rd.get("beside") or [] if b.get("kind") == "concurrency"), None)
        if par is not None:
            lines.append(_line("provider_parallel_limit", base, par["limit"]))
        level = (rd.get("plan") or {}).get("label")
        if level:
            lines.append(_line("provider_membership_level", dict(base, level=str(level)), 1))

    # ---- per-account provider metrics (#71; additive) — one sample set per
    # polled pool account, {provider, account} labeled, pool-of-one included.
    for prov in sorted(prov_results):
        incumbent = prov_incumbents.get(prov)
        for res in prov_results[prov]:
            base = {"provider": prov, "account": res["name"]}
            lines.append(_line("provider_account_poll_success", base, res.get("ok", 0)))
            good_ts = res.get("good_ts")
            age = round(time.time() - good_ts, 1) if good_ts else 0
            lines.append(_line("provider_account_data_age_seconds", base, age))
            lines.append(_line("provider_account_poll_http_code", base, res.get("http", 0)))
            lines.append(_line("provider_account_preferred", base,
                               1 if res["name"] == incumbent else 0))
            eff = _provider_effective(res)
            if eff is not None:
                lines.append(_line("provider_account_effective_percent", base, eff))
            rd = res.get("reading")
            if not rd:
                continue
            # The money pot's balance (quota-model O-D4), per account: a pool
            # sibling's credits are its own. Unknown is absent, never 0.
            credits = _reading_pot(rd, _money_pot(prov))
            bal = (_reading_limit(credits, "balance") or {}).get("amount")
            if bal is not None:
                lines.append(_line("money_balance", dict(base, pot=credits["id"],
                                                         unit=credits["unit"]), bal))
            for wid, w in _reading_windows(rd).items():
                wl = dict(base, window=wid)
                if w.get("used_pct") is not None:
                    lines.append(_line("provider_account_quota_utilization_percent", wl, w["used_pct"]))
                if w.get("resets_at") is not None:
                    lines.append(_line("provider_account_quota_window_resets_at_seconds", wl,
                                       round(w["resets_at"])))

    # ---- the model's own facts (quota-model 50 §2.11, C6): pots, entitlements,
    # every route's check, and our spend. New families only; no rule reads them.
    view = _sizing_fleet_view(t)  # under _sizing_lock, never inside _plock (67 §8)
    with t.plock:
        ctx = _policy_ctx(t, snapshot, time.time())
        # No health: the gauges read no asks, and account_health would take
        # _refresh_lock, which a token refresh holds across its network call.
        fleet = _fleet_payload(ctx, view, {}, _fleet_preferred(t, ctx))
    _burn_stamp(t, fleet, ctx["now"])  # a render of the figure (69 §4.2)
    for acct, st in sorted(ctx["standings"].items()):
        if st.get("as_of") is None:
            continue
        unscoped = {n: w for n, w in st["allowance"]["windows"].items() if w.get("scope") is None}
        allow, _ = _allowance_state(st, unscoped, ctx["now"])
        pots = [(ALLOWANCE_POT, st["allowance"], allow,
                 st["allowance"].get("open_basis") or "derived")]
        pots += [(pid, p, p.get("open"), p.get("open_basis")) for pid, p in sorted(st["money"].items())]
        for pid, p, is_open, basis in pots:
            if is_open is not None:
                lines.append(_line("quota_pot_open", {"account": acct, "pot": pid,
                                                      "basis": basis or "unknown"}, 1 if is_open else 0))
            ent = (p.get("entitlement") or {}).get("state")
            if ent is not None and ent != "unknown":
                lines.append(_line("entitlement_active", {"account": acct, "pot": pid},
                                   1 if ent == "active" else 0))
    for rid in sorted(ctx["checks"]):
        for acct, c in sorted(ctx["checks"][rid].items()):
            label = {"included": "included", "paid": f"paid:{c['pot']}"}.get(c["check"]) \
                or f"no:{c['reason']}"
            lines.append(_line("route_check", {"route": rid, "account": acct, "check": label}, 1))
    for (pid, route), total in sorted(_ledger_totals(t).items()):
        acct, pot = _split_pot(pid)
        lines.append(_line("money_spent_total", {"account": acct, "pot": pot, "route": route},
                           round(total, 6)))
    with t.served_lock:
        served = {k: (dict(v) if isinstance(v, dict) else v) for k, v in t.served.items()
                  if k not in ("ids", "boots")}
    for acct, n in sorted(served["events"].items()):
        lines.append(_line("served_events_total", {"account": acct}, n))
    for acct, usd in sorted(served["usd"].items()):
        lines.append(_line("served_api_dollars_total", {"account": acct}, round(usd, 6)))
    for (acct, model, reason), n in sorted(served["unpriced"].items()):
        lines.append(_line("served_unpriced_total", {"account": acct, "model": model, "reason": reason}, n))
    for acct, n in sorted(served["incomplete"].items()):
        lines.append(_line("served_incomplete_total", {"account": acct}, n))
    lines.append(_line("served_seq_missing_total", {}, served["seq_missing"]))
    lines.append(_line("served_duplicates_total", {}, served["duplicates"]))
    lines.extend(_sizing_metric_lines(t))
    lines.extend(_fleet_metric_lines(fleet, t.id))
    return lines


# The requests that cannot change /preferred; every other handled request marks
# it dirty once answered (quota-model 64 §2). "" is a tenant's root, its /metrics.
_PURE_READS = ("/preferred", "/metrics", "", "/events", "/sizing", "/tenants", "/tenants/events")
_TENANT_PREFIX_RE = re.compile(r"^/t/([^/]+)(/.*)?$")


class Handler(BaseHTTPRequestHandler):
    """Every tenant route is /t/<tenant>/<route>: the prefix is stripped and
    the tenant bound here, once, before any handler runs (self.tenant,
    self.tpath); a handler reads self.tenant and passes it down. A tenant
    route without the prefix, or naming a tenant the registry does not hold,
    is 404: nothing here maps "no tenant" to the household. The routes
    without a tenant are the operator's: /metrics (every tenant) and the
    registry (/tenants, key-guarded)."""

    def _bind(self):
        """-> (tenant or None, the path under the prefix, or the whole path)."""
        parsed = urlparse(self.path)
        m = _TENANT_PREFIX_RE.match(parsed.path)
        if not m:
            self.tenant, self.tpath = None, parsed.path.rstrip("/")
            return parsed
        self.tenant = _registry.get(m.group(1)) if _registry is not None else None
        self.tpath = (m.group(2) or "").rstrip("/")
        return parsed

    def _no_tenant(self, drain=False):
        if drain:
            self._drain_body()
        self._json(404, {"error": "unknown tenant"})

    def do_GET(self):  # noqa: N802
        self._bind()
        try:
            self._get()
        finally:
            if self.tpath not in _PURE_READS and self.tenant is not None:
                _events_mark(self.tenant)

    def do_POST(self):  # noqa: N802
        self._bind()
        try:
            self._post()
        finally:
            _events_mark(self.tenant)

    def do_PUT(self):  # noqa: N802
        self._bind()
        try:
            self._put()
        finally:
            _events_mark(self.tenant)

    def do_DELETE(self):  # noqa: N802
        self._bind()
        try:
            self._delete()
        finally:
            _events_mark(self.tenant)

    def _get(self):
        parsed = self._bind()
        path = self.tpath
        t = self.tenant
        if t is None:
            if parsed.path.startswith("/t/"):
                return self._no_tenant()
            if path == "/metrics":
                return self._metrics(_registry.all())
            if path == "/tenants":
                return self._tenants_get()
            if path == "/tenants/events":
                return self._tenants_events()
            self.send_response(404)
            self.end_headers()
            return
        if path in ("/metrics", ""):
            self._metrics([t])
        elif path == "/preferred":
            self._json(200, _preferred_payload(t))
        elif path == "/events":
            self._events_stream()
        elif path == "/sizing":
            self._json(200, sizing_payload(t))
        elif path == "/api-key":
            self._api_key(parsed)
        elif path == "/pin":
            self._pin(parsed)
        elif path == "/class-bind":
            self._class_bind(parsed)
        elif path == "/class-pin":
            self._class_pin(parsed)
        elif path == "/auth/providers":
            self._auth_providers()
        elif path in ("/classes", "/consumers", "/money"):
            self._doc_get(path[1:])
        elif path in ("/classes/history", "/consumers/history", "/money/history"):
            self._doc_history(path.split("/")[1], parsed)
        elif path == "/classes/catalog":
            self._classes_catalog()
        elif path == "/classes/starter":
            self._classes_starter(t)
        elif ("GET", path) in ROUTES:
            ROUTES[("GET", path)](t, self, parsed)
        else:
            self.send_response(404)
            self.end_headers()

    def _metrics(self, tenants):
        body = render(tenants).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _post(self):
        parsed = self._bind()
        path = self.tpath
        t = self.tenant
        if t is None:
            if parsed.path.startswith("/t/"):
                return self._no_tenant(drain=True)
            self._drain_body()
            self.send_response(404)
            self.end_headers()
            return
        m = re.match(r"^/auth/([a-z0-9-]+)/(begin|complete|remove)$", path)
        md = re.match(r"^/(classes|consumers|money)/(validate|reload)$", path)
        if ("POST", path) in ROUTES:
            ROUTES[("POST", path)](t, self, parsed)
        elif m:
            self._auth_flow(m.group(1), m.group(2))
        elif md and md.group(2) == "validate":
            self._doc_validate(md.group(1))
        elif md:
            self._doc_reload(md.group(1))
        elif path == "/reports":
            self._reports_post()
        elif path == "/recheck":
            self._recheck(parsed)
        else:
            # Same rule as do_PUT's fallthrough: a typo'd POST path arrives
            # with a JSON body, and closing with it unread RSTs the 404 off
            # the socket — drain first so the client sees the refusal.
            self._drain_body()
            self.send_response(404)
            self.end_headers()

    def _put(self):
        parsed = self._bind()
        path = self.tpath
        t = self.tenant
        if t is None:
            if parsed.path.startswith("/t/"):
                return self._no_tenant(drain=True)
            mt = re.match(r"^/tenants/([^/]+)$", path)
            if mt:
                return self._tenants_put(mt.group(1))
        elif path in ("/classes", "/consumers", "/money"):
            return self._doc_put(path[1:])
        self._drain_body()
        self.send_response(404)
        self.end_headers()

    def _delete(self):
        parsed = self._bind()
        path = self.tpath
        if self.tenant is None:
            if parsed.path.startswith("/t/"):
                return self._no_tenant(drain=True)
            mt = re.match(r"^/tenants/([^/]+)$", path)
            if mt:
                return self._tenants_delete(mt.group(1))
        self._drain_body()
        self.send_response(404)
        self.end_headers()

    # -- the registry (operator, key-guarded) --

    def _tenants_get(self):
        """GET /tenants: every tenant's record, [{tenant, name, lan, tap}]."""
        if not self._guard():
            return
        self._json(200, {"tenants": _registry.payload()})

    def _tenants_put(self, tid):
        """PUT /tenants/<t> {name, lan, tap}, the whole record: create the
        tenant (its dir with empty documents, and the starter as its classes
        document, _join_classes) or update its name and tap, its documents
        untouched. 200 {tenant, name, lan, tap, created}; 400 naming the bad
        id or the missing or invalid field; 409 for a second lan tenant or a
        change to lan (the household is the migration's, and its notices'
        bridge is fixed with it)."""
        if not self._guard():
            return
        body = self._read_json_body()
        if not isinstance(body, dict):
            return self._json(400, {"error": "body must be a JSON object {name, lan, tap}"})
        rec = {"tenant": tid, "name": body.get("name"), "lan": body.get("lan"), "tap": body.get("tap")}
        why = tenancy.record_problem(rec)
        if why:
            return self._json(400, {"error": why})
        # A dir already on disk (a tenants.json restored from before it, a DELETE stopped partway)
        # may hold sealed credentials: a failed join removes only what this PUT made.
        tdir = os.path.join(_registry.dir, tid)
        existed = os.path.isdir(tdir)
        before = set(os.listdir(tdir)) if existed else set()
        try:
            t, created = _registry.put(rec)
        except tenancy.RegistryConflict as e:
            return self._json(409, {"error": str(e), "tenant": tid})
        if created:
            try:
                _join_classes(t)
                _tenant_boot(t)
            except Exception as e:  # noqa: BLE001 — the tenant is removed and the caller told
                log(f"ERROR tenants: {tid}: the join failed ({type(e).__name__}: {e}); "
                    "the tenant is removed, so a PUT again makes it whole")
                body = {"error": f"the tenant's join failed ({type(e).__name__}): it was not created; "
                                 "PUT it again", "tenant": tid}
                try:
                    _registry.delete(tid, remove_dir=not existed)
                    for name in (set(os.listdir(tdir)) - before) if existed else ():
                        path = os.path.join(tdir, name)
                        if os.path.isdir(path) and not os.path.islink(path):
                            shutil.rmtree(path)
                        else:
                            os.remove(path)
                except Exception as ce:  # noqa: BLE001 — said in the reply, which is still JSON
                    log(f"ERROR tenants: {tid}: removing the failed join: {type(ce).__name__}: {ce}")
                    body["cleanup"] = f"{type(ce).__name__}: {ce}"
                return self._json(500, body)
            _poll_wakeup.set()
        _events_mark(t)
        self._json(200, dict(t.record(), created=created))

    def _tenants_delete(self, tid):
        """DELETE /tenants/<t>: the tenant's polling stops, its state leaves
        memory and its dir is removed whole, credentials included. 200 {tenant,
        deleted: true}; 404 for one the registry does not hold; 409 for the
        lan tenant."""
        if not self._guard():
            return
        self._drain_body()
        if _registry.get(tid) is None:
            return self._json(404, {"error": "unknown tenant", "tenant": tid})
        try:
            t = _registry.delete(tid)
        except tenancy.RegistryConflict as e:
            return self._json(409, {"error": str(e), "tenant": tid})
        self._json(200, {"tenant": t.id, "deleted": True})

    def _tenants_events(self):
        """GET /tenants/events: an SSE stream of the registry, `event: tenants`
        with the list as data, at once and at every change; ": keepalive"
        after EVENTS_KEEPALIVE_S with nothing sent. Key-guarded."""
        if not self._guard():
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        sent = -1
        try:
            self.wfile.write(f"retry: {EVENTS_RETRY_MS}\n\n".encode("utf-8"))
            while True:
                with _registry.cond:
                    if _registry.seq == sent:
                        _registry.cond.wait(EVENTS_KEEPALIVE_S)
                    seq = _registry.seq
                if seq != sent:
                    data = json.dumps({"tenants": _registry.payload()}, separators=(",", ":"))
                    self.wfile.write(f"id: {_EVENTS_BOOT}-{seq}\nevent: tenants\ndata: {data}\n\n"
                                     .encode("utf-8"))
                    sent = seq
                else:
                    self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            return

    def _events_stream(self):
        """GET /events (quota-model 64 §2): `retry`, the current document at
        once, then each newer one as it is published; ": keepalive" after
        EVENTS_KEEPALIVE_S with nothing sent. Newest only: a slow reader skips
        to the latest. Last-Event-ID is ignored. Holds _events_cond only to
        read the newest document, never while writing."""
        t = self.tenant
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        token, sent = object(), 0
        with t.events_cond:
            t.events_subs.add(token)
        try:
            self.wfile.write(f"retry: {EVENTS_RETRY_MS}\n\n".encode())
            quiet_since = time.monotonic()
            while True:
                data = None
                with t.events_cond:
                    while not t.stopped:
                        if t.events_pub["data"] is not None and t.events_pub["seq"] != sent:
                            sent, data = t.events_pub["seq"], t.events_pub["data"]
                            break
                        left = quiet_since + EVENTS_KEEPALIVE_S - time.monotonic()
                        if left <= 0:
                            break
                        t.events_cond.wait(left)
                if t.stopped:
                    break  # the tenant was deleted: the stream ends
                if data is not None:
                    chunk = f"id: {_EVENTS_BOOT}-{sent}\nevent: preferred\ndata: {data}\n\n"
                else:
                    chunk = ": keepalive\n\n"
                self.wfile.write(chunk.encode("utf-8"))
                quiet_since = time.monotonic()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass  # the subscriber left
        finally:
            with t.events_cond:
                t.events_subs.discard(token)
            self.close_connection = True

    def _drain_body(self):
        """Discard an unread request body before an early refusal. Closing the
        connection with unread bytes still in the socket makes the kernel RST
        it, and the client sees a connection reset instead of the 401/503."""
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = 0
        if 0 < n <= (1 << 20):
            try:
                self.rfile.read(n)
            except OSError:
                pass

    def _guard(self):
        """The /token-style X-Switch-Key gate for the #72 auth surface: 503 when
        no key is mounted, 401 on mismatch. True when the request may proceed."""
        if not SWITCH_KEY:
            self._drain_body()
            self._json(503, {"error": "SWITCH_KEY not configured"})
            return False
        if not hmac.compare_digest(self.headers.get("X-Switch-Key", ""), SWITCH_KEY):
            self._drain_body()
            self.send_response(401)
            self.end_headers()
            return False
        return True

    def _reports_post(self):
        """POST /reports {"reporter", "events": [...]} (quota-model 64 §5):
        what the proxy saw, each event applied on its own. Key-guarded: a
        report moves a dial check, and a ledger entry a budget gate. 200
        {results: [{id, result, why}], seq}; 400 for any other body. One
        recompute when anything a decision reads applied (not served, which
        is only counted); the stream carries the result."""
        t = self.tenant
        if not self._guard():
            return
        body = self._read_json_body(max_bytes=DOC_BODY_MAX)
        why = reports_body_problem(body)
        if why:
            return self._json(400, {"error": why})
        out, decisive = reports_post(t, body["events"], body.get("reporter"), time.time())
        if decisive:
            compute_preferred(t)
        self._json(200, out)

    def _recheck(self, parsed):
        """POST /recheck?account=<id> — the owner's re-check after a purchase no
        reading sees (§2.6): the account is polled at once and its
        recheck_epoch bumped. Key-guarded: it ends refusal records. 200
        {account, recheck_epoch}; 400 without an account; 404 for one the
        brain does not know."""
        t = self.tenant
        if not self._guard():
            return
        self._drain_body()
        account = (parse_qs(parsed.query).get("account") or [""])[0]
        if not account:
            return self._json(400, {"error": "account query parameter is required"})
        epoch = recheck(t, account, time.time())
        if epoch is None:
            return self._json(404, {"error": "no such account", "account": account})
        log(f"{account}: owner re-check — polling now, recheck_epoch {epoch:.0f}")
        self._json(200, {"account": account, "recheck_epoch": epoch})

    def _auth_providers(self):
        """Per-provider auth status + enrollment descriptors. Key-guarded:
        account names and failure detail are not public data."""
        t = self.tenant
        if not self._guard():
            return
        self._json(200, _auth_providers_payload(t, time.time()))

    def _auth_flow(self, pid, step):
        """POST /auth/<pid>/begin|complete|remove — the ONE enrollment surface
        every provider authority is driven through (#72), and its undo."""
        t = self.tenant
        if not self._guard():
            return
        p = PROVIDERS.get(pid)
        auth = p.get("auth") if p else None
        if not auth:
            return self._json(404, {"error": f"unknown provider {pid!r}",
                                    "providers": sorted(k for k, v in PROVIDERS.items()
                                                        if v.get("auth"))})
        body = self._read_json_body() or {}
        if step == "remove":
            return self._auth_remove(t, pid, auth, body)
        if step == "begin":
            account = body.get("account") or None
            if account is not None and \
                    account not in {r["name"] for r in auth["status"](t, time.time())}:
                return self._json(404, {"error": f"unknown {pid} account {account!r}"})
            url, state = auth["begin"](t, account)
            log(f"auth begin: {pid} account={account or '(new)'}")
            out = {"state": state, "expires_in": REAUTH_TTL,
                   "mode": "reauth" if account else "add"}
            if url:
                out["authorize_url"] = url
            return self._json(200, out)
        state = str(body.get("state") or "")
        if not state:
            return self._json(400, {"error": "state is required"})
        try:
            grant = _auth_grant_take(t, state, pid)
            res = auth["complete"](t, grant, body)
        except ValueError as e:
            return self._json(422, {"error": str(e)})
        # saved_at: every driver's complete has persisted the credential (and
        # the account loop's has moved its generation on) before it returns,
        # and every driver's poll stamps its ts before it reads a credential,
        # so a good reading with good_ts > saved_at is the new credential's.
        # Taken before the epoch advance below makes the account due again, so
        # a poll the save itself woke is followed by one that counts.
        res = dict(res, saved_at=time.time())
        if res.get("account"):
            # After any remove of the same account that overlapped this add:
            # its forget can't then drop the epoch this advance writes.
            with t.enrol_lock:
                _advance_epoch(t, res["account"], time.time())
            _poll_wakeup.set()
        log(f"auth complete: {pid} account={res.get('account')} "
            f"enrolled={res.get('enrolled')}")
        return self._json(200, res)

    def _auth_remove(self, t, pid, auth, body):
        """POST /auth/<pid>/remove {account} (C11): disconnect one account. The
        driver deletes its record from the credentials file (the file itself
        when it was the last), then the core forgets its poll state, so the
        next document no longer lists it. 400 without an account, 404 for one
        this provider does not hold, 422 when the driver cannot rewrite its
        file (nothing is forgotten then: the account is still there)."""
        account = body.get("account")
        if not isinstance(account, str) or not account.strip():
            return self._json(400, {"error": "account is required"})
        account = account.strip()
        if account not in {r["name"] for r in auth["status"](t, time.time())}:
            return self._json(404, {"error": f"unknown {pid} account {account!r}"})
        with t.enrol_lock:
            try:
                auth["remove"](t, account)
            except ValueError as e:
                return self._json(422, {"error": str(e)})
            _forget_account(t, account)
        log(f"auth remove: {pid} account={account}")
        return self._json(200, {"account": account, "removed": True})


    def _read_json_body(self, max_bytes=16384):
        """Parse the request body as JSON, or None. The default cap is sized
        for enrollment payloads; the document endpoints (#92) pass
        max_bytes=DOC_BODY_MAX — whole classes/consumers documents ride these
        bodies and must not be funneled through the 16 KB limit."""
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return None
        if not 0 < n <= max_bytes:
            # Refused WITHOUT reading — drain (bounded) so the error response
            # isn't RST'd off the socket under the unread bytes. Callers that
            # already consumed the body must never re-drain (it would block).
            if 0 < n <= (1 << 20):
                try:
                    self.rfile.read(n)
                except OSError:
                    pass
            return None
        try:
            return json.loads(self.rfile.read(n))
        except Exception:  # noqa: BLE001
            return None


    def _api_key(self, parsed):
        """GET /api-key?account=<id> (quota-model C8): the static key of an
        api-kind account (§1.2), for the proxy's adapter, which holds no key of
        its own. Secret-guarded like /openai-token: this hands out a key that
        spends money. 200 {account, api_key}; 400 without an account; 404
        {error, reason} when the account is not an api account here
        (reason "not_api") or the brain has no key for it ("no_key"). The key
        is read at each call, so a rotation is dispensed at once."""
        t = self.tenant
        if not self._guard():
            return
        account = (parse_qs(parsed.query).get("account") or [""])[0]
        if not account:
            return self._json(400, {"error": "account is required"})
        key, reason = api_key(t, account)
        if not key:
            msg = (f"{account!r} is not an api account here" if reason == "not_api"
                   else f"the brain has no key for api account {account!r}")
            return self._json(404, {"error": msg, "reason": reason})
        self._json(200, {"account": account, "api_key": key})


    def _pin(self, parsed):
        """Force (or clear) the preferred account, per lane or across all lanes.
        Guarded — it changes routing.

        /pin?account=X              pin every lane to X (back-compat global pin)
        /pin?account=X&lane=fable   pin one lane (unknown lane -> 404, never
                                    silently creates persisted state)
        /pin?account=clear[&lane=…] clear the pin(s); auto policy resumes and the
                                    lane's reason says so (the old behavior left a
                                    stale "pinned to …" reason until the next real
                                    account change).
        """
        t = self.tenant
        if not SWITCH_KEY or not hmac.compare_digest(self.headers.get("X-Switch-Key", ""), SWITCH_KEY):
            self.send_response(401)
            self.end_headers()
            return
        q = parse_qs(parsed.query)
        account = (q.get("account") or [""])[0]
        lane = (q.get("lane") or [""])[0]
        if lane and lane not in _lane_names:
            return self._json(404, {"error": f"unknown lane {lane!r}", "lanes": _lane_names})
        target_lanes = [lane] if lane else list(_lane_names)
        if account in ("", "auto", "none", "clear"):
            with t.plock:
                for ln in target_lanes:
                    ls = t.preferred["lanes"].setdefault(ln, _new_lane_state())
                    if ls.get("pinned"):
                        ls["pinned"] = None
                        ls["reason"] = "auto policy resumed"
                _save_state(t)
            log(f"pin cleared for {target_lanes} (auto policy resumed)")
        elif account in _account_loop_names(t):
            with t.plock:
                for ln in target_lanes:
                    ls = t.preferred["lanes"].setdefault(ln, _new_lane_state())
                    ls["pinned"] = account
                _save_state(t)
            log(f"pinned {target_lanes} -> {account}")
        else:
            return self._json(404, {"error": f"unknown account {account!r}"})
        compute_preferred(t)  # apply immediately (also refreshes the legacy mirror)
        self._json(200, _preferred_payload(t))

    def _class_bind(self, parsed):
        """Bind (or touch) a class session (#26): the endpoint the proxy calls on
        a session's first class-routed request. Guarded — it creates persisted
        routing state. Session-sticky by construction: an existing binding is
        returned unchanged; a new one binds to the pin if usable, else the
        pointer. 409 class_unavailable when no route's check is included or
        paid (quota-model 50 §2.8), with Retry-After when a time is known;
        503 when no member's route has an account (configuration, item 6).

        /class-bind?class=frontier&session=<x-claude-code-session-id>
        """
        t = self.tenant
        if not SWITCH_KEY or not hmac.compare_digest(self.headers.get("X-Switch-Key", ""), SWITCH_KEY):
            self.send_response(401)
            self.end_headers()
            return
        q = parse_qs(parsed.query)
        cname = (q.get("class") or [""])[0]
        sid = (q.get("session") or [""])[0]
        if not cname or not sid:
            return self._json(400, {"error": "class and session are required"})
        code, payload = _bind_class_session(t, cname, sid, time.time())
        headers = None
        if code == 409 and payload.get("retry_after_s") is not None:
            headers = {"Retry-After": str(payload["retry_after_s"])}
        self._json(code, payload, headers)

    def _class_pin(self, parsed):
        """Pin (or clear) one class to a member by id (#26) — /pin's class-axis
        sibling. Guarded — it changes routing. A pin overrides the pointer for
        new binds AND force-moves existing bound sessions on the next compute
        cycle (applied immediately below, mirroring /pin).

        /class-pin?class=frontier&member=lane:opus     pin
        /class-pin?class=frontier&member=clear         clear (auto resumes)
        """
        t = self.tenant
        if not SWITCH_KEY or not hmac.compare_digest(self.headers.get("X-Switch-Key", ""), SWITCH_KEY):
            self.send_response(401)
            self.end_headers()
            return
        q = parse_qs(parsed.query)
        cname = (q.get("class") or [""])[0]
        member = (q.get("member") or [""])[0]
        # Class lookup + state mutation under ONE _plock hold (§6 resurrection
        # race): a hot-swap landing between an outside-the-lock lookup and the
        # setdefault would resurrect (or freshly create) zombie state for a
        # deleted class that _save_state() then persists.
        err = msg = None
        with t.plock:
            visible = t.classes
            cls = next((c for c in visible if c["class"] == cname), None)
            if not cls:
                err = {"error": f"unknown class {cname!r}",
                       "classes": [c["class"] for c in visible]}
            else:
                member_ids = [_member_id(m) for m in cls["members"]]
                if member in ("", "auto", "none", "clear"):
                    cs = t.preferred["classes"].setdefault(cname, _new_class_state())
                    if cs.get("pinned"):
                        cs["pinned"] = None
                    _save_state(t)
                    msg = f"class[{cname}] pin cleared (auto policy resumed)"
                elif member in member_ids:
                    cs = t.preferred["classes"].setdefault(cname, _new_class_state())
                    cs["pinned"] = member
                    _save_state(t)
                    msg = f"class[{cname}] pinned -> {member}"
                else:
                    err = {"error": f"unknown member {member!r}", "members": member_ids}
        if err:
            return self._json(404, err)
        log(msg)
        compute_preferred(t)  # apply to bound sessions immediately
        self._json(200, _preferred_payload(t))

    # ---- runtime document config (#92, ui-revamp §6): /classes + /consumers --

    def _classes_catalog(self):
        """GET /classes/catalog: what this tenant may add to a ladder (_catalog),
        {lanes: {<lane>: {model, context_window}}, models: {"<provider>/<model>":
        {context_window, anthropic_token_ratio, calibrated_at, modality}}}, the
        same for every tenant. Key-guarded like the documents."""
        if not self._guard():
            return
        self._json(200, _catalog())

    def _classes_starter(self, t):
        """GET /classes/starter: the document a member made now would start
        with, and what the copy dropped (_starter): {doc, dropped: [{class,
        member?, pay?, why}]}, as the join derives it; the operator's
        onboarding view previews it, and a member's page resets to it. A
        dropped pay is an exact pot of the operator's, named by its account,
        so only the LAN tenant's reply carries `pay`. Key-guarded."""
        if not self._guard():
            return
        doc, dropped = _tenant_starter()
        if not t.lan:
            dropped = [{k: v for k, v in d.items() if k != "pay"} for d in dropped]
        self._json(200, {"doc": doc, "dropped": dropped})

    def _doc_get(self, kind):
        """GET /classes | /consumers — live raw doc + content-hash revision +
        derived fields + load warnings. Key-guarded like every other config
        surface: the documents name accounts and models."""
        t = self.tenant
        if not self._guard():
            return
        snap = _doc_snapshot(t, kind)
        self._json(200, snap, headers={"X-Doc-Revision": snap["revision"]})

    def _doc_history(self, kind, parsed):
        """GET /classes/history | /consumers/history — the last N history
        entries, NEWEST FIRST, as revision metadata only ({rev, ts, actor,
        reason}): each entry's full document can be an entire classes file, so
        it rides along only on ?full=1. ?limit= (default 20, cap 200) bounds
        the page; `total` is the whole log's count so the /classes History
        drawer (#94) can say "20 of 137". A missing history file is zero
        entries, not an error — a fresh brain has no edits yet. Key-guarded:
        actors and reasons name people and accounts."""
        t = self.tenant
        if not self._guard():
            return
        q = parse_qs(parsed.query)
        try:
            limit = int((q.get("limit") or ["20"])[0])
        except ValueError:
            return self._json(400, {"error": "limit must be an integer (default 20, max 200)"})
        limit = max(1, min(200, limit))
        full = (q.get("full") or ["0"])[0] == "1"
        path = _doc_path(t, kind) + ".history.jsonl"
        # Read under the per-document write lock so a concurrent PUT/reload
        # append can never hand us a half-written final line.
        with _doc_lock(t, kind):
            try:
                with open(path) as f:
                    lines = f.readlines()
            except FileNotFoundError:
                lines = []
            except OSError as e:
                return self._json(500, {"error": f"cannot read {path}: {e}"})
        entries = []
        for ln in lines:
            try:
                e = json.loads(ln)
            except ValueError:
                continue  # a torn line (crash mid-append) must not 500 the drawer
            if isinstance(e, dict):
                entries.append(e)
        keys = ("rev", "ts", "actor", "reason") + (("doc",) if full else ())
        out = [{k: e.get(k) for k in keys} for e in reversed(entries[-limit:])]
        self._json(200, {"entries": out, "total": len(entries)})

    def _doc_validate(self, kind):
        """POST /<kind>/validate — dry-run: STRICT full-document validation +
        would-be derived state + live-effect (which bindings/pins would be
        invalidated, window changes vs the live doc, and for the owner
        settings the sessions a save moves, _session_preview). Mutates
        nothing; always 200 with an `ok` flag — errors are the report, not a
        refusal."""
        t = self.tenant
        if not self._guard():
            return
        body = self._read_json_body(max_bytes=DOC_BODY_MAX)
        if not isinstance(body, dict) or "doc" not in body:
            return self._json(400, {"error": 'body must be {"doc": <document>} '
                                             f"(JSON, <= {DOC_BODY_MAX} bytes)"})
        live_rev = _live_revision(t, kind)
        errors, warnings, normalized, canon = _doc_check(t, kind, body["doc"])
        if errors:
            return self._json(200, {"ok": False, "errors": errors,
                                    "warnings": warnings, "revision": live_rev})
        eff = _doc_live_effect(t, kind, normalized)
        warnings = warnings + eff.get("consumer_warnings", [])
        if kind == "classes":
            derived = _classes_derived(normalized[0])
        elif kind == "money":
            derived = _money_derived(t, normalized)
        else:
            with t.plock:
                classes = t.classes
            derived = _consumers_view(normalized, classes)
        self._json(200, {"ok": True, "errors": [], "warnings": warnings,
                         "revision": live_rev, "would_revision": _doc_revision(canon),
                         "derived": derived, "live_effect": eff})

    def _doc_put(self, kind):
        """PUT /classes | /consumers with If-Match: <revision> — validate the
        WHOLE document (reject on any error), atomic write (tmp+rename) to the
        file this brain loads from, append to <file>.history.jsonl, hot-swap
        live state. 409 with the current revision on stale If-Match; the
        check-and-write runs under the per-document write lock (handlers are
        concurrent — an unserialized check-and-write is a lost-update race).
        actor = X-Actor header, honored here because this request is already
        key-authenticated (a gate in front sets it from the signed-in identity). The
        reply's live_effect is the dry run's at the save, but its
        session_moves are the moves made after the swap, by whichever pass
        made them: a reading that changed since the dry run shows here."""
        t = self.tenant
        if not self._guard():
            return
        body = self._read_json_body(max_bytes=DOC_BODY_MAX)
        if not isinstance(body, dict) or "doc" not in body:
            return self._json(400, {"error": 'body must be {"doc": <document>, "reason": "..."} '
                                             f"(JSON, <= {DOC_BODY_MAX} bytes)"})
        reason = str(body.get("reason") or "").strip()
        if not reason:
            return self._json(400, {"error": "reason is required (the history line a future operator reads)"})
        if_match = (self.headers.get("If-Match") or "").strip().strip('"')
        if not if_match:
            return self._json(400, {"error": "If-Match: <revision> is required (GET the document first)"})
        actor = (self.headers.get("X-Actor") or "").strip()[:200] or "unknown"
        path = _doc_path(t, kind)
        with _doc_lock(t, kind):
            live_rev = _live_revision(t, kind)
            if if_match != live_rev:
                return self._json(409, {"error": "stale If-Match — the document changed under you; "
                                                 "re-read and merge",
                                        "revision": live_rev, "if_match": if_match},
                                  headers={"X-Doc-Revision": live_rev})
            errors, doc_warnings, normalized, canon = _doc_check(t, kind, body["doc"])
            if errors:
                return self._json(422, {"error": "validation failed", "errors": errors})
            eff = _doc_live_effect(t, kind, normalized)
            new_rev = _doc_revision(canon)
            try:
                _doc_write(t, kind, canon)
            except OSError as e:
                return self._json(500, {"error": f"cannot write {path}: {e}"})
            if kind == "classes":
                _stamp_class_window_changes(t, normalized[0])
                seen = _swap_classes(t, normalized[0], canon, doc_warnings, normalized[1])
            elif kind == "money":
                seen = _swap_money(t, normalized, canon, doc_warnings)
            else:
                _swap_consumers(t, normalized, canon, doc_warnings)
            _append_history(path, new_rev, actor, reason, canon)
            log(f"doc[{kind}] rev {live_rev} -> {new_rev} by {actor!r}: {reason}")
            # Snapshot INSIDE the write lock so the response's revision is the
            # one this PUT installed, not a concurrent writer's.
            snap = _doc_snapshot(t, kind)
            snap["warnings"] = doc_warnings + eff.get("consumer_warnings", [])
            snap["live_effect"] = eff
        if kind in ("classes", "money"):
            compute_preferred(t)  # pointer/live view reflect the new config immediately
            with t.plock:
                snap["live_effect"]["session_moves"] = [ev for n, ev in t.session_journal
                                                        if n > seen]
        self._json(200, snap, headers={"X-Doc-Revision": snap["revision"]})

    def _doc_reload(self, kind):
        """POST /<kind>/reload — adopt a HAND-EDIT from disk; restart never
        required. Appends a history entry (actor "hand-edit (reload)") with the
        new content hash, so an editor holding a pre-edit revision 409s instead
        of silently clobbering the hand edit (§6 revision lifecycle). A file
        that fails to PARSE (or has the wrong top-level shape) is REFUSED with
        422 and the live config kept — reload must never wipe routing because
        a hand-edit left broken JSON behind. Per-entry problems keep the
        startup loader's drop-and-warn tolerance. Absent file = empty doc
        (removing the file is a legitimate way to retire the config)."""
        t = self.tenant
        if not self._guard():
            return
        body = self._read_json_body()
        body = body if isinstance(body, dict) else {}
        reason = str(body.get("reason") or "").strip() or "reloaded from disk"
        path = _doc_path(t, kind)
        with _doc_lock(t, kind):
            old_rev = _live_revision(t, kind)
            try:
                raw = _doc_read(t, kind)
            except Exception as e:  # noqa: BLE001
                return self._json(422, {"error": f"cannot parse {path}: {e} — live config kept"})
            warnings = []
            if kind == "classes":
                raw = [] if raw is None else raw
                if _split_classes_doc(raw) is None:
                    return self._json(422, {"error": 'top level must be {"classes": [...], "lanes": {...}} '
                                                     "or an array — live config kept"})
                normalized, lanes = _normalize_classes_doc(raw, warnings, lan=t.lan)
                _stamp_class_window_changes(t, normalized)
                _swap_classes(t, normalized, raw, warnings, lanes)
                canon = raw
            elif kind == "money":
                raw = {} if raw is None else raw
                if not isinstance(raw, dict):
                    return self._json(422, {"error": 'document must be {"pay_order": [...], "pots": {...}} '
                                                     "— live config kept"})
                _, warnings, normalized = _normalize_money(raw, strict=False)
                _swap_money(t, normalized, raw, warnings)
                canon = raw
            else:
                raw = {"consumers": []} if raw is None else raw
                if not (isinstance(raw, list)
                        or (isinstance(raw, dict) and isinstance(raw.get("consumers"), list))):
                    return self._json(422, {"error": 'document must be {"consumers": [...]} '
                                                     "(or a bare array) — live config kept"})
                _, warnings, normalized = _normalize_consumers(raw, strict=False)
                canon = raw if isinstance(raw, dict) else {"consumers": raw}
                _swap_consumers(t, normalized, canon, warnings)
            new_rev = _doc_revision(canon)
            _append_history(path, new_rev, "hand-edit (reload)", reason, canon)
            log(f"doc[{kind}] reloaded from disk: rev {old_rev} -> {new_rev}")
            snap = _doc_snapshot(t, kind)
            snap["reloaded"] = True
            snap["changed"] = old_rev != new_rev
        if kind in ("classes", "money"):
            compute_preferred(t)
        self._json(200, snap, headers={"X-Doc-Revision": snap["revision"]})

    def _json(self, code, obj, headers=None):
        # allow_nan=False: a NaN or ±inf would serialize as a bare token no
        # proxy can decode, so a bad number fails here, loudly, instead.
        try:
            body = json.dumps(obj, allow_nan=False).encode("utf-8")
        except ValueError as e:
            log(f"ERROR {self.path}: response carries a non-finite number ({e})")
            code, headers = 500, None
            body = json.dumps({"error": f"response carries a non-finite number: {e}"}).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):  # silence per-request stderr noise
        pass


_sigterm_seen = False


def _sigterm(signum, frame):  # noqa: ARG001
    """The graceful shutdown: save every tenant's sizing open state, then
    exit. A second SIGTERM, landing inside the first handler (perhaps while
    it holds a sizing lock, which is not reentrant), returns at once, taking
    no lock, so the first finishes its save and exits."""
    global _sigterm_seen
    if _sigterm_seen:
        return
    _sigterm_seen = True
    for t in _registry.all():
        _sizing_shutdown(t)
    raise SystemExit(0)


def _join_classes(t):
    """A tenant PUT /tenants just made starts with the starter as its classes
    document, written plain to its dir with a history line, then installed.
    A tenant that already has a document keeps it; the LAN tenant's own is
    never derived."""
    if t.lan or os.path.exists(t.classes_file):
        return
    doc, dropped = _tenant_starter()
    with _doc_lock(t, "classes"):
        _doc_write(t, "classes", doc)
        _append_history(t.classes_file, _doc_revision(doc), "join",
                        "the starter: the operator's onboarding classes", doc)
        warnings = []
        classes, lanes = _normalize_classes_doc(doc, warnings, lan=False)
        _stamp_class_window_changes(t, classes)
        _swap_classes(t, classes, doc, warnings, lanes)
    log(f"{t.id}: classes: the starter, {len(classes)} classes"
        + (f"; dropped {len(dropped)}: " + ", ".join(
            f"{d['class'] or d['member']}{'/' + d['member'] if d['class'] and d.get('member') else ''}"
            f" ({d['why']})" for d in dropped) if dropped else ""))


def _classes_line(t):
    """The boot log's line of a tenant's classes: the primary evidence that a
    hand edit of its classes.json landed. Ceilings are annotated inline
    (gpt/gpt-5.6-sol>kimi/kimi-k3>opus<=60)."""
    return f"{t.id}: classes: " + (", ".join(
        f"{c['class']}(window={c['context_window']}, "
        + ">".join((m["lane"] if m["kind"] == "anthropic" else f"{m['provider']}/{m['model']}")
                   + ("" if m.get("reserve_ceiling") is None else f"<={m['reserve_ceiling']:.0f}")
                   for m in c["members"]) + ")"
        for c in t.classes) or "none configured")


def boot():
    """What reads the driver registry, run once every driver has registered and
    before anything is served or restored: the registry of tenants, each made
    with its documents read (_tenant_fresh), its classes validated against
    the drivers' modalities and its windows stamped. main() runs it first; a
    test runs it with its own CREDS_DIR."""
    global _registry
    _registry = tenancy.Registry(CREDS_DIR, _tenant_fresh, retire=_tenant_retire, log=log)
    _registry.load()
    return _registry


def main():
    try:
        boot()
    except tenancy.RegistryError as e:
        log(f"refusing to start: {e}")
        raise SystemExit(1)
    loop = _account_loop()
    log(f"switchover brain: incumbent-wins high={SW_HIGH} margin={SW_MARGIN} "
        f"hard={SW_HARD} dwell={SW_DWELL}s /token={'guarded' if SWITCH_KEY else 'DISABLED (no key)'}; "
        f"store {_store.mode}; tenants {', '.join(t.id for t in _registry.all()) or 'none'} "
        f"[{_registry.path}]")
    log("lanes: " + ", ".join(
        f"{lc['lane']}(match={'|'.join(lc['match']) or '-'},cap={lc['scope_model'] or '-'})"
        for lc in _lanes))
    for t in _registry.all():
        log(_classes_line(t))
        if loop:
            loop["start"](t)  # the configured accounts, so _load_state restores only theirs
        _tenant_boot(t)
        # Consumers (#95): the runtime view logs its own dangling/shape warnings.
        for c in _consumers_view(t.consumers, t.classes):
            for w in c["warnings"]:
                log(f"{t.id}: WARN consumers[{c['id']}]: {w}")
    url = notices.bridge_url()
    log("notices bridge: " + (f"{url}, for the LAN tenant(s) "
                              f"{', '.join(t.id for t in _registry.lan()) or 'none'}"
                              if url else f"off ({notices.NO_BRIDGE})"))
    # Sizing (quota-model 66 §8) was read per tenant before /reports is
    # served; a SIGTERM (a deploy's restart) saves each open step with its samples.
    signal.signal(signal.SIGTERM, _sigterm)
    th = threading.Thread(target=poller, daemon=True)
    th.start()
    _events_start()  # GET /events: publishes the first document now
    log(f"listening on :{PORT}/metrics, poll interval {POLL_INTERVAL}s, "
        f"providers {', '.join(PROVIDERS) or 'none'}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    # The drivers import this module by name: run as a script, it would be a second
    # copy they never register into. An entry point imports it and the drivers.
    sys.exit("claude_usage_exporter.py is the brain's core: run brain.py, or brain_household.py")
