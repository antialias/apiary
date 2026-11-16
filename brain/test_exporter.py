#!/usr/bin/env python3
"""Offline tests for the switchover brain (lanes, policy, state, contract) and
the shared drivers (providers/), under brain.py's edition.

Run:  python3 test_exporter.py     (stdlib only — no network, no docker)
The household drivers' tests are household/test_household.py's.

The env vars
below MUST be set before the module import — the exporter reads them at import time.
"""
import base64
import copy
import functools
import hashlib
import io
import itertools
import json
import math
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

_TMP = tempfile.mkdtemp(prefix="claude-usage-test-")
os.environ["SWITCH_STATE_FILE"] = os.path.join(_TMP, "switch_state.json")
os.environ["SWITCH_LANES_FILE"] = os.path.join(_TMP, "lanes.json")  # absent -> DEFAULT_LANES
os.environ["SWITCH_KEY"] = "test-switch-key"
os.environ["KIMI_CREDS_FILE"] = os.path.join(_TMP, "kimi.json")  # absent -> env-key fallback
os.environ["SWITCH_CONSUMERS_FILE"] = os.path.join(_TMP, "consumers.json")  # absent -> no consumers
os.environ["SWITCH_MONEY_FILE"] = os.path.join(_TMP, "money.json")  # absent -> money defaults
os.environ["ANTHROPIC_API_CREDS_FILE"] = os.path.join(_TMP, "anthropic-api.json")  # absent -> no api keys
os.environ["CLAUDE_ACCOUNTS_FILE"] = os.path.join(_TMP, "accounts.json")  # absent -> no accounts until a test adds them
os.environ["OPENAI_CODEX_CREDS_FILE"] = os.path.join(_TMP, "openai-codex.json")  # absent -> gpt poller disabled
os.environ["LOCAL_UPSTREAM"] = ""  # unset -> local poller disabled (tests set plocal.LOCAL_UPSTREAM)
os.environ["SWITCH_BRIDGE_URL"] = ""  # set and empty -> the prepaid notices are off (tests fake the bridge)

BRAIN = os.path.dirname(os.path.abspath(__file__))


def _proxy_dir():
    """The proxy beside the brain: claude-switch-proxy in llm-routing's trunk,
    proxy in the Apiary export. Its testdata holds the goldens both sides run.
    Neither there is a broken checkout, and the suite stops here."""
    for name in ("claude-switch-proxy", "proxy"):
        d = os.path.join(BRAIN, "..", name)
        if os.path.isdir(d):
            return os.path.normpath(d)
    raise SystemExit("test_exporter: no proxy beside the brain (../claude-switch-proxy in the trunk, "
                     "../proxy in Apiary); its testdata holds the shared goldens")


def proxy_testdata(name):
    return os.path.join(_proxy_dir(), "testdata", name)

import claude_usage_exporter as ex  # noqa: E402
# The entry point whose drivers these tests run under: brain.py's shared set,
# unless a suite that extends this one (and imports it) names another first.
__import__(os.environ.get("BRAIN_TEST_EDITION", "brain"))
from providers import anthropic_api as papi  # noqa: E402
from providers import anthropic_subscription as psub  # noqa: E402
from providers import codex as pcodex  # noqa: E402
from providers import kimi as pkimi  # noqa: E402
from providers import local as plocal  # noqa: E402


# The account loop's accounts: the subscription driver's store, name -> token
# facts. gen_preferred_c2.py (the proxy's fixture generator) reaches it as ex._creds.
ACCOUNTS = psub._creds
ex._creds = psub._creds


def _reset_subscription():
    psub._cred_generation.clear()
    psub._last_refresh.clear()
    with psub._proxy_refusals_lock:
        psub._proxy_refusals.clear()
        psub._forced_refresh_at.clear()


# What reset() clears beside the core's state, and the reading adapters read()
# dispatches to by provider id; a suite whose edition brings more drivers adds its own.
RESET_HOOKS = [_reset_subscription]
READERS = {
    "gpt": lambda r, raw, as_of: pcodex._codex_reading(raw, r.get("reset_options"), as_of, r["name"])[0],
}


# Two equal accounts — no "native"/"home". Policy is incumbent-wins across N accounts.
ACCT_A = "acct-a@example.com"


ACCT_B = "acct-b@example.com"


def read(r):
    """(Re)build a test poll result's §1.5 reading from its recorded raw
    provider response, through the brain's own adapter, as the poller does.
    Tests shape `raw` like the provider's wire body and mutate it; reset()
    reads every result it installs, so the brain only ever sees a reading."""
    raw = r.get("raw")
    if raw is None:
        r["reading"] = None
        return r
    as_of = r.get("good_ts") or r.get("ts")
    if r.get("kind") != "provider":
        r["reading"] = ex._anthropic_reading(raw, as_of, r.get("tier", ""))
    elif r.get("provider") in READERS:
        r["reading"] = READERS[r["provider"]](r, raw, as_of)
    elif r.get("provider") == "anthropic-api":
        r["reading"] = papi._reading(as_of)
    elif r.get("provider") == "local":
        r["reading"] = plocal._local_reading(as_of)
    else:
        r["reading"] = pkimi._kimi_reading(raw, as_of)[0]
    return r


def acct(name, weekly, caps=None, good_age=0, session=10):
    """A minimal poll result shaped like poll_account()'s output, its `raw`
    the GET /api/oauth/usage body (10 §2). `session` is the 5-hour rolling
    window utilization (default low so it never binds unless a test sets it —
    keeps the weekly/scoped-cap cases isolated)."""
    limits = [{"kind": "weekly_scoped", "group": "model_specific",
               "scope": {"model": {"display_name": dn}},
               "percent": pct, "severity": "normal", "is_active": False, "resets_at": None}
              for dn, pct in (caps or {}).items()]
    return read({"name": name, "ok": 1, "ts": time.time(), "email": name, "tier": "max",
                 "http": 0, "err": "", "retry_after": None,
                 "good_ts": time.time() - good_age, "stale": 1 if good_age else 0,
                 "raw": {"five_hour": {"utilization": session},
                         "seven_day": {"utilization": weekly},
                         "limits": limits}})


def reset(*results):
    """Reset all module state and install the given poll results, each read
    from its raw response."""
    with ex._lock:
        ex._state.clear()
        for r in results:
            ex._state[r["name"]] = read(r) if "raw" in r else r
    with ex._plock:
        ex._preferred.update({"account": None, "since": 0.0, "reason": "init",
                              "last_switch": 0.0, "pinned": None,
                              "lanes": {ln: ex._new_lane_state() for ln in ex._lane_names},
                              "providers": {}})
    ACCOUNTS.clear()
    for hook in RESET_HOOKS:
        hook()
    ex._poll_wakeup.clear()
    for n in (ACCT_A, ACCT_B):
        ACCOUNTS[n] = {"refresh_token": "r-" + n, "access_token": "tok-" + n,
                        "expires_at": time.time() + 9999}
    reset_ledger()
    with ex._reports_lock:
        ex._reported.clear()
        ex._shut_reports.clear()
        ex._holds.clear()
        ex._limit_holds.clear()
    with ex._served_lock:
        ex._served = ex._served_fresh()
    reset_sizing()
    ex._notices.reset()
    try:
        os.remove(ex.STATE_FILE)
    except FileNotFoundError:
        pass


def sizing_bad_files():
    d = os.path.dirname(ex.SIZING_FILE) or "."
    base = os.path.basename(ex.SIZING_FILE) + ".bad-"
    return sorted(os.path.join(d, n) for n in os.listdir(d) if n.startswith(base))


def reset_sizing():
    """No sizing state and no sizing file (quota-model 66)."""
    with ex._sizing_lock:
        ex._sizing = ex._sizing_fresh()
        ex._sizing_gate.update(closing=False, inflight=0, frozen=None, set_aside=None)
    for path in [ex.SIZING_FILE, ex.SIZING_FILE + ".tmp"] + sizing_bad_files():
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def reset_ledger():
    """An empty ledger: no file, no entries, nothing reconciled."""
    with ex._ledger_lock:
        ex._ledger.update(seq=0, ids={}, spend={}, uncounted={}, unpriced={}, total={}, recon={},
                          unreadable=0, counted_at={})
    for f in (ex.LEDGER_FILE, ex.RECHECK_FILE):
        try:
            os.remove(f)
        except FileNotFoundError:
            pass


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).isoformat().replace("+00:00", "Z")


def kimi_window(pct, reset_at, limit=100.0):
    """A Kimi /usages window body: numbers as strings, limit and remaining
    only (the live payload carries no `used`)."""
    return {"limit": f"{limit:g}", "remaining": f"{limit - pct:g}", "resetTime": iso(reset_at)}


def kimi_raw(five_hour=8.0, weekly=2.0, total_limit=100.0, total_remaining=99.0,
             parallel_limit=20.0, membership="LEVEL_INTERMEDIATE", now=None):
    """A GET /coding/v1/usages body (12 §2): `usage` the weekly window,
    limits[] the 300-minute one. None drops a part."""
    now = now or time.time()
    raw = {}
    if weekly is not None:
        raw["usage"] = kimi_window(weekly, now + 604800)
    if five_hour is not None:
        raw["limits"] = [{"window": {"duration": 300, "timeUnit": "TIME_UNIT_MINUTE"},
                          "detail": kimi_window(five_hour, now + 18000)}]
    if total_limit is not None:
        raw["totalQuota"] = {"limit": f"{total_limit:g}", "remaining": f"{total_remaining:g}"}
    if parallel_limit is not None:
        raw["parallel"] = {"limit": f"{parallel_limit:g}"}
    if membership is not None:
        raw["membership"] = {"level": membership}
    return raw


def kimi_result(five_hour=8.0, weekly=2.0, total_limit=100.0, total_remaining=99.0,
                parallel_limit=20.0, membership="LEVEL_INTERMEDIATE", ok=1,
                ts=None, good_age=0, stale=0, http=0, err=""):
    """A minimal Kimi poll result shaped like poll_kimi()'s output, read from
    a recorded-shape /usages body."""
    now = ts or time.time()
    return read({"name": "kimi", "kind": "provider", "provider": "kimi", "ok": ok,
                 "ts": now, "good_ts": now - good_age, "stale": stale,
                 "http": http, "err": err, "retry_after": None,
                 "raw": kimi_raw(five_hour, weekly, total_limit, total_remaining,
                                 parallel_limit, membership, now)})


def age_lanes(seconds=ex.SW_DWELL + 100):
    """Push every lane's last_switch into the past so the dwell gate opens."""
    with ex._plock:
        for s in ex._preferred["lanes"].values():
            s["last_switch"] = time.time() - seconds


def lane_account(lane):
    with ex._plock:
        return ex._preferred["lanes"][lane]["account"]


class TestLaneEffective(unittest.TestCase):
    def test_max_of_weekly_and_scoped_cap(self):
        r = acct(ACCT_A, 41, caps={"Fable": 85})
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        self.assertEqual(ex._lane_effective(r, fable), 85)

    def test_weekly_dominates_when_higher(self):
        r = acct(ACCT_A, 90, caps={"Fable": 70})
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        self.assertEqual(ex._lane_effective(r, fable), 90)

    def test_cap_absent_falls_back_to_weekly(self):
        r = acct(ACCT_A, 41)  # no scoped caps in the payload at all
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        self.assertEqual(ex._lane_effective(r, fable), 41)

    def test_join_is_exact_display_name(self):
        # "Fable 5" must NOT join a lane configured for "Fable" — the join is
        # explicit, never substring inference (binding constraint 3).
        r = acct(ACCT_A, 41, caps={"Fable 5": 85})
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        self.assertEqual(ex._lane_effective(r, fable), 41)

    def test_default_lane_is_pure_weekly(self):
        r = acct(ACCT_A, 41, caps={"Fable": 85})
        default = next(lc for lc in ex._lanes if lc["lane"] == "default")
        self.assertEqual(ex._lane_effective(r, default), 41)

    def test_unknown_weekly_is_none(self):
        r = acct(ACCT_A, 41, caps={"Fable": 85})
        r["raw"]["seven_day"] = {}
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        self.assertIsNone(ex._lane_effective(read(r), fable))
        w = ex._reading_windows(r["reading"])["seven_day"]
        self.assertEqual((w["used_pct"], w["basis"]), (None, "unknown"),
                         "an Anthropic account always has its weekly window")

    def test_session_gates_every_lane(self):
        # The case: weekly 24%, Fable cap 46%, but the 5-hour session is
        # pegged at 100%. Every request 429s, so lane-effective must be 100 — not the
        # 46 the weekly-only math produced (which sent Fable onto a dead account).
        r = acct(ACCT_B, 24, caps={"Fable": 46}, session=100)
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        default = next(lc for lc in ex._lanes if lc["lane"] == "default")
        opus = next(lc for lc in ex._lanes if lc["lane"] == "opus")
        self.assertEqual(ex._lane_effective(r, fable), 100)
        self.assertEqual(ex._lane_effective(r, opus), 100)     # no opus cap, session still binds
        self.assertEqual(ex._lane_effective(r, default), 100)  # default lane is gated too

    def test_session_dominates_when_it_is_the_hottest_limit(self):
        r = acct(ACCT_A, 41, caps={"Fable": 70}, session=88)
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        self.assertEqual(ex._lane_effective(r, fable), 88)

    def test_unknown_session_is_unknown_not_zero(self):
        # 20 §1.3 read an unreadable session as "not binding" (0). A 5-hour
        # window the body reports with no utilization is unknown, never 0.
        for five in ({"utilization": None, "resets_at": None}, {}):
            r = acct(ACCT_A, 41, caps={"Fable": 85})
            r["raw"]["five_hour"] = five
            fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
            self.assertIsNone(ex._lane_effective(read(r), fable), five)

    def test_unreported_session_does_not_bind(self):
        # The live shape: no five_hour (absent or null) and no session entry in
        # limits[], seven_day 99. The 5h window opens at first use (10 §2), so
        # it is not reported, not unknown: the account reads included at 99 and
        # carries no five_hour window, so no series is rendered for it.
        for five in (None, "absent"):
            r = acct(ACCT_A, 99)
            if five == "absent":
                del r["raw"]["five_hour"]
            else:
                r["raw"]["five_hour"] = None
            r = read(r)
            self.assertNotIn("five_hour", ex._reading_windows(r["reading"]), five)
            self.assertEqual(ex._lane_effective(r, ex._lanes[0]), 99, five)
            st = ex._account_standing(ACCT_A, r, "anthropic", time.time())
            c = ex.dial_check({"id": "lane:default", "pay": []}, ACCT_A, st, {}, time.time())
            self.assertEqual(c["check"], "included", (five, c))
            reset(r)
            ex.compute_preferred()
            out = [l for l in ex.render().splitlines()
                   if f'account="{ACCT_A}"' in l and "five_hour" in l]
            self.assertEqual(out, [], five)

    def test_idle_session_with_no_reset_still_binds(self):
        # 10 §3: the 5h resets_at is null while idle; its percent still reads.
        r = acct(ACCT_A, 41, session=0)
        r["raw"]["five_hour"]["resets_at"] = None
        w = ex._reading_windows(read(r)["reading"])["five_hour"]
        self.assertEqual((w["used_pct"], w["resets_at"], w["reset_basis"]), (0, None, None))
        self.assertEqual(ex._lane_effective(r, ex._lanes[0]), 41)


class TestIncumbentUntilForced(unittest.TestCase):
    """A lane stays on its incumbent account and only leaves when that account is
    forced out for THAT lane — and then only the affected lane moves."""

    def test_only_the_capped_lane_moves(self):
        # Seed A as the incumbent everywhere (it's the coolest at bootstrap), then A's
        # Fable-scoped cap spikes to 96% (hard) while its overall weekly stays cool at
        # 30%. Only the Fable lane may evict; every other lane keeps the cool incumbent.
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        ex.compute_preferred()  # bootstrap: A is coolest -> incumbent on every lane
        for ln in ex._lane_names:
            self.assertEqual(lane_account(ln), ACCT_A, f"lane {ln} should bootstrap onto the coolest account")
        with ex._lock:
            ex._state[ACCT_A] = acct(ACCT_A, 30, caps={"Fable": 96})
        age_lanes()
        ex.compute_preferred()
        self.assertEqual(lane_account("fable"), ACCT_B, "the maxed lane evicts to the coolest alt")
        for ln in ("opus", "sonnet", "haiku", "default"):
            self.assertEqual(lane_account(ln), ACCT_A, f"lane {ln}: incumbent wins while still cool")

    def test_legacy_mirror_tracks_default_lane(self):
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        ex.compute_preferred()
        with ex._lock:
            ex._state[ACCT_A] = acct(ACCT_A, 30, caps={"Fable": 96})
        age_lanes()
        ex.compute_preferred()
        with ex._plock:
            self.assertEqual(ex._preferred["account"], ACCT_A)  # default lane, not fable

    def test_no_drift_back_after_recovery(self):
        # The flapping regression: once Fable evicts to B, a later recovery of A's cap
        # must NOT pull the lane back. Incumbent B wins until IT is forced out.
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        ex.compute_preferred()
        with ex._lock:
            ex._state[ACCT_A] = acct(ACCT_A, 30, caps={"Fable": 96})
        age_lanes()
        ex.compute_preferred()
        self.assertEqual(lane_account("fable"), ACCT_B)
        # A's Fable cap fully recovers (A fable-effective back to 30, B sits at 45).
        with ex._lock:
            ex._state[ACCT_A] = acct(ACCT_A, 30)
        age_lanes()
        ex.compute_preferred()
        self.assertEqual(lane_account("fable"), ACCT_B,
                         "no drift back to a now-cooler account — incumbent wins")


class TestSessionAwareRouting(unittest.TestCase):
    """End-to-end via compute_preferred: the session gate must change routing, not
    just the lane-effective number."""

    def test_does_not_flee_to_a_session_exhausted_account(self):
        # Incumbent A is hot on weekly (90%) so the lane wants to leave; the only
        # alternative's 5-hour session is pegged at 100%. Weekly-only math saw B at 30%
        # and bailed onto it (then every request 429'd). With the session in lane-
        # effective, B reads 100% and we correctly STAY on A — a 90% weekly account still
        # serves requests; a 100% session account does not.
        reset(acct(ACCT_A, 90), acct(ACCT_B, 30, session=100))
        ex.compute_preferred()          # bootstrap: A coolest (B session=100) -> incumbent A
        age_lanes()
        ex.compute_preferred()
        self.assertEqual(lane_account("default"), ACCT_A,
                         "must not route onto a session-exhausted account")

    def test_evicts_when_incumbent_session_dies(self):
        # Incumbent A is cool on weekly (20%) but its 5-hour session pegs at 100%, so it
        # can't serve a request right now (lane-effective = 100, hard). Every lane must
        # evict to the healthy account even though A looks fine on the weekly gauges.
        reset(acct(ACCT_A, 20), acct(ACCT_B, 35, session=20))
        ex.compute_preferred()          # A coolest -> incumbent on every lane
        for ln in ex._lane_names:
            self.assertEqual(lane_account(ln), ACCT_A)
        with ex._lock:
            ex._state[ACCT_A] = acct(ACCT_A, 20, session=100)
        age_lanes()
        ex.compute_preferred()
        for ln in ex._lane_names:
            self.assertEqual(lane_account(ln), ACCT_B,
                             f"lane {ln} must evict off a session-dead incumbent")


class TestComputeLaneOrdering(unittest.TestCase):
    """_compute_lane is pure (state dict + usable + now) — pin the rule ORDER."""

    def test_dwell_gates_even_the_hard_rule(self):
        ls = {"account": ACCT_A, "since": 0.0, "reason": "x",
              "last_switch": time.time() - 10, "pinned": None}
        changed = ex._compute_lane(ls, "fable", {ACCT_A: 96, ACCT_B: 10}, time.time())
        self.assertFalse(changed, "dwell must gate the hard check (existing ordering)")
        self.assertEqual(ls["account"], ACCT_A)

    def test_hard_moves_without_margin(self):
        ls = {"account": ACCT_A, "since": 0.0, "reason": "x",
              "last_switch": 0.0, "pinned": None}
        ex._compute_lane(ls, "fable", {ACCT_A: 96, ACCT_B: 90}, time.time())
        self.assertEqual(ls["account"], ACCT_B, ">=hard moves even when margin < SW_MARGIN")

    def test_hot_needs_margin(self):
        ls = {"account": ACCT_A, "since": 0.0, "reason": "x",
              "last_switch": 0.0, "pinned": None}
        ex._compute_lane(ls, "fable", {ACCT_A: 85, ACCT_B: 75}, time.time())
        self.assertEqual(ls["account"], ACCT_A, "10pts < margin 15 — must not move")
        ex._compute_lane(ls, "fable", {ACCT_A: 85, ACCT_B: 65}, time.time())
        self.assertEqual(ls["account"], ACCT_B, "20pts >= margin 15 — must move")

    def test_incumbent_wins_no_drift_to_cooler(self):
        # A cool incumbent must NOT move toward a merely-cooler account — only away from
        # a forced-out one. This is the drift/flap the redesign deletes.
        ls = {"account": ACCT_B, "since": 0.0, "reason": "x",
              "last_switch": 0.0, "pinned": None}
        ex._compute_lane(ls, "fable", {ACCT_A: 41, ACCT_B: 30}, time.time())
        self.assertEqual(ls["account"], ACCT_B, "incumbent cool -> stay put")
        ex._compute_lane(ls, "fable", {ACCT_A: 20, ACCT_B: 30}, time.time())
        self.assertEqual(ls["account"], ACCT_B, "A even cooler, but incumbent not hot -> no drift")

    def test_pin_overrides_everything(self):
        ls = {"account": ACCT_A, "since": 0.0, "reason": "x",
              "last_switch": time.time(), "pinned": ACCT_B}  # dwell would gate a policy move
        ex._compute_lane(ls, "fable", {ACCT_A: 10, ACCT_B: 99}, time.time())
        self.assertEqual(ls["account"], ACCT_B, "pin ignores dwell and utilization")


class TestIncumbentPolicy(unittest.TestCase):
    """compute_preferred-level: no privileged account, and no flapping."""

    def test_bootstrap_picks_coolest(self):
        reset(acct(ACCT_A, 60), acct(ACCT_B, 30))
        ex.compute_preferred()
        for ln in ex._lane_names:
            self.assertEqual(lane_account(ln), ACCT_B, "bootstrap seeds the coolest account")

    def test_incumbent_wins_and_does_not_flap(self):
        # Once seeded on the coolest account, flip which one is cooler and poll several
        # times: a comfortable incumbent (< SW_HIGH) must never oscillate.
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        ex.compute_preferred()                       # A coolest -> incumbent A
        self.assertEqual(lane_account("default"), ACCT_A)
        with ex._lock:
            ex._state[ACCT_A] = acct(ACCT_A, 50)     # now B is the cooler one, A still cool
            ex._state[ACCT_B] = acct(ACCT_B, 20)
        for _ in range(3):
            age_lanes()
            ex.compute_preferred()
        self.assertEqual(lane_account("default"), ACCT_A,
                         "cool incumbent must not drift toward a cooler account (no flap)")


class HTTPBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), ex.Handler)
        cls.port = cls.srv.server_address[1]
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def get(self, path, key=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}")
        if key:
            req.add_header("X-Switch-Key", key)
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            return e.code, (json.loads(body) if body else None)


class TestPinEndpoint(HTTPBase):
    def setUp(self):
        # A is the coolest -> bootstrap seeds every lane onto A (the incumbent), so a
        # single-lane pin visibly moves only that lane off A.
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        ex.compute_preferred()

    def test_lane_pin_moves_only_that_lane(self):
        code, p = self.get(f"/pin?account={ACCT_B}&lane=fable", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(p["lanes"]["fable"]["account"], ACCT_B)
        self.assertEqual(p["lanes"]["fable"]["pinned"], ACCT_B)
        for ln in ("opus", "haiku", "default"):
            self.assertEqual(p["lanes"][ln]["account"], ACCT_A)
            self.assertIsNone(p["lanes"][ln]["pinned"])
        self.assertEqual(p["account"], ACCT_A, "legacy mirror = default lane")

    def test_global_pin_hits_all_lanes(self):
        code, p = self.get(f"/pin?account={ACCT_B}", key="test-switch-key")
        self.assertEqual(code, 200)
        for ln in ex._lane_names:
            self.assertEqual(p["lanes"][ln]["pinned"], ACCT_B)
            self.assertEqual(p["lanes"][ln]["account"], ACCT_B)
        self.assertEqual(p["account"], ACCT_B)
        self.assertEqual(p["pinned"], ACCT_B)

    def test_unknown_lane_404s_and_changes_nothing(self):
        code, body = self.get(f"/pin?account={ACCT_B}&lane=fable5", key="test-switch-key")
        self.assertEqual(code, 404)
        self.assertIn("lanes", body)
        with ex._plock:
            self.assertNotIn("fable5", ex._preferred["lanes"])

    def test_unknown_account_404s(self):
        code, _ = self.get("/pin?account=nobody@nowhere&lane=fable", key="test-switch-key")
        self.assertEqual(code, 404)

    def test_clear_refreshes_reason(self):
        self.get(f"/pin?account={ACCT_B}&lane=fable", key="test-switch-key")
        code, p = self.get("/pin?account=clear&lane=fable", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertIsNone(p["lanes"]["fable"]["pinned"])
        self.assertEqual(p["lanes"]["fable"]["reason"], "auto policy resumed",
                         "the stale 'pinned to …' reason quirk must be fixed per lane")

    def test_pin_requires_key(self):
        code, _ = self.get(f"/pin?account={ACCT_B}&lane=fable")
        self.assertEqual(code, 401)


class TestPreferredContract(HTTPBase):
    """The OLD proxy decodes /preferred as {account string, reason string,
    weekly map[string]float64} and fails the WHOLE decode on any shape change —
    silent permanent passthrough. This test is the tripwire (constraint 1)."""

    def setUp(self):
        reset(acct(ACCT_A, 41, caps={"Fable": 85}), acct(ACCT_B, 30))
        ex.compute_preferred()

    def test_legacy_shape_frozen(self):
        code, p = self.get("/preferred")
        self.assertEqual(code, 200)
        # exactly what the old Go struct requires:
        self.assertIsInstance(p["account"], str)
        self.assertTrue(p["account"])
        self.assertIsInstance(p["reason"], str)
        self.assertIsInstance(p["weekly"], dict)
        for k, v in p["weekly"].items():
            self.assertIsInstance(k, str)
            self.assertTrue(v is None or isinstance(v, (int, float)),
                            f"weekly[{k}] must decode into float64 (or be null)")
        # legacy keys the payload has always carried ("native" was dropped — N accounts)
        for k in ("since", "last_switch", "pinned", "thresholds"):
            self.assertIn(k, p)
        self.assertNotIn("native", p, "the native/home concept is gone from the contract")

    def test_lane_payload_shape(self):
        _, p = self.get("/preferred")
        self.assertEqual(p["lane_rules"][-1], {"lane": "default", "match": [], "scope": None},
                         "default rule must be LAST (proxy walks first-match)")
        lanes = [r["lane"] for r in p["lane_rules"]]
        self.assertEqual(lanes, ex._lane_names)
        for ln, s in p["lanes"].items():
            for k in ("account", "reason", "pinned", "since", "last_switch", "effective"):
                self.assertIn(k, s, f"lane {ln} missing {k}")
            self.assertIsInstance(s["effective"], dict)
        self.assertEqual(p["lanes"]["fable"]["effective"][ACCT_A], 85)
        self.assertEqual(p["lanes"]["default"]["effective"][ACCT_A], 41)


class TestStateMigration(unittest.TestCase):
    def test_v1_seeds_all_lanes_and_globalizes_pin(self):
        v1 = {"account": ACCT_B, "since": 111.0, "reason": "pinned to " + ACCT_B,
              "last_switch": 222.0, "pinned": ACCT_B}
        out = ex._migrate_state(v1, ex._lane_names)
        self.assertEqual(out["account"], ACCT_B)
        for ln in ex._lane_names:
            s = out["lanes"][ln]
            self.assertEqual(s["account"], ACCT_B)
            self.assertEqual(s["pinned"], ACCT_B, "v1 global pin -> all-lane pin")
            self.assertEqual(s["last_switch"], 222.0, "dwell continuity must carry over")

    def test_v2_preserves_lanes_and_seeds_new_ones(self):
        v2 = {"account": ACCT_A, "since": 1.0, "reason": "r", "last_switch": 2.0,
              "pinned": None,
              "lanes": {"fable": {"account": ACCT_B, "since": 3.0, "reason": "moved",
                                  "last_switch": 4.0, "pinned": ACCT_B},
                        "gone-lane": {"account": ACCT_B}}}
        out = ex._migrate_state(v2, ex._lane_names)
        self.assertEqual(out["lanes"]["fable"]["account"], ACCT_B)
        self.assertEqual(out["lanes"]["fable"]["pinned"], ACCT_B)
        self.assertEqual(out["lanes"]["opus"]["account"], ACCT_A, "new lane seeds from legacy")
        self.assertNotIn("gone-lane", out["lanes"], "lanes dropped from config drop from state")

    def test_rollback_a_v1_reader_sees_the_default_lane(self):
        # New brain saves v2; a rolled-back v1 brain copies only the keys it knows.
        reset(acct(ACCT_A, 41, caps={"Fable": 85}), acct(ACCT_B, 30))
        ex.compute_preferred()
        age_lanes()
        ex.compute_preferred()
        with ex._plock:
            ex._save_state()
        with open(ex.STATE_FILE) as f:
            saved = json.load(f)
        v1_view = {k: saved[k] for k in
                   ("account", "since", "reason", "last_switch", "pinned") if k in saved}
        self.assertEqual(v1_view["account"], saved["lanes"]["default"]["account"])
        self.assertEqual(v1_view["account"], ACCT_B, "default lane bootstrapped onto the coolest account")
        self.assertIsNone(v1_view["pinned"])


class TestRenderMetrics(unittest.TestCase):
    def setUp(self):
        # A is coolest on every lane except fable (A's Fable cap 85 > B's 60), so
        # bootstrap seeds fable onto B and default/opus/… onto A.
        reset(acct(ACCT_A, 41, caps={"Fable": 85}), acct(ACCT_B, 60))
        ex.compute_preferred()
        age_lanes()
        ex.compute_preferred()  # fable -> ACCT_B, default stays on ACCT_A

    def line(self, text, metric, **labels):
        want = [f'{k}="{v}"' for k, v in labels.items()]
        for ln in text.splitlines():
            if ln.startswith(metric + "{") and all(w in ln for w in want):
                return float(ln.rsplit(" ", 1)[1])
        return None

    def test_lane_matrix_and_legacy_mirror(self):
        t = ex.render()
        self.assertEqual(self.line(t, "claude_usage_lane_preferred", lane="fable", account=ACCT_B), 1)
        self.assertEqual(self.line(t, "claude_usage_lane_preferred", lane="fable", account=ACCT_A), 0)
        self.assertEqual(self.line(t, "claude_usage_lane_preferred", lane="default", account=ACCT_A), 1)
        # legacy metric = default lane, NOT the split fable lane
        self.assertEqual(self.line(t, "claude_usage_preferred", account=ACCT_A), 1)
        self.assertEqual(self.line(t, "claude_usage_preferred", account=ACCT_B), 0)

    def test_lane_effective_and_join_health(self):
        t = ex.render()
        self.assertEqual(self.line(t, "claude_usage_lane_effective_percent", lane="fable", account=ACCT_A), 85)
        self.assertEqual(self.line(t, "claude_usage_lane_effective_percent", lane="default", account=ACCT_A), 41)
        self.assertEqual(self.line(t, "claude_usage_lane_scoped_cap_found", lane="fable", account=ACCT_A), 1)
        self.assertEqual(self.line(t, "claude_usage_lane_scoped_cap_found", lane="opus", account=ACCT_A), 0)
        # the default lane declares no scope_model -> no join-health series at all
        self.assertIsNone(self.line(t, "claude_usage_lane_scoped_cap_found", lane="default", account=ACCT_A))

    def test_global_switch_ts_is_max_over_lanes(self):
        t = ex.render()
        with ex._plock:
            want = max(s["last_switch"] for s in ex._preferred["lanes"].values())
        got = None
        for ln in t.splitlines():
            if ln.startswith("claude_usage_switch_last_timestamp_seconds "):
                got = float(ln.rsplit(" ", 1)[1])
        self.assertEqual(got, round(want))


class TestStaleExclusion(unittest.TestCase):
    def test_stale_account_is_excluded(self):
        # ACCT_B looks cool but its data is an hour old — bootstrap must not pick it.
        reset(acct(ACCT_A, 60), acct(ACCT_B, 5, good_age=3600))
        ex.compute_preferred()
        for ln in ex._lane_names:
            self.assertEqual(lane_account(ln), ACCT_A)

    def test_all_stale_with_credits_off_uses_the_last_values(self):
        """C2 deleted "fresh only, else every account": the dial check decides.
        Credits off = nothing billable can hide behind a stale reading, so the
        last values are used (basis stale) and the coolest still bootstraps."""
        a = acct(ACCT_A, 60, good_age=3600)
        b = acct(ACCT_B, 5, good_age=3600)
        for r in (a, b):
            r["raw"]["extra_usage"] = {"is_enabled": False}
        reset(a, b)
        ex.compute_preferred()
        self.assertEqual(lane_account("default"), ACCT_B)

    def test_all_stale_with_credits_on_holds_the_lane(self):
        """Credits on, so a stale account could bill a lane that may not pay:
        its check is no (unknown), and the lane keeps what it had rather than
        guess."""
        a, b = acct(ACCT_A, 60, good_age=3600), acct(ACCT_B, 5, good_age=3600)
        for r in (a, b):
            r["raw"]["extra_usage"] = {"is_enabled": True}
        reset(a, b)
        with ex._plock:
            ex._preferred["lanes"]["default"]["account"] = ACCT_A
        ex.compute_preferred()
        self.assertEqual(lane_account("default"), ACCT_A)
        self.assertIsNone(lane_account("opus"), "never bootstrapped onto an unknown account")


class TestLaneConfig(unittest.TestCase):
    def test_default_lane_always_last(self):
        self.assertEqual(ex._lane_names[-1], "default")

    def test_lanes_file_override(self):
        path = os.path.join(_TMP, "lanes-override.json")
        with open(path, "w") as f:
            json.dump([{"lane": "fable", "match": ["fable"], "scope_model": "Fable"},
                       {"lane": "default", "match": ["hijack"]},  # reserved -> ignored
                       {"lane": "fable", "match": ["dup"]}], f)   # dup -> ignored
        old = ex.LANES_FILE
        ex.LANES_FILE = path
        try:
            lanes = ex._load_lanes()
        finally:
            ex.LANES_FILE = old
        self.assertEqual([lc["lane"] for lc in lanes], ["fable", "default"])
        self.assertEqual(lanes[-1]["match"], [], "implicit default is never redefined")


class TestKimiPollNormalization(unittest.TestCase):
    def setUp(self):
        # Ensure we never hit the live Kimi endpoint during these tests.
        self._orig_key = pkimi.KIMI_API_KEY
        pkimi.KIMI_API_KEY = "sk-kimi-testdeadbeef"

    def tearDown(self):
        pkimi.KIMI_API_KEY = self._orig_key

    def _patch_get(self, raw):
        self._orig_get = pkimi._get_kimi_usages
        def _fake(_key):
            return raw
        pkimi._get_kimi_usages = _fake

    def _restore_get(self):
        pkimi._get_kimi_usages = self._orig_get

    def test_parse_weekly_and_five_hour(self):
        raw = {
            "usage": {"used": "2.0", "limit": "100.0", "remaining": "98.0",
                      "resetTime": "2026-07-18T22:00:00Z"},
            "limits": [{"window": {"timeUnit": "TIME_UNIT_MINUTE", "duration": 300},
                        "detail": {"used": "8.0", "limit": "100.0", "remaining": "92.0",
                                   "resetTime": "2026-07-18T18:00:00Z"}}],
            "totalQuota": {"limit": "100.0", "remaining": "99.0"},
            "parallel": {"limit": "20.0"},
            "membership": {"level": "LEVEL_INTERMEDIATE"},
            "subType": "TYPE_PURCHASE",
            "scope": "FEATURE_CODING",
        }
        self._patch_get(raw)
        try:
            res = pkimi.poll_kimi()
            self.assertEqual(res["ok"], 1)
            self.assertNotIn("usage", res, "the reading replaces the usage contract")
            w = ex._reading_windows(res["reading"])
            self.assertEqual(list(w), ["five_hour", "seven_day"])
            self.assertEqual(w["seven_day"]["used"], 2.0)
            self.assertEqual(w["seven_day"]["used_pct"], 2.0)
            self.assertEqual(w["seven_day"]["opens"], "anchored")
            self.assertEqual(w["five_hour"]["used"], 8.0)
            self.assertEqual(w["five_hour"]["used_pct"], 8.0)
            self.assertEqual(w["five_hour"]["period_s"], 18000)
            self.assertEqual(w["five_hour"]["resets_at"],
                             round(ex._iso_to_epoch("2026-07-18T18:00:00Z")))
            self.assertEqual(ex._provider_effective(res), 8.0)
            self.assertEqual(res["reading"]["plan"],
                             {"label": "LEVEL_INTERMEDIATE", "basis": "exact",
                              "sub_type": "TYPE_PURCHASE", "scope": "FEATURE_CODING"})
            self.assertEqual(res["reading"]["beside"],
                             [{"kind": "concurrency", "limit": 20.0, "on_exceed": "refuse",
                               "basis": "exact"}])
        finally:
            self._restore_get()

    def test_total_quota_is_read_and_never_binds(self):
        # K2: the monthly pool is shared with every Kimi feature and what drains
        # it is not known, so it is shown with basis unknown_semantics and is
        # no window: a spent pool does not close the allowance.
        res = read(kimi_result(five_hour=8.0, weekly=2.0, total_limit=100.0,
                               total_remaining=0.0))
        pot = ex._reading_pot(res["reading"], ex.ALLOWANCE_POT)
        tq = ex._reading_limit(pot, "total_quota")
        self.assertEqual((tq["kind"], tq["used_pct"], tq["basis"]),
                         ("cycle_pool", 100.0, "unknown_semantics"))
        self.assertEqual(ex._provider_effective(res), 8.0)

    def test_extra_usage_pot_is_unknown(self):
        # 12 §2: Extra Usage has no API. The pot is there; nothing about it is
        # known, so its gate is None, never False (closed) or True (open).
        res = kimi_result()
        pot = ex._reading_pot(res["reading"], ex.MONEY_POT)
        self.assertIsNone(pot["open"])
        self.assertEqual(pot["entitlement"], {"state": "unknown", "basis": "unknown"})
        self.assertEqual(ex._reading_limit(pot, "balance")["basis"], "unknown")

    def test_weekly_missing_is_no_weekly_window(self):
        # K1: a new plan has no weekly window. That is a reading without one,
        # binding on the 5-hour window alone, not a failed poll.
        self._patch_get(kimi_raw(five_hour=8.0, weekly=None))
        try:
            res = pkimi.poll_kimi()
            self.assertEqual(res["ok"], 1, res["err"])
            self.assertEqual(list(ex._reading_windows(res["reading"])), ["five_hour"])
            self.assertEqual(ex._provider_effective(res), 8.0)
        finally:
            self._restore_get()

    def test_five_hour_missing_binds_on_weekly(self):
        res = read(kimi_result(five_hour=None, weekly=5.0))
        self.assertEqual(list(ex._reading_windows(res["reading"])), ["seven_day"])
        self.assertEqual(ex._provider_effective(res), 5.0)

    def test_empty_body_stays_a_failed_read(self):
        # K3 reads {} as "not entitled"; that is C9's. Until then an empty
        # answer reads no limit, so it is a failure that keeps the last good
        # reading, never a reading of nothing.
        self._patch_get({})
        try:
            res = pkimi.poll_kimi()
            self.assertEqual(res["ok"], 0)
            self.assertIsNone(res["reading"])
            self.assertIn("no window", res["err"])
        finally:
            self._restore_get()

    def test_window_of_unknown_length_is_not_slotted(self):
        raw = kimi_raw(five_hour=None, weekly=3.0)
        raw["limits"] = [{"window": {"duration": 1, "timeUnit": "TIME_UNIT_DAY"},
                          "detail": kimi_window(40.0, time.time() + 86400)},
                         {"window": {"duration": 2, "timeUnit": "TIME_UNIT_FORTNIGHT"},
                          "detail": kimi_window(90.0, time.time() + 86400)}]
        res = pkimi._kimi_reading(raw, time.time())[0]
        w = ex._reading_windows(res)
        self.assertEqual(list(w), ["window:1d", "seven_day"],
                         "a length that fits no slot is its own window; an unknown unit is not read")
        self.assertEqual(w["window:1d"]["used_pct"], 40.0)
        self.assertEqual(ex._reading_effective({"kind": "provider", "reading": res}, None), 40.0)

    def test_string_values_parsed(self):
        raw = {
            "usage": {"used": "7.5", "limit": "100.0", "remaining": "92.5"},
            "limits": [{"window": {"timeUnit": "TIME_UNIT_MINUTE", "duration": 300},
                        "detail": {"used": "3.5", "limit": "100.0", "remaining": "96.5"}}],
        }
        self._patch_get(raw)
        try:
            res = pkimi.poll_kimi()
            w = ex._reading_windows(res["reading"])["seven_day"]
            self.assertEqual(w["used"], 7.5)
            self.assertEqual(w["limit"], 100.0)
            self.assertEqual(w["remaining"], 92.5)
        finally:
            self._restore_get()

    def test_reset_time_converted_to_epoch(self):
        raw = {
            "usage": {"used": "0", "limit": "100", "resetTime": "2026-07-18T22:00:00Z"},
        }
        self._patch_get(raw)
        try:
            res = pkimi.poll_kimi()
            rs = ex._reading_windows(res["reading"])["seven_day"]["resets_at"]
            self.assertIsInstance(rs, int)
            self.assertGreater(rs, 1_000_000_000)
        finally:
            self._restore_get()


class TestKimiMetricsRender(unittest.TestCase):
    def test_provider_metrics_present(self):
        reset(kimi_result(five_hour=8.0, weekly=2.0))
        t = ex.render()
        self.assertIsNotNone(TestRenderMetrics().line(t, "provider_effective_percent", provider="kimi"))
        self.assertEqual(TestRenderMetrics().line(t, "provider_effective_percent", provider="kimi"), 8.0)
        self.assertEqual(TestRenderMetrics().line(t, "provider_quota_utilization_percent",
                                                  provider="kimi", window="five_hour"), 8.0)
        self.assertEqual(TestRenderMetrics().line(t, "provider_quota_utilization_percent",
                                                  provider="kimi", window="seven_day"), 2.0)
        self.assertEqual(TestRenderMetrics().line(t, "provider_poll_success", provider="kimi"), 1)

    def test_provider_failure_shown(self):
        reset(kimi_result(ok=0, http=503, err="usage HTTP 503", good_age=120))
        t = ex.render()
        self.assertEqual(TestRenderMetrics().line(t, "provider_poll_success", provider="kimi"), 0)
        self.assertEqual(TestRenderMetrics().line(t, "provider_poll_http_code", provider="kimi"), 503)
        age = TestRenderMetrics().line(t, "provider_data_age_seconds", provider="kimi")
        self.assertIsNotNone(age)
        self.assertGreaterEqual(age, 100)


class TestKimiPreferredContract(unittest.TestCase):
    def test_kimi_does_not_appear_in_preferred(self):
        reset(acct(ACCT_A, 41, caps={"Fable": 85}), acct(ACCT_B, 30), kimi_result())
        ex.compute_preferred()
        p = ex._preferred_payload()
        self.assertNotIn("kimi", p["weekly"])
        for ln, s in p["lanes"].items():
            self.assertNotIn("kimi", s.get("effective") or {})

    def test_provider_does_not_participate_in_routing(self):
        # Kimi looking cool must NOT move lanes off Anthropic accounts — providers
        # are Phase 1 metrics only and have no usable account in the switchover.
        reset(acct(ACCT_A, 99), acct(ACCT_B, 98), kimi_result(weekly=5.0))
        ex.compute_preferred()  # bootstrap -> coolest Anthropic account (B), never kimi
        age_lanes()
        ex.compute_preferred()
        # Both Anthropic accounts are basically maxed; kimi looks cool but without the
        # provider filter this would route to "kimi" and break the proxy.
        self.assertEqual(lane_account("default"), ACCT_B)
        self.assertNotEqual(lane_account("default"), "kimi")
        with ex._plock:
            self.assertNotIn("kimi", ex._preferred["lanes"]["default"].get("account", ""))


class TestKimiDisabledWhenNoKey(unittest.TestCase):
    def test_no_provider_state_or_metrics_without_key(self):
        with ex._lock:
            ex._state.pop("kimi", None)
        old_key = pkimi.KIMI_API_KEY
        pkimi.KIMI_API_KEY = ""
        try:
            res = pkimi.poll_kimi()
            self.assertEqual(res["ok"], 0)
            self.assertIn("not configured", res["err"])
            t = ex.render()
            self.assertIsNone(TestRenderMetrics().line(t, "provider_poll_success", provider="kimi"))
        finally:
            pkimi.KIMI_API_KEY = old_key


class TestKimiUsedDerivation(unittest.TestCase):
    """#26 pre-flight — the LIVE Kimi payload carries limit+remaining but NO used;
    `used` must be derived, and truly-unknown usage must surface as None (never 0:
    a permanently-0% provider would poison the class pointer)."""

    LIVE_RAW = {  # the exact live shape
        "usage": {"limit": "100", "remaining": "100",
                  "resetTime": "2026-08-01T14:22:37.627546Z"},
        "limits": [{"window": {"duration": 300, "timeUnit": "TIME_UNIT_MINUTE"},
                    "detail": {"limit": "100", "remaining": "100",
                               "resetTime": "2026-07-28T14:22:37.627546Z"}}],
        "parallel": {"limit": "20"}, "totalQuota": {},
    }

    def test_used_derived_from_remaining(self):
        w = pkimi._parse_kimi_window({"limit": "100", "remaining": "63"})
        self.assertEqual(w["used"], 37.0)
        self.assertEqual(w["utilization"], 37.0)

    def test_remaining_derived_from_used(self):
        w = pkimi._parse_kimi_window({"limit": "100", "used": "8"})
        self.assertEqual(w["remaining"], 92.0)

    def test_live_payload_yields_honest_zero(self):
        r = pkimi._kimi_reading(self.LIVE_RAW, time.time())[0]
        w = ex._reading_windows(r)
        self.assertEqual(w["seven_day"]["used"], 0.0)
        self.assertEqual(w["seven_day"]["used_pct"], 0.0)
        self.assertEqual(w["five_hour"]["used_pct"], 0.0)
        self.assertEqual(ex._provider_effective({"kind": "provider", "reading": r}), 0.0,
                         "0.0 because remaining==limit — derived, not defaulted")
        pot = ex._reading_pot(r, ex.ALLOWANCE_POT)
        self.assertIsNone(ex._reading_limit(pot, "total_quota"),
                          "an empty totalQuota is no pool, not a pool at 0")

    def test_underivable_used_is_none_not_zero(self):
        w = pkimi._parse_kimi_window({"limit": "100"})  # no used, no remaining
        self.assertNotIn("used", w)
        self.assertNotIn("utilization", w)
        raw = {"usage": {"limit": "100"}}
        r = pkimi._kimi_reading(raw, time.time())[0]
        self.assertEqual(ex._reading_windows(r)["seven_day"]["basis"], "unknown")
        self.assertIsNone(ex._provider_effective({"kind": "provider", "reading": r}),
                          "limit-bearing window with unknown used must be None")

    def test_unknown_session_window_poisons_effective(self):
        raw = {"usage": {"limit": "100", "remaining": "98"},
               "limits": [{"window": {"duration": 300, "timeUnit": "TIME_UNIT_MINUTE"},
                           "detail": {"limit": "100"}}]}  # binds, usage unknown
        r = pkimi._kimi_reading(raw, time.time())[0]
        self.assertIsNone(ex._provider_effective({"kind": "provider", "reading": r}))


def standing_windows(r, provider="anthropic"):
    """The allowance windows of one result's standing, as /preferred
    accounts{} publishes them."""
    if "reading" not in r:
        read(r)
    return ex._account_standing(r["name"], r, provider, time.time())["allowance"]["windows"]


class TestTargetContract(unittest.TestCase):
    """#24 — the target contract: accounts and providers answer the same questions
    through the same functions, and the policy/payload go ONLY through them."""

    def test_kind_dispatch(self):
        self.assertEqual(ex._target_kind(acct(ACCT_A, 10)), "anthropic_account")
        self.assertEqual(ex._target_kind(kimi_result()), "provider")

    def test_effective_dispatch(self):
        fable = next(lc for lc in ex._lanes if lc["lane"] == "fable")
        a = acct(ACCT_A, 41, caps={"Fable": 85})
        self.assertEqual(ex._target_effective(a, fable), ex._lane_effective(a, fable))
        k = kimi_result(five_hour=8.0, weekly=2.0)
        # provider effective is target-global: the lane argument must not matter
        for lc in ex._lanes:
            self.assertEqual(ex._target_effective(k, lc), 8.0)

    def test_serves_filter(self):
        a, k = acct(ACCT_A, 10), kimi_result()
        for lc in ex._lanes:
            self.assertTrue(ex._serves(a, lc), "accounts serve every lane")
            self.assertFalse(ex._serves(k, lc), "providers serve no Anthropic lane (until #25)")

    def test_account_windows_with_resets(self):
        r = acct(ACCT_A, 41, caps={"Fable": 85}, session=10)
        r["raw"]["five_hour"]["resets_at"] = "2026-07-18T18:00:00Z"
        r["raw"]["seven_day"]["resets_at"] = "2026-07-25T00:00:00+00:00"
        r["raw"]["limits"][0]["resets_at"] = "2026-07-25T00:00:00Z"
        w = standing_windows(read(r))
        self.assertEqual(set(w), {"five_hour", "seven_day", "scoped:Fable"})
        self.assertEqual(w["five_hour"]["used_pct"], 10.0)
        self.assertEqual(w["seven_day"]["used_pct"], 41.0)
        self.assertEqual(w["scoped:Fable"]["used_pct"], 85.0)
        for name, entry in w.items():
            # Every window carries aliases (null on 5h/7d: the proxy names
            # those itself); the proxy's preflight requires the key.
            self.assertIn("aliases", entry, f"window {name}")
            self.assertIsInstance(entry["resets_at"], int,
                                  f"window {name}: ISO resets_at must land as epoch")
        self.assertLess(w["five_hour"]["resets_at"], w["seven_day"]["resets_at"])

    def test_account_windows_unknown_reset_is_none_not_missing(self):
        # acct() carries no resets_at anywhere: the windows still appear (they bind),
        # but resets_at is None — usable for forced-down, never a drift-up trigger.
        w = standing_windows(acct(ACCT_A, 41, caps={"Fable": 85}))
        self.assertEqual(set(w), {"five_hour", "seven_day", "scoped:Fable"})
        for name, entry in w.items():
            self.assertIsNone(entry["resets_at"], f"window {name}")

    def test_provider_windows_epoch_passthrough(self):
        w = standing_windows(kimi_result(five_hour=8.0, weekly=2.0), "kimi")
        self.assertEqual(set(w), {"five_hour", "seven_day"})
        self.assertEqual(w["five_hour"]["used_pct"], 8.0)
        self.assertEqual(w["seven_day"]["used_pct"], 2.0)
        for entry in w.values():
            self.assertIsInstance(entry["resets_at"], int)


class TestScopeAliases(unittest.TestCase):
    """C5: the brain publishes the joins the proxy reads Anthropic headers by — each
    scoped window's aliases, each lane's scope — so a header window is mapped by
    meaning, never by a name substring (quota-model 10 D1)."""

    def test_fable_window_carries_its_captured_aliases(self):
        w = standing_windows(acct(ACCT_A, 89, caps={"Fable": 100}))
        self.assertEqual(w["scoped:Fable"]["aliases"],
                         {"header": ["7d_oi"], "claim": ["seven_day_overage_included"]})
        for name in ("five_hour", "seven_day"):
            self.assertIsNone(w[name]["aliases"], "the proxy names 5h/7d itself")

    def test_unseen_family_publishes_no_aliases(self):
        # Opus, Sonnet and Mythos have no capture (10 U4): their windows still bind
        # (the poll's own scope) but name no header token — the proxy keeps such a
        # token "unmapped" rather than guess it from its spelling.
        w = standing_windows(acct(ACCT_A, 41, caps={"Fable": 85, "Opus": 100, "Mythos": 3}))
        self.assertIsNone(w["scoped:Opus"]["aliases"])
        self.assertIsNone(w["scoped:Mythos"]["aliases"])
        self.assertEqual(w["scoped:Opus"]["used_pct"], 100.0)

    def test_aliases_are_declared_not_matched_by_name(self):
        # No alias is a substring game: no Fable alias contains "fable", and every
        # header token is distinct across families.
        for dn, al in ex.SCOPE_ALIASES.items():
            self.assertEqual(set(al), {"header", "claim"}, dn)
            for a in al["header"] + al["claim"]:
                self.assertNotIn(dn.lower(), a.lower(), f"{dn}: {a}")
        toks = [t for al in ex.SCOPE_ALIASES.values() for t in al["header"]]
        self.assertEqual(len(toks), len(set(toks)))

    def test_lane_rules_publish_each_lanes_scope(self):
        reset(acct(ACCT_A, 41, caps={"Fable": 85}))
        p = ex._preferred_payload()
        scopes = {r["lane"]: r["scope"] for r in p["lane_rules"]}
        self.assertEqual(scopes, {"fable": "Fable", "opus": "Opus", "sonnet": "Sonnet",
                                  "haiku": "Haiku", "default": None})
        # The join the proxy makes is exact: lane scope -> the window key.
        fable = p["accounts"][ACCT_A]["allowance"]["windows"]["scoped:" + scopes["fable"]]
        self.assertEqual(fable["aliases"]["header"], ["7d_oi"])
        json.dumps(p)  # the payload stays JSON (aliases None -> null)


class TestPreferredHasNoTargets(HTTPBase):
    """C6: /preferred publishes each account's standing (accounts{}), its
    routes' checks (routes{}) and its poll health (account_health); the C2
    `targets` key is deleted, so no reader can fall back to it."""

    def test_no_targets_key(self):
        reset(acct(ACCT_A, 41), kimi_result())
        ex.compute_preferred()
        _, p = self.get("/preferred")
        self.assertNotIn("targets", p)
        self.assertLessEqual({ACCT_A, "kimi"}, set(p["accounts"]))

    def test_providers_do_not_leak_into_legacy_keys(self):
        reset(acct(ACCT_A, 41), kimi_result(five_hour=8.0, weekly=2.0))
        ex.compute_preferred()
        _, p = self.get("/preferred")
        self.assertNotIn("kimi", p["weekly"])
        for s in p["lanes"].values():
            self.assertNotIn("kimi", s["effective"])


def load_classes(data):
    """Round-trip class config through the real loader (file + _load_classes)."""
    path = os.path.join(_TMP, "classes-test.json")
    with open(path, "w") as f:
        if isinstance(data, str):
            f.write(data)
        else:
            json.dump(data, f)
    old = ex.CLASSES_FILE
    ex.CLASSES_FILE = path
    try:
        return ex._load_classes()
    finally:
        ex.CLASSES_FILE = old


def frontier_cfg():
    """A frontier class: opus lane P1, an unpolled gpt P2, kimi P3.
    Members deliberately OUT of priority order — the loader must sort."""
    return [{"class": "frontier", "display": "Frontier (auto)", "members": [
        {"priority": 3, "kind": "provider", "provider": "kimi", "model": "kimi-k3",
         "context_window": 256000, "anthropic_token_ratio": 0.61},
        {"priority": 1, "kind": "anthropic", "lane": "opus", "model": "claude-opus-4-8",
         "context_window": 200000},
        {"priority": 2, "kind": "provider", "provider": "gpt", "model": "gpt-5.6",
         "context_window": 400000, "anthropic_token_ratio": 0.6},
    ]}]


def local_class_cfg():
    """The #135 class: a local server's three llama-server presets, as the document declares them."""
    def member(prio, model, window):
        return {"priority": prio, "kind": "provider", "provider": "local", "model": model,
                "context_window": window, "anthropic_token_ratio": 1.0,
                "calibration_ref": "num_ctx, model's own tokenizer", "pay": []}
    return {"class": "local", "display": "Local (LAN server)", "members": [
        member(1, "local-gemma4-12b", 65536),
        member(2, "local-gpt-oss-20b", 65536),
        member(3, "local-qwen3-coder-30b", 131072)]}


def frontier_cfg_ceilings(**by_member):
    """frontier_cfg() with reserve ceilings attached by lane/provider name (#33),
    e.g. frontier_cfg_ceilings(gpt=25). Unnamed members carry no ceiling."""
    cfg = frontier_cfg()
    for m in cfg[0]["members"]:
        key = m.get("lane") or m.get("provider")
        if by_member.get(key) is not None:
            m["reserve_ceiling"] = by_member[key]
    return cfg


class TestClassGuidance(unittest.TestCase):
    BAD_GUIDANCE = (7, True, [], {}, "", "   ", "x" * 301,
                    "one\ntwo", "one\rtwo", "one\ttwo", "one\x01two", "one\x7ftwo",
                    "\nvalid", "valid\t")

    def test_strict_rejects_bad_guidance_naming_class(self):
        for guidance in self.BAD_GUIDANCE:
            with self.subTest(guidance=guidance):
                doc = frontier_cfg()
                doc[0]["guidance"] = guidance
                errors = ex._validate_classes_doc(doc)
                self.assertTrue(errors)
                self.assertTrue(any("frontier" in e and "guidance" in e for e in errors))

    def test_valid_guidance_is_stripped_and_none_is_explicit(self):
        for guidance in ("  Main tier. Settled work.  ", " " + "x" * 300 + " ", None):
            with self.subTest(guidance=guidance):
                doc = frontier_cfg()
                doc[0]["guidance"] = guidance
                self.assertEqual(ex._validate_classes_doc(doc), [])
                warnings = []
                out = ex._normalize_classes(doc, warnings)
                self.assertEqual(out[0]["guidance"], guidance.strip() if guidance else None)
                self.assertEqual(warnings, [])
        self.assertIsNone(ex._normalize_classes(frontier_cfg())[0]["guidance"])

    def test_tolerant_load_keeps_class_and_warns_for_bad_guidance(self):
        for guidance in self.BAD_GUIDANCE:
            with self.subTest(guidance=guidance):
                doc = frontier_cfg()
                doc[0]["guidance"] = guidance
                warnings = []
                out = ex._normalize_classes(doc, warnings)
                self.assertEqual([c["class"] for c in out], ["frontier"])
                self.assertIsNone(out[0]["guidance"])
                self.assertEqual(len(out[0]["members"]), 3)
                self.assertTrue(any("frontier" in w and "guidance" in w for w in warnings))
                self.assertEqual(doc[0]["guidance"], guidance, "raw document is not mutated")


class TestClassConfig(unittest.TestCase):
    """#25 — classes.json loader: per-class drop-don't-crash validation."""

    def test_absent_file_means_no_classes(self):
        old = ex.CLASSES_FILE
        ex.CLASSES_FILE = os.path.join(_TMP, "no-such-classes.json")
        try:
            self.assertEqual(ex._load_classes(), [])
        finally:
            ex.CLASSES_FILE = old

    def test_bad_json_and_non_array_load_nothing(self):
        self.assertEqual(load_classes("{not json"), [])
        self.assertEqual(load_classes({"class": "frontier"}), [])

    def test_valid_class_sorted_and_derived(self):
        out = load_classes(frontier_cfg())
        self.assertEqual(len(out), 1)
        c = out[0]
        self.assertEqual(c["class"], "frontier")
        self.assertEqual(c["display"], "Frontier (auto)")
        self.assertEqual([m["priority"] for m in c["members"]], [1, 2, 3],
                         "members sorted by priority regardless of file order")
        self.assertEqual(c["context_window"], 200000,
                         "class window is DERIVED = min(member windows)")
        self.assertEqual(c["members"][0]["lane"], "opus")
        self.assertEqual(c["members"][1]["model"], "gpt-5.6")
        self.assertEqual(c["members"][0]["model"], "claude-opus-4-8",
                         "anthropic members carry the concrete model id the proxy rewrites to (#27)")

    def test_unknown_keys_normalized_away(self):
        cfg = frontier_cfg()
        cfg[0]["members"][1]["note"] = "junk"
        out = load_classes(cfg)
        self.assertNotIn("note", out[0]["members"][0], "members normalized to schema keys")

    # ---- #33 reserve_ceiling: an OPTIONAL member field

    def test_reserve_ceiling_survives_normalization_for_both_kinds(self):
        out = load_classes(frontier_cfg_ceilings(opus=40, gpt=25))
        ms = {m.get("lane") or m.get("provider"): m for m in out[0]["members"]}
        self.assertEqual(ms["opus"]["reserve_ceiling"], 40.0)
        self.assertEqual(ms["gpt"]["reserve_ceiling"], 25.0)
        self.assertIsInstance(ms["gpt"]["reserve_ceiling"], float, "normalized to float")

    def test_absent_ceiling_loads_and_is_none(self):
        """The whitelist trap: reserve_ceiling is optional, so appending it to
        `keep` would KeyError at import and the brain would never start."""
        out = load_classes(frontier_cfg())
        self.assertEqual(len(out), 1, "a member with no ceiling must still load")
        for m in out[0]["members"]:
            self.assertIn("reserve_ceiling", m, "key always present — uniform shape")
            self.assertIsNone(m["reserve_ceiling"])

    def test_explicit_null_ceiling_is_none(self):
        cfg = frontier_cfg()
        cfg[0]["members"][0]["reserve_ceiling"] = None
        self.assertIsNone(load_classes(cfg)[0]["members"][2]["reserve_ceiling"])

    def test_invalid_ceiling_drops_the_class(self):
        for bad in ("25", True, -1, 101):
            with self.subTest(bad=bad):
                self.assertEqual(load_classes(frontier_cfg_ceilings(gpt=bad)), [],
                                 "an unvalidated ceiling would raise in the poller thread")

    def test_boundary_ceilings_are_legal(self):
        for ok in (0, 100, 80):
            with self.subTest(ok=ok):
                out = load_classes(frontier_cfg_ceilings(gpt=ok))
                self.assertEqual(len(out), 1)
                self.assertEqual(out[0]["members"][1]["reserve_ceiling"], float(ok))

    # ---- #44 anthropic_token_ratio: required on provider members, forbidden
    # on anthropic ones, and the class window derives from EFFECTIVE windows.

    def test_ratio_missing_on_provider_member_drops_class(self):
        cfg = frontier_cfg()
        del cfg[0]["members"][0]["anthropic_token_ratio"]  # kimi (file order)
        self.assertEqual(load_classes(cfg), [],
                         "min() across tokenizers without units is the #44 fatal bug")

    def test_ratio_invalid_values_drop_class(self):
        for bad in ("0.6", True, 0, -1, 4.5):
            with self.subTest(bad=bad):
                cfg = frontier_cfg()
                cfg[0]["members"][0]["anthropic_token_ratio"] = bad
                self.assertEqual(load_classes(cfg), [])

    def test_ratio_forbidden_on_anthropic_member(self):
        cfg = frontier_cfg()
        cfg[0]["members"][1]["anthropic_token_ratio"] = 1.0  # the anthropic member
        self.assertEqual(load_classes(cfg), [],
                         "r_anthropic is 1.0 by definition — declaring it invites unit confusion")

    def test_effective_windows_and_floor_member(self):
        out = load_classes(frontier_cfg())
        ms = {m.get("lane") or m.get("provider"): m for m in out[0]["members"]}
        self.assertEqual(ms["opus"]["anthropic_token_ratio"], 1.0)
        self.assertEqual(ms["opus"]["effective_window"], 200000)
        self.assertEqual(ms["gpt"]["effective_window"], 666666,
                         "floor(400000 / 0.6) — gpt's window in Anthropic units")
        self.assertEqual(ms["kimi"]["effective_window"], 419672)
        c = out[0]
        self.assertEqual(c["context_window"], 200000,
                         "derived = min(effective windows), still the opus 200k")
        self.assertEqual(c["floor_member"], "lane:opus")
        self.assertEqual(c["units"], "anthropic")

    def test_provider_floor_member_sets_units(self):
        cfg = frontier_cfg()
        # Make gpt the floor: ratio 1.0 on a 400k window still beats nothing, so
        # shrink its window relative to the others via a big ratio instead.
        cfg[0]["members"][2]["anthropic_token_ratio"] = 2.0  # gpt: 400k/2 = 200k
        cfg[0]["members"][1]["context_window"] = 400000      # opus above it
        out = load_classes(cfg)
        self.assertEqual(out[0]["context_window"], 200000)
        self.assertEqual(out[0]["floor_member"], "gpt/gpt-5.6")
        self.assertEqual(out[0]["units"], "gpt/gpt-5.6",
                         "units follow the floor member's tokenizer (#44 §160)")

    def test_derived_window_below_200k_declared_and_warned(self):
        # #135: no floor. The class serves its derived window as is; the warning
        # stays, because a client that assumes 200k for a claude-* id can overrun it.
        cfg = frontier_cfg()
        cfg[0]["members"][2]["anthropic_token_ratio"] = 4.0  # gpt: 400k/4 = 100k effective
        with mock.patch.object(ex, "log") as lg:
            out = load_classes(cfg)
        self.assertEqual(out[0]["context_window"], 100000, "advertised as derived, never clamped")
        self.assertTrue(any("below 200k" in str(c.args[0]) for c in lg.call_args_list),
                        lg.call_args_list)

    def test_sub_200k_member_accepted(self):
        # The local class's shape: members declaring 64k/128k load and validate.
        cfg = [local_class_cfg()]
        self.assertEqual(ex._validate_classes_doc(cfg), [])
        out = load_classes(cfg)
        self.assertEqual(out[0]["context_window"], 65536)
        self.assertEqual(out[0]["floor_member"], "local/local-gemma4-12b")

    def test_window_change_history_stamped_and_persisted(self):
        # load_classes swaps CLASSES_FILE to one shared tmp path, so the .windows
        # sidecar accumulates across tests — start this one from a clean slate.
        state = os.path.join(_TMP, "classes-test.json.windows")
        if os.path.exists(state):
            os.remove(state)
        self.addCleanup(lambda: os.path.exists(state) and os.remove(state))

        out1 = load_classes(frontier_cfg())
        self.assertIsNone(out1[0]["previous_context_window"], "first sighting has no history")
        self.assertIsNone(out1[0]["window_changed_at"])

        cfg = frontier_cfg()
        cfg[0]["members"][1]["context_window"] = 250000  # opus grows; kimi 419k, gpt 666k
        out2 = load_classes(cfg)
        self.assertEqual(out2[0]["context_window"], 250000)
        self.assertEqual(out2[0]["previous_context_window"], 200000)
        self.assertIsNotNone(out2[0]["window_changed_at"])
        changed_at = out2[0]["window_changed_at"]

        out3 = load_classes(cfg)  # unchanged reload carries history forward
        self.assertEqual(out3[0]["previous_context_window"], 200000)
        self.assertEqual(out3[0]["window_changed_at"], changed_at)

    def test_anthropic_member_requires_model(self):
        cfg = frontier_cfg()
        del cfg[0]["members"][1]["model"]  # the anthropic member (file order!)
        self.assertEqual(load_classes(cfg), [],
                         "no model id = the proxy has nothing to rewrite claude-<class> to")

    def test_display_defaults_to_name(self):
        cfg = frontier_cfg()
        del cfg[0]["display"]
        self.assertEqual(load_classes(cfg)[0]["display"], "frontier")

    def test_window_floor_drops_class_keeps_rest(self):
        cfg = frontier_cfg() + [{"class": "fast", "members": [
            {"priority": 1, "kind": "provider", "provider": "local", "model": "qwen",
             "context_window": 64000}]}]
        out = load_classes(cfg)
        self.assertEqual([c["class"] for c in out], ["frontier"],
                         "sub-200k member rejects its class; the rest still load")

    def test_missing_window_drops_class(self):
        cfg = frontier_cfg()
        del cfg[0]["members"][1]["context_window"]
        self.assertEqual(load_classes(cfg), [], "context_window is declared, never inferred")

    def test_unknown_lane_drops_class(self):
        cfg = frontier_cfg()
        cfg[0]["members"][1]["lane"] = "gpt5"  # the anthropic member (file order!)
        self.assertEqual(load_classes(cfg), [])

    def test_bad_or_colliding_names_dropped(self):
        cfg = ([{"class": "Frontier", "members": frontier_cfg()[0]["members"]},
                {"class": "opus", "members": frontier_cfg()[0]["members"]}]
               + frontier_cfg() + frontier_cfg())
        out = load_classes(cfg)
        self.assertEqual([c["class"] for c in out], ["frontier"],
                         "bad regex + lane collision + duplicate all dropped, first valid kept")

    def test_duplicate_priority_drops_class(self):
        cfg = frontier_cfg()
        cfg[0]["members"][0]["priority"] = 1
        self.assertEqual(load_classes(cfg), [])

    def test_provider_member_requires_provider_and_model(self):
        cfg = frontier_cfg()
        cfg[0]["members"][0]["model"] = ""
        self.assertEqual(load_classes(cfg), [])


class TestClassMembership(unittest.TestCase):
    """#25 — _class_member_targets: which polled targets could back a member.
    Pure membership, no policy (#26 ranks)."""

    def snap(self):
        return [acct(ACCT_A, 41), acct(ACCT_B, 30), kimi_result()]

    def test_anthropic_member_backed_by_every_account(self):
        m = {"priority": 1, "kind": "anthropic", "lane": "opus", "context_window": 200000}
        names = {r["name"] for r in ex._class_member_targets(m, self.snap())}
        self.assertEqual(names, {ACCT_A, ACCT_B})

    def test_provider_member_matches_provider_id(self):
        m = {"priority": 3, "kind": "provider", "provider": "kimi", "model": "kimi-k3",
             "context_window": 256000}
        names = [r["name"] for r in ex._class_member_targets(m, self.snap())]
        self.assertEqual(names, ["kimi"])

    def test_unpolled_provider_member_is_empty(self):
        m = {"priority": 2, "kind": "provider", "provider": "gpt", "model": "gpt-5.6",
             "context_window": 400000}
        self.assertEqual(ex._class_member_targets(m, self.snap()), [])


class TestPreferredClassesPayload(HTTPBase):
    """#25 — /preferred grows an additive `classes` key: config + per-member live
    view. The frozen legacy shape and the #24 `targets` key must be untouched."""

    def setUp(self):
        ex._classes = load_classes(frontier_cfg())
        reset(acct(ACCT_A, 41, caps={"Opus": 55}),
              acct(ACCT_B, 30, caps={"Opus": 20}),
              kimi_result(five_hour=8.0, weekly=2.0))
        ex.compute_preferred()

    def tearDown(self):
        ex._classes = []

    def test_classes_shape_and_derived_window(self):
        _, p = self.get("/preferred")
        c = p["classes"]["frontier"]
        self.assertEqual(c["display"], "Frontier (auto)")
        self.assertEqual(c["context_window"], 200000)
        self.assertEqual([m["priority"] for m in c["members"]], [1, 2, 3])

    def test_anthropic_member_live_view(self):
        _, p = self.get("/preferred")
        m = p["classes"]["frontier"]["members"][0]
        self.assertEqual(m["kind"], "anthropic")
        self.assertEqual(m["lane"], "opus")
        self.assertEqual(m["targets"], {ACCT_A: 55.0, ACCT_B: 30.0},
                         "per-account opus-lane effective via _target_effective")
        self.assertTrue(m["usable"])
        self.assertEqual(m["status"], "ok")

    def test_unpolled_gpt_member_is_visible_degradation(self):
        _, p = self.get("/preferred")
        m = p["classes"]["frontier"]["members"][1]
        self.assertEqual(m["model"], "gpt-5.6")
        self.assertEqual(m["targets"], {})
        self.assertFalse(m["usable"])
        self.assertEqual(m["status"], "no_target",
                         "no poller (#31) shows up as no_target, never silently ok")

    def test_provider_member_live_view(self):
        _, p = self.get("/preferred")
        m = p["classes"]["frontier"]["members"][2]
        self.assertEqual(m["targets"], {"kimi": 8.0})
        self.assertTrue(m["usable"])
        self.assertEqual(m["status"], "ok")

    def test_stale_backing_data(self):
        reset(acct(ACCT_A, 41, caps={"Opus": 55}), acct(ACCT_B, 30),
              kimi_result(good_age=ex.STALE_MAX + 100, stale=1))
        _, p = self.get("/preferred")
        m = p["classes"]["frontier"]["members"][2]
        self.assertEqual(m["status"], "stale")
        self.assertFalse(m["usable"])
        self.assertEqual(m["targets"], {"kimi": 8.0},
                         "stale numbers still shown — the status carries the caveat")

    def test_fresh_but_unknown_effective(self):
        k = kimi_result()
        k["raw"]["usage"] = {"limit": "100"}  # no used, no remaining: unknown
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30), k)
        _, p = self.get("/preferred")
        m = p["classes"]["frontier"]["members"][2]
        self.assertEqual(m["status"], "no_quota_data")
        self.assertFalse(m["usable"])
        self.assertEqual(m["targets"], {"kimi": None})

    def test_no_classes_serves_empty_object(self):
        ex._classes = []
        _, p = self.get("/preferred")
        self.assertEqual(p["classes"], {})

    def test_classes_do_not_leak_into_legacy_keys(self):
        _, p = self.get("/preferred")
        self.assertNotIn("frontier", p["lanes"])
        self.assertNotIn("frontier", p["weekly"])
        self.assertNotIn("frontier", p["accounts"])
        self.assertEqual(set(p["lane_rules"][0]), {"lane", "match", "scope"})


def opus_acct(cap, resets=False):
    """One account whose opus-lane effective is exactly `cap` (weekly/session low).
    With resets=True every window carries a resets_at so drift marks are armed.
    The resets lie ahead: a reset that has passed opens a window at 100 (the
    dial check, quota-model 50 §1.4), and a spent window must stay spent."""
    a = acct(ACCT_A, 10, caps={"Opus": cap})
    if resets:
        a["raw"]["five_hour"]["resets_at"] = "2099-07-28T18:00:00Z"
        a["raw"]["seven_day"]["resets_at"] = "2099-08-01T00:00:00Z"
        a["raw"]["limits"][0]["resets_at"] = "2099-08-01T00:00:00Z"
    return read(a)


def class_verdicts(idx=0):
    """The live verdicts of one class: {member id: {route, checks, account,
    check, effective}}."""
    with ex._lock:
        snap = list(ex._state.values())
    with ex._plock:
        ctx = ex._policy_ctx(snap, time.time())
        return ex._class_verdicts(ex._classes[idx], ctx)


def classes_reset(*results, cfg=None):
    """Install a class config + snapshots, clear class-session state, seed lanes."""
    ex._classes = load_classes(cfg if cfg is not None else frontier_cfg())
    reset(*results)
    with ex._plock:
        ex._preferred["classes"] = {}
    ex.compute_preferred()


def boot_brain(entry, classes_doc, windows=None):
    """Start `entry` (brain.py, brain_household.py) for real, in a temp creds dir
    with no accounts, on classes_doc (and a prior windows history); return its
    /preferred, the windows file it left and its log once it serves. The boot
    path, not a test's import: a load that ran before the drivers registered is
    seen here and nowhere else."""
    d = tempfile.mkdtemp(prefix="brain-boot-")
    try:
        files = {k: os.path.join(d, v) for k, v in (
            ("SWITCH_CLASSES_FILE", "classes.json"), ("SWITCH_STATE_FILE", "switch_state.json"),
            ("SWITCH_LANES_FILE", "lanes.json"), ("CLAUDE_ACCOUNTS_FILE", "accounts.json"),
            ("OPENAI_CODEX_CREDS_FILE", "openai-codex.json"), ("KIMI_CREDS_FILE", "kimi.json"),
            ("ANTHROPIC_API_CREDS_FILE", "anthropic-api.json"), ("RUNWAY_CREDS_FILE", "runway.json"),
            ("ELEVENLABS_CREDS_FILE", "elevenlabs.json"), ("TYPESAFE_CREDS_FILE", "typesafe.json"),
            ("SWITCH_KEY_FILE", "switch_key"), ("SWITCH_HARNESS_FILE", "harness.json"),
            ("SWITCH_HARNESS_KEY_FILE", "harness_key"))}
        with open(files["SWITCH_CLASSES_FILE"], "w") as f:
            json.dump(classes_doc, f)
        if windows is not None:
            with open(files["SWITCH_CLASSES_FILE"] + ".windows", "w") as f:
                json.dump(windows, f)
        with open(files["SWITCH_KEY_FILE"], "w") as f:
            f.write("boot-key")
        with socket.socket() as so:
            so.bind(("127.0.0.1", 0))
            port = so.getsockname()[1]
        env = {"PATH": os.environ.get("PATH", ""), "HOME": d, "PORT": str(port), "POLL_INTERVAL": "300",
               "LOCAL_UPSTREAM": "", "PYTHONDONTWRITEBYTECODE": "1", **files}
        log_path = os.path.join(d, "log")
        with open(log_path, "w") as log_f:
            proc = subprocess.Popen([sys.executable, entry], cwd=BRAIN, env=env, stdout=log_f,
                                    stderr=subprocess.STDOUT)
        try:
            pref = None
            for _ in range(100):
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/preferred", timeout=2) as r:
                        pref = json.load(r)
                    break
                except (OSError, ValueError):
                    if proc.poll() is not None:
                        break
                    time.sleep(0.1)
        finally:
            proc.terminate()
            proc.wait(timeout=10)
        with open(log_path) as f:
            log_text = f.read()
        try:
            with open(files["SWITCH_CLASSES_FILE"] + ".windows") as f:
                windows_after = json.load(f)
        except FileNotFoundError:
            windows_after = None
        if pref is None:
            raise AssertionError(f"{entry} never served /preferred:\n{log_text}")
        return pref, windows_after, log_text
    finally:
        shutil.rmtree(d, ignore_errors=True)


def image_boot_doc():
    """A classes.json with a chat class and an image class on gpt (a shared
    driver's image model), and the windows history a running brain left."""
    # migrate_images.py's IMAGES_CLASS, written out: the migrations are household
    # and the shared edition carries none of them.
    images = {"class": "images", "display": "Images", "request_kind": "image",
              "members": [{"priority": 1, "kind": "provider", "provider": "gpt",
                           "model": "gpt-image-2", "pay": ["gpt/credits"]}]}
    doc = {"classes": [copy.deepcopy(c) for c in frontier_cfg()] + [images]}
    return doc


class TestBootLoadsClassesAfterTheDrivers(unittest.TestCase):
    """brain.py's real start: the class document is checked against the drivers
    that registered, so an image class survives the boot and its window history
    is kept (a load at import dropped it and rewrote the history without it)."""

    def test_an_image_class_survives_the_boot(self):
        doc = image_boot_doc()
        prior = {"images": {"window": None, "previous": None, "changed_at": None}}
        pref, windows, log_text = boot_brain("brain.py", doc, prior)
        self.assertNotIn("dropped 'images'", log_text)
        self.assertIn("images", pref["classes"], log_text[-2000:])
        self.assertIn("images", windows)


class _LogCapture:
    """Collect ex.log lines for the life of a test."""

    def capture_log(self):
        self.logged = []
        orig = ex.log
        self.addCleanup(setattr, ex, "log", orig)
        ex.log = self.logged.append


class TestDocRevision(unittest.TestCase):
    def test_content_hash_is_canonical(self):
        self.assertEqual(ex._doc_revision([{"b": 1, "a": 2}]),
                         ex._doc_revision([{"a": 2, "b": 1}]),
                         "revision hashes the CANONICALIZED doc — key order is not content")

    def test_content_change_changes_revision(self):
        self.assertNotEqual(ex._doc_revision([{"a": 1}]), ex._doc_revision([{"a": 2}]))

    def test_sixteen_hex_chars(self):
        rev = ex._doc_revision(frontier_cfg())
        self.assertRegex(rev, r"^[0-9a-f]{16}$")


class TestConsumersValidation(unittest.TestCase):
    """Pure-function coverage of the §5 invariants."""

    def test_ref_grammar(self):
        self.assertEqual(ex._parse_ref("lane:opus"), ("lane", "opus"))
        self.assertEqual(ex._parse_ref("class:frontier"), ("class", "frontier"))
        for bad in ("", "lane:", "class:", "opus", "a:b/c", None, 7,
                    "kimi/kimi-k3"):  # the named form went with its rows (C2)
            self.assertIsNone(ex._parse_ref(bad), f"{bad!r} must not parse")

    def test_named_refs_are_refused_with_the_reason(self):
        """C2 deleted the direct <provider>/<model> path (quota-model 50 §2.2
        row 10): a PUT naming one is refused and says why; a file written
        before the cut still loads, without that ref, and warns."""
        doc = {"consumers": [{"id": "coding-harness", "request_shape": "claude-code",
                              "members": ["lane:opus", "kimi/kimi-k3"]}]}
        errors, _, _ = ex._normalize_consumers(doc, strict=True)
        self.assertEqual(len(errors), 1, errors)
        self.assertIn("'kimi/kimi-k3'", errors[0])
        self.assertIn("retired", errors[0])
        errors, warnings, out = ex._normalize_consumers(doc, strict=False)
        self.assertEqual(errors, [])
        self.assertEqual(out[0]["members"], ["lane:opus"])
        self.assertTrue(any("kimi/kimi-k3" in w and "retired" in w and "dropped" in w
                            for w in warnings), warnings)

    def test_strict_errors(self):
        cases = {
            "bad_id": {"consumers": [{"id": "Bad_Id", "request_shape": "other", "members": []}]},
            "bad_shape": {"consumers": [{"id": "a", "request_shape": "webhook", "members": []}]},
            "bad_ref": {"consumers": [{"id": "a", "request_shape": "other", "members": ["nope"]}]},
            "dup_id": {"consumers": [{"id": "a", "request_shape": "other", "members": []},
                                     {"id": "a", "request_shape": "other", "members": []}]},
            "dup_ref": {"consumers": [
                {"id": "a", "request_shape": "other", "members": ["lane:opus"]},
                {"id": "b", "request_shape": "other", "members": ["lane:opus"]}]},
            "not_a_doc": [1, 2],
        }
        for name, doc in cases.items():
            with self.subTest(name):
                errors, _, _ = ex._normalize_consumers(doc, strict=True)
                self.assertTrue(errors, f"{name} must be a strict error")

    def test_duplicate_id_still_errors_when_first_occurrence_is_invalid(self):
        """Mirrors _validate_classes_doc's seeding of seen from invalid-but-named
        entries: one editor round-trip must report BOTH problems, not reveal the
        duplicate only after the first entry is fixed."""
        doc = {"consumers": [
            {"id": "a", "request_shape": "webhook", "members": []},  # invalid, names "a"
            {"id": "a", "request_shape": "other", "members": []},    # duplicate of it
        ]}
        errors, _, _ = ex._normalize_consumers(doc, strict=True)
        self.assertEqual(len(errors), 2, errors)
        self.assertTrue(any("request_shape" in e for e in errors), errors)
        self.assertTrue(any("duplicate consumer id" in e for e in errors), errors)

    def test_tolerant_load_keeps_later_valid_entry_when_first_is_invalid(self):
        """The strict-only counterpart of the test above: on the tolerant load
        path the invalid first occurrence is dropped, so the valid later entry
        is the sole survivor — dropping both would maximize config loss."""
        doc = {"consumers": [
            {"id": "a", "request_shape": "webhook", "members": []},
            {"id": "a", "request_shape": "other", "members": ["lane:opus"]},
        ]}
        errors, warnings, out = ex._normalize_consumers(doc, strict=False)
        self.assertEqual(errors, [])
        self.assertEqual([c["id"] for c in out], ["a"])
        self.assertEqual(out[0]["members"], ["lane:opus"],
                         "the valid later entry survives intact")
        self.assertTrue(any("request_shape" in w for w in warnings), warnings)

    def test_dangling_is_never_a_strict_error(self):
        doc = {"consumers": [{"id": "a", "request_shape": "other",
                              "members": ["class:no-such", "lane:no-such"]}]}
        errors, _, out = ex._normalize_consumers(doc, strict=True)
        self.assertEqual(errors, [], "cross-doc/dangling refs must never reject (§5)")
        self.assertEqual(out[0]["members"], ["class:no-such", "lane:no-such"])

    def test_tolerant_load_first_wins(self):
        doc = {"consumers": [
            {"id": "a", "request_shape": "other", "members": ["lane:opus"]},
            {"id": "b", "request_shape": "other", "members": ["lane:opus", "lane:haiku"]},
            {"id": "Bad", "request_shape": "other", "members": []},
        ]}
        errors, warnings, out = ex._normalize_consumers(doc, strict=False)
        self.assertEqual(errors, [], "the tolerant path NEVER errors — hand-edits can't crash the brain")
        self.assertEqual([c["id"] for c in out], ["a", "b"])
        self.assertEqual(out[0]["members"], ["lane:opus"], "first consumer keeps the ref")
        self.assertEqual(out[1]["members"], ["lane:haiku"], "duplicate dropped from the loser")
        self.assertTrue(any("already claimed" in w for w in warnings))
        self.assertTrue(any("Bad" in w for w in warnings))

    def test_view_statuses_and_request_shape_warn(self):
        classes = load_classes(frontier_cfg())
        consumers = [
            {"id": "cc", "display": "cc", "request_shape": "claude-code",
             "members": ["lane:opus", "class:frontier"], "warnings": []},
            {"id": "bot", "display": "bot", "request_shape": "other",
             "members": ["class:gone", "lane:gone"], "warnings": []},
        ]
        view = ex._consumers_view(consumers, classes)
        cc = view[0]
        self.assertEqual([m["status"] for m in cc["members"]], ["ok", "ok"])
        self.assertEqual(cc["warnings"], [])
        bot = view[1]
        self.assertEqual([m["status"] for m in bot["members"]], ["dangling", "dangling"])
        self.assertTrue(any("unknown lane 'gone'" in w for w in bot["warnings"]))
        # anthropic member inside a class claimed by a non-claude-code consumer
        consumers[1]["members"] = ["class:frontier"]
        consumers[0]["members"] = []
        view = ex._consumers_view(consumers, classes)
        self.assertTrue(any("dead hop" in w for w in view[1]["warnings"]),
                        "request_shape mismatch is a WARN (§5 enforcement semantics)")
        self.assertEqual(view[1]["members"][0]["status"], "ok", "a warn is not dangling")

    def test_unreadable_file_loads_none_with_warning(self):
        with open(ex.CONSUMERS_FILE, "w") as f:
            f.write("{nope")
        try:
            out = ex._load_consumers()
            self.assertEqual(out, [])
            self.assertTrue(ex._consumers_warnings)
        finally:
            os.remove(ex.CONSUMERS_FILE)
            ex._consumers = []
            ex._load_consumers()


GOLDEN = proxy_testdata("dialcheck_golden.json")


# ---- C2: the dial check at the brain's dial sites (quota-model 50 §2.2-§2.4) ---
#
# Accounts in these tests: ACCT_A (Anthropic, opus_acct/opus_spent), ACCT_B (in
# the creds cache, never measured: its check is always no (unknown)), "gpt" and
# "kimi" (provider pools of one). Pot ids are "<account>/credits".

POT_A = f"{ACCT_A}/credits"


def opus_spent(credits=True, monthly_spent=False, good_age=0):
    """ACCT_A with its Opus cap at 100 until a reset ahead, and an extra_usage
    block: credits on (the pot open), off, or on with the month's limit spent."""
    a = opus_acct(100, resets=True)
    a["good_ts"] = time.time() - good_age
    a["raw"]["extra_usage"] = {"is_enabled": credits, "monthly_limit": 5000,
                                 "used_credits": 5000 if monthly_spent else 0,
                                 "utilization": 100.0 if monthly_spent else 0.0}
    return read(a)


def pay_cfg(opus=None, gpt=None, kimi=None, opus_stop=None, lanes=None):
    """frontier_cfg() (opus P1, gpt P2, kimi P3) with owner settings attached;
    a `lanes` map makes it the {"classes", "lanes"} document."""
    cfg = frontier_cfg()
    for m in cfg[0]["members"]:
        key = m.get("lane") or m.get("provider")
        pay = {"opus": opus, "gpt": gpt, "kimi": kimi}[key]
        if pay is not None:
            m["pay"] = pay
        if key == "opus" and opus_stop is not None:
            m["stop_at"] = opus_stop
    return cfg if lanes is None else {"classes": cfg, "lanes": lanes}


def install_money(doc):
    _, _, m = ex._normalize_money(doc, strict=False)
    with ex._plock:
        ex._money = m


def route_check(rid, account):
    with ex._lock:
        snap = list(ex._state.values())
    with ex._plock:
        return ex._policy_ctx(snap, time.time())["checks"][rid][account]


class PayBase(unittest.TestCase):
    """Restores every global the owner settings touch."""

    def tearDown(self):
        ex._classes = []
        ex._lane_settings = {}
        install_money({})
        with ex._poll_health_lock:
            ex._poll_schedule.clear()

    def bind(self, sid="s1", cname="frontier"):
        return ex._bind_class_session(cname, sid, time.time())


class TestStopAt(PayBase):
    """stop_at closes only the route's allowance (§2.4)."""

    def check_at(self, stop, seven_day=70.0, other_lane="lane:opus"):
        a = opus_acct(10, resets=True)
        a["raw"]["seven_day"]["utilization"] = seven_day
        classes_reset(a, cfg=pay_cfg(opus_stop=stop))
        return (route_check("frontier/lane:opus", ACCT_A), route_check(other_lane, ACCT_A))

    def test_unset_is_included(self):
        mine, _ = self.check_at(None)
        self.assertEqual(mine["check"], "included")

    def test_level_60_on_7d_stops_this_route_only(self):
        mine, lane = self.check_at({"window": "7d", "level": 60})
        self.assertEqual((mine["check"], mine["reason"]), ("no", "stop_at"))
        self.assertEqual(mine["until"], round(ex._iso_to_epoch("2099-08-01T00:00:00Z")),
                         "until the window's reset")
        self.assertEqual(lane["check"], "included", "the same account still serves lane:opus")
        mine, _ = self.check_at({"window": "7d", "level": 60}, seven_day=50.0)
        self.assertEqual(mine["check"], "included", "below the level")

    def test_level_0_never_uses_that_allowance(self):
        mine, _ = self.check_at({"window": "5h", "level": 0}, seven_day=1.0)
        self.assertEqual(mine["reason"], "stop_at")

    def test_level_100_or_more_has_no_effect(self):
        mine, _ = self.check_at({"window": "7d", "level": 100})
        self.assertEqual(mine["check"], "included")

    def test_at_100_money_answers_to_pay(self):
        a = opus_spent()
        cfg = pay_cfg(opus=[POT_A], opus_stop={"window": "Opus", "level": 60})
        classes_reset(a, cfg=cfg)
        self.assertEqual(route_check("frontier/lane:opus", ACCT_A)["check"], "paid")


class TestTolerantLoadOfRetiredKeys(PayBase):
    def test_spend_credits_is_ignored_with_a_warning_and_the_class_kept(self):
        """A document written for the previous brain still loads every class
        (the deploy gap before migrate_pay.py runs); the member pays nothing."""
        cfg = pay_cfg()
        cfg[0]["members"][2]["spend_credits"] = True  # gpt, file order
        out = load_classes(cfg)
        self.assertEqual(len(out), 1)
        gpt = next(m for m in out[0]["members"] if m.get("provider") == "gpt")
        self.assertEqual(gpt["pay"], [])
        self.assertNotIn("spend_credits", gpt)
        self.assertTrue(any("spend_credits is retired" in w and "pays nothing" in w
                            for w in ex._classes_warnings), ex._classes_warnings)

    def test_budget_keys_are_ignored_with_a_warning(self):
        out = load_classes(pay_cfg(lanes={"opus": {"pay": [], "budget": 5}}))
        self.assertEqual(len(out), 1)
        self.assertTrue(any("budget ignored" in w for w in ex._classes_warnings))

    def test_a_bad_lane_entry_is_dropped_and_the_rest_load(self):
        out = load_classes(pay_cfg(lanes={"nope": {"pay": []}, "opus": {"pay": [POT_A]}}))
        self.assertEqual(len(out), 1)
        self.assertEqual(ex._lane_settings, {"opus": {"pay": [POT_A], "stop_at": None}})
        self.assertTrue(any("lanes['nope']" in w for w in ex._classes_warnings))

    def test_a_bad_pay_shape_drops_the_class(self):
        cfg = pay_cfg(gpt="gpt/credits")
        self.assertEqual(load_classes(cfg), [])


class DialCheckGoldenTest(unittest.TestCase):
    """quota-model 50 §5.1: the dial check's golden fixtures. The proxy's
    dialcheck_test.go runs the same file, so the two evaluators agree by test,
    not by construction (§2.12 item 8)."""

    @classmethod
    def setUpClass(cls):
        with open(GOLDEN) as f:
            cls.doc = json.load(f)

    def test_stale_max_matches(self):
        self.assertEqual(self.doc["stale_max_s"], ex.STALE_MAX)

    def test_cases(self):
        for c in self.doc["cases"]:
            with self.subTest(c["name"]):
                got = ex.dial_check(c["route"], c["account"], c["standing"], c["pots"], self.doc["now"],
                                    self.doc["stale_max_s"], c["ledger"], c["request"])
                self.assertEqual(got, c["want"])

    def test_verdicts(self):
        for v in self.doc["verdicts"]:
            with self.subTest(v["name"]):
                self.assertEqual(ex.best_check(v["checks"]), v["want"])

    def test_paid_orders(self):
        for o in self.doc["paid_orders"]:
            with self.subTest(o["name"]):
                got = sorted(o["candidates"],
                             key=lambda c: ex.pay_order_key(c["pot"], c["priority"], o["pay_order"]))
                self.assertEqual([c["pot"] for c in got], o["want"])

    def test_unavailable(self):
        """§2.8: each evaluator checks every (route, account) itself, then
        builds the reply's body from those checks."""
        now = self.doc["now"]
        for u in self.doc["unavailable"]:
            with self.subTest(u["name"]):
                checks = {r["id"]: {a: ex.dial_check(r, a, u["standings"].get(a), u["pots"], now,
                                                     self.doc["stale_max_s"], u["ledger"], u["request"])
                                    for a in r["accounts"]}
                          for r in u["routes"]}
                # A check neither evaluator makes from a standing (the
                # proxy's no (blocked)) is given, in place of its own.
                for rid, given in u.get("given", {}).items():
                    checks[rid].update(given)
                got = ex.unavailable_body(u["routes"], checks, u["standings"], now, u["pots"],
                                          u["ledger"], u["request"])
                self.assertEqual(got, u["want"])


# ---- C2 review fixes (brain) ---------------------------------------------------

def payload_member(cname, mid):
    p = ex._preferred_payload()
    return next(m for m in p["classes"][cname]["members"] if m["id"] == mid)


class TestLaneSettingsStayOnTheLane(PayBase):
    """§1.9, §2.2 row 16: a bare lane's pay and stop_at bind lane:<lane> only;
    a class member on that lane chooses its account from its own route's checks."""

    def test_lane_stop_at_does_not_steer_the_member(self):
        reset_cfg = pay_cfg(lanes={"opus": {"stop_at": {"window": "7d", "level": 30}}})
        classes_reset(acct(ACCT_A, 40), acct(ACCT_B, 70), cfg=reset_cfg)
        self.assertEqual(route_check("lane:opus", ACCT_A)["reason"], "stop_at")
        self.assertIsNone(lane_account("opus"), "the lane has no account it may use")
        m = payload_member("frontier", "lane:opus")
        self.assertEqual(m["check"]["check"], "included")
        self.assertEqual(m["incumbent"], ACCT_A, "the member's own coolest included account")

    def test_member_leaves_a_lane_incumbent_frozen_on_a_spent_account(self):
        a = acct(ACCT_A, 100)
        a["raw"]["seven_day"]["resets_at"] = "2099-08-01T00:00:00Z"
        cfg = pay_cfg(lanes={"opus": {"stop_at": {"window": "7d", "level": 30}}})
        classes_reset(a, acct(ACCT_B, 50), cfg=cfg)
        with ex._plock:
            ex._preferred["lanes"]["opus"]["account"] = ACCT_A
        ex.compute_preferred()
        self.assertEqual(lane_account("opus"), ACCT_A, "no lane account may move: frozen")
        m = payload_member("frontier", "lane:opus")
        self.assertEqual(m["incumbent"], ACCT_B)
        self.assertEqual(m["checks"][ACCT_A]["check"], "no")

    def test_member_with_a_passing_account_always_has_an_incumbent(self):
        # acct-a's check passes (included, basis unknown: no 7-day number, credits
        # off) but it has no known effective, so no account is "usable" by
        # number. The member still names it: the proxy dials a member's
        # incumbent, and an empty one is refused as "no brain decision yet".
        a = acct(ACCT_A, 10)
        a["raw"]["seven_day"]["utilization"] = None
        a["raw"]["extra_usage"] = {"is_enabled": False}
        classes_reset(a, cfg=pay_cfg())
        m = payload_member("frontier", "lane:opus")
        self.assertEqual((m["checks"][ACCT_A]["check"], m["checks"][ACCT_A]["basis"]), ("included", "unknown"))
        self.assertEqual(m["checks"][ACCT_B]["check"], "no")
        self.assertEqual(m["incumbent"], ACCT_A)

    def test_same_settings_keep_the_lane_incumbent(self):
        classes_reset(acct(ACCT_A, 70), acct(ACCT_B, 40), cfg=pay_cfg())
        with ex._plock:
            ex._preferred["lanes"]["opus"]["account"] = ACCT_A
        self.assertEqual(payload_member("frontier", "lane:opus")["incumbent"], ACCT_A,
                         "incumbent-wins, as the lane decides it")


class TestPerLimitStanding(PayBase, HTTPBase):
    """§1.4: every window says when it was read, and /preferred publishes the
    stale limit the proxy judges by."""

    def test_windows_carry_their_reading_time(self):
        a = acct(ACCT_A, 30, good_age=120)
        reset(a)
        with ex._plock:
            st = ex._policy_ctx(list(ex._state.values()), time.time())["standings"][ACCT_A]
        self.assertTrue(st["allowance"]["windows"])
        for w in st["allowance"]["windows"].values():
            self.assertEqual(w["as_of"], st["as_of"])

    def test_stale_max_is_published(self):
        reset(acct(ACCT_A, 30))
        code, body = self.get("/preferred")
        self.assertEqual(code, 200)
        self.assertEqual(body["settings"]["stale_max"], ex.STALE_MAX)


class TestCreditsByIsEnabledAlone(PayBase):
    """C2: an Anthropic credits pot is on or off by is_enabled; how much is
    left is unknown until C10, so used-versus-limit says nothing."""

    def test_used_at_limit_is_still_open(self):
        a = acct(ACCT_A, 10)
        a["raw"]["extra_usage"] = {"is_enabled": True, "monthly_limit": 100,
                                     "used_credits": 100, "utilization": 100.0}
        reset(a)
        with ex._plock:
            st = ex._policy_ctx(list(ex._state.values()), time.time())["standings"][ACCT_A]
        self.assertIs(st["money"]["credits"]["open"], True)


class TestStaleEffectiveIsNull(PayBase):
    """The legacy `effective` keeps its meaning until C6: null when the
    reading behind it is older than STALE_MAX."""

    def test_stale_backing_account(self):
        a = acct(ACCT_A, 30, good_age=ex.STALE_MAX + 600)
        a["raw"]["extra_usage"] = {"is_enabled": False}
        classes_reset(a, cfg=pay_cfg())
        v = class_verdicts()["lane:opus"]
        self.assertEqual((v["check"]["check"], v["check"]["basis"]), ("included", "stale"))
        self.assertIsNone(v["effective"])
        self.assertIsNone(payload_member("frontier", "lane:opus")["effective"])


class TestReadingsAcrossTheC6Restart(_LogCapture, PayBase):
    """A reading saved before C6 holds the `usage` it replaced: never restored
    as if it were a reading, and never converted. The first C6 start converted
    them once; that migration is gone, so a file this old (a rollback past C6)
    is a cold start."""

    def test_pre_c6_saved_readings_are_not_restored(self):
        reset()
        old = {"name": ACCT_A, "kind": "anthropic_account", "good_ts": time.time() - 60,
               "usage": {"five_hour": {"utilization": 10}, "seven_day": {"utilization": 20}}}
        new = dict(acct(ACCT_B, 30, good_age=60))
        with ex._lock:
            ex._state.clear()
        saved = [old, {k: new[k] for k in ex._READING_KEYS if k in new}]
        self.capture_log()
        n = ex._restore_readings(saved)
        logged = self.logged
        self.assertEqual(n, 1)
        with ex._lock:
            self.assertNotIn(ACCT_A, ex._state)
            self.assertEqual(ex._state[ACCT_B]["stale"], 1)
        self.assertTrue(any("predate the normalized reading" in ln and ACCT_A in ln for ln in logged))
        ex.compute_preferred()
        self.assertEqual(route_check("lane:opus", ACCT_A)["reason"], "unknown",
                         "never measured until its first poll")


def results(res):
    return [r for r, _ in res]


# ---- GET /events (quota-model 64 §2) ----------------------------------------------

class SSE:
    """A /events subscriber: reads the stream block by block."""

    def __init__(self, port, timeout=5):
        import http.client
        self.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
        self.conn.request("GET", "/events", headers={"Last-Event-ID": "0-999"})
        self.resp = self.conn.getresponse()

    def block(self):
        """The next block's lines, up to its blank line."""
        lines = []
        while True:
            raw = self.resp.readline()
            if not raw:
                raise EOFError("stream closed")
            line = raw.decode("utf-8").rstrip("\n")
            if line:
                lines.append(line)
            elif lines:
                return lines

    def event(self):
        """The next block as {field: value}; a comment block as {"comment": text}."""
        out = {}
        for line in self.block():
            if line.startswith(":"):
                out.setdefault("comment", line[1:].strip())
            else:
                k, _, v = line.partition(": ")
                assert k not in out, f"field {k} twice in one block"
                out[k] = v
        return out

    def close(self):
        self.conn.close()


class TestEventStream(HTTPBase):
    """The brain serves /preferred as a stream (quota-model 64 §2), its
    publisher running as main() runs it."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.stop = threading.Event()
        cls.publisher = threading.Thread(target=ex._events_publisher, args=(cls.stop,), daemon=True)
        cls.publisher.start()

    @classmethod
    def tearDownClass(cls):
        cls.stop.set()
        ex._events_mark()
        cls.publisher.join(5)
        super().tearDownClass()

    def setUp(self):
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        ex.compute_preferred()
        ex._events_publish()  # the document for this state is the published one
        self.streams = []

    def tearDown(self):
        for s in self.streams:
            s.close()

    def connect(self):
        s = SSE(self.port)
        self.streams.append(s)
        return s

    def published(self):
        with ex._events_cond:
            return ex._events_pub["seq"], ex._events_pub["data"]

    def wait_for(self, cond, what, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if cond():
                return
            time.sleep(0.02)
        self.fail(f"timed out waiting for {what}")

    def test_first_event_on_connect(self):
        seq, data = self.published()
        s = self.connect()
        self.assertEqual(s.resp.status, 200)
        self.assertEqual(s.resp.getheader("Content-Type"), "text/event-stream")
        self.assertEqual(s.resp.getheader("Cache-Control"), "no-cache")
        self.assertEqual(s.block(), ["retry: 1000"])
        ev = s.event()
        self.assertEqual(ev, {"id": f"{ex._EVENTS_BOOT}-{seq}", "event": "preferred", "data": data},
                         "the current document at once; Last-Event-ID is ignored")
        doc = json.loads(ev["data"])
        self.assertEqual(doc["account"], ACCT_A)
        self.assertEqual(set(doc), set(ex._preferred_payload()), "the /preferred document")

    def test_a_mutation_publishes(self):
        s = self.connect()
        s.block()
        first = s.event()
        code, _ = self.get(f"/pin?account={ACCT_B}&lane=fable", key="test-switch-key")
        self.assertEqual(code, 200)
        ev = s.event()
        self.assertEqual(ev["event"], "preferred")
        self.assertGreater(int(ev["id"].rsplit("-", 1)[1]), int(first["id"].rsplit("-", 1)[1]))
        self.assertEqual(json.loads(ev["data"])["lanes"]["fable"]["pinned"], ACCT_B)

    def test_no_change_publishes_nothing(self):
        seq, _ = self.published()
        self.assertFalse(ex._events_publish(), "same state, same document")
        with mock.patch("time.time", return_value=time.time() + 60):
            self.assertFalse(ex._events_publish(), "only the clock moved")
        self.assertEqual(self.published()[0], seq)
        # End to end: a recompute that moves nothing reaches the stream as a keepalive.
        with mock.patch.object(ex, "EVENTS_KEEPALIVE_S", 0.5):
            s = self.connect()
            s.block()
            s.event()
            ex.compute_preferred()
            self.assertEqual(s.event(), {"comment": "keepalive"})
        self.assertEqual(self.published()[0], seq)

    def test_where_dirty_is_marked(self):
        # A request marks once it is answered, so each check waits for the
        # handler to finish rather than counting at the reply.
        with mock.patch.object(ex, "_events_mark") as mark:
            for path in ("/preferred", "/metrics", "/"):
                with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as r:
                    r.read()
            time.sleep(0.2)
            self.assertEqual(mark.call_count, 0, "the pure reads mark nothing")
            for what, send in (
                    ("a handled GET", lambda: self.get(f"/pin?account={ACCT_B}&lane=fable",
                                                       key="test-switch-key")),
                    ("an unknown GET", lambda: self.get("/no-such-path")),
                    ("a POST", lambda: self.assertRaises(urllib.error.HTTPError, urllib.request.urlopen,
                                                         urllib.request.Request(
                                                             f"http://127.0.0.1:{self.port}/no-such-path",
                                                             data=b"{}", method="POST"), timeout=5)),
                    ("compute_preferred", ex.compute_preferred)):
                with self.subTest(what):
                    n = mark.call_count
                    send()
                    self.wait_for(lambda: mark.call_count > n, f"{what} to mark")

    def test_keepalive_after_silence(self):
        with mock.patch.object(ex, "EVENTS_KEEPALIVE_S", 0.2):
            s = self.connect()
            s.block()
            s.event()
            t0 = time.monotonic()
            self.assertEqual(s.event(), {"comment": "keepalive"})
            self.assertGreaterEqual(time.monotonic() - t0, 0.15)
            self.assertEqual(s.event(), {"comment": "keepalive"})

    def test_a_slow_subscriber_gets_the_newest(self):
        with mock.patch.object(ex, "EVENTS_KEEPALIVE_S", 0.5):
            s = self.connect()
            s.block()
            s.event()
            # Hold the stream off its next read while three documents publish.
            with ex._events_publish_lock, ex._events_cond:
                for acct_, lane in ((ACCT_B, "fable"), (ACCT_B, "opus"), (ACCT_B, "haiku")):
                    with ex._plock:
                        for ls in ex._preferred["lanes"].values():
                            ls["pinned"] = None
                        ex._preferred["lanes"][lane]["pinned"] = acct_
                    self.assertTrue(ex._events_publish())
                newest = ex._events_pub["seq"]
            ev = s.event()
            self.assertEqual(ev["id"], f"{ex._EVENTS_BOOT}-{newest}", "skips to the newest")
            self.assertEqual(json.loads(ev["data"])["lanes"]["haiku"]["pinned"], ACCT_B)
            self.assertEqual(s.event(), {"comment": "keepalive"}, "nothing older follows")

    def test_a_disconnect_is_cleaned_up(self):
        with ex._events_cond:
            before = len(ex._events_subs)
        with mock.patch.object(ex, "EVENTS_KEEPALIVE_S", 0.05):
            s = self.connect()
            s.block()
            s.event()
            with ex._events_cond:
                self.assertEqual(len(ex._events_subs), before + 1)
            s.resp.close()
            s.conn.sock and s.conn.sock.close()
            s.close()

            def gone():
                with ex._events_cond:
                    return len(ex._events_subs) == before
            self.wait_for(gone, "the stream's subscriber to be dropped")


# ---- POST /reports (quota-model 64 §5) -------------------------------------------

RECORDS_GOLDEN = proxy_testdata("records_golden.json")


def anth_polled(t, weekly=30.0, session=10.0, five_hour=True, week_reset=None, name=ACCT_A):
    """An Anthropic poll that started at t: its 7d window at `weekly`
    resetting at week_reset (default t + 3 days), its 5h one at `session`,
    or no 5h window at all (idle) with five_hour=False."""
    raw = {"seven_day": {"utilization": weekly,
                         "resets_at": iso(t + 3 * 86400 if week_reset is None else week_reset)},
           "limits": []}
    if five_hour:
        raw["five_hour"] = {"utilization": session, "resets_at": iso(t + 3600)}
    return read({"name": name, "ok": 1, "ts": t, "email": name, "tier": "max", "http": 0,
                 "err": "", "retry_after": None, "good_ts": t, "stale": 0, "raw": raw})


def poll_lands(res):
    """A poll result replaces the account's, as the poller does, and the loop recomputes."""
    with ex._lock:
        ex._state[res["name"]] = res
    ex.compute_preferred()


def rep_reading(i, at, window="seven_day", used=None, resets=None, account=ACCT_A):
    return {"id": f"r-{i}", "kind": "reading", "account": account, "read_at": at,
            "window": window, "used_pct": used, "resets_at": resets}


def rep_refusal(i, at, until=None, by_reading=True, used=100.0, resets=None, window="seven_day",
                account=ACCT_A, ended=None, named=None):
    """A refusal event; `resets` is what the window read when it refused,
    `named` the reset the refusal itself named (sent as resets_at when set)."""
    ev = {"id": f"f-{i}", "kind": "refusal", "account": account, "read_at": at, "window": window,
          "limit": None, "class": "spent"}
    if ended is not None:
        ev["ended"] = ended
        return ev
    ev.update(until=until, by_reading=by_reading, seen={"used_pct": used, "resets_at": resets})
    if named is not None:
        ev["resets_at"] = named
    return ev


def post_reports(*events, now=None, reporter="test-host"):
    """-> [(result, why)] in order, and the body."""
    body, _ = ex.reports_post(list(events), reporter, time.time() if now is None else now)
    return [(r["result"], r["why"]) for r in body["results"]], body


def live_ctx(now=None):
    with ex._lock:
        snap = list(ex._state.values())
    with ex._plock:
        return ex._policy_ctx(snap, time.time() if now is None else now)


def live_window(wid, account=ACCT_A, now=None):
    return live_ctx(now)["standings"][account]["allowance"]["windows"].get(wid)


class RecordsGoldenTest(unittest.TestCase):
    """The brain holds a reported refusal by the proxy's rule: the proxy's
    record_golden_test.go runs the same cases through pruneRecords."""

    def test_reset_slack_matches(self):
        with open(RECORDS_GOLDEN) as f:
            self.assertEqual(json.load(f)["reset_slack_s"], ex.REPORT_RESET_SLACK_S)

    @staticmethod
    def hold(r):
        """The hold a POST /reports refusal makes (_report_refusal)."""
        return {"refused_at": r["read_at"], "until": r["until"], "resets": r["resets_at"],
                "by_reading": r["by_reading"], "seen": r["seen"]}

    def test_cases(self):
        with open(RECORDS_GOLDEN) as f:
            doc = json.load(f)
        self.assertGreaterEqual(len(doc["cases"]), 20)
        for c in doc["cases"]:
            with self.subTest(c["name"]):
                h = self.hold(c["record"])
                for i, step in enumerate(c["steps"]):
                    end, _, until = ex._hold_judge(h, c["record"]["window"], step["standing"],
                                                   step["now"])
                    self.assertEqual({"ends": end, "until": None if end else until}, step["want"],
                                     f"step {i}")
                    if end:
                        break
                    h["until"] = until  # what _prune_reports keeps

    def test_windows(self):
        with open(RECORDS_GOLDEN) as f:
            doc = json.load(f)
        self.assertGreaterEqual(len(doc["windows"]), 8)
        for c in doc["windows"]:
            with self.subTest(c["name"]):
                h = self.hold(c["record"])
                w = ex._hold_window(h, h["until"], c["window"], c["now"])
                self.assertEqual({k: w.get(k) for k in c["want"]}, c["want"])


class TestReportFreshness(PayBase):
    """One freshness for every decision: a reported window's own age."""

    def test_a_member_ranks_fresh_on_fresh_reports(self):
        old = time.time() - (ex.STALE_MAX + 600)
        classes_reset(anth_polled(old, weekly=30.0, session=10.0), cfg=frontier_cfg())
        self.assertIsNone(class_verdicts()["lane:opus"]["effective"], "the poll alone is stale")
        now = time.time()
        post_reports(rep_reading(1, now, used=40.0), rep_reading(2, now, window="five_hour", used=12.0))
        ex.compute_preferred()
        v = class_verdicts()["lane:opus"]
        self.assertEqual((v["check"]["check"], v["check"]["basis"]), ("included", "fresh"))
        self.assertEqual(v["effective"], 40.0, "ranks as fresh")
        m = next(m for m in ex._preferred_payload()["classes"]["frontier"]["members"]
                 if m["id"] == "lane:opus")
        self.assertEqual((m["status"], m["usable"], m["effective"]), ("ok", True, 40.0))
        st = ex._preferred_payload()["accounts"][ACCT_A]
        self.assertEqual(st["as_of"], old, "the account's own age stays the poll's")

    def test_settings_problems_read_the_reports(self):
        t0 = time.time() - 100
        reset(anth_polled(t0, five_hour=False))
        classes = load_classes(pay_cfg(opus_stop={"window": "5h", "level": 80}))
        dangling = lambda: [w for w in ex._settings_problems(classes, {})[1]
                            if "stop_at window '5h'" in w]
        self.assertTrue(dangling(), "no reading carries 5h yet")
        post_reports(rep_reading(1, t0 + 10, window="five_hour", used=12.0))
        self.assertEqual(dangling(), [], "a report added it")


class TestHolds(PayBase):
    """A refusal holds the window at 100 (§5), by the proxy's rule."""

    def setUp(self):
        self.t0 = time.time() - 100
        self.r7 = round(self.t0 + 3 * 86400)
        reset(anth_polled(self.t0, weekly=97.0, week_reset=self.r7))
        ex.compute_preferred()
        self.at = self.t0 + 10  # the refusal

    def tearDown(self):
        super().tearDown()
        with ex._poll_health_lock:
            ex._recheck_epochs.clear()
        ex._poll_wakeup.clear()

    def check(self, now=None):
        return live_ctx(now)["checks"]["lane:opus"][ACCT_A]

    def test_a_hold_with_until(self):
        until = time.time() + 3600
        self.assertEqual(post_reports(rep_refusal(1, self.at, until=until, used=97.0,
                                                  resets=self.r7))[0], [("applied", None)])
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["resets_at"], w["reset_basis"], w["as_of"]),
                         (100.0, until, "record", self.at), "no named reset: record")
        self.assertEqual(self.check()["check"], "no")
        self.assertEqual(ex._preferred_payload()["accounts"][ACCT_A]["allowance"]["windows"]
                         ["seven_day"]["used_pct"], 100.0, "the document shows the hold")
        self.assertEqual(ex._state[ACCT_A]["reading"]["pots"][0]["limits"][1]["used_pct"], 97.0,
                         "the poll is untouched")
        self.assertEqual(live_window("seven_day", now=until + 1)["used_pct"], 97.0,
                         "past its until the hold no longer applies")
        with mock.patch("time.time", return_value=until + 1):
            ex.compute_preferred()
        self.assertEqual(ex._holds, {}, "and is pruned")

    def test_an_until_already_past_is_ignored(self):
        res, _ = post_reports(rep_refusal(1, self.at, until=time.time() - 1))
        self.assertEqual(res[0], ("ignored", "its until has passed"))
        self.assertEqual(ex._holds, {})

    def test_no_until_ends_on_open_and_changed(self):
        post_reports(rep_refusal(1, self.at, used=97.0, resets=self.r7))
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["resets_at"]), (100.0, self.r7),
                         "held by reading: the reading's own reset while ahead")
        post_reports(rep_reading(2, self.at + 5, used=97.0))
        ex.compute_preferred()
        self.assertIn((ACCT_A, "seven_day"), ex._holds,
                      "the same period at the same use keeps holding")
        self.assertEqual(ex._holds[(ACCT_A, "seven_day")]["until"], self.r7,
                         "and learns the reading's reset")
        self.assertEqual(live_window("seven_day")["used_pct"], 100.0)
        nxt = self.r7 + 7 * 86400
        post_reports(rep_reading(3, self.at + 6, used=0.0, resets=nxt))
        self.assertEqual(live_window("seven_day")["used_pct"], 0.0, "a new period, open")
        ex.compute_preferred()
        self.assertEqual(ex._holds, {})

    def test_a_poll_that_is_open_and_changed_ends_it(self):
        post_reports(rep_refusal(1, self.at, used=97.0, resets=self.r7))
        poll_lands(anth_polled(self.at + 1, weekly=40.0, week_reset=self.r7))
        self.assertEqual(ex._holds, {})
        self.assertEqual(live_window("seven_day")["used_pct"], 40.0)

    def test_the_owners_recheck_ends_it(self):
        post_reports(rep_refusal(1, self.at, until=time.time() + 3600))
        self.assertIsNotNone(ex.recheck(ACCT_A, time.time()))
        ex.compute_preferred()
        self.assertEqual(ex._holds, {})
        self.assertEqual(live_window("seven_day")["used_pct"], 97.0)
        res, _ = post_reports(rep_refusal(2, self.at, until=time.time() + 3600))
        self.assertEqual(res[0], ("ignored", "older than the owner's re-check"))

    def test_ended_ends_only_older_holds(self):
        post_reports(rep_refusal(1, self.at, until=time.time() + 3600))
        res, _ = post_reports(rep_refusal(2, self.at - 1, ended=True))
        self.assertEqual(res[0][0], "ignored")
        self.assertIn((ACCT_A, "seven_day"), ex._holds)
        res, _ = post_reports(rep_refusal(3, self.at, ended=True))
        self.assertEqual(res[0][0], "ignored", "an end at the refusal's own instant is not newer")
        res, _ = post_reports(rep_refusal(4, self.at + 1, ended=True))
        self.assertEqual(res[0], ("applied", None))
        self.assertEqual(ex._holds, {})
        self.assertEqual(self.check()["check"], "included")

    def test_an_older_refusal_is_ignored(self):
        post_reports(rep_refusal(1, self.at, until=time.time() + 3600))
        res, _ = post_reports(rep_refusal(2, self.at - 1, until=time.time() + 7200))
        self.assertEqual(res[0][0], "ignored")
        res, _ = post_reports(rep_refusal(1, self.at, until=time.time() + 3600))
        self.assertEqual(res[0][0], "ignored", "the same refusal again changes nothing")

    def test_a_hold_on_a_window_the_poll_lacks(self):
        reset(anth_polled(self.t0, five_hour=False))
        until = time.time() + 600
        post_reports(rep_refusal(1, self.at, window="five_hour", until=until, by_reading=False,
                                 used=None))
        w = live_window("five_hour")
        self.assertEqual((w["used_pct"], w["period"], w["resets_at"], w["reset_basis"]),
                         (100.0, "5h", until, "record"))
        self.assertEqual(self.check()["check"], "no")

    def test_a_named_reset(self):
        until = round(time.time() + 6 * 3600)
        post_reports(rep_refusal(1, self.at, until=until, by_reading=False, named=self.r7))
        w = live_window("seven_day")
        self.assertEqual((w["resets_at"], w["reset_basis"]), (self.r7, "exact"),
                         "a named reset past the clamp: resets at it, exact")
        res, _ = post_reports(rep_refusal(2, self.at + 1, named="soon", until=until,
                                          by_reading=False))
        self.assertEqual(res[0][0], "refused")
        self.assertEqual(post_reports(dict(rep_refusal(3, self.at + 2, until=until, by_reading=False,
                                                       named=self.r7), ended=True))[0],
                         [("applied", None)], "an end carries the refusal's fields")

    def test_a_hold_reads_100_everywhere(self):
        post_reports(rep_reading(1, self.at - 1, used=10.0))
        ex.compute_preferred()
        self.assertEqual(ex._preferred_payload()["weekly"][ACCT_A], 10.0)
        post_reports(rep_refusal(2, self.at, until=time.time() + 3600))
        doc = ex._preferred_payload()
        self.assertEqual(doc["accounts"][ACCT_A]["allowance"]["windows"]["seven_day"]["used_pct"], 100.0)
        self.assertEqual(doc["routes"]["lane:opus"]["checks"][ACCT_A]["check"], "no")
        self.assertEqual(doc["weekly"][ACCT_A], 100.0)
        self.assertEqual(doc["lanes"]["opus"]["effective"][ACCT_A], 100.0)

    def test_unattributed_needs_an_until(self):
        res, _ = post_reports(rep_refusal(1, self.at, by_reading=False))
        self.assertEqual(res[0][0], "refused")


class TestHoldAgainstAPointBelow(PayBase):
    """A hold and a 2xx's reading a point below 100 (quota-model 64 §5 "Only
    news"; 10 "Rounding": the refusal's header reads 1.00, every 2xx at the
    window's last point 0.99, the poll half a point above the header). The
    poll reads 98. The proxy keeps blocking while its record lives, and its
    record ends only on what the brain publishes (pruneRecords judges the
    published standing) or on a 2xx sent after the refusal (`ended`), so the
    two can differ only if the brain ended a hold on a reading the proxy
    would not. These pin that it does not: the brain's judge compares a
    reading with the use seen at the refusal (seen.used_pct), not with the
    hold's 100, and only a reading read after the refusal counts. A use at
    most a point below the one seen, in the same period, is the seen reading
    again (_use_agrees), so a brain restarted during the hold keeps it on the
    99 its resync brings (64 §8). The proxy's half is reports_test.go
    TestHoldAgainstAPointBelow."""

    def setUp(self):
        self.t0 = time.time() - 1200  # the poll, inside STALE_MAX
        self.r7 = round(self.t0 + 3 * 86400)
        reset(anth_polled(self.t0, weekly=98.0, week_reset=self.r7))
        ex.compute_preferred()
        self.at = self.t0 + 10  # the refusal
        self.refresh = self.at + ex.STALE_MAX / 2  # the proxy's stale_max/2 re-send

    def reported_use(self):
        return ex._reported[ACCT_A]["windows"]["seven_day"]["used_pct"]

    def held(self):
        ex.compute_preferred()
        doc = ex._preferred_payload()
        return ((ACCT_A, "seven_day") in ex._holds,
                doc["accounts"][ACCT_A]["allowance"]["windows"]["seven_day"]["used_pct"])

    def test_the_refusals_header_at_100_then_a_99_holds(self):
        # The proxy's refusal path: the refusal's own header (1.00) is news
        # against the poll's 98, so it is reported, and seen is 100.
        res, _ = post_reports(rep_reading(1, self.at, used=100.0, resets=self.r7),
                              rep_refusal(2, self.at, used=100.0, resets=self.r7))
        self.assertEqual(res, [("applied", None)] * 2)
        res, _ = post_reports(rep_reading(3, self.refresh, used=99.0, resets=self.r7))
        self.assertEqual(res, [("applied", None)])
        self.assertEqual(self.reported_use(), {"v": 100.0, "at": self.refresh},
                         "a point below the reported 100 agrees: the use stands, the age moves")
        self.assertEqual(self.held(), (True, 100.0))

    def test_a_refusal_with_no_header_then_a_99_holds(self):
        # No utilization on the refusal: seen is the 98 the proxy held. A 99
        # is newer use for the brain, but not a lower use than the refusal's.
        post_reports(rep_refusal(1, self.at, used=98.0, resets=self.r7))
        self.assertEqual(self.held(), (True, 100.0))
        res, _ = post_reports(rep_reading(2, self.refresh, used=99.0, resets=self.r7))
        self.assertEqual(res, [("applied", None)])
        self.assertEqual(self.reported_use(), {"v": 99.0, "at": self.refresh})
        self.assertEqual(self.held(), (True, 100.0), "99 is not below the 98 seen at the refusal")

    def restart(self):
        """What a brain restart does to the reports: forget them and every
        hold; the poll's 98 is all it holds until the proxy's resync."""
        reset(anth_polled(self.t0, weekly=98.0, week_reset=self.r7))
        ex.compute_preferred()

    def test_a_restarted_brain_keeps_a_hold_seen_at_100_against_a_99(self):
        # The restart edge (64 §8). The live brain holds the refusal's 100 ...
        post_reports(rep_reading(1, self.at, used=100.0, resets=self.r7),
                     rep_refusal(2, self.at, used=100.0, resets=self.r7))
        self.assertEqual(self.held(), (True, 100.0))
        # ... restarts during the hold, and the proxy's resync brings the
        # refusal (seen 100) and the last reading it reported: the 99 a 2xx
        # answered after the refusal read, once the stale_max/2 re-send made
        # it the last reported (reports.go). The brain holds no 100 now.
        self.restart()
        res, _ = post_reports(rep_refusal(3, self.at, used=100.0, resets=self.r7),
                              rep_reading(4, self.refresh, used=99.0, resets=self.r7))
        self.assertEqual(res, [("applied", None)] * 2)
        self.assertEqual(self.reported_use(), {"v": 99.0, "at": self.refresh},
                         "no 100 held: the 99 is newer use for the brain")
        # A point below the 100 seen, in the same period, is the seen reading
        # again (_use_agrees): the hold stands, and the proxy's record stands
        # on the published reading by the same rule (records_golden.json, "a
        # 99 read after a refusal seen at 100").
        self.assertEqual(self.held(), (True, 100.0))

    def test_a_reading_before_the_refusal_says_nothing(self):
        post_reports(rep_refusal(1, self.at, used=100.0, resets=self.r7))
        post_reports(rep_reading(2, self.at - 5, used=90.0, resets=self.r7))
        self.assertEqual(self.held(), (True, 100.0), "read before the refusal: says nothing")

    def test_more_than_a_point_below_the_100_seen_ends_it(self):
        # The same period, two points below the 100 seen (the poll's 98,
        # read again after the refusal): not the seen reading again, so "open
        # and changed" ends the hold.
        post_reports(rep_refusal(1, self.at, used=100.0, resets=self.r7))
        post_reports(rep_reading(2, self.at + 5, used=98.0, resets=self.r7))
        self.assertEqual(self.held(), (False, 98.0))

    def test_a_99_in_a_new_period_ends_it(self):
        # A point below, but past the reset slack: a new period ends the hold.
        nxt = self.r7 + 7 * 86400
        post_reports(rep_refusal(1, self.at, used=100.0, resets=self.r7))
        post_reports(rep_reading(2, self.at + 5, used=99.0, resets=nxt))
        self.assertEqual(self.held(), (False, 99.0))


class TestPreferredPrices(PayBase):
    """/preferred `prices`: the API list prices a `served` event is priced by,
    published for a console's cost view."""

    def doc(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        ex.compute_preferred()
        # As a subscriber reads it: the document through JSON, NaN refused.
        return json.loads(json.dumps(ex._preferred_payload(), allow_nan=False))

    def test_the_key_is_on_the_document(self):
        d = self.doc()
        self.assertIn("prices", d)
        self.assertEqual(set(d["prices"]), {"models", "long_context"})

    def test_models_are_the_table_aliases_included(self):
        models = self.doc()["prices"]["models"]
        self.assertEqual(models, ex.API_LIST_PRICES)
        for alias, of in (("claude-mythos-5-2", "claude-fable-5-2"),
                          ("claude-mythos-5-1", "claude-fable-5-1"),
                          ("claude-haiku-4-5-20251001", "claude-haiku-4-5"),
                          ("claude-opus-4-5-20251101", "claude-opus-4-5")):
            self.assertEqual(models[alias], models[of], alias)
        self.assertEqual(models["claude-opus-5-5"]["fast"]["output"], 40.0)
        self.assertEqual(set(models["gpt-6-astra"]), {"standard", "fast", "ultrafast"})
        self.assertEqual(set(models["gpt-6-astra"]["standard"]),
                         {"input", "cache_write_30m", "cache_write_12h", "cache_read", "output"})

    def test_fable_5_2_is_priced_as_the_page_lists_it(self):
        # quota-model 10 §9, fetched 2026-11-16: $8 in, $40 out, cache reads at 0.025x input;
        # fast is $16/$80 with the caching multipliers (1.25x, 2x, 0.025x) on top. Without this
        # row, a Fable 5.2 turn is "no list price" and a cost view leaves it out.
        fable = self.doc()["prices"]["models"]["claude-fable-5-2"]
        self.assertEqual(fable["standard"], {"input": 8.0, "cache_write_5m": 10.0,
                                             "cache_write_1h": 16.0, "cache_read": 0.2,
                                             "output": 40.0})
        self.assertEqual(fable["fast"], {"input": 16.0, "cache_write_5m": 20.0,
                                         "cache_write_1h": 32.0, "cache_read": 0.4,
                                         "output": 80.0})
        self.assertNotIn("claude-fable-5-2", ex.API_LONG_CONTEXT)

    def test_long_context_names_its_threshold_and_speeds(self):
        lc = self.doc()["prices"]["long_context"]
        self.assertEqual(set(lc), set(ex.API_LONG_CONTEXT))
        for m, entry in lc.items():
            with self.subTest(model=m):
                self.assertEqual(set(entry), {"over", "speeds"})
                over, speeds = ex.API_LONG_CONTEXT[m]
                self.assertEqual(entry["over"], over)
                self.assertIsInstance(entry["over"], int)
                self.assertEqual(entry["speeds"], speeds)
        self.assertEqual(lc["claude-haiku-5-5"]["over"], 100000)
        self.assertEqual(lc["gpt-6.1-sol"]["speeds"]["ultrafast"]["output"], 90.0)

    def test_follows_the_table_and_never_reaches_it(self):
        """Derived from the tables on every render, never a second copy; a
        reader editing the document leaves the tables alone."""
        row = {k: 2 * v for k, v in ex.API_LIST_PRICES["claude-sonnet-5-5"]["standard"].items()}
        with mock.patch.dict(ex.API_LIST_PRICES, {"claude-sonnet-5-5": {"standard": row}}):
            self.assertEqual(ex._prices_payload()["models"]["claude-sonnet-5-5"]["standard"], row)
        p = ex._prices_payload()
        p["models"]["claude-opus-5-5"]["standard"]["input"] = 0
        p["long_context"]["claude-haiku-5-5"]["speeds"]["standard"]["input"] = 0
        self.assertEqual(ex.API_LIST_PRICES["claude-opus-5-5"]["standard"]["input"], 4.0)
        self.assertEqual(ex.API_LONG_CONTEXT["claude-haiku-5-5"][1]["standard"]["input"], 0.5)

    def test_a_served_price_is_the_published_tables(self):
        """Pricing by the published key gives what _served_price gives, both
        sides of a long-context threshold."""
        prices = self.doc()["prices"]

        def by_doc(model, speed, tokens):
            lc = prices["long_context"].get(model)
            prompt = sum(n for k, n in tokens.items() if k != "output")
            row = (lc["speeds"] if lc and prompt > lc["over"] else prices["models"][model])[speed]
            return sum(n * row[k] for k, n in tokens.items()) / 1e6

        for model, speed, tokens in (
                ("claude-opus-5-5", "fast", {"input": 1000, "cache_read": 5000, "output": 300}),
                ("claude-fable-5-2", "standard", {"input": 40, "cache_read": 90000, "output": 700}),
                ("claude-haiku-5-5", "standard", {"input": 2, "cache_read": 100000, "output": 9}),
                ("gpt-6.1-sol", "ultrafast", {"input": 300000, "output": 10})):
            with self.subTest(model=model):
                self.assertAlmostEqual(ex._served_price(model, speed, tokens, True)[0],
                                       by_doc(model, speed, tokens), places=12)


class TestPublisherRate(unittest.TestCase):
    """At most one document a second (64 §2), and none dropped."""

    def test_a_burst_publishes_at_most_once_an_interval_and_the_last_state(self):
        interval = 0.25
        state = {"n": 0}
        published = []

        def publish():
            published.append((time.monotonic(), state["n"]))
            return True

        stop = threading.Event()
        with mock.patch.object(ex, "EVENTS_MIN_INTERVAL_S", interval), \
                mock.patch.object(ex, "_events_publish", publish):
            t = threading.Thread(target=ex._events_publisher, args=(stop,), daemon=True)
            t.start()
            end = time.monotonic() + 1.0
            while time.monotonic() < end:
                state["n"] += 1
                ex._events_mark()
                time.sleep(0.01)
            final = state["n"]
            time.sleep(interval * 2)
            stop.set()
            ex._events_mark()
            t.join(5)
        self.assertFalse(t.is_alive())
        gaps = [b[0] - a[0] for a, b in zip(published, published[1:])]
        self.assertTrue(all(g >= interval - 0.01 for g in gaps), gaps)
        self.assertLessEqual(len(published), 1.0 / interval + 2)
        self.assertGreaterEqual(len(published), 3)
        self.assertEqual(published[-1][1], final, "the final state is published")


FLEET_CASES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata", "fleet_cases.json")


def fleet_standing(spec, now):
    """A standing (_account_standing's shape, the fields the figure reads)
    from a fleet case's short form."""
    st = {"provider": spec["provider"], "kind": spec.get("kind", "subscription"), "as_of": None,
          "next_poll_at": None, "recheck_epoch": None,
          "allowance": {"open": None, "open_basis": None, "windows": {}, "limits": {}},
          "money": {}, "beside": [], "models": {}}
    if not spec.get("measured", True):
        return st
    as_of = now - spec.get("age", 0)
    st["as_of"] = as_of
    if "recheck_ago" in spec:
        st["recheck_epoch"] = now - spec["recheck_ago"]
    st["allowance"]["open"] = spec.get("gate")

    def resets(w):
        return None if w.get("resets_in") is None else now + w["resets_in"]

    def pct(used):
        return None if used is None else float(used)
    st["allowance"]["windows"] = {
        wid: {"kind": "window", "period": ex._period_label(w["period_s"]), "period_s": w["period_s"],
              "scope": w.get("scope"), "used_pct": pct(w["used"]), "resets_at": resets(w),
              "as_of": as_of, **({"open": w["open"]} if "open" in w else {})}
        for wid, w in (spec.get("windows") or {}).items()}
    st["allowance"]["limits"] = {
        lid: {"kind": "cycle_pool", "period": p["period"], "used_pct": pct(p["used"]),
              "resets_at": resets(p), "as_of": now - p["age"] if "age" in p else as_of}
        for lid, p in (spec.get("pools") or {}).items()}
    return st


def fleet_case_figure(case):
    """The fleet a case's inputs give, through the brain's own route registry."""
    now = case["now"]
    standings = {a: fleet_standing(s, now) for a, s in case["accounts"].items()}
    roster = {a: s["provider"] for a, s in case["accounts"].items()}
    classes = [{"class": c["class"], "request_kind": "chat",
                "members": [dict({"pay": [], "stop_at": None}, **m) for m in c["members"]]}
               for c in case["classes"]]
    routes = ex._routes(classes, case["lanes"], roster)
    models = {f"{c['class']}/{ex._member_id(m)}": m["model"] for c in classes for m in c["members"]}
    # The limits the brain has no window for (67 §9), as POST /reports holds
    # them, judged live by the brain's own rule (_limit_snap).
    with mock.patch.dict(ex._limit_holds, {
            (a, lm["limit"]): {"refused_at": now - lm.get("refused_ago", 60),
                               "until": now + lm["until_in"], "class": lm.get("class", "spent")}
            for a, lms in case.get("limits", {}).items() for lm in lms}, clear=True):
        limits = ex._limit_snap(standings, now)
    health = {a: {"kind": h["kind"], "sign_in": h.get("sign_in")}
              for a, h in case.get("health", {}).items()}
    return ex._fleet_figure(standings, routes, models, case["sizing"], limits, now, ex.STALE_MAX,
                            health)


def fleet_view(windows, providers=None):
    """A sizing view (_sizing_fleet_view's shape) of learned windows:
    {acct: {wid: (mix, {model: size})}}, whole-window dollars."""
    return {"windows": {a: {wid: {"basis": "learned", "mix": mix, "sizes": sizes, "samples": 12}
                            for wid, (mix, sizes) in ws.items()}
                        for a, ws in windows.items()},
            "providers": providers or {"anthropic": {"reference": "claude-opus-5-5", "models": {
                "claude-opus-5-5": {"basis": "reference", "dollars": 1.0}}}}}


class TestFleetCases(unittest.TestCase):
    """quota-model 67 §11, the figure: testdata/fleet_cases.json, §5's worked
    example first."""

    def assertSubset(self, got, want, path="fleet"):
        if isinstance(want, dict):
            self.assertIsInstance(got, dict, path)
            if path.endswith(".refused"):
                # A refusal is asserted whole: one that names no limit has
                # no `limit` key (67 §9).
                self.assertEqual(sorted(got), sorted(want), path)
            for k, v in want.items():
                self.assertIn(k, got, f"{path}.{k}")
                self.assertSubset(got[k], v, f"{path}.{k}")
        elif isinstance(want, list):
            self.assertIsInstance(got, list, path)
            self.assertEqual(len(got), len(want), f"{path}: {got}")
            for i, (g, w) in enumerate(zip(got, want)):
                self.assertSubset(g, w, f"{path}[{i}]")
        elif isinstance(want, float) and not isinstance(want, bool):
            self.assertIsInstance(got, (int, float), path)
            self.assertAlmostEqual(got, want, delta=0.011, msg=path)
        else:
            self.assertEqual(got, want, path)

    def test_cases(self):
        with open(FLEET_CASES) as f:
            doc = json.load(f)
        self.assertTrue(doc["cases"][0]["name"].startswith("67 §5 worked example"))
        for case in doc["cases"]:
            with self.subTest(case=case["name"]):
                self.assertSubset(fleet_case_figure(case), case["expect"])

    def test_no_clock_only_field(self):
        # 67 §8: nothing in fleet follows the clock alone; two renders a
        # minute apart on the same inputs are byte-identical.
        with open(FLEET_CASES) as f:
            case = json.load(f)["cases"][0]
        first = json.dumps(fleet_case_figure(case))
        case["now"] += 60
        for spec in case["accounts"].values():
            spec["age"] = spec.get("age", 0) + 60
            for w in list((spec.get("windows") or {}).values()) + list((spec.get("pools") or {}).values()):
                if w.get("resets_in") is not None:
                    w["resets_in"] -= 60
        # The same readings, their resets the same moments, seen a minute later.
        self.assertEqual(json.dumps(fleet_case_figure(case)), first)

    def test_one_window_is_unit_free(self):
        # On one account and one window room / capacity is (cap - used) / cap
        # at any size (67 §5).
        st = fleet_standing({"provider": "anthropic", "windows": {"seven_day": {
            "period_s": 604800, "used": 35, "resets_in": 1000}}}, 1000.0)
        for sizes in ({"claude-opus-5-5": 1234.5}, {"claude-opus-5-5": 1.0}):
            view = fleet_view({"a": {"seven_day": (sizes["claude-opus-5-5"] / 3, sizes)}})
            got, _, _ = ex._fleet_account("a", st, [{"scope": "Opus", "model": "claude-opus-5-5",
                                                      "cap_of": lambda wid: 80.0, "held": None}],
                                          view, 1000.0, ex.STALE_MAX)
            self.assertAlmostEqual(100 * got[2] / got[1], 100 * (80 - 35) / 80)


class TestFleetAgreesWithTheCheck(unittest.TestCase):
    """67 §6: a binding window is closed for a route when its used reaches
    the route's cap there (100 without one) or the provider's gate shuts it;
    the account then rests (room kept) when every closed window is shorter
    than the main and the main is open, and is refused otherwise. The dial
    check and the figure read the same inputs, so they agree."""

    NOW = 1793740000
    STOPS = [None, {"window": "5h", "level": 80}, {"window": "7d", "level": 80},
             {"window": "Fable", "level": 80}, {"window": "effective", "level": 80},
             {"window": "7d", "level": 0}]

    def one(self, stop, five, seven, fable, gate=None, five_reset=3600):
        """(the dial check, the figure's entry) for one bare-lane-like class
        member on the Fable lane and one account."""
        spec = {"provider": "anthropic", "gate": gate, "windows": {
            "seven_day": {"period_s": 604800, "used": seven, "resets_in": 300000},
            "five_hour": {"period_s": 18000, "used": five, "resets_in": five_reset},
            "scoped:Fable": {"period_s": 604800, "scope": "Fable", "used": fable,
                             "resets_in": 200000}}}
        case = {"now": self.NOW, "lanes": {}, "accounts": {"sub-A": spec},
                "classes": [{"class": "c", "members": [
                    {"kind": "anthropic", "lane": "fable", "model": "claude-fable-5-1",
                     "priority": 1, "stop_at": stop}]}],
                "sizing": fleet_view({"sub-A": {"seven_day": (1000.0, {}),
                                                "scoped:Fable": (400.0, {})}})}
        fleet = fleet_case_figure(case)
        classes = [{"class": "c", "request_kind": "chat",
                    "members": [dict({"pay": []}, **m) for m in case["classes"][0]["members"]]}]
        rt = ex._routes(classes, {}, {"sub-A": "anthropic"})["c/" + ex._member_id(classes[0]["members"][0])]
        check = ex.dial_check(rt, "sub-A", fleet_standing(spec, self.NOW), {}, self.NOW)
        sel = fleet["selections"]["c"]
        return check, dict(sel["accounts"]["sub-A"], resting="sub-A" in sel["resting"])

    def test_the_reviewers_probes_rest(self):
        # A stop_at reached on a window the figure does not count: the check
        # refuses the account, and the figure rests it with its room kept.
        for stop in ({"window": "5h", "level": 80}, {"window": "effective", "level": 80}):
            with self.subTest(stop=stop["window"]):
                check, a = self.one(stop, 85, 40, 40)
                self.assertEqual((check["check"], check["reason"]), ("no", "stop_at"))
                self.assertTrue(a["resting"])
                self.assertIsNone(a["refused"])
                self.assertEqual(a["resting_until"], self.NOW + 3600)
                self.assertGreater(a["room"], 0)

    def test_a_grid(self):
        n = 0
        for stop, five, seven, fable, gate, five_reset in itertools.product(
                self.STOPS, (40, 85, 100), (40, 85, 100), (40, 85, 100), (None, False), (3600, -10)):
            check, a = self.one(stop, five, seven, fable, gate, five_reset)
            label = (stop and f"{stop['window']} {stop['level']}", five, seven, fable, gate, five_reset)
            with self.subTest(label):
                n += 1
                if check["check"] == "no":
                    self.assertTrue(a["room"] == 0 or a["resting"], (check, a))
                else:
                    self.assertEqual(check["check"], "included")
                    self.assertIsNone(a["refused"], (check, a))
                    self.assertFalse(a["resting"], (check, a))
                if a["refused"] is not None or a["resting"]:
                    self.assertEqual(check["check"], "no", a)
        self.assertEqual(n, 6 * 27 * 2 * 2)

    def test_a_passed_reset_reopens_a_stopped_window(self):
        check, a = self.one({"window": "5h", "level": 80}, 85, 40, 40, five_reset=-10)
        self.assertEqual(check["check"], "included")
        self.assertEqual((a["refused"], a["resting"]), (None, False))

    def test_a_stop_at_of_0_holds_across_the_reset(self):
        check, a = self.one({"window": "5h", "level": 0}, 40, 40, 40, five_reset=-10)
        self.assertEqual((check["check"], check["reason"]), ("no", "stop_at"))
        self.assertEqual((a["refused"], a["resting"]), (None, True))

    def test_a_stop_at_on_the_main_and_a_short_window_refuses(self):
        check, a = self.one({"window": "effective", "level": 80}, 85, 85, 40)
        self.assertEqual((check["check"], check["reason"]), ("no", "stop_at"))
        self.assertEqual(a["refused"], {"why": "stop_at", "until": self.NOW + 300000})
        self.assertEqual((a["room"], a["resting"]), (0.0, False))

    def test_a_stop_at_on_a_counted_window_is_not_refused(self):
        check, a = self.one({"window": "7d", "level": 80}, 40, 85, 40)
        self.assertEqual((check["check"], check["reason"]), ("no", "stop_at"))
        self.assertEqual((a["room"], a["refused"], a["resting"]), (0.0, None, False))
        self.assertEqual(a["windows"]["seven_day"]["capacity"], 800.0, "capacity kept at the cap")


WEEK, FIVE = 604800, 18000


def room_case(accounts, classes=(), sizing=None, limits=None, health=None, lanes=None,
              now=1793740000):
    """A fleet case (fleet_case_figure's input) for 68's tests."""
    return {"now": now, "accounts": accounts, "classes": list(classes), "lanes": lanes or {},
            "sizing": sizing or fleet_view({}), "limits": limits or {}, "health": health or {}}


def anth(seven=10, five=None, fable=None, opus=None, gate=None, age=0, seven_in=300000,
         five_in=3600, scoped_in=200000):
    """An Anthropic account's short form: a weekly main, and the 5-hour and
    the scoped Fable and Opus windows when given."""
    w = {"seven_day": {"period_s": WEEK, "used": seven, "resets_in": seven_in}}
    if five is not None:
        w["five_hour"] = {"period_s": FIVE, "used": five, "resets_in": five_in}
    if fable is not None:
        w["scoped:Fable"] = {"period_s": WEEK, "scope": "Fable", "used": fable,
                             "resets_in": scoped_in}
    if opus is not None:
        w["scoped:Opus"] = {"period_s": WEEK, "scope": "Opus", "used": opus,
                            "resets_in": scoped_in}
    return {"provider": "anthropic", "gate": gate, "age": age, "windows": w}


def fable_class(name="c", stop=None, opus_stop=None, opus=False):
    """A class on the Fable lane (priority 1) and, with opus, the Opus lane (2)."""
    members = [{"kind": "anthropic", "lane": "fable", "model": "claude-fable-5-1",
                "priority": 1, "stop_at": stop}]
    if opus:
        members.append({"kind": "anthropic", "lane": "opus", "model": "claude-opus-5-5",
                        "priority": 2, "stop_at": opus_stop})
    return {"class": name, "members": members}


def listed_of(level, acct):
    return next(e for e in level["listed"] if e["account"] == acct)


class TestFleetListedState(unittest.TestCase):
    """68 §3.1: a listed account carries its kind, its own left % on its
    binding counted window and whether it is closed, judged before the size
    by the rules that refuse a counted one."""

    NOW = 1793740000

    def fig(self, accounts, **kw):
        return fleet_case_figure(room_case(accounts, **kw))

    def test_a_spent_account_is_refused_with_its_reset(self):
        f = self.fig({"sub-A": anth(seven=100)}, classes=[fable_class()])
        e = listed_of(f["selections"]["c"], "sub-A")
        self.assertEqual(e, {"account": "sub-A", "why": "seven_day not sized yet (none)",
                             "kind": "learning", "window": "seven_day", "left_pct": 0.0,
                             "refused": {"why": "spent", "until": self.NOW + 300000},
                             "resting": False, "resting_until": None})
        self.assertEqual(f["selections"]["c"]["coverage"]["empty"], 1)
        self.assertEqual(f["accounts"]["sub-A"]["refused"], {"why": "spent", "until": self.NOW + 300000},
                         "the whole proxy's entry carries it too")
        self.assertEqual(f["accounts"]["sub-A"]["kind"], "learning")

    def test_a_stop_at_on_a_counted_window_refuses_a_listed_account(self):
        f = self.fig({"sub-A": anth(seven=85)},
                     classes=[fable_class(stop={"window": "7d", "level": 80})])
        e = listed_of(f["selections"]["c"], "sub-A")
        self.assertEqual(e["refused"], {"why": "stop_at", "until": self.NOW + 300000})
        self.assertEqual((e["window"], e["left_pct"]), ("seven_day", 0.0), "at the route's cap")
        self.assertIsNone(listed_of(f["proxy"], "sub-A")["refused"], "no stop_at at the proxy")

    def test_a_shut_gate_refuses(self):
        f = self.fig({"sub-A": anth(seven=30, gate=False)}, classes=[fable_class()])
        self.assertEqual(listed_of(f["selections"]["c"], "sub-A")["refused"]["why"], "spent")

    def test_a_limit_refuses(self):
        f = self.fig({"sub-A": anth(seven=30)}, classes=[fable_class()],
                     limits={"sub-A": [{"limit": "model:claude-fable-5-1", "class": "not_entitled",
                                        "until_in": 21600}]})
        self.assertEqual(listed_of(f["selections"]["c"], "sub-A")["refused"],
                         {"why": "not_entitled", "limit": "model:claude-fable-5-1",
                          "until": self.NOW + 21600})

    def test_a_resting_account_is_not_empty(self):
        f = self.fig({"sub-A": anth(seven=30, five=100)}, classes=[fable_class()])
        e = listed_of(f["selections"]["c"], "sub-A")
        self.assertEqual((e["refused"], e["resting"], e["resting_until"]),
                         (None, True, self.NOW + 3600))
        self.assertEqual(f["selections"]["c"]["coverage"]["empty"], 0)
        self.assertIsNone(f["selections"]["c"]["back_at"])

    def test_left_pct_by_the_min_rule(self):
        # The scoped window binding, then the main binding.
        f = self.fig({"sub-A": anth(seven=30, fable=90), "sub-B": anth(seven=70, fable=20)},
                     classes=[fable_class()])
        sel = f["selections"]["c"]
        self.assertEqual((listed_of(sel, "sub-A")["window"], listed_of(sel, "sub-A")["left_pct"]),
                         ("scoped:Fable", 10.0))
        self.assertEqual((listed_of(sel, "sub-B")["window"], listed_of(sel, "sub-B")["left_pct"]),
                         ("seven_day", 30.0))
        # The proxy counts the main alone.
        self.assertEqual(listed_of(f["proxy"], "sub-A")["window"], "seven_day")

    def test_unread_and_not_quota_carry_nulls(self):
        f = self.fig({"sub-A": {"provider": "anthropic", "measured": False},
                      "rw": {"provider": "runway", "kind": "api"}})
        for acct, kind in (("sub-A", "unread"), ("rw", "not_quota")):
            e = listed_of(f["proxy"], acct)
            self.assertEqual((e["kind"], e["window"], e["left_pct"], e["refused"], e["resting_until"]),
                             (kind, None, None, None, None))
            self.assertEqual(f["accounts"][acct]["kind"], kind)

    def test_a_counted_account_has_no_kind(self):
        f = self.fig({"sub-A": anth(seven=30)}, classes=[fable_class()],
                     sizing=fleet_view({"sub-A": {"seven_day": (1000.0, {})}}))
        self.assertIsNone(f["accounts"]["sub-A"]["kind"])
        self.assertEqual(f["selections"]["c"]["listed"], [])


class TestFleetCoverage(unittest.TestCase):
    """68 §3.2: coverage, more_at, back_at per selection and on the proxy."""

    NOW = 1793740000

    def every_kind(self):
        # sub-A counted (stale), sub-B learning and empty, sub-C learning
        # and open, sub-D unread; gpt learning (stale, listed); rw an api
        # account.
        return room_case({
            "sub-A": anth(seven=40, fable=20, age=ex.STALE_MAX + 60),
            "sub-B": anth(seven=100, seven_in=200000),
            "sub-C": anth(seven=10),
            "sub-D": {"provider": "anthropic", "measured": False},
            "gpt": {"provider": "gpt", "age": ex.STALE_MAX + 60, "windows": {
                "seven_day": {"period_s": WEEK, "used": 50, "resets_in": 100000}}},
            "rw": {"provider": "runway", "kind": "api"}},
            classes=[{"class": "c", "members": [
                {"kind": "anthropic", "lane": "fable", "model": "claude-fable-5-1", "priority": 1,
                 "stop_at": None},
                {"kind": "provider", "provider": "gpt", "model": "gpt-6.1-sol", "priority": 2,
                 "stop_at": None},
                {"kind": "provider", "provider": "runway", "model": "gpt_image_2", "priority": 3,
                 "stop_at": None}]}],
            sizing=fleet_view({"sub-A": {"seven_day": (1000.0, {}), "scoped:Fable": (400.0, {})}}))

    def test_every_kind_at_once(self):
        f = fleet_case_figure(self.every_kind())
        sel = f["selections"]["c"]
        self.assertEqual(sel["coverage"], {"counted": 1, "learning": 3, "empty": 1, "unread": 1,
                                           "stale": 1, "not_quota": 1})
        reached = {"sub-A", "sub-B", "sub-C", "sub-D", "gpt", "rw"}
        cov = sel["coverage"]
        self.assertEqual(cov["counted"] + cov["learning"] + cov["unread"] + cov["not_quota"],
                         len(reached))
        self.assertEqual(sel["stale"], ["sub-A"], "a stale listed account is not counted as stale")
        self.assertEqual(sel["back_at"], self.NOW + 200000)
        # sub-A's windows both have use; the scoped one resets first.
        self.assertEqual(sel["more_at"], self.NOW + 200000)
        self.assertEqual(sel["accounts"]["sub-A"]["more_at"], self.NOW + 200000)
        # The bare lane reaches the Anthropic accounts alone.
        self.assertEqual(f["selections"]["lane:fable"]["coverage"],
                         {"counted": 1, "learning": 2, "empty": 1, "unread": 1, "stale": 1,
                          "not_quota": 0})
        # The proxy: sub-A's main alone is sized, so it is counted there too.
        self.assertEqual(f["proxy"]["coverage"],
                         {"counted": 1, "learning": 3, "empty": 1, "unread": 1, "stale": 1,
                          "not_quota": 1})
        self.assertEqual(f["proxy"]["more_at"], self.NOW + 300000, "the main alone at the proxy")

    def test_the_counts_follow_no_clock(self):
        case = self.every_kind()
        first = json.dumps(fleet_case_figure(case))
        case["now"] += 60
        for spec in case["accounts"].values():
            spec["age"] = spec.get("age", 0) + 60
            for w in (spec.get("windows") or {}).values():
                if w.get("resets_in") is not None:
                    w["resets_in"] -= 60
        self.assertEqual(json.dumps(fleet_case_figure(case)), first)

    def figure(self, accounts, sized, classes=None, limits=None):
        return fleet_case_figure(room_case(
            accounts, classes=classes or [fable_class()], limits=limits,
            sizing=fleet_view({a: {"seven_day": (1000.0, {}), "scoped:Fable": (400.0, {})}
                               for a in sized})))

    def test_back_at(self):
        # The earliest among the empty.
        f = self.figure({"sub-A": anth(seven=100, seven_in=5000),
                         "sub-B": anth(seven=100, seven_in=9000)}, ())
        self.assertEqual(f["selections"]["c"]["back_at"], self.NOW + 5000)
        # None empty.
        f = self.figure({"sub-A": anth(seven=10)}, ())
        self.assertIsNone(f["selections"]["c"]["back_at"])
        # One empty account's until unknown: a reset the reading did not name.
        f = self.figure({"sub-A": anth(seven=100, seven_in=5000),
                         "sub-B": anth(seven=100, seven_in=None)}, ())
        self.assertEqual(f["selections"]["c"]["coverage"]["empty"], 2)
        self.assertIsNone(f["selections"]["c"]["back_at"])

    def test_more_at(self):
        # The earliest reset with use; a window with no use is left out.
        f = self.figure({"sub-A": anth(seven=0, fable=20, scoped_in=9000, seven_in=5000)}, ["sub-A"])
        self.assertEqual(f["selections"]["c"]["more_at"], self.NOW + 9000)
        # Nothing counted has use.
        f = self.figure({"sub-A": anth(seven=0, fable=0)}, ["sub-A"])
        self.assertIsNone(f["selections"]["c"]["more_at"])
        # A refusal's until: a counted account spent, another open.
        f = self.figure({"sub-A": anth(seven=100, fable=10, seven_in=7000, scoped_in=50000),
                         "sub-B": anth(seven=10, fable=10, seven_in=90000, scoped_in=80000)},
                        ["sub-A", "sub-B"])
        self.assertEqual(f["selections"]["c"]["accounts"]["sub-A"]["more_at"], self.NOW + 7000)
        self.assertEqual(f["selections"]["c"]["more_at"], self.NOW + 7000)
        # A resting account's resting_until is not when the figure rises.
        f = self.figure({"sub-A": anth(seven=10, fable=10, five=100, five_in=600,
                                       seven_in=90000, scoped_in=80000)}, ["sub-A"])
        self.assertEqual(f["selections"]["c"]["accounts"]["sub-A"]["resting_until"], self.NOW + 600)
        self.assertEqual(f["selections"]["c"]["more_at"], self.NOW + 80000)
        self.assertEqual(f["selections"]["c"]["accounts"]["sub-A"]["more_at"], self.NOW + 80000)
        # The binding counted window's reset unknown.
        f = self.figure({"sub-A": anth(seven=10, fable=10, scoped_in=None)}, ["sub-A"])
        self.assertIsNone(f["selections"]["c"]["more_at"])
        # Another window's reset unknown does not matter: it does not bind.
        f = self.figure({"sub-A": anth(seven=10, fable=10, seven_in=None)}, ["sub-A"])
        self.assertEqual(f["selections"]["c"]["more_at"], self.NOW + 200000)

    def test_more_at_names_a_reset_that_raises_the_figure(self):
        # The review's case: the main at 90 % resets in 1d, the scoped window
        # at 95 % in 5d. The scoped window binds (20 left of 400, against 100
        # of 1000), so the main's reset raises nothing.
        f = self.figure({"sub-A": anth(seven=90, fable=95, seven_in=86400, scoped_in=5 * 86400)},
                        ["sub-A"])
        self.assertEqual(f["selections"]["c"]["accounts"]["sub-A"]["more_at"], self.NOW + 5 * 86400)
        self.assertEqual(f["selections"]["c"]["more_at"], self.NOW + 5 * 86400)
        # Tied windows: the later reset (both 0 room at their own 100).
        f = fleet_case_figure(room_case(
            {"sub-A": anth(seven=50, fable=50, seven_in=86400, scoped_in=3 * 86400)},
            classes=[fable_class()],
            sizing=fleet_view({"sub-A": {"seven_day": (400.0, {}), "scoped:Fable": (400.0, {})}})))
        self.assertEqual(f["selections"]["c"]["more_at"], self.NOW + 3 * 86400)
        # A tied window with no use never gives room back by a reset.
        f = fleet_case_figure(room_case(
            {"sub-A": anth(seven=50, fable=0, seven_in=86400)}, classes=[fable_class()],
            sizing=fleet_view({"sub-A": {"seven_day": (400.0, {}), "scoped:Fable": (200.0, {})}})))
        self.assertIsNone(f["selections"]["c"]["more_at"])
        # A counted refusal whose until is unknown.
        f = self.figure({"sub-A": anth(seven=100, fable=10, seven_in=None),
                         "sub-B": anth(seven=10, fable=10)}, ["sub-A", "sub-B"])
        self.assertIsNone(f["selections"]["c"]["more_at"])

    def test_a_stop_at_of_0_on_a_spent_window_has_no_end(self):
        # The review's probe: the level-0 window also spent. Its reset ends
        # nothing: the stop_at holds across it.
        stop = {"window": "7d", "level": 0}
        f = self.figure({"sub-A": anth(seven=100, fable=40)}, ["sub-A"],
                        classes=[fable_class(stop=stop)])
        a = f["selections"]["c"]["accounts"]["sub-A"]
        self.assertEqual((a["refused"], a["more_at"]), ({"why": "spent", "until": None}, None))
        self.assertIsNone(f["selections"]["c"]["more_at"])
        f = self.figure({"sub-A": anth(seven=100, fable=40)}, (), classes=[fable_class(stop=stop)])
        self.assertEqual(listed_of(f["selections"]["c"], "sub-A")["refused"],
                         {"why": "spent", "until": None})
        self.assertIsNone(f["selections"]["c"]["back_at"])
        # A counted window capped at 0 holds the figure at 0 past a refusal
        # that ends sooner.
        f = self.figure({"sub-A": anth(seven=40, fable=100, scoped_in=5000)}, ["sub-A"],
                        classes=[fable_class(stop=stop)])
        a = f["selections"]["c"]["accounts"]["sub-A"]
        self.assertEqual(a["refused"], {"why": "spent", "until": None})
        self.assertIsNone(a["more_at"])
        # The same past a limit's known until.
        f = self.figure({"sub-A": anth(seven=40, fable=40)}, ["sub-A"],
                        classes=[fable_class(stop=stop)],
                        limits={"sub-A": [{"limit": "model:claude-fable-5-1", "class": "spent",
                                           "until_in": 600}]})
        a = f["selections"]["c"]["accounts"]["sub-A"]
        self.assertEqual(a["refused"]["until"], self.NOW + 600)
        self.assertIsNone(a["more_at"])

    def test_times_are_integer_seconds(self):
        lim = {"sub-B": [{"limit": "model:claude-fable-5-1", "class": "not_entitled",
                          "until_in": 21600.25}]}
        f = fleet_case_figure(room_case(
            {"sub-A": anth(seven=40, fable=40, seven_in=1000.5, scoped_in=900.5),
             "sub-B": anth(seven=40), "sub-C": anth(seven=40, five=100, five_in=600.5)},
            classes=[fable_class()], limits=lim,
            sizing=fleet_view({"sub-A": {"seven_day": (1000.0, {}), "scoped:Fable": (400.0, {})}})))
        sel = f["selections"]["c"]
        self.assertEqual((sel["more_at"], type(sel["more_at"])), (self.NOW + 901, int))
        self.assertEqual(sel["accounts"]["sub-A"]["more_at"], self.NOW + 901)
        self.assertEqual((sel["back_at"], type(sel["back_at"])), (self.NOW + 21601, int))
        self.assertEqual(type(listed_of(sel, "sub-B")["refused"]["until"]), int)
        self.assertEqual(listed_of(sel, "sub-C")["resting_until"], self.NOW + 601)
        self.assertEqual(type(listed_of(sel, "sub-C")["resting_until"]), int)
        self.assertEqual([(a["until"], type(a["until"])) for a in sel["asks"]],
                         [(self.NOW + 21601, int)])

    def test_a_stop_at_of_0_has_no_end(self):
        # Held across the reset (§1.9), it ends when the owner lifts it.
        stop = {"window": "7d", "level": 0}
        # Listed: refused, until null, and the selection's back_at null.
        f = self.figure({"sub-A": anth(seven=10)}, (), classes=[fable_class(stop=stop)])
        sel = f["selections"]["c"]
        self.assertEqual(listed_of(sel, "sub-A")["refused"], {"why": "stop_at", "until": None})
        self.assertIsNone(sel["back_at"])
        # Counted, the stop_at on the main and a closed scoped window: refused, until null.
        f = self.figure({"sub-A": anth(seven=10, fable=100)}, ["sub-A"],
                        classes=[fable_class(stop={"window": "effective", "level": 0})])
        a = f["selections"]["c"]["accounts"]["sub-A"]
        self.assertEqual(a["refused"], {"why": "spent", "until": None})
        self.assertIsNone(f["selections"]["c"]["more_at"])
        # Counted, on a counted window only: room 0 by the arithmetic, and
        # more_at null, not the window's reset.
        f = self.figure({"sub-A": anth(seven=10, fable=10)}, ["sub-A"],
                        classes=[fable_class(stop=stop)])
        a = f["selections"]["c"]["accounts"]["sub-A"]
        self.assertEqual((a["refused"], a["room"]), (None, 0.0))
        self.assertIsNone(a["more_at"])
        # A short window at level 0 rests with no known end.
        f = self.figure({"sub-A": anth(seven=10, fable=10, five=10)}, ["sub-A"],
                        classes=[fable_class(stop={"window": "5h", "level": 0})])
        a = f["selections"]["c"]["accounts"]["sub-A"]
        self.assertEqual((a["refused"], a["resting_until"]), (None, None))
        self.assertIn("sub-A", f["selections"]["c"]["resting"])


class TestFleetFallsThrough(unittest.TestCase):
    """68 §3.4: an account is valued by the highest-priority member whose
    route the dial check lets serve it now, windows and limits alike, and is
    refused only when every member that reaches it is refused."""

    NOW = 1793740000

    def two(self, fable, opus, seven=10, sized=True, limits=None, stop=None, opus_stop=None,
            fable_in=200000, opus_in=200000, extra=None):
        spec = anth(seven=seven, fable=fable)
        spec["windows"].update(extra or {})
        spec["windows"]["scoped:Fable"]["resets_in"] = fable_in
        spec["windows"]["scoped:Opus"] = {"period_s": WEEK, "scope": "Opus", "used": opus,
                                          "resets_in": opus_in}
        sizing = fleet_view({"sub-A": {"seven_day": (1000.0, {}), "scoped:Fable": (400.0, {}),
                                       "scoped:Opus": (600.0, {})}} if sized else {})
        case = room_case({"sub-A": spec}, classes=[fable_class(stop=stop, opus_stop=opus_stop,
                                                               opus=True)],
                         sizing=sizing, limits=limits)
        f = fleet_case_figure(case)
        classes = [{"class": "c", "request_kind": "chat",
                    "members": [dict({"pay": []}, **m) for m in case["classes"][0]["members"]]}]
        routes = ex._routes(classes, {}, {"sub-A": "anthropic"})
        st = fleet_standing(spec, self.NOW)
        checks = [ex.dial_check(routes[f"c/{ex._member_id(m)}"], "sub-A", st, {}, self.NOW)
                  for m in classes[0]["members"]]
        sel = f["selections"]["c"]
        got = sel["accounts"].get("sub-A") or listed_of(sel, "sub-A")
        return checks, got, sel

    def test_a_closed_scoped_window_falls_through_to_the_next_member(self):
        checks, a, _ = self.two(fable=100, opus=20)
        self.assertEqual([c["check"] for c in checks], ["no", "included"])
        self.assertEqual((a["refused"], a["member"], a["model"]),
                         (None, "lane:opus", "claude-opus-5-5"))
        self.assertEqual(set(a["windows"]), {"seven_day", "scoped:Opus"}, "the free member's windows")
        self.assertGreater(a["room"], 0)
        # Listed, it is not empty.
        _, e, sel = self.two(fable=100, opus=20, sized=False)
        self.assertEqual((e["refused"], e["window"], e["left_pct"]), (None, "scoped:Opus", 80.0))
        self.assertEqual(sel["coverage"]["empty"], 0)

    def test_closed_for_both_is_refused_until_the_first_frees(self):
        checks, a, _ = self.two(fable=100, opus=100, fable_in=90000, opus_in=40000)
        self.assertEqual([c["check"] for c in checks], ["no", "no"])
        self.assertEqual(a["refused"], {"why": "spent", "until": self.NOW + 40000})
        self.assertEqual((a["member"], a["room"]), ("lane:fable", 0.0), "the highest-priority member")
        _, e, sel = self.two(fable=100, opus=100, fable_in=90000, opus_in=40000, sized=False)
        self.assertEqual(e["refused"], {"why": "spent", "until": self.NOW + 40000})
        self.assertEqual(sel["back_at"], self.NOW + 40000)
        # One member's end unknown: until null.
        _, a, _ = self.two(fable=100, opus=100, fable_in=None, opus_in=40000)
        self.assertEqual(a["refused"], {"why": "spent", "until": None})

    def test_a_limit_on_one_member_and_a_window_on_the_other(self):
        lim = {"sub-A": [{"limit": "model:claude-opus-5-5", "class": "spent", "until_in": 3000}]}
        checks, a, _ = self.two(fable=100, opus=10, limits=lim, fable_in=90000)
        self.assertEqual(checks[0]["check"], "no")
        self.assertEqual(a["refused"], {"why": "spent", "limit": "model:claude-opus-5-5",
                                        "until": self.NOW + 3000}, "neither can serve")
        # The limit on the first member, the second free: valued at the second.
        lim = {"sub-A": [{"limit": "model:claude-fable-5-1", "class": "spent", "until_in": 3000}]}
        _, a, _ = self.two(fable=10, opus=10, limits=lim)
        self.assertEqual((a["refused"], a["member"]), (None, "lane:opus"))

    def test_a_resting_member_beside_a_refused_one_rests_the_account(self):
        # The review's probe: 7d 40, 5h 100 (back in 1h), Fable 40, Opus 100.
        # The Fable member rests, the Opus member is refused: the account
        # rests, room kept, as it did before 68 and as Fable alone does.
        five = {"five_hour": {"period_s": FIVE, "used": 100, "resets_in": 3600}}

        def case(opus, limits=None, opus_in=200000, extra=five):
            checks, a, sel = self.two(fable=40, opus=opus, seven=40, limits=limits,
                                      opus_in=opus_in, extra=extra)
            return a, sel
        a, sel = case(100)
        self.assertEqual((a["member"], a["refused"], a["room"], a["resting_until"]),
                         ("lane:fable", None, 240.0, self.NOW + 3600))
        self.assertIn("sub-A", sel["resting"])
        # The same with a spent hold on the Opus model instead of its window.
        lim = {"sub-A": [{"limit": "model:claude-opus-5-5", "class": "spent", "until_in": 7200}]}
        a, sel = case(40, limits=lim)
        self.assertEqual((a["refused"], a["room"], a["resting_until"]), (None, 240.0, self.NOW + 3600))
        # Back when the first member is: a short Fable window rests the Fable
        # member for 1h, and the Opus window frees the Opus member sooner.
        fable5 = {"scoped:Fable5h": {"period_s": FIVE, "scope": "Fable", "used": 100,
                                     "resets_in": 3600}}
        a, _ = case(100, opus_in=1800, extra=fable5)
        self.assertEqual((a["member"], a["refused"], a["resting_until"]),
                         ("lane:fable", None, self.NOW + 1800))
        # An unknown end among them: null.
        a, _ = case(100, opus_in=None, extra=fable5)
        self.assertEqual((a["refused"], a["resting_until"]), (None, None))
        # Listed, it is resting, not empty.
        _, e, sel = self.two(fable=40, opus=100, seven=40, sized=False, extra=five)
        self.assertEqual((e["refused"], e["resting"], e["resting_until"]),
                         (None, True, self.NOW + 3600))
        self.assertEqual(sel["coverage"]["empty"], 0)

    def test_the_grid_agrees_with_the_checks(self):
        n = 0
        stops = (None, {"window": "Fable", "level": 80}, {"window": "7d", "level": 80})
        for fable, opus, seven, stop, opus_stop, sized in itertools.product(
                (40, 85, 100), (40, 85, 100), (40, 85, 100), stops, stops, (True, False)):
            checks, a, sel = self.two(fable, opus, seven, sized, stop=stop, opus_stop=opus_stop)
            label = (fable, opus, seven, stop, opus_stop, sized)
            with self.subTest(label):
                n += 1
                verdicts = [c["check"] for c in checks]
                if "included" in verdicts:
                    self.assertIsNone(a["refused"], (checks, a))
                    if sized:
                        want = ("lane:fable", "lane:opus")[verdicts.index("included")]
                        self.assertEqual(a["member"], want)
                        self.assertNotIn("sub-A", sel["resting"])
                else:
                    self.assertEqual(verdicts, ["no", "no"])
                    for c in checks:
                        self.assertIn(c["reason"], ("spent", "stop_at"))
                    if sized:
                        self.assertTrue(a["refused"] is not None or a["room"] == 0, (checks, a))
                    else:
                        # No short window here: a listed account is refused
                        # exactly when every member's check says no.
                        self.assertIsNotNone(a["refused"], (checks, a))
        self.assertEqual(n, 27 * 9 * 2)


class FakeLlamaRouter:
    """b12110 GET /models before first load: empty args, merged INI preset.

    Every request is recorded: a poll must make exactly one GET /models, never
    /props or a POST that could load a model.
    """

    def __init__(self, held):
        self.held = held  # {preset id: configured ctx-size or None}
        self.requests = []
        fake = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def send(self, code, body):
                b = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def do_GET(self):
                fake.requests.append(("GET", self.path))
                if self.path != "/models":
                    return self.send(404, {"error": "not found"})
                self.send(200, {"data": [{"id": n, "aliases": [], "object": "model",
                    "status": {"value": "unloaded", "args": [], "preset":
                        f"[{n}]\nmodel = /models/{n}.gguf\n" + (f"ctx-size = {ctx}\n" if ctx else "")}}
                    for n, ctx in fake.held.items()], "object": "list"})

            def do_POST(self):
                fake.requests.append(("POST", self.path))
                self.send(400, {"error": "a poll must never POST"})

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


LOCAL_HELD = {"local-gemma4-12b": 65536, "local-gpt-oss-20b": 65536,
              "local-qwen3-coder-30b": 131072}


class TestLocalPoller(unittest.TestCase):
    """poll_local checks the router's non-loading inventory and merged presets."""

    def setUp(self):
        self.addCleanup(setattr, plocal, "LOCAL_UPSTREAM", plocal.LOCAL_UPSTREAM)
        self.addCleanup(setattr, ex, "_classes", ex._classes)
        ex._classes = load_classes([local_class_cfg()])

    def serve(self, held):
        o = FakeLlamaRouter(held)
        self.addCleanup(o.close)
        plocal.LOCAL_UPSTREAM = o.url
        return o

    def test_unset_disabled(self):
        plocal.LOCAL_UPSTREAM = ""
        self.assertFalse(ex.PROVIDERS["local"]["enabled"]())
        r = plocal.poll_local()
        self.assertEqual((r["ok"], r["reading"]), (0, None))
        self.assertEqual(r["err"], "LOCAL_UPSTREAM is not set")

    def test_ok(self):
        server = self.serve(dict(LOCAL_HELD))
        self.assertTrue(ex.PROVIDERS["local"]["enabled"]())
        r = plocal.poll_local()
        self.assertEqual((r["ok"], r["err"], r["name"], r["provider"]), (1, "", "local", "local"))
        self.assertEqual(server.requests, [("GET", "/models")])
        rd = r["reading"]
        self.assertEqual(rd["source"], "GET /models")
        self.assertEqual((rd["kind"], rd["plan"]["label"]), ("subscription", "local model server"))
        allowance, credits = rd["pots"]
        self.assertEqual((allowance["open"], allowance["limits"]), (True, []))
        self.assertEqual((credits["id"], credits["open"]), ("credits", False))

    def test_refused(self):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()  # nothing listens: connection refused
        plocal.LOCAL_UPSTREAM = f"http://127.0.0.1:{port}"
        r = plocal.poll_local()
        self.assertEqual((r["ok"], r["reading"]), (0, None))
        self.assertEqual(r["err"], f"http://127.0.0.1:{port}: connection refused — "
                                   "check that llama-server is running there")

    def test_mismatch_names_model_and_both_numbers(self):
        held = dict(LOCAL_HELD)
        held["local-qwen3-coder-30b"] = 32768
        self.serve(held)
        r = plocal.poll_local()
        self.assertEqual((r["ok"], r["reading"]), (0, None))
        self.assertEqual(r["err"], "local-qwen3-coder-30b: llama-server configures context 32768, "
                                   "the class declares context_window 131072")

    def test_ctx_size_unset_is_a_mismatch(self):
        held = dict(LOCAL_HELD)
        held["local-gemma4-12b"] = None
        self.serve(held)
        r = plocal.poll_local()
        self.assertEqual(r["err"], "local-gemma4-12b: llama-server configures context unset, "
                                   "the class declares context_window 65536")

    def test_missing_model(self):
        held = dict(LOCAL_HELD)
        del held["local-gpt-oss-20b"]
        self.serve(held)
        r = plocal.poll_local()
        self.assertEqual((r["ok"], r["reading"]), (0, None))
        self.assertEqual(r["err"], "local-gpt-oss-20b is declared but llama-server does not hold it")

    def test_no_class_declared_polls_models_only(self):
        ex._classes = []
        self.serve({})
        self.assertEqual(plocal.poll_local()["ok"], 1)


class TestLocalRouterContext(unittest.TestCase):
    def test_unloaded_preset_before_first_spawn(self):
        self.assertEqual(plocal._local_context({"status": {"value": "unloaded", "args": [],
            "preset": "[local]\nctx-size = 65536\nparallel = 1\n"}}),
            (65536, "configures context"))

    def test_loaded_actual_slot_wins_over_configuration(self):
        model = {"id": "local", "meta": {"n_ctx": 32768},
                 "status": {"preset": "[local]\nctx-size = 65536\n"}}
        self.assertEqual(plocal._local_check({"data": [model]}, {"local": 65536}),
            "local: llama-server serves context 32768, the class declares context_window 65536")

    def test_args_after_unload(self):
        self.assertEqual(plocal._local_context({"status": {"args": ["llama-server", "--ctx-size", "65536"]}}),
                         (65536, "configures context"))

    def test_training_window_is_not_configured_context(self):
        self.assertEqual(plocal._local_context({"context_length": 131072}), (None, "configures context"))

    def test_auto_context_is_not_verified(self):
        self.assertEqual(plocal._local_context({"status": {"preset": "[local]\nctx-size = 0\n"}}),
                         (None, "configures context"))

    def test_aliases_are_router_names_too(self):
        model = {"id": "canonical", "aliases": ["local"],
                 "status": {"preset": "[canonical]\nctx-size = 65536\n"}}
        self.assertEqual(plocal._local_check({"data": [model]}, {"local": 65536}), "")


class TestKeyedPages(HTTPBase):
    """/auth is keyed like the endpoints it drives. A request with the switch
    key (a gate in front of the brain injects it) gets the page; one without
    gets the key shell, which asks once, keeps the key in sessionStorage and
    loads the page with it. The read-only surfaces stay open."""

    def fetch(self, path, key=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}")
        if key:
            req.add_header("X-Switch-Key", key)
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, r.headers.get("Content-Type", ""), r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.headers.get("Content-Type", ""), e.read().decode()

    def test_without_the_key_the_shell_asks_for_it(self):
        for key in (None, "wrong"):
            with self.subTest(key=key):
                code, ctype, body = self.fetch("/auth", key)
                self.assertEqual(code, 401)
                self.assertIn("text/html", ctype)
                self.assertIn("This page needs the switch key.", body)
                self.assertIn("sessionStorage", body)
                self.assertIn("keyedFetch(location.pathname + location.search)", body)
                self.assertNotIn("Provider accounts", body)

    def test_with_the_key_the_page_is_served_and_sends_it_on_every_request(self):
        code, ctype, body = self.fetch("/auth", "test-switch-key")
        self.assertEqual(code, 200)
        self.assertIn("Provider accounts", body)
        self.assertIn('headers["X-Switch-Key"] = key', body)
        self.assertIn('keyedFetch("/auth/providers"', body)
        self.assertNotIn('await fetch("/', body, "every page request goes through keyedFetch")
        self.assertNotIn("test-switch-key", body)
        self.assertNotIn("const SWITCH_KEY_SLOT", body, "the page is written into the shell's window")

    def test_503_when_no_key_is_mounted(self):
        orig = ex.SWITCH_KEY
        ex.SWITCH_KEY = ""
        self.addCleanup(setattr, ex, "SWITCH_KEY", orig)
        self.assertEqual(self.fetch("/auth", "test-switch-key")[0], 503)

    def test_the_read_surfaces_stay_open(self):
        reset(acct(ACCT_A, 30))
        for path in ("/preferred", "/metrics", "/sizing"):
            with self.subTest(path=path):
                self.assertEqual(self.fetch(path)[0], 200)


def runway_raw(balance=500, daily=3, max_daily=200, concurrent=2, monthly=10000):
    """A GET /v1/organization body (13 §2), in a recorded shape (tier 1,
    gpt_image_2 = {2, 200}) but for the balance and the count. None drops a
    field: unknown, never 0."""
    tier = {"models": {"gpt_image_2": {}}}
    if monthly is not None:
        tier["maxMonthlyCreditSpend"] = monthly
    if max_daily is not None:
        tier["models"]["gpt_image_2"]["maxDailyGenerations"] = max_daily
    if concurrent is not None:
        tier["models"]["gpt_image_2"]["maxConcurrentGenerations"] = concurrent
    raw = {"tier": tier, "usage": {"models": {}}}
    if balance is not None:
        raw["creditBalance"] = balance
    if daily is not None:
        raw["usage"]["models"]["gpt_image_2"] = {"dailyGenerations": daily}
    return raw


def runway_result(ok=1, good_age=0, **raw):
    """A Runway poll result shaped like poll_runway()'s output."""
    now = time.time()
    return read({"name": "runway", "kind": "provider", "provider": "runway", "ok": ok,
                 "ts": now, "good_ts": now - good_age, "stale": 0, "http": 0, "err": "",
                 "retry_after": None, "raw": runway_raw(**raw)})


class TestTokenEndpoint(HTTPBase):
    def setUp(self):
        # A is coolest on every lane EXCEPT fable (its Fable cap is maxed), so bootstrap
        # seeds fable onto B and every other lane (incl. default) onto A.
        reset(acct(ACCT_A, 30, caps={"Fable": 96}), acct(ACCT_B, 45))
        ex.compute_preferred()
        age_lanes()
        ex.compute_preferred()  # fable -> ACCT_B, default stays on ACCT_A

    def test_account_preferred_resolves_to_default_lane(self):
        code, d = self.get("/token?account=preferred", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["account"], ACCT_A,
                         "'preferred' = the default lane, not any split lane")

    def test_token_requires_key(self):
        code, _ = self.get("/token?account=preferred")
        self.assertEqual(code, 401)


class _Resp:
    """A stub Anthropic response for ex.urlopen."""

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


def _http_error(code):
    return urllib.error.HTTPError(psub.TOKEN_URL, code, "refused", hdrs=None, fp=None)


class TestRefreshLog(HTTPBase):
    """Every access-token refresh revokes the copies dispensed before it, so the
    brain logs each one with who asked: one line, the new token's fingerprint
    the switch proxy's fp() of the same token."""

    def setUp(self):
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        self.logged = []
        self.addCleanup(setattr, ex, "log", ex.log)
        ex.log = self.logged.append
        self.calls = []
        orig = ex.urlopen
        self.addCleanup(setattr, ex, "urlopen", orig)
        ex.urlopen = self.stub

    def stub(self, req, timeout=0):
        self.calls.append(req.full_url)
        if req.full_url == psub.TOKEN_URL:
            return _Resp({"access_token": "fresh-" + str(len(self.calls)), "expires_in": 28800,
                          "refresh_token": "rotated"})
        if req.full_url.endswith("/api/oauth/profile"):
            return _Resp({"account": {"email": ACCT_A}, "organization": {"rate_limit_tier": "max"}})
        return _Resp({"five_hour": {"utilization": 1.0}, "seven_day": {"utilization": 2.0},
                      "limits": []})

    def refreshed(self):
        return [line for line in self.logged if "token refreshed" in line]

    def test_the_fingerprint_is_the_proxys(self):
        # main.go fp(): hex(sha256(token))[:8], "(none)" for no token.
        self.assertEqual(psub._token_fp("sk-ant-oat01-example"),
                         hashlib.sha256(b"sk-ant-oat01-example").hexdigest()[:8])
        self.assertEqual(psub._token_fp(None), "(none)")
        self.assertEqual(psub._token_fp(""), "(none)")

    def test_the_quota_poll_says_poll(self):
        psub._creds[ACCT_A]["expires_at"] = time.time() + 30
        self.assertEqual(psub.poll_account(ACCT_A)["ok"], 1)
        [line] = self.refreshed()
        self.assertTrue(line.startswith(f"{ACCT_A}: token refreshed (poll) — old had 0m left, "),
                        line)
        self.assertEqual(psub._last_refresh[ACCT_A]["why"], "poll")

    def test_the_proxys_fetch_says_token(self):
        psub._creds[ACCT_A]["expires_at"] = time.time() + 30
        code, d = self.get(f"/token?account={ACCT_A}", key="test-switch-key")
        self.assertEqual((code, d["access_token"]), (200, "fresh-1"))
        [line] = self.refreshed()
        self.assertIn(f"{ACCT_A}: token refreshed (token) — ", line)
        self.assertTrue(line.endswith(", fp " + hashlib.sha256(b"fresh-1").hexdigest()[:8]), line)

    def test_no_old_token_has_no_time_left(self):
        psub._creds[ACCT_A].update(access_token=None, expires_at=0)
        psub._ensure_token(ACCT_A, why="token")
        [line] = self.refreshed()
        self.assertIn("old had 0m left", line)
        self.assertIsNone(psub._last_refresh[ACCT_A]["old_left_s"])

    def test_a_token_with_time_left_is_not_refreshed_or_logged(self):
        self.assertEqual(psub._ensure_token(ACCT_A, why="token"), "tok-" + ACCT_A)
        self.assertEqual((self.calls, self.refreshed()), ([], []))
        self.assertNotIn(ACCT_A, psub._last_refresh)


def _fp(token):
    return hashlib.sha256(token.encode()).hexdigest()[:8]


class TestTokenRefusedFp(HTTPBase):
    """GET /token?refused_fp=: the proxy saying Anthropic refused its copy of
    the account's token. A stale copy gets the current token; a refused
    current token is checked once and refreshed only if the check is refused
    too, at most once a minute (a blind refresh revokes the cloud sessions'
    copies)."""

    def setUp(self):
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))
        self.logged = []
        self.addCleanup(setattr, ex, "log", ex.log)
        ex.log = self.logged.append
        self.calls, self.bearers = [], []
        self.usage = None  # what the usage check answers: None = 200, else raised
        orig = ex.urlopen
        self.addCleanup(setattr, ex, "urlopen", orig)
        ex.urlopen = self.stub
        with ex._poll_health_lock:
            ex._poll_schedule[ACCT_A] = time.time() + 900
        self.addCleanup(ex._poll_schedule.clear)

    def stub(self, req, timeout=0):
        if req.full_url == psub.TOKEN_URL:
            self.calls.append("refresh")
            if isinstance(self.refresh_answer, BaseException):
                raise self.refresh_answer
            return _Resp(self.refresh_answer)
        self.calls.append(urllib.parse.urlparse(req.full_url).path)
        self.bearers.append(req.get_header("Authorization"))
        if self.usage is not None:
            raise self.usage
        return _Resp({"five_hour": {"utilization": 1.0}, "seven_day": {"utilization": 2.0}})

    refresh_answer = {"access_token": "fresh", "expires_in": 28800, "refresh_token": "rotated"}

    def token(self, refused_fp=None, account=ACCT_A):
        q = f"/token?account={account}" + (f"&refused_fp={refused_fp}" if refused_fp else "")
        return self.get(q, key="test-switch-key")

    def woken(self):
        with ex._poll_health_lock:
            return ex._poll_wakeup.is_set() and ACCT_A not in ex._poll_schedule

    def test_without_it_token_is_unchanged(self):
        code, d = self.token()
        self.assertEqual((code, d["access_token"]), (200, "tok-" + ACCT_A))
        self.assertEqual((self.calls, self.logged, psub._proxy_refusals), ([], [], {}))
        self.assertFalse(self.woken())

    def test_a_stale_copy_is_recorded_and_the_current_token_dispensed(self):
        code, d = self.token(_fp("the-revoked-copy"))
        self.assertEqual((code, d["access_token"]), (200, "tok-" + ACCT_A))
        self.assertEqual(self.calls, [], "no check, no refresh")
        rec = psub._proxy_refusals[ACCT_A]
        self.assertEqual((rec["fp"], rec["matched_current"]), (_fp("the-revoked-copy"), False))
        self.assertAlmostEqual(rec["at"], time.time(), delta=5)
        self.assertEqual(self.logged, [
            f"{ACCT_A}: proxy refused fp {_fp('the-revoked-copy')} — a stale copy; "
            f"dispensing the current token, fp {_fp('tok-' + ACCT_A)}"])
        self.assertFalse(self.woken())

    def test_a_refused_current_token_that_still_works_is_not_refreshed(self):
        code, d = self.token(_fp("tok-" + ACCT_A))
        self.assertEqual((code, d["access_token"]), (200, "tok-" + ACCT_A))
        self.assertEqual(self.calls, ["/api/oauth/usage"])
        self.assertEqual(self.bearers, ["Bearer tok-" + ACCT_A], "the check uses the current token")
        self.assertTrue(psub._proxy_refusals[ACCT_A]["matched_current"])
        self.assertEqual(self.logged, [f"{ACCT_A}: proxy refused fp {_fp('tok-' + ACCT_A)} — "
                                       "the current token; usage check HTTP 200: not refreshing"])
        self.assertTrue(self.woken(), "the poll re-checks at once")

    def test_a_check_that_cannot_tell_does_not_refresh(self):
        for answer, said in ((_http_error(429), "HTTP 429"),
                             (urllib.error.URLError("down"), "URLError")):
            with self.subTest(said):
                self.usage = answer
                code, d = self.token(_fp("tok-" + ACCT_A))
                self.assertEqual((code, d["access_token"]), (200, "tok-" + ACCT_A))
                self.assertNotIn("refresh", self.calls)
                self.assertIn(f"usage check {said}: not refreshing", self.logged[-1])

    def test_a_dead_current_token_is_refreshed_at_most_once_a_minute(self):
        self.usage = _http_error(401)
        code, d = self.token(_fp("tok-" + ACCT_A))
        self.assertEqual((code, d["access_token"]), (200, "fresh"))
        self.assertEqual(self.calls, ["/api/oauth/usage", "refresh"])
        self.assertEqual(self.logged[0], f"{ACCT_A}: proxy refused fp {_fp('tok-' + ACCT_A)} — "
                                         "the current token; usage check HTTP 401: refreshing")
        self.assertIn(f"{ACCT_A}: token refreshed (forced: proxy refused) — ", self.logged[1])
        self.assertEqual(psub._last_refresh[ACCT_A]["why"], "forced: proxy refused")
        self.assertTrue(self.woken())
        # The new token refused too, inside the minute: checked, not refreshed.
        self.calls.clear()
        ex._poll_wakeup.clear()
        code, d = self.token(_fp("fresh"))
        self.assertEqual((code, d["access_token"]), (200, "fresh"))
        self.assertEqual(self.calls, ["/api/oauth/usage"])
        self.assertRegex(self.logged[-1], r"usage check HTTP 401: refreshed \d+s ago, not again yet$")
        self.assertTrue(ex._poll_wakeup.is_set(), "the poll is woken either way")
        # A minute on, it may refresh again.
        with psub._proxy_refusals_lock:
            psub._forced_refresh_at[ACCT_A] -= psub.FORCED_REFRESH_MIN_S
        self.calls.clear()
        self.token(_fp("fresh"))
        self.assertEqual(self.calls, ["/api/oauth/usage", "refresh"])

    def test_a_dead_grant_answers_502_and_wakes_the_poll(self):
        self.usage = _http_error(401)
        self.refresh_answer = _http_error(400)
        code, d = self.token(_fp("tok-" + ACCT_A))
        self.assertEqual(code, 502)
        self.assertNotIn("tok-", json.dumps(d))
        self.assertTrue(self.woken(), "the poll shows the dead grant as auth_dead")

    def test_another_accounts_refusal_is_its_own(self):
        code, d = self.token(_fp("tok-" + ACCT_B), account=ACCT_B)
        self.assertEqual((code, d["access_token"]), (200, "tok-" + ACCT_B))
        self.assertEqual(set(psub._proxy_refusals), {ACCT_B})


class TestOpenAITokenEndpoint(HTTPBase):
    """#33 — the guarded codex-bearer dispenser (/openai-token)."""

    def setUp(self):
        clear_openai_creds()
        self.addCleanup(clear_openai_creds)

    def test_requires_key(self):
        seed_openai_creds()
        code, _ = self.get("/openai-token")
        self.assertEqual(code, 401)

    def test_no_creds_file_503(self):
        code, d = self.get("/openai-token", key="test-switch-key")
        self.assertEqual(code, 503)
        self.assertIn("no codex creds", d["error"])

    def test_switch_key_unset_503(self):
        seed_openai_creds()
        orig = ex.SWITCH_KEY
        ex.SWITCH_KEY = ""
        self.addCleanup(setattr, ex, "SWITCH_KEY", orig)
        code, _ = self.get("/openai-token", key="test-switch-key")
        self.assertEqual(code, 503)

    def test_dispenses_account_id_token_expiry(self):
        seeded = seed_openai_creds()
        code, d = self.get("/openai-token", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["access_token"], "at-test-1")
        self.assertEqual(d["account_id"], "acct-uuid-test",
                         "account_id is a mandatory codex request header — the key name matters")
        self.assertAlmostEqual(d["expires_at"], seeded["expires_at"], delta=1)
        self.assertNotIn("account", d, "don't blind-copy _token's key name")

    def test_dead_chain_adopts_reseeded_file(self):
        # In-memory copy holds a dead RT + expired AT; the on-disk file has been
        # re-seeded externally. One refresh-401 must force
        # the re-read and dispense from the adopted chain — same shape poll_gpt
        # already proves for the poller path.
        seed_openai_creds(refresh_token="rt-new", access_token="at-new",
                          expires_at=time.time() + 999999)
        pcodex._openai_pool = {"gpt": {"refresh_token": "rt-dead", "access_token": "at-dead",
                                   "expires_at": 0, "account_id": "acct-uuid-test"}}
        pcodex._openai_pool_shape = "legacy"
        pin_openai_cache_mtime()  # defeat the mtime auto-adopt; force the 401 path
        orig = pcodex._oauth_refresh

        def fake_refresh(creds, url, cid, persist):
            if creds.get("refresh_token") == "rt-dead":
                raise urllib.error.HTTPError(url, 401, "already used", {}, None)
            return orig(creds, url, cid, persist)

        pcodex._oauth_refresh = fake_refresh
        self.addCleanup(setattr, pcodex, "_oauth_refresh", orig)
        code, d = self.get("/openai-token", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["access_token"], "at-new")

    def test_refresh_failure_502(self):
        seed_openai_creds(expires_at=0)  # forces a refresh attempt

        def boom(creds, url, cid, persist):
            raise urllib.error.HTTPError(url, 500, "boom", {}, None)

        orig = pcodex._oauth_refresh
        pcodex._oauth_refresh = boom
        self.addCleanup(setattr, pcodex, "_oauth_refresh", orig)
        code, d = self.get("/openai-token", key="test-switch-key")
        self.assertEqual(code, 502)


class TestProviderRegistry(unittest.TestCase):
    """#32 — the module-level PROVIDERS map is the poll loop's only dispatch
    source: real poller functions, and enabled-callables that stay live."""

    def test_registry_members(self):
        # #72: anthropic joins as an auth FACADE — registered, never polled here.
        self.assertEqual(set(ex.PROVIDERS), {"kimi", "gpt", "anthropic", "local", "anthropic-api"})

    def test_poll_dispatch_identity(self):
        self.assertIs(ex.PROVIDERS["kimi"]["poll"], pkimi.poll_kimi)
        self.assertIs(ex.PROVIDERS["gpt"]["poll"], pcodex.poll_gpt)

    def test_anthropic_facade_never_dispatches(self):
        # poll=None would be a None(...) call in the poller thread; the dispatch
        # builder and the pool-id set must both skip the facade row.
        self.assertIsNone(ex.PROVIDERS["anthropic"]["poll"])
        self.assertTrue(ex.PROVIDERS["anthropic"]["enabled"]())
        self.assertNotIn("anthropic", ex._provider_poll_targets())
        self.assertEqual(set(ex._pollable_provider_ids()), {"kimi", "gpt", "local"})

    def test_kimi_enabled_is_live(self):
        old = pkimi.KIMI_API_KEY
        try:
            pkimi.KIMI_API_KEY = "sk-kimi-testdeadbeef"
            self.assertTrue(ex.PROVIDERS["kimi"]["enabled"]())
            pkimi.KIMI_API_KEY = ""
            self.assertFalse(ex.PROVIDERS["kimi"]["enabled"]())
        finally:
            pkimi.KIMI_API_KEY = old

    def test_gpt_enabled_is_live(self):
        path = pcodex.OPENAI_CODEX_CREDS_FILE
        self.assertFalse(ex.PROVIDERS["gpt"]["enabled"]())
        try:
            with open(path, "w") as f:
                f.write("{}")
            self.assertTrue(ex.PROVIDERS["gpt"]["enabled"]())
        finally:
            os.unlink(path)
        self.assertFalse(ex.PROVIDERS["gpt"]["enabled"]())


class TestAccountHealth(HTTPBase):
    """Epic A — a dead refresh token must be queryable as data, not greppable as a
    log line. Until `account_health` exists, "POLL FAILED" is only visible in
    `docker logs`, and the router keeps trusting a frozen last-good reading."""

    def setUp(self):
        ex._poll_fails.clear()
        ex._poll_schedule.clear()
        self.addCleanup(ex._poll_fails.clear)
        self.addCleanup(ex._poll_schedule.clear)
        # The unified roster spans every authority: a creds file leaked by
        # another class would add gpt/kimi rows and break the exact-set asserts.
        clear_openai_creds()
        clear_kimi_creds()

    def test_error_classification(self):
        # The poll result carries only http + prose; the kind keys off the code.
        self.assertEqual(ex._classify_poll_error({"ok": 1, "http": 0}), "none")
        self.assertEqual(ex._classify_poll_error({"ok": 0, "http": 400}), "auth")
        self.assertEqual(ex._classify_poll_error({"ok": 0, "http": 401}), "auth")
        self.assertEqual(ex._classify_poll_error({"ok": 0, "http": 429}), "rate_limit")
        self.assertEqual(ex._classify_poll_error({"ok": 0, "http": 500}), "transient")
        self.assertEqual(ex._classify_poll_error({"ok": 0, "http": 0}), "transient")

    def test_healthy_account_reports_ok(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        ex.compute_preferred()
        _, p = self.get("/preferred")
        t = {a["name"]: a for a in p["account_health"]["accounts"]}[ACCT_A]
        self.assertEqual(t["poll_ok"], 1)
        self.assertEqual(t["last_error_kind"], "none")
        self.assertEqual(t["consecutive_failures"], 0)
        self.assertEqual(t["next_retry_in_s"], 0)
        h = p["account_health"]
        self.assertEqual(h["worst"], "ok")
        self.assertEqual({a["name"] for a in h["accounts"]}, {ACCT_A, ACCT_B})

    def test_auth_failure_marks_account_dead_and_worst(self):
        dead = {"name": ACCT_A, "ok": 0, "ts": time.time(), "email": ACCT_A, "tier": "",
                "usage": {"five_hour": {"utilization": 18}, "seven_day": {"utilization": 84}},
                "http": 400, "err": "refresh HTTP 400 (refresh token dead?)",
                "retry_after": None, "good_ts": time.time() - 7200, "stale": 1}
        reset(dead, acct(ACCT_B, 100))
        ex._poll_fails[ACCT_A] = 3
        ex._poll_schedule[ACCT_A] = time.time() + 3600
        ex.compute_preferred()
        _, p = self.get("/preferred")
        t = {a["name"]: a for a in p["account_health"]["accounts"]}[ACCT_A]
        self.assertEqual(t["poll_ok"], 0)
        self.assertEqual(t["last_error_kind"], "auth")
        self.assertEqual(t["consecutive_failures"], 3)
        self.assertGreater(t["next_retry_in_s"], 3500)
        h = p["account_health"]
        self.assertEqual(h["worst"], "auth_dead")
        a = next(a for a in h["accounts"] if a["name"] == ACCT_A)
        self.assertEqual(a["health"], "auth_dead")
        self.assertEqual(a["stale"], 1)
        self.assertGreater(a["age_s"], 7000)
        b = next(a for a in h["accounts"] if a["name"] == ACCT_B)
        self.assertEqual(b["health"], "ok")

    def test_rate_limit_is_degraded_not_dead(self):
        limited = {"name": ACCT_A, "ok": 0, "ts": time.time(), "email": ACCT_A, "tier": "",
                   "usage": {"seven_day": {"utilization": 60}},
                   "http": 429, "err": "usage HTTP 429", "retry_after": 120,
                   "good_ts": time.time() - 600, "stale": 1}
        reset(limited, acct(ACCT_B, 30))
        ex.compute_preferred()
        _, p = self.get("/preferred")
        h = p["account_health"]
        self.assertEqual(h["worst"], "degraded",
                         "a 429 self-clears — degraded, never auth_dead")
        a = next(a for a in h["accounts"] if a["name"] == ACCT_A)
        self.assertEqual(a["health"], "degraded")
        self.assertEqual(a["last_error_kind"], "rate_limit")

    def test_provider_accounts_join_the_roster(self):
        """Inverts the pre-#72 'providers are not accounts' rule: every provider
        authority now HAS a re-auth/enroll flow, so its accounts belong on the
        strip — a kimi key with a live authority is an account like any other.
        (Without a key the kimi authority reports an empty roster and the strip
        is anthropic-only, which the other tests in this class rely on.)"""
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30), kimi_result())
        old = pkimi.KIMI_API_KEY
        pkimi.KIMI_API_KEY = "sk-kimi-testdeadbeef"
        try:
            ex.compute_preferred()
            _, p = self.get("/preferred")
        finally:
            pkimi.KIMI_API_KEY = old
        rows = {a["name"]: a for a in p["account_health"]["accounts"]}
        self.assertIn("kimi", rows)
        self.assertEqual(rows["kimi"]["provider"], "kimi")
        self.assertEqual(rows["kimi"]["health"], "ok")
        self.assertEqual(rows[ACCT_A]["provider"], "anthropic")

    def test_unified_rows_carry_no_auth_page_extras(self):
        """/preferred is UNGUARDED — the authority status() extras (kimi key
        fingerprint, token expiries) are /auth/providers material (key-guarded)
        and must not ride along into the public payload."""
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30), kimi_result())
        old = pkimi.KIMI_API_KEY
        pkimi.KIMI_API_KEY = "sk-kimi-testdeadbeef"
        try:
            ex.compute_preferred()
            _, p = self.get("/preferred")
        finally:
            pkimi.KIMI_API_KEY = old
        allowed = set(ex._HEALTH_ROW_KEYS) | {"provider"}
        for a in p["account_health"]["accounts"]:
            self.assertLessEqual(set(a), allowed, a["name"])
            self.assertNotIn("key_fingerprint", a)
            self.assertNotIn("token_expires_at", a)

    def test_dead_gpt_chain_trips_worst(self):
        """The asymmetry this rework kills: a dead codex chain must redden the
        strip exactly as a dead anthropic refresh token does. Health comes from
        the same per-target poll bookkeeping, keyed by chain name."""
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        self.addCleanup(clear_openai_creds)
        dead = gpt_result(name="gpt-a", http=401, ok=0, good_age=7200,
                          err="refresh HTTP 401")
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30), dead, gpt_result(name="gpt-b"))
        ex.compute_preferred()
        _, p = self.get("/preferred")
        h = p["account_health"]
        self.assertEqual(h["worst"], "auth_dead")
        rows = {a["name"]: a for a in h["accounts"]}
        self.assertEqual(rows["gpt-a"]["health"], "auth_dead")
        self.assertEqual(rows["gpt-a"]["provider"], "gpt")
        self.assertEqual(rows["gpt-b"]["health"], "ok")
        self.assertEqual(rows[ACCT_A]["health"], "ok")
        self.assertEqual(set(rows), {ACCT_A, ACCT_B, "gpt-a", "gpt-b"})


class TestReauthFlow(HTTPBase):
    """Epic B — re-authorization is a browser flow, not an agent task. The PKCE
    exchange and profile verify run against a stub Anthropic host; the creds
    splice is tested against a real accounts.json in the temp dir."""

    def setUp(self):
        ex._reauth_pending.clear()
        self.addCleanup(ex._reauth_pending.clear)
        # A real accounts.json holding BOTH accounts — the splice must not clobber.
        self.accounts = [{"name": ACCT_A, "refresh_token": "old-rt-a"},
                         {"name": ACCT_B, "refresh_token": "old-rt-b",
                          "access_token": "tok-b", "expires_at": 1}]
        with open(psub.ACCOUNTS_FILE, "w") as f:
            json.dump(self.accounts, f)

    def post(self, path, obj, key=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}",
                                     data=json.dumps(obj).encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        if key:
            req.add_header("X-Switch-Key", key)
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            return e.code, (json.loads(body) if body else None)

    def test_splice_replaces_only_the_named_disk_and_live_entry(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        old_b = dict(psub._creds[ACCT_B])
        psub._splice_refresh_token(ACCT_A, "new-rt-a")
        arr = json.load(open(psub.ACCOUNTS_FILE))
        a = next(x for x in arr if x["name"] == ACCT_A)
        b = next(x for x in arr if x["name"] == ACCT_B)
        self.assertEqual(a["refresh_token"], "new-rt-a")
        self.assertNotIn("access_token", a)
        self.assertNotIn("expires_at", a)
        self.assertEqual(b["refresh_token"], "old-rt-b", "must not clobber the other account")
        self.assertEqual(b["access_token"], "tok-b", "untouched entry keeps its fields")
        self.assertEqual(len(arr), 2)
        self.assertEqual(psub._creds[ACCT_A], {
            "refresh_token": "new-rt-a", "access_token": None, "expires_at": 0,
        }, "the next poll must not retain the dead in-memory credential")
        self.assertEqual(psub._creds[ACCT_B], old_b)
        self.assertEqual(psub._cred_generation[ACCT_A], 1)

    def test_splice_refuses_an_unconfigured_email(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        before = {n: dict(c) for n, c in psub._creds.items()}
        with self.assertRaises(ValueError):
            psub._splice_refresh_token("mallory@example.com", "rt")
        arr = json.load(open(psub.ACCOUNTS_FILE))
        self.assertEqual(len(arr), 2, "an unconfigured email must not add an account")
        self.assertEqual(psub._creds, before, "failed validation must not change live credentials")

    def test_splice_persistence_failure_leaves_live_cache_unchanged(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        before = {n: dict(c) for n, c in psub._creds.items()}
        orig_replace = ex.os.replace
        ex.os.replace = lambda src, dst: (_ for _ in ()).throw(OSError("disk full"))
        self.addCleanup(setattr, ex.os, "replace", orig_replace)

        with self.assertRaisesRegex(ValueError, "cannot update"):
            psub._splice_refresh_token(ACCT_A, "new-rt-a")

        self.assertEqual(psub._creds, before)
        self.assertNotIn(ACCT_A, psub._cred_generation)

    def test_begin_mints_a_one_shot_pending_grant(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        url, state = psub._reauth_begin(ACCT_A)
        self.assertIn("code_challenge=", url)
        self.assertIn("user%3Aprofile", url, "the scope set must include user:profile")
        self.assertIn(state, ex._reauth_pending)
        # TTL: a stale grant is pruned
        ex._reauth_pending[state]["minted_at"] = time.time() - ex.REAUTH_TTL - 1
        with ex._reauth_lock:
            ex._reauth_prune(time.time())
        self.assertNotIn(state, ex._reauth_pending)

    def test_begin_endpoint_requires_key_and_known_account(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        code, _ = self.post("/reauth/begin", {"account": ACCT_A})
        self.assertEqual(code, 401)
        code, d = self.post("/reauth/begin", {"account": "nobody@example.com"},
                            key="test-switch-key")
        self.assertEqual(code, 404)

    def test_complete_consumes_grant_and_writes_creds(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        ex._poll_fails[ACCT_A] = 5
        ex._poll_schedule[ACCT_A] = time.time() + 3600
        # Stub the two Anthropic calls: token exchange + profile verify. Patch the
        # module-local name, NOT urllib.request.urlopen globally — the test's own
        # HTTP client (self.post) uses the real one.
        orig_post = ex.urlopen
        calls = []

        class R:
            def __init__(self, payload): self.payload = payload
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return json.dumps(self.payload).encode()

        def fake_urlopen(req, timeout=0):
            calls.append(req.full_url)
            if "oauth/token" in req.full_url:
                return R({"refresh_token": "fresh-rt", "access_token": "at"})
            return R({"account": {"email": ACCT_A}, "organization": {}})

        ex.urlopen = fake_urlopen
        self.addCleanup(setattr, ex, "urlopen", orig_post)
        _, state = psub._reauth_begin(ACCT_A)
        code, d = self.post("/reauth/complete", {"state": state, "code": "the-code"},
                            key="test-switch-key")
        self.assertEqual(code, 200, d)
        self.assertEqual(d["account"], ACCT_A)
        arr = json.load(open(psub.ACCOUNTS_FILE))
        a = next(x for x in arr if x["name"] == ACCT_A)
        self.assertEqual(a["refresh_token"], "fresh-rt")
        self.assertNotIn(state, ex._reauth_pending, "grant must be one-shot")
        self.assertNotIn(ACCT_A, ex._poll_fails, "backoff cleared — due immediately")
        self.assertNotIn(ACCT_A, ex._poll_schedule)
        self.assertTrue(ex._poll_wakeup.is_set(), "sleeping poller must be woken")
        self.assertEqual(psub._creds[ACCT_A]["refresh_token"], "fresh-rt")
        self.assertIsNone(psub._creds[ACCT_A]["access_token"])

    def test_complete_with_unknown_state_422s(self):
        code, d = self.post("/reauth/complete", {"state": "nope", "code": "c"},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("begin again", d["error"])

    def test_profile_403_rejects_the_grant(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        orig = psub._profile_facts
        psub._profile_facts = lambda tok: (None, None)  # an inference-only token
        self.addCleanup(setattr, psub, "_profile_facts", orig)
        orig_post = ex.urlopen

        class R:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self):
                return json.dumps({"refresh_token": "rt", "access_token": "at"}).encode()

        ex.urlopen = lambda req, timeout=0: R()
        self.addCleanup(setattr, ex, "urlopen", orig_post)
        _, state = psub._reauth_begin(ACCT_A)
        code, d = self.post("/reauth/complete", {"state": state, "code": "c"},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("user:profile", d["error"])
        arr = json.load(open(psub.ACCOUNTS_FILE))
        a = next(x for x in arr if x["name"] == ACCT_A)
        self.assertEqual(a["refresh_token"], "old-rt-a",
                         "a profile-less grant must not touch the creds file")

    def test_status_endpoint_reports_health_with_key(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        code, _ = self.get("/reauth/status")
        self.assertEqual(code, 401)
        code, d = self.get("/reauth/status", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertIn("accounts", d)

    def test_page_loads_accounts_without_exposing_the_switch_key(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/reauth",
                                     headers={"X-Switch-Key": "test-switch-key"})
        with urllib.request.urlopen(req, timeout=5) as r:
            body = r.read().decode()
        self.assertIn("Re-authorize", body)
        self.assertIn("Loading accounts", body)
        self.assertIn("refresh();", body)
        self.assertIn("Anthropic/Claude subscription OAuth accounts only", body)
        self.assertIn("OpenAI/Codex", body)
        self.assertIn("Moonshot/Kimi", body)
        self.assertIn("WATCH_UNTIL = Date.now() + 90000", body)
        self.assertIn("setTimeout(() => refresh(true), 3000)", body)
        # Success must not strand the user: the consumed grant is cleared, the
        # form retires, and a route back appears.
        self.assertIn('document.getElementById("code").value = ""', body)
        self.assertIn('show("step1", false)', body)
        self.assertIn('id="done"', body)
        self.assertIn('href="/">Back</a>', body)
        self.assertNotIn("a.err", body, "raw operator errors must not reach the page")
        self.assertNotIn("claude_oauth.py", body)
        self.assertNotIn("test-switch-key", body)
        self.assertNotIn('id="key"', body)
        self.assertIn('keyedFetch("/reauth/status"', body)
        self.assertNotIn('await fetch("/', body)


def local_result(ok=1, good_age=0):
    """A local poll result shaped like poll_local()'s output."""
    now = time.time()
    return read({"name": "local", "kind": "provider", "provider": "local", "ok": ok,
                 "ts": now, "good_ts": now - good_age, "stale": 0, "http": 0, "err": "",
                 "retry_after": None, "raw": {}})


# ---- #26: pointer + session-sticky + reset-boundary drift-up ------------------

def provider_result(provider, five_hour=8.0, weekly=2.0, good_age=0, ts=None):
    """A poll result for an arbitrary provider (the brain treats any
    kind=provider result uniformly): "gpt" reads a wham/usage body with a 5h
    and a 7d window, any other a Kimi /usages body."""
    if provider == "gpt":
        return gpt_result(weekly=weekly, five_hour=five_hour, good_age=good_age, ts=ts,
                          allowed=max(weekly, five_hour) < 100)
    r = kimi_result(five_hour=five_hour, weekly=weekly, good_age=good_age, ts=ts)
    r["name"] = provider
    r["provider"] = provider
    return read(r)


def class_pointer(idx=0):
    """The live pointer of one class, computed exactly as the policy does."""
    with ex._lock:
        snap = list(ex._state.values())
    with ex._plock:
        ctx = ex._policy_ctx(snap, time.time())
        cls = ex._classes[idx]
        return ex._class_pointer(cls, ex._class_verdicts(cls, ctx), ctx["money"]["pay_order"])


def set_opus(cap=None, fh_reset=None):
    """Mutate the single account's Opus cap and/or 5h resets_at, then recompute."""
    with ex._lock:
        r = ex._state[ACCT_A]
        if cap is not None:
            r["raw"]["limits"][0]["percent"] = cap
        if fh_reset is not None:
            r["raw"]["five_hour"]["resets_at"] = fh_reset
        read(r)
    ex.compute_preferred()


def bound_member(sid, cname="frontier"):
    with ex._plock:
        s = ex._preferred["classes"].get(cname, {}).get("sessions", {}).get(sid)
        return s["member"] if s else None


def age_session(sid, cname="frontier", seconds=ex.SW_DWELL + 100):
    with ex._plock:
        ex._preferred["classes"][cname]["sessions"][sid]["last_switch"] = \
            time.time() - seconds


class ClassPolicyBase(unittest.TestCase):
    """One account (no lane-switch interference) + fabricated gpt + kimi targets:
    every member of the frontier class is independently controllable."""

    def setUp(self):
        classes_reset(opus_acct(30, resets=True),
                      provider_result("gpt", five_hour=8.0),
                      kimi_result(five_hour=12.0, weekly=2.0))

    def tearDown(self):
        ex._classes = []


class TestClassPointer(ClassPolicyBase):
    def pointer(self):
        return class_pointer()

    def test_priority_honored_when_all_cool(self):
        self.assertEqual(self.pointer(), "lane:opus")

    def test_hot_p1_skipped(self):
        set_opus(cap=85)
        self.assertEqual(self.pointer(), "gpt/gpt-5.6")

    def test_unknown_member_skipped(self):
        with ex._lock:
            del ex._state["gpt"]
        set_opus(cap=85)
        self.assertEqual(self.pointer(), "kimi/kimi-k3")

    def test_stale_member_skipped(self):
        set_opus(cap=85)
        with ex._lock:
            ex._state["gpt"] = provider_result("gpt", good_age=ex.STALE_MAX + 100)
        self.assertEqual(self.pointer(), "kimi/kimi-k3")

    def test_all_hot_binds_the_coolest_known_member(self):
        """#88: SW_HIGH is a preference, not an admission filter. Every member
        over 80 with real quota left -> the coolest one, not a 503."""
        set_opus(cap=85)
        with ex._lock:
            ex._state["gpt"] = provider_result("gpt", five_hour=90.0)
            ex._state["kimi"] = kimi_result(five_hour=95.0)
        self.assertEqual(self.pointer(), "lane:opus", "85 < 90 < 95: coolest wins")

    def test_all_hot_ties_break_by_priority(self):
        set_opus(cap=90)
        with ex._lock:
            ex._state["gpt"] = provider_result("gpt", five_hour=90.0)
            ex._state["kimi"] = kimi_result(five_hour=90.0)
        self.assertEqual(self.pointer(), "lane:opus", "equal heat: priority order")

    def test_spent_members_are_never_the_last_resort(self):
        """A member at 100 has nothing to spend; seating a session there can only
        429. All spent -> None (typed 503), the case the ladder cannot fix."""
        set_opus(cap=100)
        with ex._lock:
            ex._state["gpt"] = provider_result("gpt", five_hour=100.0)
            ex._state["kimi"] = kimi_result(five_hour=100.0)
        self.assertIsNone(self.pointer())
        with ex._lock:
            ex._state["kimi"] = kimi_result(five_hour=99.0)
        self.assertEqual(self.pointer(), "kimi/kimi-k3", "99 is still spendable")

    def test_all_unknown_is_none(self):
        set_opus(cap=85)
        with ex._lock:
            del ex._state["gpt"]
            del ex._state["kimi"]
        with ex._lock:
            ex._state["gpt"] = provider_result("gpt", good_age=ex.STALE_MAX + 100)
        # opus is the only known member -> last resort binds it even at 85
        self.assertEqual(self.pointer(), "lane:opus")


class TestClassBind(ClassPolicyBase):
    def test_bind_creates_at_pointer(self):
        code, p = ex._bind_class_session("frontier", "s1", time.time())
        self.assertEqual(code, 200)
        self.assertTrue(p["created"])
        self.assertIs(p["pinned"], False)
        self.assertEqual(p["member"]["id"], "lane:opus")
        self.assertEqual(p["member"]["lane"], "opus")

    def test_rebind_is_sticky_and_touches_last_seen(self):
        now = time.time()
        ex._bind_class_session("frontier", "s1", now - 100)
        code, p = ex._bind_class_session("frontier", "s1", now)
        self.assertEqual(code, 200)
        self.assertFalse(p["created"])
        with ex._plock:
            s = ex._preferred["classes"]["frontier"]["sessions"]["s1"]
            self.assertEqual(s["last_seen"], now)

    def test_bind_honors_pin(self):
        with ex._plock:
            ex._preferred["classes"].setdefault("frontier", ex._new_class_state())
            ex._preferred["classes"]["frontier"]["pinned"] = "kimi/kimi-k3"
        code, p = ex._bind_class_session("frontier", "s1", time.time())
        self.assertEqual(code, 200)
        self.assertEqual(p["member"]["id"], "kimi/kimi-k3")
        self.assertIs(p["pinned"], True)
        with ex._plock:
            ex._preferred["classes"]["frontier"]["pinned"] = None
        code, p = ex._bind_class_session("frontier", "s1", time.time())
        self.assertEqual(code, 200)
        self.assertIs(p["pinned"], False, "unpin must override a stale binding reason")

    def test_unknown_class_404(self):
        code, p = ex._bind_class_session("nope", "s1", time.time())
        self.assertEqual(code, 404)
        self.assertIn("frontier", p["classes"])

    def test_no_member_with_an_account_is_the_typed_503(self):
        """§2.8 item 6: a class none of whose routes has an account has no
        check, so nothing to wait for and nothing to fund. That is
        configuration, not quota: 503, never the 409 money reply."""
        cfg = [{"class": "frontier", "members": [
            {"priority": 1, "kind": "provider", "provider": "nobody", "model": "m-1",
             "context_window": 400000, "anthropic_token_ratio": 0.6}]}]
        classes_reset(opus_acct(30), cfg=cfg)
        code, p = ex._bind_class_session("frontier", "s1", time.time())
        self.assertEqual(code, 503)
        self.assertEqual(p, {"error": "no member of class 'frontier' has an account",
                             "class": "frontier", "routes": ["frontier/nobody/m-1"]})
        with ex._plock:
            self.assertNotIn("s1", ex._preferred["classes"]["frontier"]["sessions"], "nothing is bound")

    def test_binding_survives_state_roundtrip(self):
        ex._bind_class_session("frontier", "s1", time.time())
        with ex._plock:
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["member"] = "kimi/kimi-k3"
            ex._save_state()
        ex._load_state()
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3")


class TestSessionStickyFlapRegression(ClassPolicyBase):
    """The pre-e507635 flap weather: a recovered higher-priority member does NOT
    pull a bound session back — only a window reset can (once)."""

    def test_recovered_p1_never_pulls_session_without_reset(self):
        ex._bind_class_session("frontier", "s1", time.time())
        set_opus(cap=96)                       # hard-forced off opus
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                         "forced-down goes to the highest-priority acceptable")
        for cap in (30, 85, 30, 85, 30):       # oscillate: the deleted drift-home bait
            age_session("s1")
            set_opus(cap=cap)
            self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                             f"no movement on recovery alone (opus at {cap}%)")

    def test_new_session_takes_the_pointer_while_old_stays(self):
        ex._bind_class_session("frontier", "s1", time.time())
        set_opus(cap=96)
        age_session("s1")
        set_opus(cap=30)                       # recovered: pointer back to opus
        code, p = ex._bind_class_session("frontier", "s2", time.time())
        self.assertEqual(p["member"]["id"], "lane:opus",
                         "priority re-honored at the session boundary")
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "old session sticky")


class TestForcedDown(ClassPolicyBase):
    def test_hard_prefers_highest_priority_not_coolest(self):
        ex._bind_class_session("frontier", "s1", time.time())
        with ex._lock:  # gpt 50, kimi 8 — kimi is coolest, gpt outranks it
            ex._state["gpt"] = provider_result("gpt", five_hour=50.0)
        set_opus(cap=96)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6")

    def test_hot_margin_skips_inadequate_higher_member(self):
        ex._bind_class_session("frontier", "s1", time.time())
        with ex._lock:  # gpt only 13pts cooler (fails margin), kimi 25pts cooler
            ex._state["gpt"] = provider_result("gpt", five_hour=72.0)
            ex._state["kimi"] = kimi_result(five_hour=60.0)
        set_opus(cap=85)
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3",
                         "highest-priority member MEETING the margin wins")

    def test_dwell_gates_forced_down(self):
        ex._bind_class_session("frontier", "s1", time.time())
        with ex._plock:
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["last_switch"] = time.time()
        set_opus(cap=96)
        self.assertEqual(bound_member("s1"), "lane:opus", "dwell holds the move")
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "fires once dwell opens")


class TestDriftUp(ClassPolicyBase):
    def force_down(self, sid="s1"):
        ex._bind_class_session("frontier", sid, time.time())
        set_opus(cap=96)
        self.assertEqual(bound_member(sid), "gpt/gpt-5.6")
        age_session(sid)

    def test_reset_boundary_with_cool_member_fires_once(self):
        self.force_down()
        set_opus(cap=30, fh_reset="2026-07-28T23:00:00Z")  # boundary + comfortable
        self.assertEqual(bound_member("s1"), "lane:opus", "the sanctioned move")

    def test_no_reset_no_move_even_at_zero(self):
        self.force_down()
        set_opus(cap=0)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6")

    def test_warm_boundary_is_consumed_not_deferred(self):
        self.force_down()
        set_opus(cap=70, fh_reset="2026-07-28T23:00:00Z")  # boundary but > 65
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "70 > comfort band")
        age_session("s1")
        set_opus(cap=30)                                   # cools mid-window
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                         "window already consumed — no late fire")
        age_session("s1")
        set_opus(fh_reset="2026-07-29T04:00:00Z")          # NEXT window resets
        self.assertEqual(bound_member("s1"), "lane:opus", "fresh window fires")

    def test_unknown_resets_never_fire(self):
        classes_reset(opus_acct(30, resets=False),         # no resets_at anywhere
                      provider_result("gpt", five_hour=8.0),
                      kimi_result())
        self.force_down()
        set_opus(cap=10)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                         "no resets_at = usable for forced-down, never drift-up")

    def test_dwell_defers_but_does_not_cancel_drift_up(self):
        self.force_down()
        with ex._plock:
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["last_switch"] = time.time()
        set_opus(cap=30, fh_reset="2026-07-28T23:00:00Z")
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "dwell holds it")
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "lane:opus",
                         "unconsumed boundary fires after dwell opens")


# ---- #33 reserve ceilings: a preference tier, never a filter ------------------

class CeilingBase(ClassPolicyBase):
    """ClassPolicyBase with reserve ceilings installable per test — same three
    independently-controllable members, only the config differs. Baseline
    effectives: opus 30, gpt 8, kimi 12."""

    def install(self, **ceilings):
        classes_reset(opus_acct(30, resets=True),
                      provider_result("gpt", five_hour=8.0),
                      kimi_result(five_hour=12.0, weekly=2.0),
                      cfg=frontier_cfg_ceilings(**ceilings))

    def pointer(self):
        return class_pointer()

    def set_provider(self, name, five_hour):
        with ex._lock:
            ex._state[name] = (kimi_result(five_hour=five_hour) if name == "kimi"
                               else provider_result(name, five_hour=five_hour))
        ex.compute_preferred()


class TestCeilingPointer(CeilingBase):
    def test_under_ceiling_is_todays_pointer(self):
        self.install(gpt=25)
        set_opus(cap=85)
        self.assertEqual(self.pointer(), "gpt/gpt-5.6", "8% is well inside a 25% reserve")

    def test_member_over_its_ceiling_is_passed_over(self):
        self.install(gpt=5)                      # gpt sits at 8
        set_opus(cap=85)
        self.assertEqual(self.pointer(), "kimi/kimi-k3",
                         "the reserve holds while anything else is bindable")

    def test_ceiling_yields_rather_than_refuse_service(self):
        self.install(gpt=5)
        set_opus(cap=85)
        self.set_provider("kimi", 90.0)
        self.assertEqual(self.pointer(), "gpt/gpt-5.6",
                         "spending the reserve beats a 503")

    def test_every_member_over_ceiling_still_binds_200(self):
        """The load-bearing availability test: ceilings must not manufacture a
        503 while real quota remains."""
        self.install(opus=10, gpt=5, kimi=5)     # 30/8/12 — all over, all < SW_HIGH
        code, p = ex._bind_class_session("frontier", "s1", time.time())
        self.assertEqual(code, 200)
        self.assertEqual(p["member"]["id"], "lane:opus", "falls back to priority order")

    def test_all_hot_over_ceiling_binds_coolest_not_none(self):
        """#88 rewrote the old contract ("None in exactly the pre-ceiling cases"):
        ceilings still never manufacture a 503, and neither does SW_HIGH."""
        self.install(opus=10, gpt=5, kimi=5)
        set_opus(cap=85)
        self.set_provider("gpt", 90.0)
        self.set_provider("kimi", 95.0)
        self.assertEqual(self.pointer(), "lane:opus", "coolest known member, ceilings ignored")

    def test_ceiling_at_or_above_high_is_inert(self):
        self.install(opus=85)                    # above SW_HIGH=80: never binds first
        set_opus(cap=79)
        self.assertEqual(self.pointer(), "lane:opus")
        set_opus(cap=82)
        self.assertEqual(self.pointer(), "gpt/gpt-5.6", "SW_HIGH gated it, not the ceiling")

    def test_zero_ceiling_is_emergency_reserve_only(self):
        self.install(gpt=0)
        set_opus(cap=85)
        self.assertEqual(self.pointer(), "kimi/kimi-k3", "never a first-pass preference")
        self.set_provider("kimi", 90.0)
        self.assertEqual(self.pointer(), "gpt/gpt-5.6", "still the last resort")


class TestCeilingForcedDown(CeilingBase):
    def test_hot_prefers_a_target_inside_its_reserve(self):
        self.install(gpt=5)
        ex._bind_class_session("frontier", "s1", time.time())
        set_opus(cap=85)                         # gpt and kimi both clear SW_MARGIN
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3",
                         "without the ceiling gpt would win on priority")

    def test_hot_falls_back_to_a_reserved_target(self):
        self.install(gpt=5)
        ex._bind_class_session("frontier", "s1", time.time())
        self.set_provider("kimi", 75.0)          # only 10pts cooler — fails margin
        set_opus(cap=85)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                         "the only member meeting the margin is taken, reserve or not")

    def test_hard_prefers_a_target_inside_its_reserve(self):
        self.install(gpt=5)
        ex._bind_class_session("frontier", "s1", time.time())
        set_opus(cap=96)
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3")

    def test_hard_falls_back_to_a_reserved_target(self):
        self.install(gpt=5)
        ex._bind_class_session("frontier", "s1", time.time())
        with ex._lock:
            del ex._state["kimi"]                # nothing else usable at all
        set_opus(cap=96)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                         "a hard-forced session never stays on a 96% member")


class TestCeilingEviction(CeilingBase):
    """Without eviction the reserve is theater: bindings are sticky and the
    forced-down rules only fire at SW_HIGH, so a session that lands on a
    reserved member at 0% would ride it straight through its ceiling."""

    def land_on_gpt(self, sid="s1", **ceilings):
        self.install(**ceilings)
        ex._bind_class_session("frontier", sid, time.time())
        set_opus(cap=96)
        self.assertEqual(bound_member(sid), "gpt/gpt-5.6")
        age_session(sid)

    def test_crossing_the_ceiling_evicts_the_session(self):
        self.land_on_gpt(gpt=10)
        self.set_provider("gpt", 20.0)           # over ceiling, still far below SW_HIGH
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3")

    def test_dwell_gates_eviction(self):
        self.land_on_gpt(gpt=10)
        with ex._plock:
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["last_switch"] = time.time()
        self.set_provider("gpt", 20.0)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "dwell holds the move")
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3", "fires once dwell opens")

    def test_no_target_below_means_stay_put(self):
        self.land_on_gpt(gpt=10)
        self.set_provider("kimi", 90.0)          # nothing acceptable below gpt
        self.set_provider("gpt", 20.0)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                         "never evict to nowhere — holding a reserve is not worth a drop")

    def test_eviction_never_moves_up(self):
        self.land_on_gpt(gpt=10)
        set_opus(cap=30)                         # P1 fully recovered and coolest
        self.set_provider("gpt", 20.0)
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3",
                         "up-moves stay rationed to the #26 drift-up rule")

    def test_pin_beats_the_ceiling(self):
        self.install(gpt=5)                      # gpt at 8 is already over
        with ex._plock:
            ex._preferred["classes"].setdefault("frontier", ex._new_class_state())
            ex._preferred["classes"]["frontier"]["pinned"] = "gpt/gpt-5.6"
        code, p = ex._bind_class_session("frontier", "s1", time.time())
        self.assertEqual(p["member"]["id"], "gpt/gpt-5.6")
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6",
                         "an explicit human override outranks the reserve")


class TestCeilingDriftUp(CeilingBase):
    def force_down(self, **ceilings):
        self.install(**ceilings)
        ex._bind_class_session("frontier", "s1", time.time())
        set_opus(cap=96)
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6")
        age_session("s1")

    def marks(self, mid="lane:opus"):
        with ex._plock:
            return ex._preferred["classes"]["frontier"]["sessions"]["s1"]["marks"][mid]

    def test_reserved_member_is_not_drifted_up_to_but_consumes_its_boundary(self):
        self.force_down(opus=20)
        before = dict(self.marks())
        set_opus(cap=30, fh_reset="2026-07-28T23:00:00Z")   # boundary, 30 <= 65 comfort
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "30% is past a 20% reserve")
        self.assertNotEqual(self.marks(), before,
                            "the boundary is still CONSUMED — one chance per window")
        age_session("s1")
        set_opus(cap=10)                                    # cools deep inside the reserve
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "no late fire mid-window")

    def test_control_under_ceiling_fires(self):
        self.force_down(opus=50)
        set_opus(cap=30, fh_reset="2026-07-28T23:00:00Z")
        self.assertEqual(bound_member("s1"), "lane:opus",
                         "same setup, ceiling clear — the sanctioned move happens")


class TestClassSessionLifecycle(ClassPolicyBase):
    def test_idle_session_pruned(self):
        ex._bind_class_session("frontier", "s1", time.time())
        with ex._plock:
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["last_seen"] = \
                time.time() - ex.SESSION_IDLE - 100
        ex.compute_preferred()
        self.assertIsNone(bound_member("s1"))

    def test_unusable_member_rebinds_to_pointer(self):
        ex._bind_class_session("frontier", "s1", time.time())
        with ex._plock:  # simulate a config edit orphaning the binding
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["member"] = "gone/x"
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "lane:opus")

    def test_migrate_carries_and_filters(self):
        d = {"account": ACCT_A, "lanes": {},
             "classes": {"frontier": {"pinned": "lane:opus",
                                      "sessions": {"ok": {"member": "lane:opus"},
                                                   "bad": {"member": 7}}},
                         "gone": {"sessions": {"s": {"member": "x"}}}}}
        out = ex._migrate_state(d, ex._lane_names, ["frontier"])
        self.assertEqual(out["classes"]["frontier"]["pinned"], "lane:opus")
        self.assertEqual(list(out["classes"]["frontier"]["sessions"]), ["ok"])
        self.assertNotIn("gone", out["classes"])

    def test_migrate_without_classes_key_is_clean(self):
        out = ex._migrate_state({"account": ACCT_A}, ex._lane_names, ["frontier"])
        self.assertEqual(out["classes"], {"frontier": ex._new_class_state()})


class TestClassEndpointsHTTP(HTTPBase):
    KEY = "test-switch-key"

    def setUp(self):
        classes_reset(opus_acct(30, resets=True),
                      provider_result("gpt", five_hour=8.0),
                      kimi_result())

    def tearDown(self):
        ex._classes = []

    def test_bind_requires_key(self):
        code, _ = self.get("/class-bind?class=frontier&session=s1")
        self.assertEqual(code, 401)

    def test_bind_requires_params(self):
        code, _ = self.get("/class-bind?class=frontier", key=self.KEY)
        self.assertEqual(code, 400)

    def test_bind_ok(self):
        code, p = self.get("/class-bind?class=frontier&session=http-s1", key=self.KEY)
        self.assertEqual(code, 200)
        self.assertEqual(p["member"]["id"], "lane:opus")
        self.assertEqual(p["member"]["model"], "claude-opus-4-8",
                         "the proxy actuates from this — bind must carry the Anthropic model id")
        self.assertTrue(p["created"])

    def test_class_pin_moves_bound_sessions_and_new_binds(self):
        self.get("/class-bind?class=frontier&session=http-s1", key=self.KEY)
        code, p = self.get("/class-pin?class=frontier&member=kimi/kimi-k3", key=self.KEY)
        self.assertEqual(code, 200)
        self.assertEqual(p["classes"]["frontier"]["pinned"], "kimi/kimi-k3")
        self.assertEqual(bound_member("http-s1"), "kimi/kimi-k3", "pin applies immediately")
        _, b = self.get("/class-bind?class=frontier&session=http-s2", key=self.KEY)
        self.assertEqual(b["member"]["id"], "kimi/kimi-k3")
        code, p = self.get("/class-pin?class=frontier&member=clear", key=self.KEY)
        self.assertIsNone(p["classes"]["frontier"]["pinned"])

    def test_class_pin_unknown_member_404(self):
        code, p = self.get("/class-pin?class=frontier&member=nope", key=self.KEY)
        self.assertEqual(code, 404)
        self.assertIn("lane:opus", p["members"])

    def test_preferred_carries_policy_view(self):
        self.get("/class-bind?class=frontier&session=http-s1", key=self.KEY)
        _, p = self.get("/preferred")
        c = p["classes"]["frontier"]
        self.assertEqual(c["pointer"], "lane:opus")
        self.assertIsNone(c["pinned"])
        self.assertIn("http-s1", c["sessions"])
        self.assertNotIn("marks", c["sessions"]["http-s1"], "marks are internal")
        self.assertNotIn("pay_settings", c["sessions"]["http-s1"], "so is the pay digest")
        m0 = c["members"][0]
        self.assertEqual(m0["id"], "lane:opus")
        self.assertEqual(m0["model"], "claude-opus-4-8")
        self.assertEqual(m0["effective"], 30.0)
        gpt = c["members"][1]
        self.assertEqual(gpt["effective"], 8.0)


class TestCeilingHTTPContract(HTTPBase):
    """What the proxy and a human operator actually see. gpt carries a ceiling
    it has already crossed (8% vs 5%); kimi carries none."""
    KEY = "test-switch-key"

    def setUp(self):
        classes_reset(opus_acct(30, resets=True),
                      provider_result("gpt", five_hour=8.0),
                      kimi_result(),
                      cfg=frontier_cfg_ceilings(gpt=5))

    def tearDown(self):
        ex._classes = []

    def test_preferred_publishes_ceiling_and_reserved(self):
        _, p = self.get("/preferred")
        ms = {m["id"]: m for m in p["classes"]["frontier"]["members"]}
        self.assertEqual(ms["gpt/gpt-5.6"]["reserve_ceiling"], 5.0)
        self.assertTrue(ms["gpt/gpt-5.6"]["reserved"])
        self.assertIsNone(ms["kimi/kimi-k3"]["reserve_ceiling"])
        self.assertFalse(ms["kimi/kimi-k3"]["reserved"])
        self.assertFalse(ms["lane:opus"]["reserved"], "no ceiling is never reserved")

    def test_reserve_does_not_touch_usable_or_status(self):
        """The proxy's 429 detour ladder filters on `usable` (class.go). A member
        held back as a PREFERENCE must stay reachable in an emergency."""
        _, p = self.get("/preferred")
        gpt = next(m for m in p["classes"]["frontier"]["members"] if m["id"] == "gpt/gpt-5.6")
        self.assertTrue(gpt["usable"], "reserved but still routable")
        self.assertEqual(gpt["status"], "ok")

    def test_bind_carries_the_ceiling(self):
        _, b = self.get("/class-bind?class=frontier&session=http-c1", key=self.KEY)
        self.assertEqual(b["member"]["id"], "lane:opus")
        self.assertIn("reserve_ceiling", b["member"])


# ---- #30/#31 — codex OAuth machinery + gpt poller -----------------------------
#
# The poller reads two free GETs (quota-model 11 S6, S8). The bodies below are
# built from the documented response shapes (11 S6's field list; codex-rs
# RateLimitStatusPayload, RateLimitWindowSnapshot, CreditStatusDetails) with
# neutral values. They are fixtures, not captures.

def wham_window(pct, secs, reset_at=None):
    return {"used_percent": pct, "limit_window_seconds": secs,
            "reset_after_seconds": secs // 2,
            "reset_at": reset_at if reset_at is not None else int(time.time()) + secs // 2}


def wham_usage(primary=12.0, primary_secs=604800, secondary=None, secondary_secs=18000,
               allowed=True, credits=None, spend_reached=False, additional=None,
               plan="pro", reset_at=None):
    """A GET wham/usage body. The default is Pro's shape: one weekly primary
    window, a null secondary, credits tracking off. `credits` overrides fields
    of the credits object (its balance is a decimal STRING on the wire)."""
    c = {"has_credits": False, "unlimited": False, "overage_limit_reached": False,
         "balance": "0", "approx_local_messages": [0, 0], "approx_cloud_messages": [0, 0]}
    c.update(credits or {})
    return {
        "plan_type": plan,
        "rate_limit": {
            "allowed": allowed, "limit_reached": not allowed,
            "primary_window": wham_window(primary, primary_secs, reset_at),
            "secondary_window": (wham_window(secondary, secondary_secs)
                                 if secondary is not None else None)},
        "additional_rate_limits": additional,
        "model_usage": {"gpt-test": {"available": True, "available_at": None,
                                     "credits_would_enable": False}},
        "credits": c,
        "spend_control": {"reached": spend_reached, "individual_limit": None},
        "rate_limit_reached_type": None,
        "promo": None,
        "rate_limit_reset_credits": {"available_count": 0, "applicable_available_count": 0},
    }


def wham_reset_credits(available=0, eligible=True):
    """A GET wham/rate-limit-reset-credits body (11 S8)."""
    return {"credits": [], "available_count": available, "total_earned_count": 0,
            "immediate_reset_purchase_eligible": eligible, "history_enabled": False}


def patch_codex_reads(test, usage, reset_credits=None):
    """Serve both codex reads from fixtures for the life of `test`. Each value
    may be a body or a callable(tok, acct_id) that returns one or raises."""
    for attr, val in (("_get_codex_usage", usage),
                      ("_get_codex_reset_credits",
                       reset_credits if reset_credits is not None else wham_reset_credits())):
        orig = getattr(pcodex, attr)
        test.addCleanup(setattr, pcodex, attr, orig)
        setattr(pcodex, attr, val if callable(val) else (lambda tok, acct_id, v=val: v))


def seed_openai_creds(**over):
    """Write a valid codex creds file at the exporter's configured path."""
    data = {"refresh_token": "rt-test-1", "access_token": "at-test-1",
            "expires_at": time.time() + 999999, "account_id": "acct-uuid-test"}
    data.update(over)
    with open(pcodex.OPENAI_CODEX_CREDS_FILE, "w") as f:
        json.dump(data, f)
    return data


def clear_openai_creds():
    pcodex._openai_pool = None
    pcodex._openai_pool_mtime = None
    pcodex._openai_pool_shape = "legacy"
    try:
        os.remove(pcodex.OPENAI_CODEX_CREDS_FILE)
    except FileNotFoundError:
        pass


def seed_openai_pool(*entries):
    """Write an ARRAY-form codex creds file (#71): each entry needs at least a
    name; refresh_token/access_token/expires_at/account_id default per-name."""
    data = []
    for e in entries:
        n = e["name"]
        base = {"name": n, "refresh_token": f"rt-{n}", "access_token": f"at-{n}",
                "expires_at": time.time() + 999999, "account_id": f"acct-{n}"}
        base.update(e)
        data.append(base)
    with open(pcodex.OPENAI_CODEX_CREDS_FILE, "w") as f:
        json.dump(data, f)
    pcodex._openai_pool = None
    pcodex._openai_pool_mtime = None
    return data


def pin_openai_cache_mtime():
    """Record the current creds file's mtime as already-seen, so the next
    non-force load serves the in-memory pool — the escape-hatch scenario
    (dead in-memory chain, force=True re-read) stays constructible now that
    the loader adopts out-of-band rewrites via the mtime guard."""
    pcodex._openai_pool_mtime = os.stat(pcodex.OPENAI_CODEX_CREDS_FILE).st_mtime


def gpt_result(weekly=12.0, ok=1, good_age=0, http=0, err="", ts=None, name="gpt",
               allowed=True, credits=None, spend_reached=None, five_hour=None):
    """A minimal gpt poll result shaped like poll_gpt()'s output, read from a
    wham/usage body: Pro's one weekly window at `weekly` (plus a 5h one when
    `five_hour` is set), rate_limit.allowed = `allowed`. `name` distinguishes
    pool accounts (#71); the default is the legacy singleton target.
    `credits` overrides fields of the wire credits object (a number balance is
    sent as the wire's decimal string); without it, credits tracking is off.
    `spend_reached` sets spend_control.reached."""
    now = ts or time.time()
    c = dict(credits or {})
    if isinstance(c.get("balance"), (int, float)):
        c["balance"] = f"{c['balance']}"
    raw = wham_usage(primary=weekly, secondary=five_hour, allowed=allowed, credits=c,
                     spend_reached=bool(spend_reached), reset_at=int(now) + 604800)
    if spend_reached is None:
        raw["spend_control"] = None
    return read({"name": name, "kind": "provider", "provider": "gpt", "ok": ok,
                 "ts": now, "good_ts": now - good_age, "stale": 1 if good_age else 0,
                 "http": http, "err": err, "retry_after": None, "raw": raw,
                 "reset_options": pcodex._codex_reset_options(wham_reset_credits()),
                 "source": "GET wham/usage + GET wham/rate-limit-reset-credits"})


class TestGptPollNormalization(_LogCapture, unittest.TestCase):
    """C1 — poll_gpt: the free wham reads → the normalized provider usage shape.
    Emits provider="gpt" (the classes.json member join key), NOT "openai"."""

    def setUp(self):
        seed_openai_creds()
        pcodex._openai_pool = None
        self.capture_log()

    def tearDown(self):
        clear_openai_creds()

    def windows(self, res):
        return ex._reading_windows(res["reading"])

    def test_pro_single_weekly_window(self):
        patch_codex_reads(self, wham_usage(primary=12.0, reset_at=1790000000))
        res = pcodex.poll_gpt()
        self.assertEqual(res["ok"], 1)
        self.assertEqual(res["name"], "gpt")
        self.assertEqual(res["provider"], "gpt")
        self.assertNotIn("usage", res, "the reading replaces the usage contract")
        w = self.windows(res)
        self.assertEqual(list(w), ["seven_day"], "Pro has no 5-hour window")
        wk = w["seven_day"]
        self.assertEqual((wk["used"], wk["limit"], wk["used_pct"]), (12.0, 100.0, 12.0))
        self.assertEqual(wk["period_s"], 604800)
        self.assertEqual(wk["resets_at"], 1790000000)
        self.assertEqual(wk["reset_basis"], "early_possible")
        self.assertEqual(wk["opens"], "first_use")
        pot = ex._reading_pot(res["reading"], ex.ALLOWANCE_POT)
        self.assertEqual((pot["open"], pot["open_basis"]), (True, "exact"))
        self.assertEqual(res["reading"]["plan"], {"label": "pro", "basis": "exact"})
        self.assertEqual(res["reading"]["models"],
                         {"gpt-test": {"available": True, "available_at": None,
                                       "credits_would_enable": False,
                                       "basis": "unknown_semantics"}})
        self.assertEqual(ex._provider_effective(res), 12.0)
        self.assertIn("wham/usage", res["source"])
        self.assertIn("wham/rate-limit-reset-credits", res["source"])

    def test_two_windows_labelled_by_seconds_not_position(self):
        """O-D6: a plan with both windows sends the SHORT one as primary. The old
        mapping (primary → seven_day) swapped them."""
        patch_codex_reads(self, wham_usage(primary=30.0, primary_secs=18000,
                                           secondary=55.0, secondary_secs=604800))
        w = self.windows(pcodex.poll_gpt())
        self.assertEqual(w["five_hour"]["used_pct"], 30.0)
        self.assertEqual(w["five_hour"]["period_s"], 18000)
        self.assertEqual(w["seven_day"]["used_pct"], 55.0)
        self.assertEqual(w["seven_day"]["period_s"], 604800)
        # ...and the same windows in the other order land in the same slots.
        patch_codex_reads(self, wham_usage(primary=55.0, primary_secs=604800,
                                           secondary=30.0, secondary_secs=18000))
        res = pcodex.poll_gpt()
        self.assertEqual(self.windows(res)["five_hour"]["used_pct"], 30.0)
        self.assertEqual(self.windows(res)["seven_day"]["used_pct"], 55.0)
        self.assertEqual(ex._provider_effective(res), 55.0)

    def test_rounded_length_takes_its_slot(self):
        patch_codex_reads(self, wham_usage(primary=20.0, primary_secs=604800 - 60))
        self.assertEqual(list(self.windows(pcodex.poll_gpt())), ["seven_day"])

    def test_zero_length_window_does_not_apply(self):
        patch_codex_reads(self, wham_usage(primary=12.0, secondary=44.0, secondary_secs=0))
        res = pcodex.poll_gpt()
        self.assertEqual(list(self.windows(res)), ["seven_day"])
        self.assertEqual(ex._provider_effective(res), 12.0)

    def test_window_of_another_length_is_its_own_window_and_binds(self):
        # A 1-day window is not forced into the 5h slot (O-D6) and is not
        # dropped either: it is window:1d, account-wide, so it binds every
        # route like the others (§1.5).
        patch_codex_reads(self, wham_usage(primary=12.0, secondary=90.0,
                                           secondary_secs=86400))
        res = pcodex.poll_gpt()
        w = self.windows(res)
        self.assertEqual(list(w), ["seven_day", "window:1d"])
        self.assertEqual((w["window:1d"]["period_s"], w["window:1d"]["scope"]), (86400, None))
        self.assertEqual(ex._provider_effective(res), 90.0)

    def test_allowed_false_closes_the_allowance_below_100(self):
        """The provider's gate wins over the percent (50 §1.5)."""
        patch_codex_reads(self, wham_usage(primary=40.0, allowed=False))
        res = pcodex.poll_gpt()
        pot = ex._reading_pot(res["reading"], ex.ALLOWANCE_POT)
        self.assertIs(pot["open"], False)
        self.assertIs(pot["limit_reached"], True)
        self.assertEqual(self.windows(res)["seven_day"]["used_pct"], 40.0,
                         "the window keeps its honest percent")
        self.assertEqual(ex._provider_effective(res), 100.0, "effective reads spent")

    def test_overrun_past_100_is_kept(self):
        patch_codex_reads(self, wham_usage(primary=100.08, allowed=False))
        res = pcodex.poll_gpt()
        self.assertEqual(self.windows(res)["seven_day"]["remaining"], 0.0)
        self.assertEqual(ex._provider_effective(res), 100.08)

    def test_credits_and_spend_control_are_read(self):
        patch_codex_reads(self, wham_usage(
            primary=100.0, allowed=False, spend_reached=False,
            credits={"has_credits": True, "balance": "250.5000000000"}))
        pot = ex._reading_pot(pcodex.poll_gpt()["reading"], ex.MONEY_POT)
        self.assertEqual((pot["unit"], pot["open"], pot["open_basis"]),
                         ("codex_credit", True, "derived"))
        self.assertEqual(ex._reading_limit(pot, "balance"),
                         {"id": "balance", "kind": "balance", "amount": 250.5,
                          "advisory": True, "basis": "exact"})
        self.assertEqual(ex._reading_limit(pot, "spend_control"),
                         {"id": "spend_control", "kind": "spend_cap",
                          "setter": "owner@provider", "reached": False, "limit": None,
                          "basis": "exact"})
        self.assertIs(ex._reading_limit(pot, "topup_cap")["reached"], False)

    def test_money_gate_is_the_inferred_combination(self):
        # (has_credits ∨ unlimited) ∧ ¬overage_limit_reached ∧ ¬spend_control.reached
        cases = [({"has_credits": True}, False, True),
                 ({"unlimited": True}, False, True),
                 ({}, False, False),
                 ({"has_credits": True, "overage_limit_reached": True}, False, False),
                 ({"has_credits": True}, True, False)]
        for credits, spend, want in cases:
            with self.subTest(credits=credits, spend=spend):
                patch_codex_reads(self, wham_usage(credits=credits, spend_reached=spend))
                pot = ex._reading_pot(pcodex.poll_gpt()["reading"], ex.MONEY_POT)
                self.assertIs(pot["open"], want)

    def test_unparseable_balance_is_unknown_not_zero(self):
        for bal in (None, "", "n/a", "NaN"):
            with self.subTest(balance=bal):
                patch_codex_reads(self, wham_usage(credits={"has_credits": True,
                                                            "balance": bal}))
                pot = ex._reading_pot(pcodex.poll_gpt()["reading"], ex.MONEY_POT)
                b = ex._reading_limit(pot, "balance")
                self.assertEqual((b["amount"], b["basis"]), (None, "unknown"))
                self.assertIs(pot["open"], True, "the balance never gates (11 S4)")

    def test_absent_spend_control_is_unknown(self):
        body = wham_usage(credits={"has_credits": True})
        body["spend_control"] = None
        patch_codex_reads(self, body)
        pot = ex._reading_pot(pcodex.poll_gpt()["reading"], ex.MONEY_POT)
        self.assertEqual(ex._reading_limit(pot, "spend_control")["basis"], "unknown")

    def test_additional_rate_limits_are_scoped_windows_with_their_own_gate(self):
        """O-D7: each additional limit is read generically. Its windows are
        scoped to the limit's name, carry its allowed gate, and bind only the
        route whose scope (a provider member's model) that name is."""
        extra = [{"limit_name": "test_cap", "metered_feature": "test_feature",
                  "rate_limit": {"allowed": False, "limit_reached": True,
                                 "primary_window": wham_window(77.0, 604800),
                                 "secondary_window": None}}]
        patch_codex_reads(self, wham_usage(primary=12.0, additional=extra))
        res = pcodex.poll_gpt()
        w = self.windows(res)["scoped:test_cap:7d"]
        self.assertEqual((w["scope"], w["used_pct"], w["open"], w["open_basis"]),
                         ("test_cap", 77.0, False, "exact"))
        self.assertEqual((w["feature"], w["limit_reached"]), ("test_feature", True))
        self.assertEqual(ex._provider_effective(res), 12.0, "binds no other route")
        self.assertEqual(ex._reading_effective(res, "test_cap"), 100.0,
                         "its own gate closed: spent for the route it binds")
        self.assertEqual(ex._reading_effective(res, "other-model"), 12.0)

    def test_additional_limit_without_a_window_still_gates(self):
        extra = [{"limit_name": "gate_only",
                  "rate_limit": {"allowed": False, "primary_window": None}}]
        patch_codex_reads(self, wham_usage(primary=12.0, additional=extra))
        res = pcodex.poll_gpt()
        w = self.windows(res)["scoped:gate_only"]
        self.assertEqual((w["used_pct"], w["basis"], w["open"]), (None, "unknown", False))
        self.assertEqual(ex._reading_effective(res, "gate_only"), 100.0)

    def test_malformed_additional_limit_is_logged_not_read(self):
        patch_codex_reads(self, wham_usage(primary=12.0, additional=["junk"]))
        res = pcodex.poll_gpt()
        self.assertEqual(list(self.windows(res)), ["seven_day"])
        self.assertTrue(any("additional_rate_limits[0]" in ln for ln in self.logged))

    def test_reset_options_come_from_the_second_read(self):
        patch_codex_reads(self, wham_usage(), wham_reset_credits(available=2, eligible=False))
        pot = ex._reading_pot(pcodex.poll_gpt()["reading"], ex.ALLOWANCE_POT)
        self.assertEqual(pot["reset_options"],
                         {"reset_credits": 2, "purchase_eligible": False, "basis": "exact"})

    def test_reset_read_failure_leaves_options_unknown_not_the_poll(self):
        def boom(tok, acct_id):
            raise urllib.error.HTTPError(pcodex.OPENAI_RESET_CREDITS_URL, 503, "x", {}, None)
        patch_codex_reads(self, wham_usage(primary=12.0), boom)
        res = pcodex.poll_gpt()
        self.assertEqual(res["ok"], 1, "the usage read already measured")
        ro = ex._reading_pot(res["reading"], ex.ALLOWANCE_POT)["reset_options"]
        self.assertEqual((ro["basis"], ro["reset_credits"], ro["err"]),
                         ("unknown", None, "HTTP 503"))
        self.assertTrue(any("reset-credits" in ln for ln in self.logged))

    def test_missing_rate_limit_is_failure(self):
        body = wham_usage()
        del body["rate_limit"]
        patch_codex_reads(self, body)
        res = pcodex.poll_gpt()
        self.assertEqual(res["ok"], 0)
        self.assertIn("rate_limit.allowed", res["err"])

    def test_no_weekly_window_is_a_reading_not_a_failure(self):
        # A plan with only a 5-hour window reads as one: the provider's gate
        # and that window, nothing invented for the slot it lacks.
        patch_codex_reads(self, wham_usage(primary=10.0, primary_secs=18000))
        res = pcodex.poll_gpt()
        self.assertEqual(res["ok"], 1, res["err"])
        self.assertEqual(list(self.windows(res)), ["five_hour"])
        self.assertEqual(ex._provider_effective(res), 10.0)

    def test_usage_http_error_fails_loud(self):
        """No salvage: the GET answers 200 when the window is spent, so an error
        status is a failed poll, 429 included."""
        for code in (429, 500, 503):
            with self.subTest(code=code):
                def boom(tok, acct_id, c=code):
                    raise urllib.error.HTTPError(pcodex.OPENAI_USAGE_URL, c, "no",
                                                 {"Retry-After": "120"}, None)
                patch_codex_reads(self, boom)
                res = pcodex.poll_gpt()
                self.assertEqual(res["ok"], 0)
                self.assertEqual(res["http"], code)
                self.assertIn(f"usage HTTP {code}", res["err"])
                self.assertEqual(res["retry_after"], 120)

    def test_usage_401_refreshes_once_and_retries(self):
        """The refresh path is unchanged: one forced refresh on a usage 401, and
        the reset-credits read never refreshes."""
        calls = []

        def usage(tok, acct_id):
            calls.append(tok)
            if len(calls) == 1:
                raise urllib.error.HTTPError(pcodex.OPENAI_USAGE_URL, 401, "no", {}, None)
            return wham_usage(primary=9.0)
        refreshes = []
        orig = pcodex._oauth_refresh
        self.addCleanup(setattr, pcodex, "_oauth_refresh", orig)
        pcodex._oauth_refresh = lambda creds, url, cid, persist: refreshes.append(1) or "at-refreshed"
        seen = {}

        def resets(tok, acct_id):
            seen["tok"] = tok
            raise urllib.error.HTTPError(pcodex.OPENAI_RESET_CREDITS_URL, 401, "no", {}, None)
        patch_codex_reads(self, usage, resets)
        res = pcodex.poll_gpt()
        self.assertEqual(calls, ["at-test-1", "at-refreshed"])
        self.assertEqual(len(refreshes), 1)
        self.assertEqual(seen["tok"], "at-refreshed", "the second read rides the new token")
        self.assertEqual(res["ok"], 1)
        self.assertEqual(ex._reading_pot(res["reading"], ex.ALLOWANCE_POT)
                         ["reset_options"]["err"], "HTTP 401")

    def test_both_reads_are_free_gets_with_the_codex_headers(self):
        """The wire: two GETs, no body, bearer + workspace id + codex-cli pair."""
        seen = []

        class Resp:
            def __init__(self, body):
                self._b = json.dumps(body).encode()

            def read(self):
                return self._b

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake(req, timeout=None):
            seen.append(req)
            return Resp(wham_usage() if req.full_url == pcodex.OPENAI_USAGE_URL
                        else wham_reset_credits())
        orig = ex.urlopen
        self.addCleanup(setattr, ex, "urlopen", orig)
        ex.urlopen = fake
        self.assertEqual(pcodex.poll_gpt()["ok"], 1)
        self.assertEqual([r.full_url for r in seen],
                         [pcodex.OPENAI_USAGE_URL, pcodex.OPENAI_RESET_CREDITS_URL])
        for r in seen:
            self.assertEqual(r.get_method(), "GET")
            self.assertIsNone(r.data)
            self.assertEqual(r.get_header("Authorization"), "Bearer at-test-1")
            self.assertEqual(r.get_header("Chatgpt-account-id"), "acct-uuid-test")
            self.assertEqual(r.get_header("Originator"), "codex-cli")
            self.assertTrue(r.get_header("User-agent").startswith("codex-cli/"))

    def test_no_creds_file_is_disabled(self):
        clear_openai_creds()
        res = pcodex.poll_gpt()
        self.assertEqual(res["ok"], 0)
        self.assertIn("no codex creds", res["err"])


class TestOldCodexPollIsGone(unittest.TestCase):
    """C1 deletes the billed POST /responses poll outright: no flag, no fallback.
    The names are assembled so a grep of the repo for them finds nothing."""

    def test_names_are_gone(self):
        for name in ("_get_gpt" + "_usage", "OPENAI_POLL" + "_MODEL",
                     "OPENAI_RESPONSES" + "_URL", "_codex_quota" + "_headers",
                     "_gpt" + "_window", "_gpt" + "_credits"):
            with self.subTest(name=name):
                self.assertFalse(hasattr(ex, name) or hasattr(pcodex, name))

    def test_source_never_posts_to_responses(self):
        src = ""
        for mod in (ex, pcodex):
            with open(mod.__file__) as f:
                src += f.read()
        for needle in ("OPENAI_POLL" + "_MODEL", "_get_gpt" + "_usage",
                       "backend-api/codex/" + "responses"):
            self.assertNotIn(needle, src)


class TestCodexMoneyGate(unittest.TestCase):
    """C1 — (has_credits ∨ unlimited) ∧ ¬overage_limit_reached ∧ ¬spend_control.reached.
    The balance is advisory and never enters it (50 §1.5, M5)."""

    def gate(self, has=False, unl=False, overage=False, spend=False, balance=None):
        return pcodex._codex_credits_pot({"has_credits": has, "unlimited": unl,
                                      "overage_limit_reached": overage,
                                      "balance": balance},
                                     {"reached": spend})["open"]

    def test_each_input(self):
        self.assertIs(self.gate(has=True), True)
        self.assertIs(self.gate(unl=True), True, "unlimited is kept")
        self.assertIs(self.gate(), False, "neither enabled nor unlimited")
        self.assertIs(self.gate(has=True, overage=True), False, "top-up cap reached")
        self.assertIs(self.gate(has=True, spend=True), False, "spend_control reached")
        self.assertIs(self.gate(unl=True, overage=True), False)
        self.assertIs(self.gate(unl=True, spend=True), False)

    def test_balance_never_gates(self):
        self.assertIs(self.gate(has=True, overage=True, balance=500.0), False,
                      "a balance does not open a closed gate")
        self.assertIs(self.gate(has=True, balance=0.0), True,
                      "a zero balance does not close an open one")

    def test_no_credits_object_is_no_pot(self):
        # A read with no credits object says nothing of the pot: no pot in the
        # reading, so the standing's credits are unknown, never closed.
        self.assertIsNone(pcodex._codex_credits_pot(None, None))
        body = wham_usage()
        del body["credits"]
        r = pcodex._codex_reading(body, None, time.time())[0]
        self.assertIsNone(ex._reading_pot(r, ex.MONEY_POT))
        st = ex._account_standing("gpt", {"name": "gpt", "kind": "provider", "provider": "gpt",
                                          "good_ts": time.time(), "reading": r},
                                  "gpt", time.time())
        self.assertIsNone(st["money"]["credits"]["open"])

    def test_missing_spend_control_does_not_close(self):
        self.assertIs(pcodex._codex_credits_pot({"has_credits": True}, None)["open"], True)


class TestGptMetricsRender(unittest.TestCase):
    """#31 — the generic provider_* render loop picks up the gpt series; C1 adds
    money_balance."""

    L = staticmethod(lambda t, m, **kw: TestRenderMetrics().line(t, m, **kw))

    def test_gpt_series_alongside_kimi(self):
        reset(kimi_result(five_hour=8.0, weekly=2.0), gpt_result(weekly=12.0))
        t = ex.render()
        L = self.L
        self.assertEqual(L(t, "provider_poll_success", provider="gpt"), 1)
        self.assertEqual(L(t, "provider_effective_percent", provider="gpt"), 12.0)
        self.assertEqual(L(t, "provider_quota_utilization_percent",
                           provider="gpt", window="seven_day"), 12.0)
        self.assertEqual(L(t, "provider_membership_level", provider="gpt", level="pro"), 1)
        self.assertEqual(L(t, "provider_effective_percent", provider="kimi"), 8.0,
                         "kimi series untouched by the gpt poller")
        self.assertNotIn('window="scoped_', t)

    def test_a_provider_window_reset_rides_provider_quota_alone(self):
        # claude_usage_window_resets_at_seconds keeps its samples: the
        # Anthropic accounts' resets, never a provider's, which ride
        # provider_quota_* as they always have.
        a = acct(ACCT_A, 30)
        a["raw"]["seven_day"]["resets_at"] = "2027-01-01T00:00:00Z"
        reset(a, gpt_result(weekly=12.0, five_hour=5.0), kimi_result())
        t = ex.render()
        resets = [ln for ln in t.splitlines() if ln.startswith("claude_usage_window_resets_at_seconds{")]
        self.assertTrue(any(f'account="{ACCT_A}"' in ln for ln in resets), resets)
        self.assertEqual([ln for ln in resets if 'account="gpt"' in ln or 'account="kimi"' in ln], [])
        self.assertIn('provider_quota_window_resets_at_seconds{provider="gpt"', t)

    def test_gpt_failure_retains_last_good(self):
        reset(gpt_result(ok=0, http=401, err="token refresh HTTP 401", good_age=120))
        t = ex.render()
        L = self.L
        self.assertEqual(L(t, "provider_poll_success", provider="gpt"), 0)
        self.assertEqual(L(t, "provider_poll_http_code", provider="gpt"), 401)
        self.assertGreaterEqual(L(t, "provider_data_age_seconds", provider="gpt"), 100)
        self.assertEqual(L(t, "provider_effective_percent", provider="gpt"), 12.0,
                         "last-good usage keeps serving while polls fail")

    def test_money_balance_per_account(self):
        reset(gpt_result(weekly=100.0, allowed=False,
                         credits={"has_credits": True, "balance": 250.5}),
              kimi_result())
        t = ex.render()
        self.assertIn("# TYPE money_balance gauge", t)
        self.assertEqual(self.L(t, "money_balance", provider="gpt", account="gpt",
                                pot="credits", unit="codex_credit"), 250.5)
        self.assertEqual(self.L(t, "provider_effective_percent", provider="gpt"), 100.0)
        self.assertNotIn('money_balance{provider="kimi"', t)

    def test_unknown_balance_is_absent_not_zero(self):
        reset(gpt_result(credits={"has_credits": True, "balance": None}))
        self.assertIsNone(self.L(ex.render(), "money_balance", provider="gpt"))

    def test_allowed_false_reads_spent_on_metrics(self):
        reset(gpt_result(weekly=40.0, allowed=False))
        t = ex.render()
        self.assertEqual(self.L(t, "provider_effective_percent", provider="gpt"), 100.0)
        self.assertEqual(self.L(t, "provider_account_effective_percent",
                                provider="gpt", account="gpt"), 100.0)
        self.assertEqual(self.L(t, "provider_quota_utilization_percent",
                                provider="gpt", window="seven_day"), 40.0)


class TestGptPreferredMoney(HTTPBase):
    """/preferred accounts{} carry the allowance gate, the reset options and
    the credits pot (balance advisory, spend_control, the derived gate); the
    C2 targets key carries windows only."""

    def setUp(self):
        r = gpt_result(weekly=100.0, allowed=False, spend_reached=False,
                       credits={"has_credits": True, "balance": 250.5})
        r["reset_options"] = {"reset_credits": 0, "purchase_eligible": True, "basis": "exact"}
        reset(r, kimi_result())

    def test_accounts_carry_allowance_and_money(self):
        _, p = self.get("/preferred")
        a = p["accounts"]["gpt"]["allowance"]
        self.assertEqual((a["open"], a["open_basis"], a["limit_reached"]), (False, "exact", True))
        self.assertEqual(a["reset_options"],
                         {"reset_credits": 0, "purchase_eligible": True, "basis": "exact"})
        m = p["accounts"]["gpt"]["money"]["credits"]
        self.assertEqual((m["unit"], m["open"], m["open_basis"], m["balance"]),
                         ("codex_credit", True, "derived", 250.5))
        self.assertEqual((m["has_credits"], m["unlimited"]), (True, False))
        self.assertEqual(m["limits"]["spend_control"]["reached"], False)
        self.assertEqual(m["limits"]["topup_cap"]["reached"], False)
        self.assertIsNone(p["accounts"]["kimi"]["money"]["credits"]["open"])

    def test_spend_control_reached_closes_money_with_balance_present(self):
        r = gpt_result(weekly=100.0, allowed=False, spend_reached=True,
                       credits={"has_credits": True, "balance": 250.5})
        reset(r)
        _, p = self.get("/preferred")
        m = p["accounts"]["gpt"]["money"]["credits"]
        self.assertIs(m["open"], False)
        self.assertEqual(m["balance"], 250.5)


class _FakeTokenResp:
    def __init__(self, data):
        self._d = json.dumps(data).encode()

    def read(self):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class TestOAuthMachinery(unittest.TestCase):
    """#30 — generic rotate-and-persist OAuth helpers driving the codex chain."""

    def setUp(self):
        seed_openai_creds()
        pcodex._openai_pool = None
        self.creds = pcodex._load_openai_creds()["gpt"]
        self._orig_urlopen = ex.urlopen
        self.calls = []

    def tearDown(self):
        ex.urlopen = self._orig_urlopen
        clear_openai_creds()

    def _patch_token(self, payload):
        def fake(req, timeout=None):
            self.calls.append(json.loads(req.data.decode()))
            return _FakeTokenResp(payload)
        ex.urlopen = fake

    def _patch_refuse(self, code=401):
        def fake(req, timeout=None):
            self.calls.append(json.loads(req.data.decode()))
            raise urllib.error.HTTPError(req.full_url, code, "nope", None, None)
        ex.urlopen = fake

    def test_refresh_rotates_and_persists(self):
        self._patch_token({"access_token": "at-2", "refresh_token": "rt-2",
                           "expires_in": 864000})
        tok = pcodex._oauth_refresh(self.creds, pcodex.OPENAI_TOKEN_URL, pcodex.OPENAI_CLIENT_ID,
                                pcodex._persist_openai_creds)
        self.assertEqual(tok, "at-2")
        self.assertEqual(self.creds["refresh_token"], "rt-2")
        self.assertEqual(self.calls[0]["refresh_token"], "rt-test-1")
        self.assertEqual(self.calls[0]["client_id"], pcodex.OPENAI_CLIENT_ID)
        with open(pcodex.OPENAI_CODEX_CREDS_FILE) as f:
            on_disk = json.load(f)
        self.assertEqual(on_disk["refresh_token"], "rt-2",
                         "rotation persisted atomically — losing it kills the chain")
        self.assertEqual(on_disk["account_id"], "acct-uuid-test")

    def test_ensure_cached_outside_margin(self):
        self._patch_refuse()
        tok = pcodex._oauth_ensure_token(self.creds, pcodex.OPENAI_TOKEN_URL,
                                     pcodex.OPENAI_CLIENT_ID, pcodex._persist_openai_creds,
                                     margin=3600)
        self.assertEqual(tok, "at-test-1")
        self.assertEqual(self.calls, [], "no network when the token is comfortably fresh")

    def test_ensure_refreshes_inside_margin(self):
        self.creds["expires_at"] = time.time() + 60
        self._patch_token({"access_token": "at-3", "refresh_token": "rt-3"})
        tok = pcodex._oauth_ensure_token(self.creds, pcodex.OPENAI_TOKEN_URL,
                                     pcodex.OPENAI_CLIENT_ID, pcodex._persist_openai_creds,
                                     margin=3600)
        self.assertEqual(tok, "at-3")

    def test_throttle_defers_early_refresh(self):
        self.creds["expires_at"] = time.time() + 1800       # inside margin...
        self.creds["earliest_refresh_at"] = time.time() + 900  # ...but throttled
        self._patch_refuse()
        tok = pcodex._oauth_ensure_token(self.creds, pcodex.OPENAI_TOKEN_URL,
                                     pcodex.OPENAI_CLIENT_ID, pcodex._persist_openai_creds,
                                     margin=3600)
        self.assertEqual(tok, "at-test-1", "still-valid token rides out the throttle")
        self.assertEqual(self.calls, [])

    def test_throttle_ignored_when_expired(self):
        self.creds["expires_at"] = time.time() - 10
        self.creds["earliest_refresh_at"] = time.time() + 900
        self._patch_token({"access_token": "at-4", "refresh_token": "rt-4"})
        tok = pcodex._oauth_ensure_token(self.creds, pcodex.OPENAI_TOKEN_URL,
                                     pcodex.OPENAI_CLIENT_ID, pcodex._persist_openai_creds,
                                     margin=3600)
        self.assertEqual(tok, "at-4", "hard-expired refreshes regardless of throttle")

    def test_poll_adopts_reseeded_file_on_refresh_401(self):
        # In-memory chain is dead; an operator re-seeded the file out-of-band —
        # poll_gpt's 401 escape hatch must adopt it without a restart.
        seed_openai_creds(refresh_token="rt-new", access_token="at-new",
                          account_id="acct-new")
        pcodex._openai_pool = {"gpt": {"refresh_token": "rt-dead", "access_token": "",
                                   "expires_at": 0, "account_id": "acct-old"}}
        pcodex._openai_pool_shape = "legacy"
        pin_openai_cache_mtime()  # defeat the mtime auto-adopt; force the 401 path
        self._patch_refuse(401)  # the rt-dead refresh 401s
        seen = {}

        def fake_usage(tok, acct_id):
            seen["tok"], seen["acct"] = tok, acct_id
            return wham_usage(primary=9.0)
        patch_codex_reads(self, fake_usage)
        res = pcodex.poll_gpt()
        self.assertEqual(res["ok"], 1)
        self.assertEqual(seen["tok"], "at-new")
        self.assertEqual(seen["acct"], "acct-new")
        self.assertEqual(len(self.calls), 1, "exactly one refresh attempt on the dead chain")


class TestGptMemberUsableIntegration(HTTPBase):
    """#31 end state — a polled gpt target flips the frontier gpt member from
    no_target to usable (fixture priorities are local to frontier_cfg)."""

    def setUp(self):
        ex._classes = load_classes(frontier_cfg())
        reset(acct(ACCT_A, 41, caps={"Opus": 55}), acct(ACCT_B, 30, caps={"Opus": 20}),
              kimi_result(five_hour=8.0, weekly=2.0), gpt_result(weekly=12.0))
        ex.compute_preferred()

    def tearDown(self):
        ex._classes = []

    def test_polled_gpt_member_becomes_usable(self):
        _, p = self.get("/preferred")
        m = p["classes"]["frontier"]["members"][1]
        self.assertEqual(m["model"], "gpt-5.6")
        self.assertEqual(m["targets"], {"gpt": 12.0})
        self.assertTrue(m["usable"])
        self.assertEqual(m["status"], "ok")

    def test_gpt_in_accounts_payload(self):
        _, p = self.get("/preferred")
        a = p["accounts"]["gpt"]
        self.assertEqual(a["provider"], "gpt")
        self.assertEqual(set(a["allowance"]["windows"]), {"seven_day"})
        self.assertEqual(a["allowance"]["windows"]["seven_day"]["used_pct"], 12.0)


class TestOpenAICredsPool(unittest.TestCase):
    """#71 — the codex creds file accepts legacy dict AND array form; loads are
    mtime-guarded for live adoption; persistence is shape-preserving (a
    rolled-back brain must never find a list on disk it can't .get() on)."""

    def setUp(self):
        clear_openai_creds()
        self.addCleanup(clear_openai_creds)

    def test_legacy_dict_is_a_pool_of_one_named_gpt(self):
        seed_openai_creds()
        pool = pcodex._load_openai_creds()
        self.assertEqual(set(pool), {"gpt"})
        self.assertEqual(pool["gpt"]["refresh_token"], "rt-test-1")
        self.assertEqual(pcodex._openai_pool_shape, "legacy")
        self.assertIsNone(pcodex._openai_account_names(),
                          "legacy shape = single-target poll mode")

    def test_array_form_keys_by_name(self):
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        pool = pcodex._load_openai_creds()
        self.assertEqual(set(pool), {"gpt-a", "gpt-b"})
        self.assertEqual(pcodex._openai_pool_shape, "array")
        self.assertEqual(pcodex._openai_account_names(), ["gpt-a", "gpt-b"])

    def test_array_entries_validated_and_collisions_skipped(self):
        reset()  # installs ACCT_A/ACCT_B into psub._creds
        seed_openai_pool({"name": "gpt-a"},
                         {"name": ""},                       # no name
                         {"name": "gpt-x", "refresh_token": ""},  # no RT
                         {"name": "gpt-a"},                  # duplicate
                         {"name": ACCT_A},                   # anthropic collision
                         {"name": "kimi"})                   # provider collision
        pool = pcodex._load_openai_creds()
        self.assertEqual(set(pool), {"gpt-a"})

    def test_out_of_band_rewrite_adopted_via_mtime(self):
        seed_openai_creds()
        self.assertEqual(set(pcodex._load_openai_creds()), {"gpt"})
        # Operator converts legacy -> array to add an account: next non-force
        # load must see it — live adoption, no restart, no force flag.
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        pcodex._openai_pool = {"gpt": {"refresh_token": "stale"}}  # warm wrong cache
        self.assertEqual(set(pcodex._load_openai_creds()), {"gpt-a", "gpt-b"})

    def test_persist_preserves_legacy_shape(self):
        seed_openai_creds()
        pool = pcodex._load_openai_creds()
        pool["gpt"]["refresh_token"] = "rt-rotated"
        pcodex._persist_openai_creds()
        with open(pcodex.OPENAI_CODEX_CREDS_FILE) as f:
            on_disk = json.load(f)
        self.assertIsInstance(on_disk, dict, "legacy file must NEVER become a list")
        self.assertEqual(on_disk["refresh_token"], "rt-rotated")

    def test_persist_preserves_array_shape_and_sibling_chains(self):
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        pool = pcodex._load_openai_creds()
        pool["gpt-a"]["refresh_token"] = "rt-a-rotated"
        pcodex._persist_openai_creds()
        with open(pcodex.OPENAI_CODEX_CREDS_FILE) as f:
            on_disk = json.load(f)
        self.assertIsInstance(on_disk, list)
        by_name = {e["name"]: e for e in on_disk}
        self.assertEqual(by_name["gpt-a"]["refresh_token"], "rt-a-rotated")
        self.assertEqual(by_name["gpt-b"]["refresh_token"], "rt-gpt-b",
                         "rotating one chain must not touch its siblings")

    def test_poll_gpt_account_selects_chain_and_names_result(self):
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        seen = {}

        def fake_usage(tok, acct_id):
            seen["tok"], seen["acct"] = tok, acct_id
            return wham_usage(primary=7.0)
        patch_codex_reads(self, fake_usage)
        res = pcodex.poll_gpt(account="gpt-b")
        self.assertEqual(res["ok"], 1)
        self.assertEqual(res["name"], "gpt-b")
        self.assertEqual(res["provider"], "gpt")
        self.assertEqual(seen["tok"], "at-gpt-b")
        self.assertEqual(seen["acct"], "acct-gpt-b",
                         "each pool entry rides its own workspace id")

    def test_poll_gpt_unknown_account_fails_loud(self):
        seed_openai_pool({"name": "gpt-a"})
        res = pcodex.poll_gpt(account="gpt-nope")
        self.assertEqual(res["ok"], 0)
        self.assertEqual(res["name"], "gpt-nope", "the target keeps its identity")
        self.assertIn("no codex creds", res["err"])


class TestProviderPoolPolicy(unittest.TestCase):
    """#71 — _compute_provider_pools: the lane policy verbatim, keyed
    provider:<id>, over each provider's polled targets."""

    def pool_state(self, pid="gpt"):
        with ex._plock:
            return dict(ex._preferred["providers"].get(pid) or {})

    def age_pools(self, seconds=ex.SW_DWELL + 100):
        with ex._plock:
            for s in ex._preferred["providers"].values():
                s["last_switch"] = time.time() - seconds

    def test_pool_of_one_bootstraps_and_stays(self):
        reset(gpt_result(weekly=12.0))
        ex.compute_preferred()
        self.assertEqual(self.pool_state()["account"], "gpt")
        self.age_pools()
        ex.compute_preferred()
        self.assertEqual(self.pool_state()["account"], "gpt")

    def test_bootstrap_picks_coolest(self):
        reset(gpt_result(weekly=60.0, name="gpt-a"), gpt_result(weekly=10.0, name="gpt-b"))
        ex.compute_preferred()
        self.assertEqual(self.pool_state()["account"], "gpt-b")

    def test_incumbent_wins_until_forced(self):
        reset(gpt_result(weekly=10.0, name="gpt-a"), gpt_result(weekly=60.0, name="gpt-b"))
        ex.compute_preferred()
        self.assertEqual(self.pool_state()["account"], "gpt-a")
        # a is warmer now but below HIGH — no drift toward the cooler account
        with ex._lock:
            ex._state["gpt-a"] = gpt_result(weekly=70.0, name="gpt-a")
            ex._state["gpt-b"] = gpt_result(weekly=20.0, name="gpt-b")
        self.age_pools()
        ex.compute_preferred()
        self.assertEqual(self.pool_state()["account"], "gpt-a")

    def test_hot_plus_margin_evicts_to_coolest(self):
        reset(gpt_result(weekly=10.0, name="gpt-a"), gpt_result(weekly=20.0, name="gpt-b"))
        ex.compute_preferred()
        with ex._lock:
            ex._state["gpt-a"] = gpt_result(weekly=ex.SW_HIGH + 5, name="gpt-a")
        self.age_pools()
        ex.compute_preferred()
        self.assertEqual(self.pool_state()["account"], "gpt-b")

    def test_dwell_gates_the_flip(self):
        reset(gpt_result(weekly=10.0, name="gpt-a"), gpt_result(weekly=20.0, name="gpt-b"))
        ex.compute_preferred()
        with ex._plock:  # bootstrap leaves last_switch=0; simulate a recent switch
            ex._preferred["providers"]["gpt"]["last_switch"] = time.time()
        with ex._lock:
            ex._state["gpt-a"] = gpt_result(weekly=ex.SW_HIGH + 5, name="gpt-a")
        ex.compute_preferred()  # dwell has NOT elapsed
        self.assertEqual(self.pool_state()["account"], "gpt-a")
        self.age_pools()
        ex.compute_preferred()  # dwell elapsed -> eviction proceeds
        self.assertEqual(self.pool_state()["account"], "gpt-b")

    def test_kimi_is_a_pool_of_one_through_the_same_path(self):
        reset(kimi_result())
        ex.compute_preferred()
        self.assertEqual(self.pool_state("kimi")["account"], "kimi")

    def test_member_backing_follows_pool_incumbent(self):
        ex._classes = load_classes(frontier_cfg())
        self.addCleanup(setattr, ex, "_classes", [])
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30),
              gpt_result(weekly=10.0, name="gpt-a"), gpt_result(weekly=60.0, name="gpt-b"))
        ex.compute_preferred()
        member = next(m for m in ex._classes[0]["members"] if m.get("provider") == "gpt")
        with ex._lock:
            snap = list(ex._state.values())
        with ex._plock:
            backing = ex._member_backing(member, snap)
            self.assertEqual(backing["name"], "gpt-a", "the pool incumbent speaks")
        v = class_verdicts()["gpt/gpt-5.6"]
        self.assertEqual((v["account"], v["effective"]), ("gpt-a", 10.0),
                         "both accounts are included: the incumbent wins the tie")

    def test_state_v4_roundtrip_and_rollback_view(self):
        reset(gpt_result(weekly=12.0, name="gpt-a"), gpt_result(weekly=50.0, name="gpt-b"))
        ex.compute_preferred()
        with ex._plock:
            ex._save_state()
        with open(ex.STATE_FILE) as f:
            saved = json.load(f)
        self.assertEqual(saved["providers"]["gpt"]["account"], "gpt-a")
        # roll-forward restore keeps the incumbent; unconfigured providers drop
        out = ex._migrate_state(saved, ex._lane_names, (), ["gpt"])
        self.assertEqual(out["providers"]["gpt"]["account"], "gpt-a")
        out = ex._migrate_state(saved, ex._lane_names, (), [])
        self.assertEqual(out["providers"], {})
        # a rolled-back (pre-#71) brain copies only keys it knows — the flat
        # legacy view is untouched by the additive providers key
        for k in ("account", "since", "reason", "last_switch", "pinned"):
            self.assertIn(k, saved)


class TestOpenAITokenPool(HTTPBase):
    """#71 — /openai-token?account= dispenses per-chain; no param = incumbent."""

    def setUp(self):
        clear_openai_creds()
        self.addCleanup(clear_openai_creds)

    def test_account_param_dispenses_that_chain(self):
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        code, d = self.get("/openai-token?account=gpt-b", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["name"], "gpt-b")
        self.assertEqual(d["access_token"], "at-gpt-b")
        self.assertEqual(d["account_id"], "acct-gpt-b")
        self.assertNotIn("account", d, "account_id is the workspace UUID; don't shadow it")

    def test_unknown_account_404(self):
        seed_openai_pool({"name": "gpt-a"})
        code, d = self.get("/openai-token?account=gpt-nope", key="test-switch-key")
        self.assertEqual(code, 404)
        self.assertIn("unknown codex account", d["error"])

    def test_no_param_follows_pool_incumbent(self):
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        reset(gpt_result(weekly=60.0, name="gpt-a"), gpt_result(weekly=10.0, name="gpt-b"))
        ex.compute_preferred()  # bootstrap -> coolest = gpt-b
        code, d = self.get("/openai-token", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["name"], "gpt-b")
        self.assertEqual(d["access_token"], "at-gpt-b")

    def test_legacy_file_no_param_is_the_pre_pool_response(self):
        seed_openai_creds()
        code, d = self.get("/openai-token", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["name"], "gpt")
        self.assertEqual(d["access_token"], "at-test-1")
        self.assertEqual(d["account_id"], "acct-uuid-test")


class TestPoolPayloadAndMetrics(HTTPBase):
    """#71 — /preferred publishes member incumbent + top-level providers; render
    dedupes the legacy provider_* series and adds provider_account_*."""

    def setUp(self):
        ex._classes = load_classes(frontier_cfg())
        self.addCleanup(setattr, ex, "_classes", [])
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30), kimi_result(),
              gpt_result(weekly=10.0, name="gpt-a"), gpt_result(weekly=60.0, name="gpt-b"))
        ex.compute_preferred()

    def test_preferred_publishes_member_incumbents(self):
        _, p = self.get("/preferred")
        members = {m["id"]: m for m in p["classes"]["frontier"]["members"]}
        self.assertEqual(members["gpt/gpt-5.6"]["incumbent"], "gpt-a")
        self.assertEqual(members["gpt/gpt-5.6"]["targets"],
                         {"gpt-a": 10.0, "gpt-b": 60.0})
        self.assertEqual(members["lane:opus"]["incumbent"],
                         p["lanes"]["opus"]["account"],
                         "anthropic members mirror the lane incumbent — symmetric")

    def test_preferred_publishes_provider_pools(self):
        _, p = self.get("/preferred")
        gpt = p["providers"]["gpt"]
        self.assertEqual(gpt["account"], "gpt-a")
        self.assertEqual(gpt["pool"], {"gpt-a": 10.0, "gpt-b": 60.0})
        self.assertEqual(p["providers"]["kimi"]["pool"], {"kimi": 8.0})

    def test_render_emits_legacy_series_once_per_provider(self):
        t = ex.render()
        gpt_eff = [ln for ln in t.splitlines()
                   if ln.startswith('provider_effective_percent{provider="gpt"}')]
        self.assertEqual(len(gpt_eff), 1,
                         "duplicate samples of one series kill the whole scrape")
        self.assertEqual(float(gpt_eff[0].rsplit(" ", 1)[1]), 10.0,
                         "the legacy series speaks with the incumbent's numbers")

    def test_render_emits_per_account_family(self):
        t = ex.render()

        def val(metric, **labels):
            want = [f'{k}="{v}"' for k, v in labels.items()]
            for ln in t.splitlines():
                if ln.startswith(metric + "{") and all(w in ln for w in want):
                    return float(ln.rsplit(" ", 1)[1])
            return None
        self.assertEqual(val("provider_pool_size", provider="gpt"), 2)
        self.assertEqual(val("provider_account_effective_percent",
                             provider="gpt", account="gpt-a"), 10.0)
        self.assertEqual(val("provider_account_effective_percent",
                             provider="gpt", account="gpt-b"), 60.0)
        self.assertEqual(val("provider_account_preferred",
                             provider="gpt", account="gpt-a"), 1)
        self.assertEqual(val("provider_account_preferred",
                             provider="gpt", account="gpt-b"), 0)
        self.assertEqual(val("provider_account_preferred",
                             provider="kimi", account="kimi"), 1)


# ---- #72 unified provider auth ----------------------------------------------

def clear_kimi_creds():
    """Remove the enrolled kimi key file and reset the module's mtime cache so
    _kimi_key() falls back to the (test-empty) env var."""
    pkimi._kimi_file_key = None
    pkimi._kimi_file_mtime = None
    try:
        os.remove(pkimi.KIMI_CREDS_FILE)
    except FileNotFoundError:
        pass


def fake_id_token(account_id="ws-uuid-1"):
    """An unsigned JWT carrying codex's workspace-id claim — _jwt_claims never
    verifies signatures, so a '.sig' tail is enough."""
    def seg(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
    claims = {"https://api.openai.com/auth": {"chatgpt_account_id": account_id}}
    return seg({"alg": "none"}) + "." + seg(claims) + ".sig"


class AuthHTTP(HTTPBase):
    """HTTPBase plus a JSON POST helper and the #72 hygiene contract: every
    subclass leaves the grant store, both creds files, and the kimi cache the
    way it found them."""

    def post(self, path, obj, key=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}",
                                     data=json.dumps(obj).encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        if key:
            req.add_header("X-Switch-Key", key)
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            return e.code, (json.loads(body) if body else None)

    def setUp(self):
        ex._reauth_pending.clear()
        self.addCleanup(ex._reauth_pending.clear)
        self.addCleanup(clear_openai_creds)
        self.addCleanup(clear_kimi_creds)


class TestAuthRegistryShape(unittest.TestCase):
    """#72 — every PROVIDERS row's authority record is complete and uniformly
    shaped: the declared-data half serializes, the callable half is callable.
    A future provider is one row; this test is the row's checklist."""

    ALLOWED_KINDS = {"oauth-pkce", "oauth-rotating", "static-key"}
    ALLOWED_ROTATION = {"strict", "independent", "none"}
    ALLOWED_FLOWS = {"paste-code", "paste-url", "paste-key"}

    def test_every_row_carries_a_complete_authority(self):
        # Runway has no authority: its key is a file the owner places
        # (RUNWAY_CREDS_FILE), and enrolling one would mean a probe of the
        # live API, which C8 does not make. ElevenLabs is the same: a key file
        # the owner places (ELEVENLABS_CREDS_FILE). Nor has local (#135): a LAN
        # address in a file, no credential at all. Every reader skips an
        # auth-less row.
        # The household drivers' rows are TestAuthRegistryShapeHousehold's.
        self.assertIsNone(ex.PROVIDERS["local"].get("auth"), "local")
        for pid, p in ex.PROVIDERS.items():
            if pid in ("runway", "elevenlabs", "typesafe", "local"):
                continue
            a = p.get("auth")
            self.assertIsNotNone(a, f"{pid}: every registered provider is authorizable")
            self.assertIn(a["kind"], self.ALLOWED_KINDS, pid)
            self.assertTrue(a["label"], pid)
            self.assertIn(a["custody"]["rotation"], self.ALLOWED_ROTATION, pid)
            self.assertTrue(a["custody"]["note"], pid)
            e = a["enroll"]
            self.assertIn(e["flow"], self.ALLOWED_FLOWS, pid)
            self.assertIsInstance(e["can_add"], bool, pid)
            self.assertTrue(e["instructions"], pid)
            for f in e["fields"]:
                self.assertEqual(set(f), {"name", "label", "secret"}, f"{pid}: {f}")
            for fn in ("status", "begin", "complete"):
                self.assertTrue(callable(a[fn]), f"{pid}.{fn}")

    def test_custody_semantics_are_the_documented_ones(self):
        self.assertEqual(ex.PROVIDERS["gpt"]["auth"]["custody"]["rotation"], "strict",
                         "the brain is the SOLE refresher of a codex chain")
        self.assertEqual(ex.PROVIDERS["anthropic"]["auth"]["custody"]["rotation"],
                         "independent")
        self.assertEqual(ex.PROVIDERS["kimi"]["auth"]["custody"]["rotation"], "none")

    def test_declared_data_is_json_serializable_without_callables(self):
        payload = ex._auth_providers_payload(time.time())
        json.dumps(payload)  # a callable leaking in raises TypeError here


class TestPreferredProvidersRollbackSafety(AuthHTTP):
    """The proxy decodes /preferred providers.* as pool-steering data (#71); the
    anthropic facade row must never leak into it — an unknown pool id would
    change routing on a payload the proxy already ships decoding for."""

    def test_facade_never_reaches_the_preferred_payload(self):
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45), kimi_result())
        ex.compute_preferred()
        _, p = self.get("/preferred")
        self.assertNotIn("anthropic", p["providers"])
        self.assertLessEqual(set(p["providers"]), {"kimi", "gpt"})

    def test_state_migration_drops_a_facade_key(self):
        # Even a state file that somehow grew an anthropic pool entry is healed
        # on load — the migrate keys off the POLLABLE id set.
        migrated = ex._migrate_state(
            {"providers": {"anthropic": {"account": "x"}, "kimi": {"account": "kimi"}}},
            ex._lane_names, [], ex._pollable_provider_ids())
        self.assertEqual(set(migrated["providers"]), {"kimi", "gpt", "local"})


class TestAuthProvidersEndpoint(AuthHTTP):
    """GET /auth/providers — key-guarded, complete, and secret-free."""

    def setUp(self):
        super().setUp()
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))

    def test_requires_key(self):
        code, _ = self.get("/auth/providers")
        self.assertEqual(code, 401)

    def test_503_when_no_key_is_mounted(self):
        old = ex.SWITCH_KEY
        ex.SWITCH_KEY = ""
        self.addCleanup(setattr, ex, "SWITCH_KEY", old)
        code, d = self.get("/auth/providers", key="test-switch-key")
        self.assertEqual(code, 503)

    def test_every_provider_appears_with_accounts_and_descriptor(self):
        code, d = self.get("/auth/providers", key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(set(d["providers"]), {"anthropic", "anthropic-api", "kimi", "gpt"})
        anth = d["providers"]["anthropic"]
        self.assertEqual({a["name"] for a in anth["accounts"]}, {ACCT_A, ACCT_B})
        self.assertEqual(anth["enroll"]["flow"], "paste-code")
        self.assertEqual(d["providers"]["kimi"]["accounts"], [],
                         "keyless kimi = nothing enrolled, not a phantom row")
        self.assertEqual(d["providers"]["gpt"]["accounts"], [],
                         "no creds file = empty pool")

    def test_gpt_accounts_reflect_the_pool(self):
        seed_openai_pool({"name": "gpt-a"}, {"name": "gpt-b"})
        code, d = self.get("/auth/providers", key="test-switch-key")
        self.assertEqual([a["name"] for a in d["providers"]["gpt"]["accounts"]],
                         ["gpt-a", "gpt-b"])

    def test_no_secret_material_ever_serializes(self):
        # Seed every credential store, then scan the whole payload byte-string:
        # live anthropic tokens, pool refresh tokens, and the enrolled kimi key
        # must all be absent — fingerprints only.
        seed_openai_pool({"name": "gpt-a"})
        pkimi._persist_kimi_key("sk-kimi-super-secret-123")
        code, d = self.get("/auth/providers", key="test-switch-key")
        blob = json.dumps(d)
        for secret in ("r-" + ACCT_A, "tok-" + ACCT_A, "rt-gpt-a", "at-gpt-a",
                       "sk-kimi-super-secret-123"):
            self.assertNotIn(secret, blob)
        # No credential-bearing KEY may serialize either ("api_key" is exempt:
        # it is the declared FIELD NAME in kimi's enrollment descriptor).
        for key_name in ("refresh_token", "access_token", "verifier"):
            self.assertNotIn(f'"{key_name}"', blob)
        kimi = d["providers"]["kimi"]["accounts"][0]
        self.assertEqual(kimi["source"], "file")
        self.assertTrue(kimi["key_fingerprint"])

    def anthropic_row(self, name=ACCT_A):
        code, d = self.get("/auth/providers", key="test-switch-key")
        self.assertEqual(code, 200)
        return next(a for a in d["providers"]["anthropic"]["accounts"] if a["name"] == name)

    def test_rows_carry_the_proxys_refusal_its_last_serve_and_the_last_refresh(self):
        row = self.anthropic_row()
        self.assertEqual((row["proxy_refusal"], row["served_at"], row["last_refresh"]),
                         (None, None, None))
        orig = ex.urlopen
        self.addCleanup(setattr, ex, "urlopen", orig)
        ex.urlopen = lambda req, timeout=0: _Resp({"access_token": "fresh", "expires_in": 28800})
        with mock.patch.object(ex, "log", lambda m: None):
            psub._refresh(ACCT_A, why="harness-login min_ttl=3600s")
            psub._proxy_refused(ACCT_A, _fp("tok-" + ACCT_A))  # the copy the refresh revoked
        row = self.anthropic_row()
        ref = row["proxy_refusal"]
        self.assertEqual(set(ref), {"at", "fp", "matched_current"})
        self.assertEqual((ref["fp"], ref["matched_current"]), (_fp("tok-" + ACCT_A), False))
        self.assertIsNone(row["served_at"], "refused, and not served since: still refused")
        self.assertEqual(set(row["last_refresh"]), {"at", "why"})
        self.assertEqual(row["last_refresh"]["why"], "harness-login min_ttl=3600s")
        self.assertEqual(row["token_expires_at"], psub._creds[ACCT_A]["expires_at"])
        self.assertEqual(row["health"], "ok")
        # The proxy's retry on the fresh token is served: the refusal is behind it.
        [(res, why)], _ = post_reports(served(1, at=ref["at"] + 1))
        self.assertEqual(res, "applied", why)
        row = self.anthropic_row()
        self.assertEqual(row["proxy_refusal"], ref)
        self.assertGreater(row["served_at"], ref["at"])
        self.assertEqual(self.anthropic_row(ACCT_B)["served_at"], None)
        # A served event reported late does not move it back.
        self.assertEqual(post_reports(served(2, at=ref["at"] - 30))[0], [("applied", None)])
        self.assertEqual(self.anthropic_row()["served_at"], row["served_at"])

    def test_a_refusal_changes_no_health_and_nothing_public(self):
        before = {a["name"]: a["health"] for a in psub._anthropic_auth_status(time.time())}
        with mock.patch.object(ex, "log", lambda m: None):
            psub._proxy_refused(ACCT_A, _fp("a-stale-copy"))
            with mock.patch.object(psub, "_get", lambda path, token: {}):
                psub._proxy_refused(ACCT_B, _fp("tok-" + ACCT_B))
        rows = psub._anthropic_auth_status(time.time())
        self.assertEqual({a["name"]: a["health"] for a in rows}, before)
        self.assertEqual(set(before.values()), {"ok"})
        self.assertEqual(ex._account_health([ACCT_A, ACCT_B], time.time())["worst"], "ok")
        allowed = set(ex._HEALTH_ROW_KEYS) | {"provider"}
        for a in ex._unified_account_health(time.time())["accounts"]:
            self.assertLessEqual(set(a), allowed, a["name"])
        with ex._poll_health_lock:
            ex._poll_schedule.clear()


class TestReauthAliasFrozen(AuthHTTP):
    """/reauth stays the byte-frozen Anthropic alias over the SAME grant store
    the /auth surface uses — one store, tagged grants, no drift between the two
    front doors. (TestReauthFlow pins the alias's own behavior verbatim.)"""

    def test_reauth_begin_mints_an_anthropic_reauth_grant(self):
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        _, state = psub._reauth_begin(ACCT_A)
        g = ex._reauth_pending[state]
        self.assertEqual(g["provider"], "anthropic")
        self.assertEqual(g["mode"], "reauth")
        self.assertEqual(g["state"], state)

    def test_reauth_complete_refuses_a_foreign_providers_state(self):
        _, state = pkimi._kimi_auth_begin()
        code, d = self.post("/reauth/complete", {"state": state, "code": "c"},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("started for kimi", d["error"])
        self.assertNotIn(state, ex._reauth_pending, "consumed even on refusal — one-shot")


class TestAuthPendingIsolation(AuthHTTP):
    """One flat grant store, provider-tagged: a state begun on one provider can
    never complete on another, and every take is one-shot."""

    def test_cross_provider_complete_422s(self):
        _, state = pkimi._kimi_auth_begin()
        code, d = self.post("/auth/gpt/complete", {"state": state,
                                                   "name": "gpt-b", "redirect_url": "x"},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("started for kimi", d["error"])

    def test_grants_are_one_shot(self):
        _, state = pkimi._kimi_auth_begin()
        self.post("/auth/kimi/complete", {"state": state, "api_key": ""},
                  key="test-switch-key")
        code, d = self.post("/auth/kimi/complete", {"state": state, "api_key": "x"},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("begin again", d["error"])

    def test_unknown_provider_404s_with_the_roster(self):
        code, d = self.post("/auth/nope/begin", {}, key="test-switch-key")
        self.assertEqual(code, 404)
        self.assertEqual(d["providers"], ["anthropic", "anthropic-api", "gpt", "kimi"])

    def test_mutations_require_the_key(self):
        code, _ = self.post("/auth/kimi/begin", {})
        self.assertEqual(code, 401)
        code, _ = self.post("/auth/kimi/complete", {"state": "s"})
        self.assertEqual(code, 401)


class TestAnthropicEnrollAppends(AuthHTTP):
    """#72 — the anthropic authority: add-mode enrolls by VERIFIED profile email;
    reauth-mode keeps the frozen never-add refusal; a configured email is always
    a splice, never a duplicate."""

    NEW = "acct-c@example.com"

    def setUp(self):
        super().setUp()
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        with open(psub.ACCOUNTS_FILE, "w") as f:
            json.dump([{"name": ACCT_A, "refresh_token": "old-rt-a"},
                       {"name": ACCT_B, "refresh_token": "old-rt-b"}], f)
        self.addCleanup(os.remove, psub.ACCOUNTS_FILE)

    def _stub_exchange(self, email):
        orig = ex.urlopen

        def fake(req, timeout=0):
            if "oauth/token" in req.full_url:
                return _FakeTokenResp({"refresh_token": "fresh-rt", "access_token": "at"})
            return _FakeTokenResp({"account": {"email": email}, "organization": {}})
        ex.urlopen = fake
        self.addCleanup(setattr, ex, "urlopen", orig)

    def test_add_mode_appends_a_new_account(self):
        self._stub_exchange(self.NEW)
        code, d = self.post("/auth/anthropic/begin", {}, key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["mode"], "add")
        self.assertIn("authorize_url", d)
        code, r = self.post("/auth/anthropic/complete",
                            {"state": d["state"], "code": "the-code"},
                            key="test-switch-key")
        self.assertEqual(code, 200, r)
        self.assertEqual(r, {"account": self.NEW, "enrolled": True, "reauthorized": False})
        arr = json.load(open(psub.ACCOUNTS_FILE))
        self.assertEqual([a["name"] for a in arr], [ACCT_A, ACCT_B, self.NEW])
        self.assertEqual(arr[2]["refresh_token"], "fresh-rt")
        self.assertEqual(psub._creds[self.NEW]["refresh_token"], "fresh-rt")
        self.assertEqual(psub._cred_generation[self.NEW], 1)
        self.assertTrue(ex._poll_wakeup.is_set(), "the new account is polled immediately")

    def test_add_mode_with_a_configured_email_splices_instead(self):
        # Approving as an already-configured account mid-enrollment must repair
        # that account — never duplicate it.
        self._stub_exchange(ACCT_A)
        _, d = self.post("/auth/anthropic/begin", {}, key="test-switch-key")
        code, r = self.post("/auth/anthropic/complete",
                            {"state": d["state"], "code": "c"}, key="test-switch-key")
        self.assertEqual(code, 200, r)
        self.assertEqual(r, {"account": ACCT_A, "enrolled": False, "reauthorized": True})
        arr = json.load(open(psub.ACCOUNTS_FILE))
        self.assertEqual(len(arr), 2)
        self.assertEqual(arr[0]["refresh_token"], "fresh-rt")

    def test_reauth_mode_still_never_adds(self):
        # The frozen /reauth policy survives as MODE policy: a reauth-mode grant
        # whose verified email is unknown is refused, nothing persists.
        self._stub_exchange("mallory@example.com")
        _, d = self.post("/auth/anthropic/begin", {"account": ACCT_A},
                         key="test-switch-key")
        self.assertEqual(d["mode"], "reauth")
        code, r = self.post("/auth/anthropic/complete",
                            {"state": d["state"], "code": "c"}, key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("not a configured account", r["error"])
        self.assertEqual(len(json.load(open(psub.ACCOUNTS_FILE))), 2)
        self.assertNotIn("mallory@example.com", psub._creds)

    def test_begin_with_unknown_account_404s(self):
        code, _ = self.post("/auth/anthropic/begin", {"account": "nobody@x"},
                            key="test-switch-key")
        self.assertEqual(code, 404)

    def test_an_email_colliding_with_a_provider_name_is_refused(self):
        # _state is one flat namespace (#71) — an account named "kimi" would
        # fuse with the provider target's bookkeeping.
        self._stub_exchange("kimi")
        _, d = self.post("/auth/anthropic/begin", {}, key="test-switch-key")
        code, r = self.post("/auth/anthropic/complete",
                            {"state": d["state"], "code": "c"}, key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("collides", r["error"])
        self.assertEqual(len(json.load(open(psub.ACCOUNTS_FILE))), 2)

    def test_missing_code_is_a_422_not_a_burned_exchange(self):
        _, d = self.post("/auth/anthropic/begin", {}, key="test-switch-key")
        code, r = self.post("/auth/anthropic/complete", {"state": d["state"]},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("code is required", r["error"])


class TestCodexEnrollBegin(AuthHTTP):
    """#72 — the codex authorize URL is the codex-CLI shape (constants block:
    ASSUMED, fail-visible), minted against the shared grant store."""

    def test_begin_mints_the_codex_cli_authorize_url(self):
        url, state = pcodex._codex_auth_begin()
        self.assertTrue(url.startswith(pcodex.CODEX_AUTHORIZE + "?"))
        q = {k: v[0] for k, v in
             urllib.parse.parse_qs(urllib.parse.urlparse(url).query).items()}
        self.assertEqual(q["client_id"], pcodex.OPENAI_CLIENT_ID)
        self.assertEqual(q["redirect_uri"], pcodex.CODEX_REDIRECT)
        self.assertEqual(q["scope"], pcodex.CODEX_SCOPE)
        self.assertEqual(q["state"], state)
        self.assertEqual(q["code_challenge_method"], "S256")
        self.assertEqual(q["codex_cli_simplified_flow"], "true")
        self.assertEqual(q["id_token_add_organizations"], "false")
        g = ex._reauth_pending[state]
        self.assertEqual((g["provider"], g["mode"]), ("gpt", "add"))

    def test_begin_against_a_pool_name_is_reauth_mode(self):
        seed_openai_pool({"name": "gpt-a"})
        code, d = self.post("/auth/gpt/begin", {"account": "gpt-a"},
                            key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertEqual(d["mode"], "reauth")
        code, _ = self.post("/auth/gpt/begin", {"account": "gpt-z"},
                            key="test-switch-key")
        self.assertEqual(code, 404)


class TestCodexEnrollComplete(AuthHTTP):
    """#72 — the paste-the-redirect-URL exchange: name gate, state match, JSON
    exchange, workspace-id claim, and a usage PROBE before anything persists."""

    def setUp(self):
        super().setUp()
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        self._orig_urlopen = ex.urlopen
        self._orig_get = pcodex._get_codex_usage
        self.addCleanup(setattr, ex, "urlopen", self._orig_urlopen)
        self.addCleanup(setattr, pcodex, "_get_codex_usage", self._orig_get)
        self.exchanges = []

    def _stub(self, tok_payload=None, probe=None, tok_error=None):
        def fake(req, timeout=0):
            self.exchanges.append(json.loads(req.data.decode()))
            if tok_error:
                raise urllib.error.HTTPError(req.full_url, tok_error, "no", None, None)
            return _FakeTokenResp(tok_payload)
        ex.urlopen = fake
        if probe is not None:
            pcodex._get_codex_usage = probe

    def _complete(self, state, name="gpt-b", code="the-code", url_state=None):
        url = (f"{pcodex.CODEX_REDIRECT}?code={code}&state={url_state or state}"
               if code else pcodex.CODEX_REDIRECT)
        return self.post("/auth/gpt/complete",
                         {"state": state, "name": name, "redirect_url": url},
                         key="test-switch-key")

    def test_happy_path_probes_then_persists_array_form(self):
        self._stub({"refresh_token": "rt-new", "access_token": "at-new",
                    "expires_in": 600, "id_token": fake_id_token("ws-42")},
                   probe=lambda tok, acct: wham_usage(primary=12.0))
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state)
        self.assertEqual(code, 200, r)
        self.assertEqual(r, {"account": "gpt-b", "enrolled": True, "reauthorized": False})
        on_disk = json.load(open(pcodex.OPENAI_CODEX_CREDS_FILE))
        self.assertIsInstance(on_disk, list, "a fresh enrollment persists ARRAY form")
        entry = on_disk[0]
        self.assertEqual((entry["name"], entry["refresh_token"], entry["account_id"]),
                         ("gpt-b", "rt-new", "ws-42"))
        self.assertEqual(self.exchanges[0]["grant_type"], "authorization_code")
        self.assertEqual(self.exchanges[0]["redirect_uri"], pcodex.CODEX_REDIRECT)
        self.assertTrue(ex._poll_wakeup.is_set())
        self.assertIn("gpt-b", ex._provider_poll_targets(),
                      "the new chain becomes a poll target immediately")

    def test_probe_is_the_pollers_free_get(self):
        """C1: the probe is GET wham/usage on the new chain's token and workspace
        id — the poller's own read, never an inference."""
        seen = []

        class Resp(_FakeTokenResp):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake(req, timeout=0):
            seen.append(req)
            if req.full_url == pcodex.CODEX_TOKEN_URL:
                return Resp({"refresh_token": "rt-new", "access_token": "at-new",
                             "id_token": fake_id_token("ws-42")})
            return Resp(wham_usage(primary=12.0))
        ex.urlopen = fake
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state)
        self.assertEqual(code, 200, r)
        probe = seen[1]
        self.assertEqual(len(seen), 2, "one exchange, one probe")
        self.assertEqual(probe.full_url, pcodex.OPENAI_USAGE_URL)
        self.assertEqual(probe.get_method(), "GET")
        self.assertIsNone(probe.data)
        self.assertEqual(probe.get_header("Authorization"), "Bearer at-new")
        self.assertEqual(probe.get_header("Chatgpt-account-id"), "ws-42")

    def test_spent_window_is_a_passing_probe(self):
        """The GET answers 200 with allowed false when the window is spent."""
        self._stub({"refresh_token": "rt", "access_token": "at",
                    "id_token": fake_id_token()},
                   probe=lambda t, a: wham_usage(primary=100.0, allowed=False))
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state)
        self.assertEqual(code, 200, r)

    def test_probe_429_or_shapeless_body_persists_nothing(self):
        def probe_429(tok, acct):
            raise urllib.error.HTTPError("u", 429, "limit", {}, None)
        for probe, why in ((probe_429, "usage probe HTTP 429"),
                           (lambda t, a: {"detail": "Not Found"}, "no rate_limit")):
            with self.subTest(why=why):
                self._stub({"refresh_token": "rt", "access_token": "at",
                            "id_token": fake_id_token()}, probe=probe)
                _, state = pcodex._codex_auth_begin()
                code, r = self._complete(state)
                self.assertEqual(code, 422)
                self.assertIn(why, r["error"])
                self.assertFalse(os.path.exists(pcodex.OPENAI_CODEX_CREDS_FILE))

    def test_bad_name_fails_before_the_exchange(self):
        self._stub({}, probe=lambda t, a: {})
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state, name="Bad Name!")
        self.assertEqual(code, 422)
        self.assertIn("lowercase", r["error"])
        self.assertEqual(self.exchanges, [], "the one-shot code must not be burned "
                                            "on a name we could have refused first")
        code, r = self._complete(state)
        self.assertEqual(code, 422, "the grant itself is still one-shot")

    def test_name_colliding_with_an_anthropic_account_is_refused(self):
        # Emails fail the charset gate outright ("@"); the collision check
        # guards regex-passing names, so plant one in the live cache.
        self._stub({}, probe=lambda t, a: {})
        psub._creds["weird"] = {"refresh_token": "r", "access_token": None, "expires_at": 0}
        self.addCleanup(psub._creds.pop, "weird", None)
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state, name="weird")
        self.assertEqual(code, 422)
        self.assertIn("collides", r["error"])

    def test_url_without_code_422s(self):
        self._stub({}, probe=lambda t, a: {})
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state, code=None)
        self.assertEqual(code, 422)
        self.assertIn("no ?code=", r["error"])

    def test_url_from_another_attempt_422s(self):
        self._stub({}, probe=lambda t, a: {})
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state, url_state="other-state")
        self.assertEqual(code, 422)
        self.assertIn("different attempt", r["error"])

    def test_exchange_refusal_persists_nothing(self):
        self._stub(tok_error=400)
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state)
        self.assertEqual(code, 422)
        self.assertIn("nothing was saved", r["error"])
        self.assertFalse(os.path.exists(pcodex.OPENAI_CODEX_CREDS_FILE))

    def test_missing_workspace_claim_persists_nothing(self):
        self._stub({"refresh_token": "rt", "access_token": "at", "id_token": "junk"},
                   probe=lambda t, a: {})
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state)
        self.assertEqual(code, 422)
        self.assertIn("chatgpt_account_id", r["error"])
        self.assertFalse(os.path.exists(pcodex.OPENAI_CODEX_CREDS_FILE))

    def test_failing_probe_persists_nothing(self):
        def probe_500(tok, acct):
            raise urllib.error.HTTPError("u", 500, "boom", None, None)
        self._stub({"refresh_token": "rt", "access_token": "at",
                    "id_token": fake_id_token()}, probe=probe_500)
        _, state = pcodex._codex_auth_begin()
        code, r = self._complete(state)
        self.assertEqual(code, 422)
        self.assertIn("nothing was saved", r["error"])
        self.assertFalse(os.path.exists(pcodex.OPENAI_CODEX_CREDS_FILE))


class TestCodexEnrollShapeConversion(unittest.TestCase):
    """#72 — enrollment is the ONE path allowed to convert a legacy dict creds
    file to array form (an explicit operator act, logged); re-enrolling the
    legacy singleton under its own name stays rollback-neutral."""

    def setUp(self):
        clear_openai_creds()
        self.addCleanup(clear_openai_creds)

    def entry(self, name):
        return {"name": name, "refresh_token": f"rt-{name}", "access_token": f"at-{name}",
                "expires_at": time.time() + 999, "account_id": f"ws-{name}"}

    def test_growing_a_legacy_pool_converts_to_array(self):
        seed_openai_creds()
        pcodex._load_openai_creds(force=True)
        replaced = pcodex._codex_pool_enroll(self.entry("gpt-b"))
        self.assertFalse(replaced)
        on_disk = json.load(open(pcodex.OPENAI_CODEX_CREDS_FILE))
        self.assertIsInstance(on_disk, list)
        self.assertEqual({e["name"] for e in on_disk}, {"gpt", "gpt-b"})
        self.assertEqual(pcodex._openai_pool_shape, "array")

    def test_reenrolling_the_sole_legacy_singleton_keeps_dict_shape(self):
        seed_openai_creds()
        pcodex._load_openai_creds(force=True)
        replaced = pcodex._codex_pool_enroll(self.entry("gpt"))
        self.assertTrue(replaced)
        on_disk = json.load(open(pcodex.OPENAI_CODEX_CREDS_FILE))
        self.assertIsInstance(on_disk, dict,
                              "repairing the singleton must stay rollback-neutral")
        self.assertEqual(on_disk["refresh_token"], "rt-gpt")
        self.assertEqual(pcodex._openai_pool_shape, "legacy")

    def test_enrolling_into_an_array_pool_stays_array(self):
        seed_openai_pool({"name": "gpt-a"})
        pcodex._load_openai_creds(force=True)
        pcodex._codex_pool_enroll(self.entry("gpt-b"))
        on_disk = json.load(open(pcodex.OPENAI_CODEX_CREDS_FILE))
        self.assertEqual({e["name"] for e in on_disk}, {"gpt-a", "gpt-b"})


class TestKimiEnrollAndKeySource(AuthHTTP):
    """#72 — kimi's static key: file wins over env, enrollment validates by
    probe before persisting, and the response says the proxy holds its own copy."""

    def setUp(self):
        super().setUp()
        reset(acct(ACCT_A, 41), acct(ACCT_B, 30))
        self._orig_key = pkimi.KIMI_API_KEY
        self._orig_usages = pkimi._get_kimi_usages
        self.addCleanup(setattr, pkimi, "KIMI_API_KEY", self._orig_key)
        self.addCleanup(setattr, pkimi, "_get_kimi_usages", self._orig_usages)
        clear_kimi_creds()

    def test_key_resolution_file_wins_over_env(self):
        pkimi.KIMI_API_KEY = ""
        self.assertEqual(pkimi._kimi_key(), "")
        self.assertEqual(pkimi._kimi_key_source(), "none")
        self.assertFalse(ex.PROVIDERS["kimi"]["enabled"]())
        pkimi.KIMI_API_KEY = "sk-kimi-env"
        self.assertEqual(pkimi._kimi_key(), "sk-kimi-env")
        self.assertEqual(pkimi._kimi_key_source(), "env")
        pkimi._persist_kimi_key("sk-kimi-file")
        self.assertEqual(pkimi._kimi_key(), "sk-kimi-file")
        self.assertEqual(pkimi._kimi_key_source(), "file")

    def test_out_of_band_file_edit_is_adopted_live(self):
        pkimi._persist_kimi_key("sk-kimi-one")
        with open(pkimi.KIMI_CREDS_FILE, "w") as f:
            json.dump({"api_key": "sk-kimi-two"}, f)
        os.utime(pkimi.KIMI_CREDS_FILE, (time.time() + 5, time.time() + 5))
        self.assertEqual(pkimi._kimi_key(), "sk-kimi-two", "mtime guard adopts edits")

    def test_enroll_validates_then_persists(self):
        pkimi.KIMI_API_KEY = ""
        probed = []

        def fake_usages(key):
            probed.append(key)
            return {"usage": {"limit": "100", "remaining": "99"}}
        pkimi._get_kimi_usages = fake_usages
        code, d = self.post("/auth/kimi/begin", {}, key="test-switch-key")
        self.assertEqual(code, 200)
        self.assertNotIn("authorize_url", d, "paste-key has no authorize step")
        code, r = self.post("/auth/kimi/complete",
                            {"state": d["state"], "api_key": " sk-kimi-new "},
                            key="test-switch-key")
        self.assertEqual(code, 200, r)
        self.assertEqual(r["account"], "kimi")
        self.assertIn("SWITCH_KIMI_KEY", r["note"],
                      "the operator must learn the proxy holds its own copy")
        self.assertEqual(probed, ["sk-kimi-new"], "validated with the TRIMMED key")
        self.assertEqual(json.load(open(pkimi.KIMI_CREDS_FILE)), {"api_key": "sk-kimi-new"})
        self.assertEqual(pkimi._kimi_key(), "sk-kimi-new")
        self.assertTrue(ex.PROVIDERS["kimi"]["enabled"]())
        self.assertTrue(ex._poll_wakeup.is_set())

    def test_rejected_key_persists_nothing_and_never_echoes(self):
        pkimi.KIMI_API_KEY = ""

        def refuse(key):
            raise urllib.error.HTTPError("u", 401, "no", None, None)
        pkimi._get_kimi_usages = refuse
        _, d = self.post("/auth/kimi/begin", {}, key="test-switch-key")
        code, r = self.post("/auth/kimi/complete",
                            {"state": d["state"], "api_key": "sk-kimi-bogus"},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("401", r["error"])
        self.assertNotIn("sk-kimi-bogus", r["error"], "the key never rides an error")
        self.assertFalse(os.path.exists(pkimi.KIMI_CREDS_FILE))

    def test_junk_quota_payload_persists_nothing(self):
        pkimi.KIMI_API_KEY = ""
        pkimi._get_kimi_usages = lambda key: {"whatever": True}
        _, d = self.post("/auth/kimi/begin", {}, key="test-switch-key")
        code, r = self.post("/auth/kimi/complete",
                            {"state": d["state"], "api_key": "sk-kimi-x"},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("no usable quota", r["error"])
        self.assertFalse(os.path.exists(pkimi.KIMI_CREDS_FILE))

    def test_empty_key_422s(self):
        _, d = self.post("/auth/kimi/begin", {}, key="test-switch-key")
        code, r = self.post("/auth/kimi/complete",
                            {"state": d["state"], "api_key": "   "},
                            key="test-switch-key")
        self.assertEqual(code, 422)
        self.assertIn("api_key is required", r["error"])


class TestAuthPageServed(AuthHTTP):
    """GET /auth — the descriptor-driven page, keyed like its endpoints, and
    the markup never carries the switch key."""

    def test_page_is_served_and_key_free(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/auth",
                                     headers={"X-Switch-Key": "test-switch-key"})
        with urllib.request.urlopen(req, timeout=5) as r:
            self.assertIn("text/html", r.headers.get("Content-Type", ""))
            body = r.read().decode()
        self.assertIn("Provider accounts", body)
        self.assertIn('keyedFetch("/auth/providers"', body)
        self.assertIn('"/auth/" + PID + "/complete"', body)
        self.assertIn('href="/">Back</a>', body)
        self.assertIn("WATCH", body)
        self.assertNotIn("test-switch-key", body)
        self.assertNotIn('id="key"', body)

    def test_page_says_when_anthropic_refuses_the_proxy(self):
        """A refusal after the proxy's last served request is red and says
        the laptop can't fix it; one before it is a quiet stale-copy line. The
        token's expiry and last refresh always show."""
        body = ex._AUTH_HTML
        self.assertIn("a.served_at && a.served_at > r.at", body)
        self.assertIn("Anthropic is refusing the switch proxy's requests with this account since ", body)
        self.assertIn(" — laptop /login can't fix this.", body)
        self.assertIn('"Last proxy refusal " + when(r.at) + " — stale copy replaced.', body)
        self.assertIn('"Token expires " + when(a.token_expires_at)', body)
        self.assertIn("a.last_refresh.why", body)
        self.assertIn("tokenDetail(a)", body.split("function refresh(")[1])


# ---- #92: the runtime document API (/classes + /consumers) --------------------

KEY = "test-switch-key"


def sonnet_class():
    return {"class": "beta", "display": "Beta", "members": [
        {"priority": 1, "kind": "anthropic", "lane": "sonnet",
         "model": "claude-sonnet-4-5", "context_window": 200000}]}


def consumers_doc():
    return {"consumers": [
        {"id": "coding-harness", "display": "Coding harness", "request_shape": "claude-code",
         "members": ["lane:opus", "lane:sonnet"]},
        {"id": "gateway", "display": "Agent gateway", "request_shape": "other",
         "members": ["class:frontier"]},
    ]}


class DocAPIBase(HTTPBase):
    """Real files on disk + live HTTP: CLASSES_FILE/CONSUMERS_FILE point at
    per-test paths, the module loaders install them, tearDown restores every
    global the doc layer owns so test-class ordering can never leak state."""

    def req(self, method, path, body=None, key=KEY, headers=None, raw=None):
        data = raw if raw is not None else (
            json.dumps(body).encode() if body is not None else None)
        r = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}",
                                   data=data, method=method)
        if key:
            r.add_header("X-Switch-Key", key)
        if data is not None:
            r.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            r.add_header(k, v)
        try:
            with urllib.request.urlopen(r, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode()), dict(resp.headers)
        except urllib.error.HTTPError as e:
            b = e.read().decode()
            return e.code, (json.loads(b) if b else None), dict(e.headers)

    def install_classes(self, cfg):
        with open(self.classes_path, "w") as f:
            json.dump(cfg, f)
        ex._classes = ex._load_classes()

    def install_consumers(self, doc):
        with open(ex.CONSUMERS_FILE, "w") as f:
            json.dump(doc, f)
        ex._consumers = ex._load_consumers()

    def setUp(self):
        self.classes_path = os.path.join(_TMP, f"classes-{self.__class__.__name__}.json")
        self._old_classes_file = ex.CLASSES_FILE
        ex.CLASSES_FILE = self.classes_path
        for suffix in ("", ".windows", ".history.jsonl"):
            try:
                os.remove(self.classes_path + suffix)
            except FileNotFoundError:
                pass
        for suffix in ("", ".history.jsonl"):
            try:
                os.remove(ex.CONSUMERS_FILE + suffix)
            except FileNotFoundError:
                pass
        self.install_classes(frontier_cfg())
        ex._consumers = ex._load_consumers()  # absent file -> none
        reset(opus_acct(30, resets=True),
              provider_result("gpt", five_hour=8.0),
              kimi_result(five_hour=12.0, weekly=2.0))
        with ex._plock:
            ex._preferred["classes"] = {}
        ex.compute_preferred()

    def tearDown(self):
        ex.CLASSES_FILE = self._old_classes_file
        with ex._plock:
            ex._classes = []
            ex._classes_doc = []
            ex._classes_warnings = []
            ex._consumers = []
            ex._consumers_doc = {"consumers": []}
            ex._consumers_warnings = []
            ex._lane_settings = {}
            ex._preferred["classes"] = {}
        try:
            os.remove(ex.CONSUMERS_FILE)
        except FileNotFoundError:
            pass


class TestClassesDocGet(DocAPIBase):
    def test_guidance_round_trips_raw_and_changes_revision(self):
        _, before, _ = self.req("GET", "/classes")
        cfg = frontier_cfg()
        cfg[0]["guidance"] = "  Main tier. Settled work.  "
        self.install_classes(cfg)
        code, p, _ = self.req("GET", "/classes")
        self.assertEqual(code, 200)
        self.assertEqual(p["doc"], cfg, "GET preserves guidance in the raw document")
        self.assertNotEqual(p["revision"], before["revision"])
        self.assertEqual(ex._classes[0]["guidance"], "Main tier. Settled work.")
        cfg[0]["guidance"] = "Different purpose."
        self.install_classes(cfg)
        _, changed, _ = self.req("GET", "/classes")
        self.assertNotEqual(changed["revision"], p["revision"])

    def test_preferred_carries_guidance_or_null(self):
        for guidance in ("  Main tier. Settled work.  ", None):
            with self.subTest(guidance=guidance):
                cfg = frontier_cfg()
                if guidance is not None:
                    cfg[0]["guidance"] = guidance
                self.install_classes(cfg)
                code, p, _ = self.req("GET", "/preferred")
                self.assertEqual(code, 200)
                self.assertEqual(p["classes"]["frontier"]["guidance"],
                                 guidance.strip() if guidance else None)

    def test_bad_guidance_warning_surfaces_without_dropping_class(self):
        cfg = frontier_cfg()
        cfg[0]["guidance"] = "bad\nguidance"
        self.install_classes(cfg)
        _, p, _ = self.req("GET", "/classes")
        self.assertEqual(p["doc"], cfg)
        self.assertIsNone(ex._classes[0]["guidance"])
        self.assertTrue(any("frontier" in w and "guidance" in w for w in p["warnings"]))

    def test_requires_key(self):
        code, _, _ = self.req("GET", "/classes", key=None)
        self.assertEqual(code, 401)

    def test_doc_revision_derived_warnings(self):
        code, p, hdrs = self.req("GET", "/classes")
        self.assertEqual(code, 200)
        self.assertEqual(p["doc"], frontier_cfg(), "GET serves the RAW document")
        self.assertEqual(p["revision"], ex._doc_revision(frontier_cfg()))
        self.assertEqual(hdrs.get("X-Doc-Revision"), p["revision"])
        self.assertEqual(p["warnings"], [])
        d = p["derived"]["frontier"]
        self.assertEqual(d["context_window"], 200000)
        self.assertEqual(d["floor_member"], "lane:opus")
        self.assertEqual(d["request_kind"], "chat", "the default kind is said outright (a console reads it)")
        self.assertEqual([m["id"] for m in d["members"]],
                         ["lane:opus", "gpt/gpt-5.6", "kimi/kimi-k3"])

    def test_load_warnings_surface(self):
        cfg = frontier_cfg() + [{"class": "bad", "members": []}]
        self.install_classes(cfg)
        _, p, _ = self.req("GET", "/classes")
        self.assertTrue(any("bad" in w for w in p["warnings"]),
                        "a dropped class at load must be visible on GET, not just a log line")
        self.assertEqual(p["doc"], cfg, "the raw doc keeps the bad entry — warnings tell the story")


class TestClassesValidate(DocAPIBase):
    """Item 6 hardening is exercised END TO END here: every genuinely broken
    shape must be a strict validation ERROR (while the live loader only
    drop-warns). classes.example.json must stay valid (checked in
    TestExampleConfigStaysValid)."""

    def validate(self, doc):
        code, p, _ = self.req("POST", "/classes/validate", body={"doc": doc})
        self.assertEqual(code, 200, "validate is a report, not a refusal")
        return p

    def test_guidance_validation_report(self):
        for guidance in TestClassGuidance.BAD_GUIDANCE:
            with self.subTest(guidance=guidance):
                doc = frontier_cfg()
                doc[0]["guidance"] = guidance
                p = self.validate(doc)
                self.assertFalse(p["ok"])
                self.assertTrue(any("frontier" in e and "guidance" in e for e in p["errors"]))
        for guidance in ("  Valid purpose.  ", None):
            with self.subTest(guidance=guidance):
                doc = frontier_cfg()
                doc[0]["guidance"] = guidance
                self.assertTrue(self.validate(doc)["ok"])

    def test_valid_doc_reports_ok_and_would_revision(self):
        doc = frontier_cfg() + [sonnet_class()]
        p = self.validate(doc)
        self.assertTrue(p["ok"])
        self.assertEqual(p["errors"], [])
        self.assertEqual(p["would_revision"], ex._doc_revision(doc))
        self.assertEqual(p["revision"], ex._doc_revision(frontier_cfg()),
                         "live revision rides along so the editor can detect staleness")
        self.assertIn("beta", p["derived"])
        self.assertEqual(p["live_effect"]["new_classes"], ["beta"])

    def test_broken_shapes_error(self):
        base = frontier_cfg()
        dup_prio = frontier_cfg()
        dup_prio[0]["members"][0]["priority"] = 1  # kimi (file order) collides with opus
        no_model = frontier_cfg()
        del no_model[0]["members"][1]["model"]  # anthropic member needs lane AND model
        anth_ratio = frontier_cfg()
        anth_ratio[0]["members"][1]["anthropic_token_ratio"] = 1.0
        bad_ratio = frontier_cfg()
        bad_ratio[0]["members"][0]["anthropic_token_ratio"] = 4.5  # (0, 4]
        zero_window = frontier_cfg()
        zero_window[0]["members"][0]["context_window"] = 0
        lane_collision = frontier_cfg() + [dict(sonnet_class(), **{"class": "opus"})]
        dup_name = frontier_cfg() + frontier_cfg()
        # {"classes": [...]} is the document's own shape since C2 (§1.9); an
        # object without a classes array, or a lanes map that is not one, is not.
        bad_cases = {"no_classes_array": {"klasses": base},
                     "lanes_not_object": {"classes": base, "lanes": []},
                     "scalar": "frontier",
                     "dup_priority": dup_prio,
                     "anthropic_without_model": no_model,
                     "ratio_on_anthropic": anth_ratio,
                     "ratio_out_of_range": bad_ratio,
                     "window_not_positive": zero_window,
                     "id_collides_with_lane": lane_collision,
                     "duplicate_class_name": dup_name}
        for name, doc in bad_cases.items():
            with self.subTest(name):
                p = self.validate(doc)
                self.assertFalse(p["ok"], f"{name} must fail strict validation")
                self.assertTrue(p["errors"])

    def test_live_effect_deletion_bindings_pins_windows(self):
        code, p = ex._bind_class_session("frontier", "sess-le", time.time())
        self.assertEqual(code, 200)
        with ex._plock:
            ex._preferred["classes"]["frontier"]["pinned"] = "kimi/kimi-k3"
        doc = [sonnet_class()]  # deletes frontier entirely
        p = self.validate(doc)
        eff = p["live_effect"]
        self.assertEqual(eff["deleted_classes"], ["frontier"])
        self.assertEqual(eff["new_classes"], ["beta"])
        self.assertIn({"class": "frontier", "member": "kimi/kimi-k3"}, eff["invalidated_pins"])
        self.assertTrue(any(b["session"] == "sess-le" for b in eff["invalidated_bindings"]))
        # window change: raising opus's declared window moves the derived min to kimi's
        wider = frontier_cfg()
        wider[0]["members"][1]["context_window"] = 800000  # opus (file order idx 1)
        eff = self.validate(wider)["live_effect"]
        self.assertEqual(eff["window_changes"],
                         [{"class": "frontier", "from": 200000, "to": 419672}])
        self.assertEqual(eff["deleted_classes"], [])

    def test_validate_mutates_nothing(self):
        before = ex._doc_revision(ex._classes_doc)
        self.validate([sonnet_class()])
        self.assertEqual(ex._doc_revision(ex._classes_doc), before)
        with ex._plock:
            self.assertEqual([c["class"] for c in ex._classes], ["frontier"])


class TestClassesPut(DocAPIBase):
    def put(self, doc, if_match=None, reason="test edit", actor=None, key=KEY):
        headers = {}
        if if_match is not None:
            headers["If-Match"] = if_match
        if actor:
            headers["X-Actor"] = actor
        body = {"doc": doc}
        if reason is not None:
            body["reason"] = reason
        return self.req("PUT", "/classes", body=body, key=key, headers=headers)

    def live_rev(self):
        return ex._doc_revision(ex._classes_doc)

    def test_put_rejects_bad_guidance_without_changing_live_document(self):
        before = self.live_rev()
        for guidance in TestClassGuidance.BAD_GUIDANCE:
            with self.subTest(guidance=guidance):
                doc = frontier_cfg()
                doc[0]["guidance"] = guidance
                code, p, _ = self.put(doc, if_match=before)
                self.assertEqual(code, 422)
                self.assertTrue(any("frontier" in e and "guidance" in e for e in p["errors"]))
                self.assertEqual(self.live_rev(), before)

    def test_put_happy_path_writes_swaps_and_logs_history(self):
        doc = frontier_cfg() + [sonnet_class()]
        code, p, hdrs = self.put(doc, if_match=self.live_rev(), actor="tester@example.com",
                                 reason="add beta")
        self.assertEqual(code, 200)
        self.assertEqual(p["revision"], ex._doc_revision(doc))
        self.assertEqual(hdrs.get("X-Doc-Revision"), p["revision"])
        with open(self.classes_path) as f:
            self.assertEqual(json.load(f), doc, "atomic write landed the exact document")
        with ex._plock:
            self.assertEqual([c["class"] for c in ex._classes], ["frontier", "beta"],
                             "hot-swap: live config replaced, no restart")
            self.assertIn("beta", ex._preferred["classes"], "new class seeded fresh state")
        with open(self.classes_path + ".history.jsonl") as f:
            entries = [json.loads(line) for line in f]
        self.assertEqual(len(entries), 1)
        e = entries[0]
        self.assertEqual(e["rev"], p["revision"])
        self.assertEqual(e["actor"], "tester@example.com")
        self.assertEqual(e["reason"], "add beta")
        self.assertEqual(e["doc"], doc)
        self.assertIn("ts", e)
        # the new class is live on /preferred immediately
        _, pref, _ = self.req("GET", "/preferred")
        self.assertIn("beta", pref["classes"])

    def test_stale_if_match_409_changes_nothing(self):
        rev = self.live_rev()
        code, p, hdrs = self.put([sonnet_class()], if_match="0123456789abcdef")
        self.assertEqual(code, 409)
        self.assertEqual(p["revision"], rev, "the 409 body carries the CURRENT revision")
        self.assertEqual(hdrs.get("X-Doc-Revision"), rev)
        with ex._plock:
            self.assertEqual([c["class"] for c in ex._classes], ["frontier"])
        self.assertFalse(os.path.exists(self.classes_path + ".history.jsonl"),
                         "a refused write must not append history")

    def test_invalid_doc_422_and_no_write(self):
        bad = frontier_cfg()
        bad[0]["members"][0]["priority"] = 1  # duplicate priority
        code, p, _ = self.put(bad, if_match=self.live_rev())
        self.assertEqual(code, 422)
        self.assertTrue(p["errors"])
        with open(self.classes_path) as f:
            self.assertEqual(json.load(f), frontier_cfg(), "file untouched on rejection")

    def test_missing_reason_or_if_match_400(self):
        code, p, _ = self.put(frontier_cfg(), if_match=self.live_rev(), reason=None)
        self.assertEqual(code, 400)
        self.assertIn("reason", p["error"])
        code, p, _ = self.put(frontier_cfg(), reason="x")
        self.assertEqual(code, 400)
        self.assertIn("If-Match", p["error"])

    def test_put_requires_key(self):
        code, _, _ = self.put(frontier_cfg(), if_match=self.live_rev(), key=None)
        self.assertEqual(code, 401)

    def test_actor_defaults_to_unknown(self):
        code, p, _ = self.put(frontier_cfg() + [sonnet_class()], if_match=self.live_rev())
        self.assertEqual(code, 200)
        with open(self.classes_path + ".history.jsonl") as f:
            self.assertEqual(json.loads(f.readline())["actor"], "unknown")

    def test_deleting_consumer_referenced_class_succeeds_with_warning(self):
        self.install_consumers(consumers_doc())  # gateway references class:frontier
        code, p, _ = self.put([sonnet_class()], if_match=self.live_rev(),
                              reason="retire frontier")
        self.assertEqual(code, 200, "cross-doc refs are WARN, never rejection (§5)")
        self.assertTrue(any("gateway" in w for w in p["warnings"]))
        self.assertTrue(any("frontier" in w for w in p["live_effect"]["consumer_warnings"]))
        # and the consumer's ref now reads dangling on /preferred
        _, pref, _ = self.req("GET", "/preferred")
        oc = next(c for c in pref["consumers"] if c["id"] == "gateway")
        self.assertEqual(oc["members"][0]["status"], "dangling")

    def test_body_over_16k_is_accepted(self):
        doc = frontier_cfg() + [sonnet_class()]
        reason = "why " * 8000  # ~32 KB — over _read_json_body's default cap
        code, _, _ = self.put(doc, if_match=self.live_rev(), reason=reason)
        self.assertEqual(code, 200, "document endpoints must not inherit the 16 KB cap")

    def test_body_over_doc_cap_400(self):
        body = json.dumps({"doc": frontier_cfg(),
                           "reason": "x" * (ex.DOC_BODY_MAX + 1024)}).encode()
        code, _, _ = self.req("PUT", "/classes", raw=body,
                              headers={"If-Match": self.live_rev()})
        self.assertEqual(code, 400)


class TestClassesHotSwap(DocAPIBase):
    """§6 hot-swap semantics through the real PUT path, plus the resurrection
    race: after a swap deletes a class, no endpoint may recreate its state."""

    def put(self, doc):
        return self.req("PUT", "/classes", body={"doc": doc, "reason": "swap test"},
                        headers={"If-Match": ex._doc_revision(ex._classes_doc)})

    def test_deleted_class_state_dropped_and_persisted(self):
        ex._bind_class_session("frontier", "sess-hs", time.time())
        code, _, _ = self.put([sonnet_class()])
        self.assertEqual(code, 200)
        with ex._plock:
            self.assertNotIn("frontier", ex._preferred["classes"],
                             "deleted-class state dropped")
            self.assertIn("beta", ex._preferred["classes"])
        with open(ex.STATE_FILE) as f:
            persisted = json.load(f)
        self.assertNotIn("frontier", persisted["classes"],
                         "the drop is persisted — no zombie resurrection on restart")

    def test_removed_member_invalidates_binding_and_pin(self):
        with ex._plock:
            ex._preferred["classes"].setdefault("frontier", ex._new_class_state())
            ex._preferred["classes"]["frontier"]["pinned"] = "kimi/kimi-k3"
        ex._bind_class_session("frontier", "sess-rm", time.time())  # binds to the pin
        self.assertEqual(bound_member("sess-rm"), "kimi/kimi-k3")
        no_kimi = frontier_cfg()
        no_kimi[0]["members"] = [m for m in no_kimi[0]["members"]
                                 if m.get("provider") != "kimi"]
        code, _, _ = self.put(no_kimi)
        self.assertEqual(code, 200)
        with ex._plock:
            cs = ex._preferred["classes"]["frontier"]
            self.assertIsNone(cs["pinned"], "pin to a vanished member invalidated")
            self.assertNotIn("sess-rm", cs["sessions"],
                             "binding to a vanished member invalidated — the proxy rebinds")

    def test_surviving_binding_untouched(self):
        ex._bind_class_session("frontier", "sess-keep", time.time())
        member = bound_member("sess-keep")
        code, _, _ = self.put(frontier_cfg() + [sonnet_class()])
        self.assertEqual(code, 200)
        self.assertEqual(bound_member("sess-keep"), member,
                         "a swap that keeps the member must not churn its sessions")

    def test_deleted_class_cannot_be_rebound_or_pinned(self):
        """The §6 resurrection race, structurally: the class lookup now runs
        under _plock at every setdefault site, so a deleted class 404s and
        leaves no zombie state behind for _save_state to persist."""
        self.put([sonnet_class()])  # frontier is gone
        code, p = ex._bind_class_session("frontier", "sess-z", time.time())
        self.assertEqual(code, 404)
        self.assertEqual(p["classes"], ["beta"])
        code, _, _ = self.req("GET", "/class-pin?class=frontier&member=lane:opus")
        self.assertEqual(code, 404)
        code, _, _ = self.req("GET", "/class-bind?class=frontier&session=sess-z2")
        self.assertEqual(code, 404)
        with ex._plock:
            self.assertNotIn("frontier", ex._preferred["classes"],
                             "no zombie state for the deleted class")
        with open(ex.STATE_FILE) as f:
            self.assertNotIn("frontier", json.load(f)["classes"])


class TestClassesReload(DocAPIBase):
    def test_hand_edit_reload_bumps_revision_and_guards_stale_put(self):
        old_rev = ex._doc_revision(ex._classes_doc)
        doc = frontier_cfg() + [sonnet_class()]
        with open(self.classes_path, "w") as f:
            json.dump(doc, f)  # the hand edit
        code, p, hdrs = self.req("POST", "/classes/reload")
        self.assertEqual(code, 200)
        self.assertTrue(p["reloaded"])
        self.assertTrue(p["changed"])
        self.assertEqual(p["revision"], ex._doc_revision(doc))
        self.assertNotEqual(p["revision"], old_rev, "a hand edit changes the revision by construction")
        self.assertEqual(hdrs.get("X-Doc-Revision"), p["revision"])
        with ex._plock:
            self.assertEqual([c["class"] for c in ex._classes], ["frontier", "beta"])
        with open(self.classes_path + ".history.jsonl") as f:
            e = json.loads(f.readlines()[-1])
        self.assertEqual(e["actor"], "hand-edit (reload)")
        self.assertEqual(e["rev"], p["revision"])
        # an editor still holding the pre-edit revision must 409, not clobber
        code, p2, _ = self.req("PUT", "/classes",
                               body={"doc": frontier_cfg(), "reason": "stale editor"},
                               headers={"If-Match": old_rev})
        self.assertEqual(code, 409)
        self.assertEqual(p2["revision"], p["revision"])

    def test_reload_requires_key(self):
        code, _, _ = self.req("POST", "/classes/reload", key=None)
        self.assertEqual(code, 401)

    def test_broken_json_refused_live_config_kept(self):
        rev = ex._doc_revision(ex._classes_doc)
        with open(self.classes_path, "w") as f:
            f.write("{not json")
        code, p, _ = self.req("POST", "/classes/reload")
        self.assertEqual(code, 422)
        self.assertIn("live config kept", p["error"])
        with ex._plock:
            self.assertEqual([c["class"] for c in ex._classes], ["frontier"],
                             "reload must never wipe live routing over broken JSON")
        self.assertEqual(ex._doc_revision(ex._classes_doc), rev)

    def test_unchanged_reload_reports_changed_false(self):
        code, p, _ = self.req("POST", "/classes/reload")
        self.assertEqual(code, 200)
        self.assertFalse(p["changed"])


class TestExampleConfigStaysValid(unittest.TestCase):
    """A deployed classes.json is shaped like classes.example.json — the
    strict validator must accept every currently-deployed shape (additive
    compatibility is the hard rule)."""

    def test_example_passes_strict_validation(self):
        # pay names pots by account: validation checks them against the
        # accounts the brain knows, so the example's gpt pool must be read.
        reset(acct(ACCT_A, 10), gpt_result(), kimi_result())
        here = BRAIN
        with open(os.path.join(here, "classes.example.json")) as f:
            doc = json.load(f)
        self.assertEqual(ex._validate_classes_doc(doc), [])
        out = ex._normalize_classes(doc)
        self.assertEqual(len(out), len(doc), "tolerant load keeps every example class")


class TestConsumersDocAPI(DocAPIBase):
    def put(self, doc, if_match=None, reason="consumer edit", key=KEY):
        return self.req("PUT", "/consumers", body={"doc": doc, "reason": reason},
                        key=key,
                        headers={"If-Match": if_match or ex._doc_revision(ex._consumers_doc)})

    def test_absent_file_empty_doc_zero_warnings(self):
        code, p, hdrs = self.req("GET", "/consumers")
        self.assertEqual(code, 200)
        self.assertEqual(p["doc"], {"consumers": []})
        self.assertEqual(p["warnings"], [])
        self.assertEqual(p["derived"], [])
        self.assertEqual(hdrs.get("X-Doc-Revision"), p["revision"])
        _, pref, _ = self.req("GET", "/preferred")
        self.assertEqual(pref["consumers"], [])

    def test_get_requires_key(self):
        code, _, _ = self.req("GET", "/consumers", key=None)
        self.assertEqual(code, 401)

    def test_put_happy_path_and_preferred_exposure(self):
        code, p, _ = self.put(consumers_doc())
        self.assertEqual(code, 200)
        self.assertEqual(p["revision"], ex._doc_revision(consumers_doc()))
        with open(ex.CONSUMERS_FILE) as f:
            self.assertEqual(json.load(f), consumers_doc())
        with open(ex.CONSUMERS_FILE + ".history.jsonl") as f:
            self.assertEqual(json.loads(f.readline())["rev"], p["revision"])
        _, pref, _ = self.req("GET", "/preferred")
        ids = [c["id"] for c in pref["consumers"]]
        self.assertEqual(ids, ["coding-harness", "gateway"])
        ch = pref["consumers"][0]
        self.assertEqual(ch["request_shape"], "claude-code")
        self.assertEqual([m["status"] for m in ch["members"]], ["ok", "ok"])
        # derived consumer fields: the lane and the class each name their consumer
        self.assertEqual(pref["lanes"]["opus"]["consumer"], "coding-harness")
        self.assertIsNone(pref["lanes"]["haiku"]["consumer"])
        self.assertEqual(pref["classes"]["frontier"]["consumer"], "gateway")
        # gateway is request_shape=other but frontier has an anthropic member -> WARN
        oc = pref["consumers"][1]
        self.assertTrue(any("dead hop" in w for w in oc["warnings"]))

    def test_put_duplicate_ref_422(self):
        doc = consumers_doc()
        doc["consumers"][1]["members"].append("lane:opus")  # already coding-harness's
        code, p, _ = self.put(doc)
        self.assertEqual(code, 422)
        self.assertTrue(any("already claimed" in e for e in p["errors"]))

    def test_put_dangling_class_ref_succeeds_with_warning(self):
        doc = {"consumers": [{"id": "x", "request_shape": "other",
                              "members": ["class:not-yet"]}]}
        code, p, _ = self.put(doc)
        self.assertEqual(code, 200, "dangling is WARN, never rejection — no create-order deadlock")
        _, pref, _ = self.req("GET", "/preferred")
        self.assertEqual(pref["consumers"][0]["members"][0]["status"], "dangling")

    def test_bare_array_doc_normalized_to_wrapper(self):
        code, p, _ = self.put(consumers_doc()["consumers"])
        self.assertEqual(code, 200)
        self.assertEqual(p["doc"], consumers_doc(),
                         "the §5 example's bare-array shape canonicalizes to the wrapper")
        self.assertEqual(p["revision"], ex._doc_revision(consumers_doc()))

    def test_stale_if_match_409(self):
        self.put(consumers_doc())
        code, p, _ = self.put({"consumers": []}, if_match="feedfacefeedface")
        self.assertEqual(code, 409)
        self.assertEqual(p["revision"], ex._doc_revision(consumers_doc()))
        with ex._plock:
            self.assertEqual(len(ex._consumers), 2, "live consumers untouched")

    def test_validate_dry_run(self):
        doc = consumers_doc()
        code, p, _ = self.req("POST", "/consumers/validate", body={"doc": doc})
        self.assertEqual(code, 200)
        self.assertTrue(p["ok"])
        self.assertEqual(p["would_revision"], ex._doc_revision(doc))
        self.assertEqual([c["id"] for c in p["derived"]], ["coding-harness", "gateway"])
        with ex._plock:
            self.assertEqual(ex._consumers, [], "validate mutates nothing")

    def test_reload_hand_edit(self):
        self.install_consumers(consumers_doc())
        edited = consumers_doc()
        edited["consumers"][0]["members"].append("lane:sonnet")
        with open(ex.CONSUMERS_FILE, "w") as f:
            json.dump(edited, f)
        code, p, _ = self.req("POST", "/consumers/reload")
        self.assertEqual(code, 200)
        self.assertTrue(p["changed"])
        self.assertEqual(p["revision"], ex._doc_revision(edited))
        with open(ex.CONSUMERS_FILE + ".history.jsonl") as f:
            e = json.loads(f.readlines()[-1])
        self.assertEqual(e["actor"], "hand-edit (reload)")
        _, pref, _ = self.req("GET", "/preferred")
        self.assertEqual(pref["lanes"]["sonnet"]["consumer"], "coding-harness")

    def test_class_hot_swap_flips_ref_to_dangling_live(self):
        self.install_consumers(consumers_doc())
        _, pref, _ = self.req("GET", "/preferred")
        oc = next(c for c in pref["consumers"] if c["id"] == "gateway")
        self.assertEqual(oc["members"][0]["status"], "ok")
        code, _, _ = self.req("PUT", "/classes",
                              body={"doc": [sonnet_class()], "reason": "drop frontier"},
                              headers={"If-Match": ex._doc_revision(ex._classes_doc)})
        self.assertEqual(code, 200)
        _, pref, _ = self.req("GET", "/preferred")
        oc = next(c for c in pref["consumers"] if c["id"] == "gateway")
        self.assertEqual(oc["members"][0]["status"], "dangling",
                         "dangling-ness is computed live, not frozen at load")


class TestDocHistory(DocAPIBase):
    """GET /classes/history | /consumers/history — the /classes page's History
    evidence drawer (#94): last-N revision metadata newest-first, each entry's
    full document only on ?full=1 (a doc can be an entire classes file)."""

    def put_classes(self, doc, reason):
        return self.req("PUT", "/classes", body={"doc": doc, "reason": reason},
                        headers={"If-Match": ex._doc_revision(ex._classes_doc)})

    def test_requires_key(self):
        for path in ("/classes/history", "/consumers/history"):
            code, _, _ = self.req("GET", path, key=None)
            self.assertEqual(code, 401, path)

    def test_missing_history_file_is_empty_not_an_error(self):
        for path in ("/classes/history", "/consumers/history"):
            code, p, _ = self.req("GET", path)
            self.assertEqual(code, 200, path)
            self.assertEqual(p, {"entries": [], "total": 0}, path)

    def test_newest_first_metadata_only(self):
        self.put_classes(frontier_cfg() + [sonnet_class()], reason="add beta")
        self.put_classes(frontier_cfg(), reason="drop beta")
        code, p, _ = self.req("GET", "/classes/history")
        self.assertEqual(code, 200)
        self.assertEqual(p["total"], 2)
        self.assertEqual([e["reason"] for e in p["entries"]],
                         ["drop beta", "add beta"], "newest first")
        for e in p["entries"]:
            self.assertEqual(set(e), {"rev", "ts", "actor", "reason"},
                             "no doc per entry by default — they can be whole classes files")
        self.assertEqual(p["entries"][0]["rev"], ex._doc_revision(ex._classes_doc),
                         "the newest entry's rev is the live revision")

    def test_full_includes_doc(self):
        doc = frontier_cfg() + [sonnet_class()]
        self.put_classes(doc, reason="add beta")
        _, p, _ = self.req("GET", "/classes/history?full=1")
        self.assertEqual(p["entries"][0]["doc"], doc)
        _, p, _ = self.req("GET", "/classes/history?full=0")
        self.assertNotIn("doc", p["entries"][0])

    def test_limit_default_20_and_cap_200(self):
        for i in range(230):
            ex._append_history(self.classes_path, f"rev{i}", "t", f"r{i}", [])
        _, p, _ = self.req("GET", "/classes/history")
        self.assertEqual(p["total"], 230, "total counts the whole log, not the page")
        self.assertEqual(len(p["entries"]), 20, "default limit is 20")
        self.assertEqual(p["entries"][0]["rev"], "rev229")
        _, p, _ = self.req("GET", "/classes/history?limit=5")
        self.assertEqual([e["rev"] for e in p["entries"]],
                         [f"rev{i}" for i in range(229, 224, -1)])
        _, p, _ = self.req("GET", "/classes/history?limit=1000")
        self.assertEqual(len(p["entries"]), 200, "limit is capped at 200")

    def test_bad_limit_400(self):
        code, _, _ = self.req("GET", "/classes/history?limit=nope")
        self.assertEqual(code, 400)

    def test_torn_line_skipped(self):
        ex._append_history(self.classes_path, "rev-ok", "t", "fine", [])
        with open(self.classes_path + ".history.jsonl", "a") as f:
            f.write('{"rev": "torn half')  # crash mid-append
        code, p, _ = self.req("GET", "/classes/history")
        self.assertEqual(code, 200, "a torn line must not 500 the drawer")
        self.assertEqual(p["total"], 1)
        self.assertEqual(p["entries"][0]["rev"], "rev-ok")

    def test_consumers_history_after_put_and_reload(self):
        self.req("PUT", "/consumers", body={"doc": consumers_doc(), "reason": "seed"},
                 headers={"If-Match": ex._doc_revision(ex._consumers_doc)})
        with open(ex.CONSUMERS_FILE, "w") as f:
            json.dump({"consumers": []}, f)
        self.req("POST", "/consumers/reload", body={})
        code, p, _ = self.req("GET", "/consumers/history")
        self.assertEqual(code, 200)
        self.assertEqual(p["total"], 2)
        self.assertEqual([e["actor"] for e in p["entries"]],
                         ["hand-edit (reload)", "unknown"], "newest first")
        self.assertEqual(p["entries"][1]["reason"], "seed")


class TestPost404BodyDrain(DocAPIBase):
    def test_typoed_post_path_with_large_body_gets_a_clean_404(self):
        """do_POST's fallthrough must drain like do_PUT's: a typo'd path
        arrives with the JSON body already in flight, and closing with it
        unread makes the kernel RST the 404 off the socket — the client would
        see a connection reset (an exception here), not this status."""
        body = json.dumps({"doc": ["x" * (512 * 1024)], "reason": "typo"}).encode()
        code, _, _ = self.req("POST", "/classes/validat", raw=body)
        self.assertEqual(code, 404)


class TestPreferredConsumerContract(DocAPIBase):
    """Item 5: /preferred exposure — consumers[], derived consumer fields, and
    the member-level incumbent the tanks' shared-window join needs."""

    def test_member_incumbent_present(self):
        _, pref, _ = self.req("GET", "/preferred")
        for m in pref["classes"]["frontier"]["members"]:
            self.assertIn("incumbent", m,
                          "member incumbent is the §4 shared-window join key")
        anth = pref["classes"]["frontier"]["members"][0]
        self.assertEqual(anth["incumbent"], ACCT_A,
                         "anthropic member incumbent = its lane's account")

    def test_top_level_providers_present(self):
        _, pref, _ = self.req("GET", "/preferred")
        self.assertIn("providers", pref)

    def test_lane_and_class_consumer_default_null(self):
        _, pref, _ = self.req("GET", "/preferred")
        self.assertIsNone(pref["lanes"]["opus"]["consumer"])
        self.assertIsNone(pref["classes"]["frontier"]["consumer"])

    def test_typed_sessions_still_exposed(self):
        ex._bind_class_session("frontier", "sess-ty", time.time())
        _, pref, _ = self.req("GET", "/preferred")
        s = pref["classes"]["frontier"]["sessions"]["sess-ty"]
        for k in ("member", "since", "last_seen", "reason"):
            self.assertIn(k, s)
        self.assertNotIn("marks", s, "marks stay internal bookkeeping")
        self.assertNotIn("pay_settings", s, "so does the pay digest")


def gpt_on_credits(pct=100.0, has_credits=True, good_age=0, unlimited=False,
                   overage=False, spend=False, balance=None):
    """A gpt poll at `pct` on its weekly window (a 5h one at 0), allowed while
    under 100, carrying the wham credits object and spend_control. The
    balance defaults to a neutral 250 while credits are enabled, else 0."""
    r = provider_result("gpt", five_hour=0.0, weekly=pct, good_age=good_age)
    r["raw"]["credits"].update(
        has_credits=has_credits, unlimited=unlimited, overage_limit_reached=overage,
        balance=f"{balance if balance is not None else (250.0 if has_credits else 0.0)}")
    r["raw"]["spend_control"] = {"reached": spend, "individual_limit": None}
    return read(r)


class TestStanding(PayBase):
    """_account_standing: the golden shape, from today's readings."""

    def standing(self, name):
        with ex._lock:
            snap = list(ex._state.values())
        with ex._plock:
            return ex._policy_ctx(snap, time.time())["standings"][name]

    def test_anthropic_shape(self):
        reset(opus_spent())
        st = self.standing(ACCT_A)
        self.assertEqual(set(st), {"provider", "kind", "as_of", "next_poll_at", "recheck_epoch",
                                   "source", "plan", "allowance", "money", "beside", "models"})
        self.assertIsNone(st["recheck_epoch"], "no owner re-check yet")
        self.assertEqual((st["provider"], st["kind"]), ("anthropic", "subscription"))
        self.assertEqual(st["plan"], {"label": "max", "basis": "derived"})
        self.assertIsNone(st["allowance"]["open"], "Anthropic states no gate of its own")
        self.assertEqual(set(st["allowance"]["windows"]), {"five_hour", "seven_day", "scoped:Opus"})
        w = st["allowance"]["windows"]["scoped:Opus"]
        self.assertEqual((w["used_pct"], w["period"], w["scope"]), (100.0, "7d", "Opus"))
        self.assertEqual(st["allowance"]["windows"]["five_hour"]["period"], "5h")
        self.assertIsNone(st["allowance"]["windows"]["five_hour"]["scope"])
        for w in st["allowance"]["windows"].values():
            self.assertEqual(w["as_of"], st["as_of"], "a window's as_of is its reading's")
        self.assertEqual([x["kind"] for x in st["allowance"]["windows"]["scoped:Opus"]["sources"]],
                         ["weekly_scoped"], "the limits[] entries it was read from")
        cr = st["money"]["credits"]
        self.assertEqual((cr["open"], cr["open_basis"], cr["balance"], cr["unit"]),
                         (True, "exact", None, "usd"))
        self.assertEqual(cr["limits"]["balance"]["basis"], "unknown",
                         "no field carries the balance (U6): unknown, never 0")
        self.assertEqual(cr["limits"]["monthly_limit"]["limit"], 5000.0)

    def test_anthropic_credits_open_by_extra_usage(self):
        # Absent extra_usage says neither on nor off: unknown, never closed
        # (C2; C10 reads it).
        cases = ((None, None), ({}, None), ({"is_enabled": False}, False),
                 ({"is_enabled": True}, True),
                 ({"is_enabled": True, "monthly_limit": 100, "used_credits": 100}, True),
                 ({"is_enabled": True, "utilization": 100.0}, True))
        for extra, want in cases:
            with self.subTest(extra=extra):
                a = acct(ACCT_A, 10)
                if extra is not None:
                    a["raw"]["extra_usage"] = extra
                reset(a)
                self.assertIs(self.standing(ACCT_A)["money"]["credits"]["open"], want)

    def test_anthropic_credits_unknown_holds_a_stale_route(self):
        # §1.6 step 4: a stale reading whose money may be open is no(unknown).
        reset(acct(ACCT_A, 10, good_age=3600))
        c = route_check("lane:opus", ACCT_A)
        self.assertEqual((c["check"], c["reason"], c["basis"]), ("no", "unknown", "stale"))

    def test_codex_and_kimi(self):
        reset(gpt_result(weekly=40.0, credits={"has_credits": True, "balance": 12.5}),
              kimi_result())
        g = self.standing("gpt")
        self.assertEqual(g["provider"], "gpt")
        self.assertIs(g["allowance"]["open"], True)
        self.assertEqual(g["allowance"]["open_basis"], "exact")
        self.assertEqual(g["allowance"]["reset_options"]["basis"], "exact")
        cr = g["money"]["credits"]
        self.assertEqual((cr["open"], cr["open_basis"], cr["balance"], cr["unit"]),
                         (True, "derived", 12.5, "codex_credit"))
        self.assertIs(cr["limits"]["balance"]["advisory"], True)
        self.assertEqual(g["models"]["gpt-test"]["basis"], "unknown_semantics")
        self.assertEqual(g["allowance"]["windows"]["seven_day"]["used_pct"], 40.0)
        k = self.standing("kimi")
        self.assertEqual(k["provider"], "kimi")
        self.assertIsNone(k["allowance"]["open"])
        cr = k["money"]["credits"]
        self.assertEqual((cr["open"], cr["balance"], cr["unit"]), (None, None, None),
                         "Kimi's Extra Usage has no API: unknown, never closed")
        self.assertEqual(k["beside"][0]["kind"], "concurrency")
        self.assertEqual(set(k["allowance"]["windows"]), {"five_hour", "seven_day"})

    def test_never_measured_and_next_poll(self):
        reset(acct(ACCT_A, 10))
        with ex._poll_health_lock:
            ex._poll_schedule[ACCT_B] = time.time() + 600
        st = self.standing(ACCT_B)
        self.assertIsNone(st["as_of"], "in the creds cache, no reading yet")
        self.assertAlmostEqual(st["next_poll_at"], time.time() + 600, delta=2)
        self.assertEqual(route_check("lane:opus", ACCT_B)["reason"], "unknown")

    def test_failed_poll_keeps_last_good_as_of(self):
        """The poller keeps the last good usage and good_ts on a failure; the
        standing then ages rather than blanks."""
        a = acct(ACCT_A, 10)
        a.update(ok=0, stale=1, good_ts=time.time() - 900, err="usage HTTP 500", http=500)
        reset(a)
        self.assertAlmostEqual(self.standing(ACCT_A)["as_of"], time.time() - 900, delta=2)


class TestPollBackoff(unittest.TestCase):
    """quota-model 50 §2.10: no HTTP response retries at the normal cadence; an
    HTTP error honours Retry-After, else backs off to at most POLL_MAX_BACKOFF,
    below STALE_MAX."""

    def test_no_http_response_retries_at_poll_interval(self):
        for fails in (1, 2, 5, 20):
            self.assertEqual(ex._poll_backoff({"http": 0, "retry_after": None}, fails),
                             ex.POLL_INTERVAL)

    def test_http_error_honours_retry_after(self):
        self.assertEqual(ex._poll_backoff({"http": 429, "retry_after": 42}, 3), 42)

    def test_http_error_backs_off_below_stale_max(self):
        got = [ex._poll_backoff({"http": 503, "retry_after": None}, n) for n in range(1, 8)]
        self.assertEqual(got[0], ex.POLL_INTERVAL * 2)
        self.assertEqual(max(got), ex.POLL_MAX_BACKOFF)
        self.assertLess(ex.POLL_MAX_BACKOFF, ex.STALE_MAX)

    def test_poller_schedules_and_publishes_next_poll(self):
        """Three poller passes over one provider target: a DNS failure, an HTTP
        503, a success. The schedule (which the standing publishes as
        next_poll_at) follows _poll_backoff, and a success resets the count.
        Dispatch and creds are stubbed: nothing reaches the network."""
        reset()
        outcomes = iter((dict(http=0, err="usage URLError: dns"),
                         dict(http=503, err="usage HTTP 503"), None))

        def poll(prev, account=None):  # noqa: ARG001
            r = kimi_result()
            o = next(outcomes)
            if o:
                r.update(ok=0, usage=None, **o)
            return r

        class Stop(Exception):
            pass

        def wait(_t):
            raise Stop

        for obj, attr, val in ((psub, "_load_creds", lambda: []),
                               (ex, "_provider_poll_targets", lambda: {"kimi": ("kimi", None)}),
                               (ex.PROVIDERS["kimi"], "poll", poll),
                               (ex._poll_wakeup, "wait", wait)):
            old = obj[attr] if isinstance(obj, dict) else getattr(obj, attr)
            if isinstance(obj, dict):
                obj[attr] = val
                self.addCleanup(obj.__setitem__, attr, old)
            else:
                setattr(obj, attr, val)
                self.addCleanup(setattr, obj, attr, old)
        self.addCleanup(ex._poll_schedule.clear)
        self.addCleanup(ex._poll_fails.clear)

        def one_pass():
            with ex._poll_health_lock:
                ex._poll_schedule["kimi"] = 0
            t0 = time.time()
            with self.assertRaises(Stop):
                ex.poller()
            return t0

        t0 = one_pass()
        self.assertAlmostEqual(ex._poll_schedule["kimi"], t0 + ex.POLL_INTERVAL, delta=5,
                               msg="no HTTP response: the normal cadence")
        t0 = one_pass()
        self.assertEqual(ex._poll_fails["kimi"], 2)
        self.assertAlmostEqual(ex._poll_schedule["kimi"],
                               t0 + min(ex.POLL_INTERVAL * 4, ex.POLL_MAX_BACKOFF), delta=5)
        with ex._plock:
            st = ex._policy_ctx(list(ex._state.values()), time.time())["standings"]["kimi"]
        self.assertEqual(st["next_poll_at"], round(ex._poll_schedule["kimi"]))
        t0 = one_pass()
        self.assertEqual(ex._poll_fails["kimi"], 0, "a success is never a failure")
        self.assertAlmostEqual(ex._poll_schedule["kimi"], t0 + ex.POLL_INTERVAL, delta=5)

    def test_poller_goes_on_past_an_idle_five_hour_window(self):
        """Anthropic serves "five_hour": null while that window is idle. A
        poll's log line that read it as a dict once killed the poller thread,
        and no account was polled again. A whole pass, through poll_account and
        its adapter: the idle account is a good poll with no five_hour window,
        the next account is polled, and the pass runs compute_preferred and the
        save."""
        reset()
        bodies = {ACCT_A: {"five_hour": None, "seven_day": {"utilization": 99.0}, "limits": []},
                  ACCT_B: {"five_hour": {"utilization": 10.0}, "seven_day": {"utilization": 40.0},
                           "limits": []}}
        polled, ran = [], []

        def get(path, token):
            if path == "/api/oauth/profile":
                return {"account": {"email": token}, "organization": {"rate_limit_tier": "max"}}
            polled.append(token)
            return json.loads(json.dumps(bodies[token]))

        class Stop(Exception):
            pass

        def wait(_t):
            raise Stop

        compute, save = ex.compute_preferred, ex._save_state
        for obj, attr, val in ((psub, "_load_creds", lambda: [ACCT_A, ACCT_B]),
                               (ex, "_provider_poll_targets", lambda: {}),
                               (psub, "_ensure_token", lambda name, **_: name),
                               (psub, "_get", get),
                               (ex, "compute_preferred", lambda *a, **k: (ran.append("compute"), compute(*a, **k))[1]),
                               (ex, "_save_state", lambda *a, **k: (ran.append("save"), save(*a, **k))[1]),
                               (ex._poll_wakeup, "wait", wait)):
            self.addCleanup(setattr, obj, attr, getattr(obj, attr))
            setattr(obj, attr, val)
        self.addCleanup(ex._poll_schedule.clear)
        self.addCleanup(ex._poll_fails.clear)
        with self.assertRaises(Stop):
            ex.poller()
        self.assertEqual(polled, [ACCT_A, ACCT_B])
        self.assertEqual(ex._poll_fails.get(ACCT_A), 0, "the idle account is a good poll")
        self.assertIn("compute", ran)
        self.assertIn("save", ran)
        with ex._lock:
            self.assertEqual(ex._state[ACCT_A]["ok"], 1)
            self.assertNotIn("five_hour", ex._reading_windows(ex._state[ACCT_A]["reading"]))
            self.assertEqual(ex._state[ACCT_B]["ok"], 1)

    def test_a_body_of_the_wrong_types_fails_its_own_poll_only(self):
        """A provider body whose fields have the wrong types raises in the
        adapter. That target's poll fails, visibly: POLL FAILED with the
        exception and its traceback, backed off like any failed poll, its last
        good reading served stale. The other targets are polled in the same
        pass, and the pass runs compute_preferred and the save."""
        def bad_codex(b):
            b["additional_rate_limits"] = 7

        def bad_kimi(b):
            b["limits"] = 7

        for broken, spoil in (("gpt", bad_codex), ("kimi", bad_kimi)):
            with self.subTest(broken=broken):
                self._wrong_types_pass(broken, spoil)

    def _wrong_types_pass(self, broken, spoil):
        reset()
        ex._poll_schedule.clear()
        ex._poll_fails.clear()
        seed_openai_creds()
        pcodex._openai_pool = None
        self.addCleanup(clear_openai_creds)
        bodies = {"gpt": wham_usage(), "kimi": kimi_raw()}
        polled, ran, logged = [], [], []

        def body(target):
            polled.append(target)
            return json.loads(json.dumps(bodies[target]))

        def get(path, token):
            if path == "/api/oauth/profile":
                return {"account": {"email": token}, "organization": {"rate_limit_tier": "max"}}
            polled.append(token)
            return {"five_hour": {"utilization": 10.0}, "seven_day": {"utilization": 40.0}, "limits": []}

        class Stop(Exception):
            pass

        def wait(_t):
            raise Stop

        compute, save = ex.compute_preferred, ex._save_state
        patch_codex_reads(self, lambda tok, acct_id: body("gpt"))
        for obj, attr, val in ((pkimi, "KIMI_API_KEY", "sk-kimi-testdeadbeef"),
                               (pkimi, "_get_kimi_usages", lambda key: body("kimi")),
                               (psub, "_load_creds", lambda: [ACCT_A]),
                               (ex, "_provider_poll_targets",
                                lambda: {"gpt": ("gpt", None), "kimi": ("kimi", None)}),
                               (psub, "_ensure_token", lambda name, **_: name),
                               (psub, "_get", get),
                               (ex, "log", logged.append),
                               (ex, "compute_preferred", lambda *a, **k: (ran.append("compute"), compute(*a, **k))[1]),
                               (ex, "_save_state", lambda *a, **k: (ran.append("save"), save(*a, **k))[1]),
                               (ex._poll_wakeup, "wait", wait)):
            self.addCleanup(setattr, obj, attr, getattr(obj, attr))
            setattr(obj, attr, val)
        self.addCleanup(ex._poll_schedule.clear)
        self.addCleanup(ex._poll_fails.clear)
        with self.assertRaises(Stop):
            ex.poller()  # every target good: the last good reading
        with ex._lock:
            good = ex._state[broken]["reading"]
        ex._poll_schedule.clear()
        polled.clear(), ran.clear(), logged.clear()
        spoil(bodies[broken])
        t0 = time.time()
        with self.assertRaises(Stop):
            ex.poller()
        self.assertEqual(sorted(polled), sorted([ACCT_A, "gpt", "kimi"]), "every target is polled")
        self.assertEqual(ran, ["compute", "save"])
        with ex._lock:
            res = ex._state[broken]
            others = {n: r["ok"] for n, r in ex._state.items() if n != broken}
        self.assertEqual(others, {ACCT_A: 1, ({"gpt", "kimi"} - {broken}).pop(): 1})
        self.assertEqual((res["ok"], res["stale"]), (0, 1))
        self.assertIs(res["reading"], good, "the last good reading is served, stale")
        self.assertTrue(res["err"].startswith("raised TypeError: "), res["err"])
        self.assertEqual(ex._poll_fails[broken], 1)
        self.assertAlmostEqual(ex._poll_schedule[broken], t0 + ex.POLL_INTERVAL, delta=5)
        failed = [i for i, ln in enumerate(logged) if ln.startswith(f"{broken}: POLL FAILED — raised TypeError")]
        self.assertEqual(len(failed), 1, logged)
        self.assertIn("(serving last-good)", logged[failed[0]])
        tb = logged[failed[0] + 1]
        self.assertTrue(tb.startswith("Traceback (most recent call last):"), tb)
        self.assertIn("_codex_reading" if broken == "gpt" else "_kimi_reading", tb)

    def test_poller_persists_under_the_policy_lock(self):
        """The poller's save reads _preferred, which the policy and the HTTP
        handlers mutate under _plock; it must hold it too (a dict changing
        size mid-dump raises, and a torn decision would be persisted)."""
        reset()

        class Stop(Exception):
            pass

        def wait(_t):
            raise Stop

        held = []
        snapshot = ex._readings_snapshot

        def spy():
            held.append(ex._plock.locked())
            return snapshot()

        for obj, attr, val in ((psub, "_load_creds", lambda: []),
                               (ex, "_provider_poll_targets", lambda: {"kimi": ("kimi", None)}),
                               (ex.PROVIDERS["kimi"], "poll", lambda prev, account=None: kimi_result()),
                               (ex._poll_wakeup, "wait", wait),
                               (ex, "_readings_snapshot", spy)):
            old = obj[attr] if isinstance(obj, dict) else getattr(obj, attr)
            if isinstance(obj, dict):
                obj[attr] = val
                self.addCleanup(obj.__setitem__, attr, old)
            else:
                setattr(obj, attr, val)
                self.addCleanup(setattr, obj, attr, old)
        self.addCleanup(ex._poll_schedule.clear)
        self.addCleanup(ex._poll_fails.clear)
        with ex._poll_health_lock:
            ex._poll_schedule["kimi"] = 0
        with self.assertRaises(Stop):
            ex.poller()
        self.assertGreaterEqual(len(held), 2, "the policy's save and the poller's own")
        self.assertTrue(all(held), f"every save holds _plock: {held}")


class TestPointerOnChecks(PayBase):
    """§2.2 row 1, §2.3: passes 1-2 and the coolest over included members, then
    the first paid member by pay_order, else None."""

    def test_included_beats_a_higher_priority_paid_member(self):
        classes_reset(opus_spent(), provider_result("gpt", five_hour=97.0),
                      kimi_result(five_hour=100.0), cfg=pay_cfg(opus=[POT_A]))
        v = class_verdicts()
        self.assertEqual(v["lane:opus"]["check"]["check"], "paid")
        self.assertEqual(v["gpt/gpt-5.6"]["check"]["check"], "included")
        self.assertEqual(class_pointer(), "gpt/gpt-5.6",
                         "a 97 % included member is still pointable, and ahead of money")

    def test_pass_3_is_the_first_paid_member_by_pay_order(self):
        cfg = pay_cfg(opus=[POT_A], gpt=["gpt/credits"])
        classes_reset(opus_spent(), gpt_on_credits(), kimi_result(five_hour=100.0), cfg=cfg)
        for order, want in (([], "lane:opus"),                 # member priority alone
                            (["gpt/credits"], "gpt/gpt-5.6"),  # listed before unlisted
                            (["kimi/credits"], "lane:opus"),   # listed but not drawn
                            ([POT_A, "gpt/credits"], "lane:opus"),
                            (["gpt/credits", POT_A], "gpt/gpt-5.6")):
            with self.subTest(pay_order=order):
                install_money({"pay_order": order})
                self.assertEqual(class_pointer(), want)

    def test_none_when_nothing_is_included_or_paid(self):
        classes_reset(opus_spent(), gpt_on_credits(), kimi_result(five_hour=100.0),
                      cfg=pay_cfg())
        self.assertEqual({mid: v["check"]["check"] for mid, v in class_verdicts().items()},
                         {"lane:opus": "no", "gpt/gpt-5.6": "no", "kimi/kimi-k3": "no"})
        self.assertIsNone(class_pointer())

    def test_fresh_included_ranks_before_stale_included(self):
        """Credits off: nothing billable behind a stale reading, so ACCT_A is
        included on its last value (basis stale) and ranks after fresh."""
        a = opus_acct(10, resets=True)
        a["good_ts"] = time.time() - ex.STALE_MAX - 100
        a["raw"]["extra_usage"] = {"is_enabled": False}
        classes_reset(a, provider_result("gpt", five_hour=50.0), kimi_result(), cfg=pay_cfg())
        v = class_verdicts()
        self.assertEqual((v["lane:opus"]["check"]["check"], v["lane:opus"]["check"]["basis"]),
                         ("included", "stale"))
        self.assertEqual(class_pointer(), "gpt/gpt-5.6")
        with ex._lock:
            del ex._state["gpt"]
            del ex._state["kimi"]
        self.assertEqual(class_pointer(), "lane:opus", "stale is still used when nothing fresh is")


class TestClassBindUnavailableHTTP(PayBase, HTTPBase):
    """§5.2's C3 line, offline: a spent class answers 409 with a Retry-After
    that matches available_at; with no time, no Retry-After."""

    def call(self, sid):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/class-bind?class=frontier&session={sid}")
        req.add_header("X-Switch-Key", "test-switch-key")
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, r.headers, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, e.headers, json.loads(e.read().decode())

    def test_retry_after_matches_available_at(self):
        classes_reset(opus_spent(), gpt_on_credits(), kimi_result(five_hour=100.0), cfg=pay_cfg())
        before = time.time()
        code, hdr, p = self.call("h1")
        self.assertEqual(code, 409)
        self.assertEqual(int(hdr["Retry-After"]), p["retry_after_s"])
        self.assertLessEqual(abs(before + p["retry_after_s"] - p["available_at"]), 2)

    def test_no_time_no_retry_after(self):
        gpt = gpt_on_credits()
        gpt["raw"]["rate_limit"]["primary_window"].update(reset_at=None, reset_after_seconds=None)
        classes_reset(gpt, cfg=[{"class": "frontier", "members": [
            {"priority": 1, "kind": "provider", "provider": "gpt", "model": "gpt-5.6",
             "context_window": 400000, "anthropic_token_ratio": 0.6}]}])
        code, hdr, p = self.call("h2")
        self.assertEqual(code, 409)
        self.assertIsNone(hdr.get("Retry-After"))
        self.assertIsNone(p["retry_after_s"])


    def test_a_reached_budget_retries_at_the_period_end(self):
        """C6's budget `no` in C3's reply: with no reset read, the time is the
        budget period's end (exact), and the owner option is raising the
        budget. A budget of 0 is reached with nothing spent (§1.9)."""
        gpt = gpt_on_credits(pct=100.0)
        gpt["raw"]["rate_limit"]["primary_window"].update(reset_at=None, reset_after_seconds=None)
        gpt["reset_options"] = None  # no reset-credits read: the budget option alone
        classes_reset(gpt, cfg=[{"class": "frontier", "members": [
            {"priority": 1, "kind": "provider", "provider": "gpt", "model": "gpt-5.6",
             "context_window": 400000, "anthropic_token_ratio": 0.6, "pay": ["gpt/credits"]}]}])
        install_money({"pots": {"gpt/credits": {"budget": 0}}})
        before = time.time()
        code, hdr, p = self.call("h3")
        self.assertEqual(code, 409)
        end = ex._budget_period(before)[1]
        a = p["members"][0]["accounts"][0]
        self.assertEqual((a["why"], a["until"], a["window"]), ("budget", end, None))
        self.assertEqual((p["available_at"], p["available_basis"]), (end, "exact"))
        self.assertEqual(int(hdr["Retry-After"]), p["retry_after_s"])
        self.assertEqual([(o["kind"], o["pot"], o["action"]) for o in p["options"]],
                         [("budget", "gpt/credits", "raise gpt/credits's budget")])


class TestBindAndPinOnChecks(PayBase):
    """§2.2 rows 2 and 3."""

    def test_bind_included(self):
        classes_reset(opus_acct(30, resets=True), cfg=pay_cfg())
        code, p = self.bind()
        self.assertEqual(code, 200)
        self.assertEqual((p["route"], p["check"]["check"]), ("frontier/lane:opus", "included"))

    def test_bind_paid(self):
        classes_reset(opus_spent(), gpt_on_credits(), kimi_result(five_hour=100.0),
                      cfg=pay_cfg(gpt=["gpt/credits"]))
        code, p = self.bind()
        self.assertEqual(code, 200)
        self.assertEqual(p["route"], "frontier/gpt/gpt-5.6")
        self.assertEqual(p["check"], {"check": "paid", "pot": "gpt/credits", "reason": None,
                                      "until": None, "basis": "fresh"})

    def test_bind_none_is_409_class_unavailable(self):
        """§2.2 row 2, §2.8: every member `no`, each with a time. The body
        names every route and account, the earliest time and the options."""
        classes_reset(opus_spent(), gpt_on_credits(), kimi_result(five_hour=100.0),
                      cfg=pay_cfg())
        now = time.time()
        code, p = ex._bind_class_session("frontier", "s1", now)
        self.assertEqual(code, 409)
        self.assertEqual((p["error"], p["class"]), ("class_unavailable", "frontier"))
        by_route = {m["route"]: m["accounts"] for m in p["members"]}
        self.assertEqual(list(by_route), ["frontier/lane:opus", "frontier/gpt/gpt-5.6",
                                          "frontier/kimi/kimi-k3"])
        gpt = by_route["frontier/gpt/gpt-5.6"][0]
        self.assertEqual((gpt["account"], gpt["check"], gpt["why"]), ("gpt", "no", "pay"))
        kimi = by_route["frontier/kimi/kimi-k3"][0]
        self.assertEqual((kimi["why"], kimi["window"]), ("pay", "five_hour"))
        untils = [a["until"] for accts in by_route.values() for a in accts
                  if isinstance(a["until"], (int, float))]
        self.assertEqual(p["available_at"], min(untils))
        self.assertEqual(p["retry_after_s"], max(1, math.ceil(p["available_at"] - now)))
        self.assertIs(p["retryable"], True)
        kinds = {(o["kind"], o["pot"]) for o in p["options"]}
        self.assertIn(("permit", "gpt/credits"), kinds, "credits are open; the route may not pay")
        self.assertIn(("permit", POT_A), kinds)

    def test_bind_until_paid_only_is_not_retryable(self):
        """§2.8 item 2: a `no (pay)` with no reset known contributes no time;
        with nothing else, the reply carries no Retry-After and is the
        non-retryable money reply: only the owner's permit would serve."""
        gpt = gpt_on_credits()
        gpt["raw"]["rate_limit"]["primary_window"].update(reset_at=None, reset_after_seconds=None)
        gpt["reset_options"] = None  # no reset-credits read: the permit option alone
        cfg = [{"class": "frontier", "members": [
            {"priority": 1, "kind": "provider", "provider": "gpt", "model": "gpt-5.6",
             "context_window": 400000, "anthropic_token_ratio": 0.6}]}]
        classes_reset(gpt, cfg=cfg)
        code, p = self.bind()
        self.assertEqual(code, 409)
        self.assertEqual(p["members"][0]["accounts"][0]["until"], "until_paid")
        self.assertIsNone(p["available_at"])
        self.assertIsNone(p["retry_after_s"])
        self.assertIs(p["retryable"], False)
        self.assertEqual([o["kind"] for o in p["options"]], ["permit"])

    def test_bind_topup_moves_the_time_of_a_closed_pot(self):
        """§1.9 topup: credits closed and the allowance reset unknown. `off`
        retries at the next poll (no reset known); `on` refills then too, and
        a fund option names the pot either way."""
        for topup in ("off", "on"):
            with self.subTest(topup=topup):
                gpt = gpt_on_credits(has_credits=False)
                gpt["reset_options"] = None  # no reset-credits read: the fund option alone
                cfg = [{"class": "frontier", "members": [
                    {"priority": 1, "kind": "provider", "provider": "gpt", "model": "gpt-5.6",
                     "context_window": 400000, "anthropic_token_ratio": 0.6, "pay": ["gpt/credits"]}]}]
                classes_reset(gpt, cfg=cfg)
                install_money({"pay_order": [], "pots": {"gpt/credits": {"topup": topup}}})
                now = time.time()
                with ex._poll_health_lock:
                    ex._poll_schedule["gpt"] = now + 600
                code, p = ex._bind_class_session("frontier", f"s-{topup}", now)
                self.assertEqual(code, 409)
                acct = p["members"][0]["accounts"][0]
                self.assertEqual(acct["why"], "spent")
                reopen = round(gpt["raw"]["rate_limit"]["primary_window"]["reset_at"])
                want = min(reopen, round(now + 600)) if topup == "on" else reopen
                self.assertEqual((acct["until"], p["available_at"]), (want, want))
                self.assertEqual([o["kind"] for o in p["options"]], ["fund"])

    def test_an_existing_binding_is_not_refused(self):
        """Only a NEW session gets the 409: a bound one keeps its binding and
        the proxy's walk answers it (§2.2 row 6)."""
        classes_reset(opus_acct(30, resets=True), cfg=pay_cfg())
        self.assertEqual(self.bind()[0], 200)
        set_opus(cap=100)
        self.assertEqual(self.bind()[0], 200)

    def test_an_existing_binding_reports_its_check_now(self):
        classes_reset(opus_acct(30, resets=True), kimi_result(), cfg=pay_cfg())
        self.bind()
        set_opus(cap=100)
        with ex._plock:  # hold the session where it is to read the bound check
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["member"] = "lane:opus"
        code, p = self.bind()
        self.assertEqual(code, 200)
        self.assertFalse(p["created"])
        self.assertEqual(p["check"]["check"], "no")

    def test_pin_admits_a_member_whose_check_is_not_no(self):
        classes_reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result(),
                      cfg=pay_cfg(gpt=["gpt/credits"]))
        with ex._plock:
            ex._preferred["classes"]["frontier"] = {"pinned": "gpt/gpt-5.6", "sessions": {}}
        code, p = self.bind()
        self.assertEqual((code, p["member"]["id"], p["check"]["check"]),
                         (200, "gpt/gpt-5.6", "paid"), "the route's own pay admits it")

    def test_pin_refuses_no_and_never_grants_pay(self):
        """gpt at 100 with credits open but pay [] reads no (pay): the pin does
        not seat the session there; the pointer does."""
        classes_reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result(),
                      cfg=pay_cfg())
        with ex._plock:
            ex._preferred["classes"]["frontier"] = {"pinned": "gpt/gpt-5.6", "sessions": {}}
        code, p = self.bind()
        self.assertEqual((code, p["member"]["id"]), (200, "lane:opus"))
        self.assertEqual(p["reason"], "bind: pointer")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "lane:opus", "nor does the session policy")


class TestSessionMovesOnChecks(_LogCapture, PayBase):
    """§2.2 row 4, §2.3 session moves."""

    def test_rebind_when_the_bound_member_reads_no(self):
        classes_reset(opus_acct(30, resets=True), kimi_result(), cfg=pay_cfg())
        self.bind()
        with ex._plock:  # inside dwell: a `no` rebind is not dwell-gated
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["last_switch"] = time.time()
        self.capture_log()
        set_opus(cap=100)
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3")
        line = next(ln for ln in self.logged if "session s1 -> kimi/kimi-k3" in ln)
        # No extra_usage in the reading: credits may be on (unknown), and the
        # route pays from nothing, so pay, not spent.
        self.assertIn("rebind: lane:opus no:pay", line)
        self.assertIn("check=included(fresh)", line, "every bind/rebind line names the check")

    def test_paid_moves_to_included_at_the_dwell_gate_and_not_before(self):
        cfg = pay_cfg(gpt=["gpt/credits"])
        classes_reset(opus_spent(), gpt_on_credits(), kimi_result(five_hour=100.0), cfg=cfg)
        self.assertEqual(self.bind()[1]["member"]["id"], "gpt/gpt-5.6")
        with ex._plock:
            ex._preferred["classes"]["frontier"]["sessions"]["s1"]["last_switch"] = time.time()
        with ex._lock:
            ex._state["kimi"] = kimi_result(five_hour=10.0)  # kimi's window reset
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6", "inside dwell: still on money")
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3", "money ends once allowance returns")

    def test_paid_stays_while_the_pointer_is_paid(self):
        """No moves within the paid tier on readings: the pointer moves to
        opus's credits when they open, and the session stays on gpt's. Only
        an owner change to the pay settings re-seats it (TestRepay)."""
        cfg = pay_cfg(opus=[POT_A], gpt=["gpt/credits"])
        classes_reset(opus_spent(credits=False), gpt_on_credits(), kimi_result(five_hour=100.0),
                      cfg=cfg)
        install_money({"pay_order": [POT_A]})
        self.assertEqual(self.bind()[1]["member"]["id"], "gpt/gpt-5.6", "opus's credits are off")
        with ex._lock:
            ex._state[ACCT_A] = opus_spent()
        self.assertEqual(class_pointer(), "lane:opus")
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6")

    def test_forced_rules_never_move_onto_a_paid_member(self):
        """opus included at 96 (>= hard); gpt is paid at 100. The hard rule
        compares effective between included members only, so the session
        stays; once kimi is included and cooler, the hard rule takes it there."""
        cfg = pay_cfg(gpt=["gpt/credits"])
        classes_reset(opus_acct(30, resets=True), gpt_on_credits(),
                      kimi_result(five_hour=100.0), cfg=cfg)
        self.bind()
        set_opus(cap=96)
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "lane:opus")
        with ex._lock:
            ex._state["kimi"] = kimi_result(five_hour=20.0)
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "kimi/kimi-k3")

    def test_paid_member_without_an_effective_is_not_rebound(self):
        """An api-like member: its allowance gate is closed and no window
        carries a number, so effective is None, and it is paid. Lacking an
        effective is not a reason to move it (§2.3)."""
        g = gpt_result(allowed=False, credits={"has_credits": True, "balance": 9.0})
        g["raw"]["rate_limit"]["primary_window"]["used_percent"] = None
        classes_reset(opus_spent(), g, kimi_result(five_hour=100.0),
                      cfg=pay_cfg(gpt=["gpt/credits"]))
        v = class_verdicts()["gpt/gpt-5.6"]
        self.assertEqual((v["check"]["check"], v["effective"]), ("paid", None))
        self.bind()
        age_session("s1")
        ex.compute_preferred()
        self.assertEqual(bound_member("s1"), "gpt/gpt-5.6")


class TestPaySettings(PayBase):
    """pay: [] never spends; a listed pot may pay — on an Anthropic member, a
    codex member and a bare lane alike."""

    def test_anthropic_member(self):
        for pay, want in (([], "no"), ([POT_A], "paid")):
            with self.subTest(pay=pay):
                classes_reset(opus_spent(), cfg=pay_cfg(opus=pay))
                c = route_check("frontier/lane:opus", ACCT_A)
                self.assertEqual(c["check"], want)
                if want == "paid":
                    self.assertEqual(c["pot"], POT_A)
                else:
                    self.assertEqual(c["reason"], "pay")

    def test_anthropic_credits_off_or_spent_is_spent(self):
        classes_reset(opus_spent(credits=False), cfg=pay_cfg(opus=[POT_A]))
        self.assertEqual(route_check("frontier/lane:opus", ACCT_A)["reason"], "spent")
        # This month's used credits say nothing until C10 (units unknown, U7).
        classes_reset(opus_spent(monthly_spent=True), cfg=pay_cfg(opus=[POT_A]))
        self.assertEqual(route_check("frontier/lane:opus", ACCT_A)["check"], "paid")

    def test_codex_member(self):
        for pay, want in (([], "no"), (["gpt/credits"], "paid")):
            with self.subTest(pay=pay):
                classes_reset(gpt_on_credits(), cfg=pay_cfg(gpt=pay))
                self.assertEqual(route_check("frontier/gpt/gpt-5.6", "gpt")["check"], want)

    def test_bare_lane(self):
        """A bare lane is a route with pay [] by default; the lanes map grants
        it, and the lane switchover then sits on the paid account."""
        classes_reset(opus_spent(), cfg=pay_cfg())
        self.assertEqual(route_check("lane:opus", ACCT_A)["reason"], "pay")
        self.assertIsNone(lane_account("opus"), "no account the lane may use")
        classes_reset(opus_spent(), cfg=pay_cfg(lanes={"opus": {"pay": [POT_A]}}))
        self.assertEqual(route_check("lane:opus", ACCT_A)["check"], "paid")
        self.assertEqual(route_check("lane:fable", ACCT_A)["check"], "included",
                         "the Opus cap binds the opus lane only")
        self.assertEqual(lane_account("opus"), ACCT_A)

    def test_a_dangling_pay_pot_binds_nothing(self):
        classes_reset(gpt_on_credits(), cfg=pay_cfg(gpt=["gone/credits"]))
        self.assertEqual(route_check("frontier/gpt/gpt-5.6", "gpt")["reason"], "pay")


class TestTopup(PayBase):
    """topup moves only a refusal's `until` (§1.9)."""

    def test_off_waits_for_the_reset_and_on_for_the_next_poll(self):
        g = gpt_on_credits(has_credits=False)
        g["raw"]["rate_limit"]["primary_window"]["reset_at"] = int(time.time()) + 86400
        classes_reset(g, cfg=pay_cfg(gpt=["gpt/credits"]))
        with ex._poll_health_lock:
            ex._poll_schedule["gpt"] = time.time() + 300
        c = route_check("frontier/gpt/gpt-5.6", "gpt")
        self.assertEqual((c["check"], c["reason"]), ("no", "spent"))
        self.assertEqual(c["until"], g["raw"]["rate_limit"]["primary_window"]["reset_at"])
        install_money({"pots": {"gpt/credits": {"topup": "on"}}})
        c = route_check("frontier/gpt/gpt-5.6", "gpt")
        self.assertAlmostEqual(c["until"], time.time() + 300, delta=2,
                               msg="an empty pot the provider refills: until the next reading")

    def test_off_with_no_reset_known_is_retried_at_the_next_poll(self):
        g = gpt_on_credits(has_credits=False)
        del g["raw"]["rate_limit"]["primary_window"]["reset_at"]
        self.addCleanup(ex._poll_schedule.clear)
        with ex._poll_health_lock:
            ex._poll_schedule["gpt"] = 1_900_000_000.0
        classes_reset(g, cfg=pay_cfg(gpt=["gpt/credits"]))
        self.assertEqual(route_check("frontier/gpt/gpt-5.6", "gpt")["until"], 1_900_000_000.0)


class TestPreferredDialCheckContract(PayBase, HTTPBase):
    """quota-model 50 §2.7: what the proxy receives."""

    def setUp(self):
        cfg = pay_cfg(gpt=["gpt/credits"], opus_stop={"window": "7d", "level": 100},
                      lanes={"fable": {"pay": [POT_A]}})
        cfg["classes"][0]["members"][2]["calibrated_at"] = "2026-10-15"  # gpt, file order
        cfg["classes"][0]["members"][2]["calibration_ref"] = "c1-run-7"
        classes_reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result(), cfg=cfg)
        install_money({"pay_order": ["gpt/credits"], "pots": {"kimi/credits": {"topup": "on"}}})

    def test_accounts_routes_pots_and_settings(self):
        _, p = self.get("/preferred")
        self.assertEqual(set(p["accounts"]), {ACCT_A, ACCT_B, "gpt", "kimi"})
        self.assertEqual(set(p["routes"]),
                         {"frontier/lane:opus", "frontier/gpt/gpt-5.6", "frontier/kimi/kimi-k3"}
                         | {f"lane:{ln}" for ln in ex._lane_names})
        r = p["routes"]["frontier/gpt/gpt-5.6"]
        self.assertEqual(set(r), {"kind", "class", "member", "lane", "provider", "scope",
                                  "accounts", "pay", "stop_at", "priority", "request_kind",
                                  "checks", "check", "warnings"})
        self.assertEqual((r["kind"], r["class"], r["member"], r["provider"], r["accounts"],
                          r["pay"], r["priority"], r["request_kind"]),
                         ("member", "frontier", "gpt/gpt-5.6", "gpt", ["gpt"],
                          ["gpt/credits"], 2, "chat"))
        self.assertEqual(r["check"]["check"], "paid")
        lane = p["routes"]["lane:fable"]
        self.assertEqual((lane["kind"], lane["class"], lane["scope"], lane["accounts"],
                          lane["pay"], lane["stop_at"], lane["priority"]),
                         ("lane", None, "Fable", [ACCT_A, ACCT_B], [POT_A], None, None))
        self.assertEqual(set(lane["checks"]), {ACCT_A, ACCT_B})
        self.assertEqual(lane["check"]["check"], "included")
        opus = p["routes"]["frontier/lane:opus"]
        self.assertEqual(opus["scope"], "Opus")
        self.assertTrue(any("no effect" in w for w in opus["warnings"]))
        self.assertEqual(set(p["pots"]), {f"{ACCT_A}/credits", f"{ACCT_B}/credits",
                                          "gpt/credits", "kimi/credits"})
        self.assertEqual(p["pots"]["gpt/credits"],
                         {"account": "gpt", "pot": "credits", "topup": "off",
                          "topup_basis": "declared", "pay_rank": 0, "budget": None,
                          "route_budgets": {}, "countable": True, "warnings": []})
        self.assertIs(p["pots"][f"{ACCT_A}/credits"]["countable"], False,
                      "Anthropic credits are not countable until U1 (§1.7)")
        self.assertIs(p["pots"]["kimi/credits"]["countable"], False)
        self.assertIsNone(p["pots"]["kimi/credits"]["pay_rank"])
        self.assertTrue(p["pots"]["kimi/credits"]["warnings"], "topup on for Kimi warns")
        self.assertEqual(p["pay_order"], ["gpt/credits"])
        with ex._plock:
            want = {"classes": ex._doc_revision(ex._classes_doc),
                    "consumers": ex._doc_revision(ex._consumers_doc),
                    "money": ex._doc_revision(ex._money_doc), "stale_max": ex.STALE_MAX}
        self.assertEqual(p["settings"], want)
        self.assertTrue(any("Kimi" in w for w in p["settings_warnings"]))

    def test_member_additions(self):
        _, p = self.get("/preferred")
        ms = {m["id"]: m for m in p["classes"]["frontier"]["members"]}
        g = ms["gpt/gpt-5.6"]
        for k in ("route", "pay", "stop_at", "scope", "checks", "check", "effective",
                  "usable", "status", "targets", "incumbent", "reserved",
                  "anthropic_token_ratio", "calibrated_at", "calibration_ref"):
            self.assertIn(k, g)
        self.assertNotIn("funded", g)
        self.assertNotIn("spend_credits", g)
        self.assertEqual((g["route"], g["pay"], g["check"]["check"], g["effective"]),
                         ("frontier/gpt/gpt-5.6", ["gpt/credits"], "paid", 100.0))
        self.assertEqual((g["calibrated_at"], g["calibration_ref"]), ("2026-10-15", "c1-run-7"))
        self.assertNotIn("calibrated_at", ms["kimi/kimi-k3"])  # no value, no key
        self.assertNotIn("calibrated_at", ms["lane:opus"])
        self.assertEqual(ms["lane:opus"]["scope"], "Opus")
        self.assertEqual(set(ms["lane:opus"]["checks"]), {ACCT_A, ACCT_B})

    def test_class_bind_returns_route_and_check(self):
        code, p = self.get("/class-bind?class=frontier&session=http-1", key=KEY)
        self.assertEqual(code, 200)
        self.assertEqual(p["route"], "frontier/lane:opus")
        self.assertEqual(p["check"]["check"], "included")


class TestClassesSettingsValidation(PayBase):
    """§1.9: every invalid setting is refused at PUT with a reason."""

    def setUp(self):
        reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result())

    def errors(self, doc):
        return ex._validate_classes_doc(doc)

    def assert_refused(self, doc, *needles):
        errs = self.errors(doc)
        self.assertTrue(errs, f"must be refused: {doc!r}")
        for n in needles:
            self.assertTrue(any(n in e for e in errs), (n, errs))

    def test_valid_settings_pass(self):
        doc = pay_cfg(opus=[POT_A], gpt=["gpt/credits"], kimi=["kimi/credits"],
                      opus_stop={"window": "Opus", "level": 60},
                      lanes={"opus": {"pay": [POT_A, f"{ACCT_B}/credits"],
                                      "stop_at": {"window": "effective", "level": 80}}})
        self.assertEqual(self.errors(doc), [])

    def test_pay_rejections(self):
        self.assert_refused(pay_cfg(opus="x/credits"), "pay must be an array")
        self.assert_refused(pay_cfg(opus=[POT_A, POT_A]), "pay lists a pot twice")
        self.assert_refused(pay_cfg(opus=[f"{ACCT_A}/allowance"]), "not a money pot")
        self.assert_refused(pay_cfg(lanes={"opus": {"pay": ["kimi/allowance"]}}),
                            "lane:opus: pay 'kimi/allowance'")
        # A pot of a known account that can never back the route (another
        # provider's) is structure: refused (§1.9, §5.1).
        self.assert_refused(pay_cfg(opus=["gpt/credits"]),
                            "pay 'gpt/credits': 'gpt' is an account of gpt; it can never back")
        self.assert_refused(pay_cfg(lanes={"opus": {"pay": ["kimi/credits"]}}),
                            "lane:opus: pay 'kimi/credits': 'kimi' is an account of kimi")
        # What depends on the pool is accepted with a warning (dangling).
        for cfg, needle in ((pay_cfg(opus=["nobody/credits"]), "unknown pot"),):
            with self.subTest(needle=needle):
                self.assertEqual(ex._validate_classes_doc(cfg), [])
                self.assertTrue(any(needle in w for w in ex._doc_check("classes", cfg)[1]))

    def test_stop_at_rejections(self):
        for stop, needle in (({"window": "7d"}, "stop_at must be {window, level}"),
                             ({"window": "7d", "level": -1}, "level must be a finite number in [0, 100]"),
                             ({"window": "7d", "level": "60"}, "level must be a finite number"),
                             ({"window": "7d", "level": True}, "level must be a finite number"),
                             ({"window": "", "level": 5}, "stop_at.window")):
            with self.subTest(stop=stop):
                self.assert_refused(pay_cfg(opus_stop=stop), needle)

    def test_a_window_no_reading_carries_is_a_warning(self):
        for window in ("30d", "Fable"):
            with self.subTest(window=window):
                errs, warns, _, _ = ex._doc_check("classes", pay_cfg(opus_stop={"window": window, "level": 5}))
                self.assertEqual(errs, [])
                self.assertTrue(any("no reading of this route's" in w for w in warns), warns)

    def test_lanes_rejections(self):
        self.assert_refused(pay_cfg(lanes={"nope": {"pay": []}}), "unknown lane")
        self.assert_refused(pay_cfg(lanes={"opus": {"pay": [], "color": 1}}), "unknown key 'color'")
        self.assert_refused(pay_cfg(lanes={"opus": []}), "must be an object")
        self.assert_refused({"classes": frontier_cfg(), "extra": 1}, "unknown top-level key")

    def test_budget_and_spend_credits_are_refused_with_their_reasons(self):
        m = pay_cfg()
        m[0]["members"][0]["budget"] = 10
        self.assert_refused(m, "budget", "money document")
        self.assert_refused(pay_cfg(lanes={"opus": {"route_budgets": {}}}), "route_budgets",
                            "money document")
        self.assert_refused({"classes": frontier_cfg(), "budget": 1}, "money document")
        m = pay_cfg()
        m[0]["members"][2]["spend_credits"] = True
        self.assert_refused(m, "spend_credits is retired", "migrate_pay.py")
        m[0]["members"][2]["spend_credits"] = False
        self.assert_refused(m, "spend_credits is retired")

    def test_calibration_fields(self):
        m = pay_cfg()
        m[0]["members"][2]["calibrated_at"] = 20261015
        self.assert_refused(m, "calibrated_at must be a non-empty string")
        m = pay_cfg()
        m[0]["members"][1]["calibration_ref"] = "x"  # opus, file order
        self.assert_refused(m, "provider members only")

    def test_warnings_are_accepted(self):
        errs, warns, _, _ = ex._doc_check("classes", pay_cfg(opus_stop={"window": "7d", "level": 100}))
        self.assertEqual(errs, [])
        self.assertTrue(any("no effect" in w for w in warns), warns)
        with ex._lock:
            del ex._state[ACCT_A]
        errs, warns, _, _ = ex._doc_check("classes", pay_cfg(opus_stop={"window": "30d", "level": 5}))
        self.assertEqual(errs, [], "no account measured: accepted")
        self.assertTrue(any("unchecked" in w for w in warns), warns)


class TestMoneyValidation(PayBase):
    def setUp(self):
        reset(opus_acct(30), gpt_on_credits(), kimi_result())

    def check(self, doc):
        return ex._doc_check("money", doc)

    def test_valid(self):
        errs, warns, norm, canon = self.check(
            {"pay_order": ["gpt/credits", POT_A], "pots": {"gpt/credits": {"topup": "on"}}})
        self.assertEqual((errs, warns), ([], []))
        self.assertEqual(norm, {"pay_order": ["gpt/credits", POT_A],
                                "pots": {"gpt/credits": {"topup": "on"}}})

    def test_rejections(self):
        cases = (
            ([], "document must be"),
            (None, "document must be"),
            ({"pay_order": "gpt/credits"}, "pay_order must be an array"),
            ({"pay_order": ["gpt/credits", "gpt/credits"]}, "twice"),
            ({"pay_order": ["gpt/allowance"]}, "not a money pot"),
            ({"pots": {"gpt/wallet": {"topup": "off"}}}, "unknown pot"),
            ({"pots": {"gpt/allowance": {"topup": "off"}}}, "allowance refills at its reset"),
            ({"pots": {"gpt/credits": {"topup": "maybe"}}}, "topup must be 'off' or 'on'"),
            ({"pots": {"gpt/allowance": {"topup": "on"}}}, "allowance refills at its reset"),
            ({"pots": {"gpt/credits": {"budget": -1}}}, "budget must be a number >= 0"),
            ({"pots": {"gpt/credits": {"budget": "5"}}}, "budget must be a number >= 0"),
            ({"pots": {"gpt/credits": {"budget": True}}}, "budget must be a number >= 0"),
            ({"pots": {"gpt/credits": {"route_budgets": {"frontier/gpt/gpt-5.6": -2}}}},
             "route_budgets must be an object"),
            ({"pots": {"gpt/credits": {"route_budgets": [1]}}}, "route_budgets must be an object"),
            ({"pots": {"gpt/credits": {"route_budgets": {"lane:nope": 1}}}}, "unknown route"),
            ({"pots": {"kimi/credits": {"budget": 5}}}, "has none (§1.7)"),
            ({"pots": {POT_A: {"budget": 5}}}, "has none (§1.7)"),
            ({"pots": {"gpt/allowance": {"budget": 5}}}, "allowance refills at its reset"),
            ({"budget": 5}, "a budget is per pot"),
            ({"route_budgets": {}}, "a budget is per pot"),
            ({"pots": {"gpt/credits": {"colour": 1}}}, "unknown key"),
            ({"pay_order": [], "extra": 1}, "unknown top-level key"),
        )
        for doc, needle in cases:
            with self.subTest(doc=doc):
                errs = self.check(doc)[0]
                self.assertTrue(any(needle in e for e in errs), (needle, errs))

    def test_kimi_topup_on_warns(self):
        errs, warns, _, _ = self.check({"pots": {"kimi/credits": {"topup": "on"}}})
        self.assertEqual(errs, [])
        self.assertTrue(any("Kimi documents no top-up" in w for w in warns))


class TestMoneyDocAPI(DocAPIBase, PayBase):
    """/money has the classes/consumers API: lock, history, revision, If-Match,
    reason, key guard."""

    def setUp(self):
        super().setUp()
        self.money_path = os.path.join(_TMP, f"money-{self.__class__.__name__}.json")
        self._old_money_file = ex.MONEY_FILE
        ex.MONEY_FILE = self.money_path
        for suffix in ("", ".history.jsonl"):
            try:
                os.remove(self.money_path + suffix)
            except FileNotFoundError:
                pass
        with ex._plock:
            ex._money, ex._money_doc, ex._money_warnings = ex._load_money(), {}, []

    def tearDown(self):
        ex.MONEY_FILE = self._old_money_file
        with ex._plock:
            ex._money_doc, ex._money_warnings = {}, []
        DocAPIBase.tearDown(self)
        PayBase.tearDown(self)

    def put(self, doc, rev, reason="test", key=KEY):
        hdr = {"If-Match": rev} if rev is not None else {}
        return self.req("PUT", "/money", {"doc": doc, "reason": reason}, key=key,
                        headers=dict(hdr, **{"X-Actor": "tester"}))

    def test_get_put_history_and_preferred(self):
        code, p, _ = self.req("GET", "/money")
        self.assertEqual(code, 200)
        self.assertEqual(p["doc"], {})
        self.assertEqual(set(p["derived"]["money_pots"]),
                         {f"{ACCT_A}/credits", f"{ACCT_B}/credits", "gpt/credits", "kimi/credits"})
        doc = {"pay_order": ["gpt/credits"], "pots": {"gpt/credits": {"topup": "on"}}}
        code, p, h = self.put(doc, p["revision"], reason="gpt first")
        self.assertEqual(code, 200, p)
        self.assertEqual(p["revision"], ex._doc_revision(doc))
        self.assertEqual(h["X-Doc-Revision"], p["revision"])
        with open(self.money_path) as f:
            self.assertEqual(json.load(f), doc)
        _, pref = self.get("/preferred")
        self.assertEqual(pref["pay_order"], ["gpt/credits"])
        self.assertEqual(pref["pots"]["gpt/credits"]["topup"], "on")
        self.assertEqual(pref["settings"]["money"], p["revision"])
        code, hist, _ = self.req("GET", "/money/history")
        self.assertEqual((code, hist["total"]), (200, 1))
        self.assertEqual((hist["entries"][0]["actor"], hist["entries"][0]["reason"]),
                         ("tester", "gpt first"))

    def test_a_put_reseats_a_session_on_money_before_it_answers(self):
        self.install_classes(pay_cfg(opus=[POT_A], gpt=["gpt/credits"]))
        reset(opus_spent(), gpt_on_credits(), kimi_result(five_hour=100.0))
        code, p, _ = self.put({"pay_order": ["gpt/credits"]}, self.req("GET", "/money")[1]["revision"])
        self.assertEqual(code, 200, p)
        self.assertEqual(self.bind()[1]["member"]["id"], "gpt/gpt-5.6")
        code, p, _ = self.put({"pay_order": [POT_A]}, p["revision"], reason="opus credits first")
        self.assertEqual(code, 200, p)
        s = self.get("/preferred")[1]["classes"]["frontier"]["sessions"]["s1"]
        self.assertEqual((s["member"], s["reason"]),
                         ("lane:opus", "repay: pay settings changed, leaving paid gpt/gpt-5.6"))

    def test_if_match_and_reason_rules(self):
        rev = self.req("GET", "/money")[1]["revision"]
        self.assertEqual(self.put({}, None)[0], 400, "If-Match is required")
        self.assertEqual(self.put({}, rev, reason="")[0], 400, "a reason is required")
        code, p, _ = self.put({}, "0000000000000000")
        self.assertEqual((code, p["revision"]), (409, rev))
        self.assertEqual(self.put({}, rev, key="wrong")[0], 401)
        code, p, _ = self.put({"pay_order": ["gpt/allowance"]}, rev)
        self.assertEqual(code, 422)
        self.assertTrue(any("not a money pot" in e for e in p["errors"]))

    def test_validate_and_reload(self):
        code, p, _ = self.req("POST", "/money/validate",
                              {"doc": {"pots": {"kimi/credits": {"topup": "on"}}}})
        self.assertEqual(code, 200)
        self.assertTrue(p["ok"])
        self.assertTrue(p["warnings"])
        code, p, _ = self.req("POST", "/money/validate", {"doc": {"budget": 1}})
        self.assertFalse(p["ok"])
        with open(self.money_path, "w") as f:
            json.dump({"pay_order": ["gpt/credits"]}, f)
        code, p, _ = self.req("POST", "/money/reload", {"reason": "hand edit"})
        self.assertEqual(code, 200)
        self.assertTrue(p["changed"])
        with ex._plock:
            self.assertEqual(ex._money["pay_order"], ["gpt/credits"])
        with open(self.money_path, "w") as f:
            f.write("[1]")
        self.assertEqual(self.req("POST", "/money/reload", {})[0], 422,
                         "a wrong shape keeps the live settings")


class TestClassesDocAPIWithSettings(DocAPIBase, PayBase):
    def tearDown(self):
        DocAPIBase.tearDown(self)
        PayBase.tearDown(self)

    def test_put_dict_document_installs_lane_settings(self):
        rev = self.req("GET", "/classes")[1]["revision"]
        doc = pay_cfg(gpt=["gpt/credits"], lanes={"opus": {"pay": [POT_A]}})
        with ex._lock:
            ex._state["gpt"] = gpt_on_credits()
        code, p, _ = self.req("PUT", "/classes", {"doc": doc, "reason": "pay"},
                              headers={"If-Match": rev})
        self.assertEqual(code, 200, p)
        self.assertEqual(p["doc"], doc, "served as stored")
        with ex._plock:
            self.assertEqual(ex._lane_settings, {"opus": {"pay": [POT_A], "stop_at": None}})
        _, pref = self.get("/preferred")
        self.assertEqual(pref["routes"]["lane:opus"]["pay"], [POT_A])
        self.assertEqual(pref["classes"]["frontier"]["pointer"], "lane:opus")

    def test_put_refuses_spend_credits_with_the_reason(self):
        rev = self.req("GET", "/classes")[1]["revision"]
        doc = frontier_cfg()
        doc[0]["members"][2]["spend_credits"] = True
        code, p, _ = self.req("PUT", "/classes", {"doc": doc, "reason": "x"},
                              headers={"If-Match": rev})
        self.assertEqual(code, 422)
        self.assertTrue(any("migrate_pay.py" in e for e in p["errors"]))


class TestSettingsNumbersAreFinite(PayBase, HTTPBase):
    """A non-finite or out-of-range number never reaches a document, and the
    brain never serves one: every proxy would fail to decode /preferred."""

    def setUp(self):
        reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result())

    def test_put_refuses_non_finite_and_out_of_range_numbers(self):
        for stop in ({"window": "7d", "level": float("nan")},
                     {"window": "7d", "level": float("inf")},
                     {"window": "7d", "level": 101}):
            with self.subTest(stop=stop):
                errs = ex._validate_classes_doc(pay_cfg(opus_stop=stop))
                self.assertTrue(any("level must be a finite number in [0, 100]" in e for e in errs), errs)
                errs = ex._validate_classes_doc(pay_cfg(lanes={"opus": {"stop_at": stop}}))
                self.assertTrue(errs, "lanes too")
        for key, val in (("reserve_ceiling", float("nan")), ("anthropic_token_ratio", float("inf"))):
            with self.subTest(key=key):
                cfg = pay_cfg()
                cfg[0]["members"][2][key] = val  # gpt, file order
                self.assertTrue(ex._validate_classes_doc(cfg))

    def test_any_non_finite_number_refuses_the_document(self):
        for kind, doc in (("money", {"pay_order": [], "pots": {"gpt/credits": {"topup": "on", "x": float("inf")}}}),
                          ("classes", {"classes": pay_cfg(), "lanes": {"opus": {"stop_at": {"window": "7d", "level": float("nan")}}}})):
            with self.subTest(kind=kind):
                errs = ex._doc_check(kind, doc)[0]
                self.assertTrue(any("numbers must be finite" in e for e in errs), errs)

    def test_tolerant_load_drops_a_non_finite_level(self):
        self.assertEqual(load_classes(pay_cfg(opus_stop={"window": "7d", "level": float("inf")})), [])

    def keep_docs(self):
        for name in ("_classes_doc", "_classes_warnings", "_lane_settings", "_money_doc",
                     "_money_warnings", "_money", "CLASSES_FILE", "MONEY_FILE"):
            self.addCleanup(setattr, ex, name, getattr(ex, name))

    def write(self, name, text):
        path = os.path.join(_TMP, name)
        with open(path, "w") as f:
            f.write(text)
        return path

    def served_with_revisions(self):
        for path in ("/classes", "/consumers", "/money", "/preferred"):
            code, body = self.get(path, key=ex.SWITCH_KEY)
            self.assertEqual(code, 200, f"{path}: {body}")
            if path != "/preferred":
                self.assertRegex(body["revision"], r"^[0-9a-f]{16}$", path)

    def test_a_hand_edited_non_finite_in_classes_json_is_ignored_at_load(self):
        # A NaN no reader of the brain needs (a member's note) must not take
        # down /preferred or the document API, and GET /classes must still
        # carry the revision a fixing PUT needs.
        self.keep_docs()
        cfg = pay_cfg()
        cfg[0]["members"][0]["note"] = float("nan")
        cfg[0]["members"][2]["reserve_ceiling"] = float("inf")
        ex.CLASSES_FILE = self.write("classes-nan.json", json.dumps({"classes": cfg, "lanes": {}}))
        out = ex._load_classes()
        ex._swap_classes(out, ex._classes_doc, ex._classes_warnings, ex._lane_settings)
        self.assertEqual([c["class"] for c in out], ["frontier"])
        self.assertTrue(any("non-finite" in w and "note" in w for w in ex._classes_warnings),
                        ex._classes_warnings)
        self.assertTrue(any("non-finite" in w and "reserve_ceiling" in w for w in ex._classes_warnings))
        self.assertIsNone(ex._non_finite_path(ex._classes_doc))
        self.served_with_revisions()
        code, body = self.get("/classes", key=ex.SWITCH_KEY)
        self.assertNotIn("note", body["doc"]["classes"][0]["members"][0])

    def test_a_hand_edited_non_finite_in_money_json_is_ignored_at_load(self):
        self.keep_docs()
        ex.MONEY_FILE = self.write("money-nan.json",
                                   '{"pay_order": ["gpt/credits", NaN], "pots": {"gpt/credits": '
                                   '{"topup": "on"}}, "note": Infinity}')
        m = ex._load_money()
        ex._swap_money(m, ex._money_doc, ex._money_warnings)
        self.assertEqual(m["pay_order"], ["gpt/credits"])
        self.assertEqual(m["pots"]["gpt/credits"], {"topup": "on"})
        self.assertTrue(any("non-finite" in w and "pay_order" in w for w in ex._money_warnings),
                        ex._money_warnings)
        self.assertIsNone(ex._non_finite_path(ex._money_doc))
        self.served_with_revisions()

    def test_a_non_finite_provider_number_reads_unknown(self):
        # The adapter owns the provider's quirks (§1.5): a NaN is no reading.
        a = opus_acct(30, resets=True)
        a["raw"]["seven_day"]["utilization"] = float("nan")
        reset(a)
        code, p = self.get("/preferred")
        self.assertEqual(code, 200)
        w = p["accounts"][ACCT_A]["allowance"]["windows"]["seven_day"]
        self.assertEqual((w["used_pct"], w["basis"]), (None, "unknown"))

    def test_a_non_finite_number_fails_loudly_at_the_brain(self):
        a = opus_acct(30, resets=True)
        ex._reading_windows(a["reading"])["five_hour"]["used"] = float("nan")
        with ex._lock:
            ex._state.clear()
            ex._state[a["name"]] = a  # past the adapter, as a bug would put it
        ex.compute_preferred()
        code, body = self.get("/preferred")
        self.assertEqual(code, 500)
        self.assertIn("non-finite", body["error"])


class TestClassesUnknownKeysRefused(PayBase):
    """§1.9: nothing is dropped or ignored."""

    def setUp(self):
        reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result())

    def test_class_and_member_keys(self):
        for where, key in (("class", "budget"), ("class", "pay"), ("class", "colour"),
                           ("member", "stop-at"), ("member", "id")):
            with self.subTest(where=where, key=key):
                cfg = pay_cfg()
                (cfg[0] if where == "class" else cfg[0]["members"][2])[key] = 1
                errs = ex._validate_classes_doc(cfg)
                self.assertTrue(any(repr(key) in e or key in e for e in errs), errs)


class TestReadingsSurviveRestart(PayBase):
    """A restarted brain publishes its last readings with their true age, so a
    proxy that restarts beside it is not left with every as_of null until each
    account's first good poll; the stale rule decides as for any old reading."""

    def restart(self):
        with ex._plock:
            ex._save_state()
        with ex._lock:
            ex._state.clear()
        ex._load_state()

    def test_readings_round_trip_with_their_age(self):
        a = acct(ACCT_A, 30, good_age=600)
        a["raw"]["extra_usage"] = {"is_enabled": False}
        g = gpt_result(weekly=40.0, credits={"has_credits": True, "balance": 12.5})
        reset(a, g)
        before = {n: (r["good_ts"], r["reading"]) for n, r in ex._state.items()}
        targets = ex._provider_poll_targets
        ex._provider_poll_targets = lambda: {"gpt": ("gpt", None)}
        self.addCleanup(setattr, ex, "_provider_poll_targets", targets)
        self.restart()
        with ex._lock:
            after = {n: (r["good_ts"], r["reading"]) for n, r in ex._state.items()}
            self.assertEqual(ex._state[ACCT_A]["stale"], 1, "not polled since the restart")
            self.assertEqual(ex._state[ACCT_A]["ok"], 0)
        self.assertEqual(after, before)
        with ex._plock:
            st = ex._policy_ctx(list(ex._state.values()), time.time())["standings"]
        self.assertEqual(st[ACCT_A]["as_of"], before[ACCT_A][0])
        self.assertEqual(route_check("lane:opus", ACCT_A)["check"], "included")

    def test_an_old_reading_restores_stale(self):
        a = acct(ACCT_A, 30, good_age=ex.STALE_MAX + 600)
        a["raw"]["extra_usage"] = {"is_enabled": True}
        reset(a)
        self.restart()
        ex.compute_preferred()
        c = route_check("lane:opus", ACCT_A)
        self.assertEqual((c["check"], c["reason"], c["basis"]), ("no", "unknown", "stale"))

    def test_a_reading_for_an_account_no_longer_configured_is_not_restored(self):
        # The ghost probe: a saved reading for an account that left the creds
        # file (and is no provider target) would join the roster, could become
        # a lane's incumbent the proxy has no credentials for, and would export
        # a data_age that climbs forever.
        a = acct(ACCT_A, 100)
        a["raw"]["extra_usage"] = {"is_enabled": False}  # fresh and spent
        reset(a)
        ghost = acct("ghost@example.com", 5, good_age=2400)
        ghost["raw"]["extra_usage"] = {"is_enabled": False}
        with ex._lock:
            ex._state[ghost["name"]] = ghost
        self.restart()
        with ex._lock:
            self.assertNotIn("ghost@example.com", ex._state)
        ex.compute_preferred()
        p = ex._preferred_payload()
        self.assertNotIn("ghost@example.com", p["accounts"])
        self.assertNotIn("ghost@example.com", p["routes"]["lane:opus"]["checks"])
        self.assertNotEqual(p["lanes"]["opus"]["account"], "ghost@example.com")
        self.assertNotIn("ghost@example.com", ex.render())

    def test_an_account_already_polled_keeps_its_own_result(self):
        reset(acct(ACCT_A, 30))
        with ex._lock:
            live = dict(ex._state[ACCT_A])
        old = dict(acct(ACCT_A, 90, good_age=3000))
        self.assertEqual(ex._restore_readings([old]), 0)
        with ex._lock:
            self.assertEqual(ex._state[ACCT_A], live, "the live poll's result stands")

    def test_no_secret_is_persisted(self):
        reset(acct(ACCT_A, 30))
        with ex._plock:
            ex._save_state()
        with open(ex.STATE_FILE) as f:
            text = f.read()
        self.assertNotIn("tok-" + ACCT_A, text)
        self.assertNotIn("r-" + ACCT_A, text)


class TestSettingsCheckedAgainstReadingsWarnOnly(PayBase):
    """A problem that depends on what the brain has read is a warning (and
    dangling at runtime), never a PUT error; structure is still an error."""

    def setUp(self):
        reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result())

    def test_a_live_document_error_is_shown_as_an_error(self):
        # A hand-edited live document the tolerant load keeps: another
        # provider's pot on a lane is structure, shown as an error at runtime
        # (it binds nothing), not as dangling.
        classes_reset(opus_acct(30, resets=True), gpt_on_credits(), kimi_result(),
                      cfg=pay_cfg(lanes={"opus": {"pay": ["gpt/credits"]}}))
        w = ex._preferred_payload()["routes"]["lane:opus"]["warnings"]
        self.assertTrue(any(x.startswith("error: pay 'gpt/credits'") for x in w), w)
        self.assertEqual(route_check("lane:opus", ACCT_A)["check"], "included")

    def test_classes(self):
        for cfg, needle in ((pay_cfg(gpt=["gone/credits"]), "unknown pot"),
                            (pay_cfg(opus_stop={"window": "30d", "level": 5}), "no reading")):
            with self.subTest(needle=needle):
                errs, warns, _, _ = ex._doc_check("classes", cfg)
                self.assertEqual(errs, [])
                self.assertTrue(any(needle in w for w in warns), warns)
        for cfg, needle in ((pay_cfg(gpt=["gpt/allowance"]), "not a money pot"),
                            (pay_cfg(gpt=[POT_A]), "is an account of anthropic; it can never back")):
            with self.subTest(needle=needle):
                errs, _, _, _ = ex._doc_check("classes", cfg)
                self.assertTrue(any(needle in e for e in errs), f"structure stays an error: {errs}")

    def test_money(self):
        for doc, needle in (({"pay_order": ["gone/credits"]}, "unknown pot"),
                            ({"pots": {"gone/credits": {"topup": "on"}}}, "unknown pot")):
            with self.subTest(doc=doc):
                errs, warns, _, _ = ex._doc_check("money", doc)
                self.assertEqual(errs, [])
                self.assertTrue(any(needle in w for w in warns), warns)
        for doc, needle in (({"pay_order": ["gpt/allowance"]}, "not a money pot"),
                            ({"pots": {"gpt/wallet": {}}}, "unknown pot"),
                            ({"pots": {"gpt/allowance": {"topup": "off"}}}, "allowance")):
            with self.subTest(doc=doc):
                errs = ex._doc_check("money", doc)[0]
                self.assertTrue(any(needle in e for e in errs), errs)


class TestSwitchoverMutationsKilled(PayBase):
    """Mutations a review found surviving (_usable_by_check, _pool_route)."""

    def test_a_lane_never_sits_on_a_stale_account_while_a_fresh_one_serves(self):
        stale = acct(ACCT_B, 10, good_age=ex.STALE_MAX + 600)
        stale["raw"]["extra_usage"] = {"is_enabled": False}  # no money at risk: usable stale
        fresh = acct(ACCT_A, 50)
        fresh["raw"]["extra_usage"] = {"is_enabled": False}
        classes_reset(fresh, stale, cfg=pay_cfg())
        self.assertEqual(route_check("lane:opus", ACCT_B)["basis"], "stale")
        self.assertEqual(lane_account("opus"), ACCT_A, "fresh before a cooler stale one")

    def test_a_pool_never_sits_on_money(self):
        g = gpt_on_credits(pct=100.0)
        g["raw"]["rate_limit"]["primary_window"]["reset_at"] = int(time.time()) + 86400
        classes_reset(g, cfg=pay_cfg(gpt=["gpt/credits"]))
        with ex._plock:
            pool = (ex._preferred.get("providers") or {}).get("gpt") or {}
        self.assertIsNone(pool.get("account"), "the pool route pays nothing")
        self.assertEqual(class_verdicts()["gpt/gpt-5.6"]["check"]["check"], "paid",
                         "the member that may pay still gets its paid verdict")


# ---- C6: the normalized reading, standing, the ledger and its budget gate ------

GPT_ROUTE = "frontier/gpt/gpt-5.6"


ROSTER = {ACCT_A: "anthropic", ACCT_B: "anthropic", "gpt": "gpt", "kimi": "kimi"}


def anthropic_body(**over):
    """A GET /api/oauth/usage body in a recorded shape (10 §2):
    the two top-level windows, and limits[] with every kind seen (session,
    weekly_all, weekly_scoped Fable) plus two kinds never seen, one unscoped
    and one scoped, which the adapter must read rather than drop (D7)."""
    body = {
        "five_hour": {"utilization": 22.0, "resets_at": "2099-07-28T18:00:00Z"},
        "seven_day": {"utilization": 61.0, "resets_at": "2099-08-01T16:00:00+00:00"},
        "limits": [
            {"kind": "session", "group": "session", "percent": 22, "severity": "normal",
             "is_active": True, "resets_at": "2099-07-28T18:00:00Z", "scope": None},
            {"kind": "weekly_all", "group": "weekly", "percent": 61, "severity": "warning",
             "is_active": False, "resets_at": "2099-08-01T16:00:00Z", "scope": None},
            {"kind": "weekly_scoped", "group": "model_specific", "percent": 100,
             "severity": "critical", "is_active": False, "resets_at": "2099-08-01T16:00:00Z",
             "scope": {"model": {"display_name": "Fable"}}},
            {"kind": "monthly_all", "group": "monthly", "percent": 35, "severity": "normal",
             "is_active": False, "resets_at": None, "scope": None},
            {"kind": "daily_scoped", "group": "weekly", "percent": 80, "severity": "normal",
             "is_active": False, "resets_at": "2099-07-29T00:00:00Z",
             "scope": {"model": {"display_name": "Opus"}}},
        ],
    }
    body.update(over)
    return body


def anthropic_reading(**over):
    return ex._anthropic_reading(anthropic_body(**over), 1_800_000_000.0, "max")


class TestAnthropicReading(unittest.TestCase):
    """The Anthropic adapter (§1.5) from a recorded-shape body."""

    def test_every_limits_kind_is_a_window(self):
        w = ex._reading_windows(anthropic_reading())
        self.assertEqual(list(w), ["five_hour", "seven_day", "scoped:Fable",
                                   "limit:monthly_all", "limit:daily_scoped:Opus"])
        self.assertEqual([s["kind"] for s in w["five_hour"]["sources"]], ["session"])
        self.assertEqual([s["kind"] for s in w["seven_day"]["sources"]], ["weekly_all"])
        self.assertEqual(w["seven_day"]["sources"][0]["severity"], "warning")
        self.assertEqual((w["five_hour"]["used_pct"], w["five_hour"]["period_s"],
                          w["five_hour"]["opens"]), (22.0, 18000, "first_use"))
        self.assertEqual((w["seven_day"]["used_pct"], w["seven_day"]["opens"]), (61.0, "anchored"))
        self.assertEqual(w["seven_day"]["resets_at"], round(ex._iso_to_epoch("2099-08-01T16:00:00Z")))

    def test_a_scoped_window_carries_its_aliases(self):
        f = ex._reading_windows(anthropic_reading())["scoped:Fable"]
        self.assertEqual((f["scope"], f["used_pct"], f["period"]), ("Fable", 100.0, "7d"))
        self.assertEqual(f["aliases"], ex.SCOPE_ALIASES["Fable"])
        self.assertIsNone(ex._reading_windows(anthropic_reading())["limit:daily_scoped:Opus"]["aliases"],
                          "a family never captured has no aliases yet")

    def test_an_unknown_kind_is_kept_with_its_group_period_or_none(self):
        w = ex._reading_windows(anthropic_reading())
        self.assertIsNone(w["limit:monthly_all"]["period_s"], "no group period known: unknown, not guessed")
        self.assertEqual(w["limit:daily_scoped:Opus"]["period_s"], 604800)

    def test_an_unknown_unscoped_kind_binds_every_route_and_a_scoped_one_its_own(self):
        res = {"name": ACCT_A, "reading": anthropic_reading()}
        lanes = {lc["lane"]: lc for lc in ex._lanes}
        self.assertEqual(ex._lane_effective(res, lanes["default"]), 61.0)
        self.assertEqual(ex._lane_effective(res, lanes["opus"]), 80.0, "the Opus kind binds opus")
        self.assertEqual(ex._lane_effective(res, lanes["fable"]), 100.0)
        body = anthropic_body()
        body["limits"][3]["percent"] = 97
        res = {"name": ACCT_A, "reading": ex._anthropic_reading(body, 0, "")}
        self.assertEqual(ex._lane_effective(res, lanes["default"]), 97.0)

    def test_null_utilization_is_unknown(self):
        r = anthropic_reading(seven_day={"utilization": None, "resets_at": None})
        w = ex._reading_windows(r)["seven_day"]
        self.assertEqual((w["used_pct"], w["basis"], w["resets_at"]), (None, "unknown", None))
        self.assertIsNone(ex._lane_effective({"reading": r}, ex._lanes[0]))

    def test_a_missing_window_with_a_limits_entry_reads_from_the_entry(self):
        body = anthropic_body()
        del body["five_hour"]
        w = ex._reading_windows(ex._anthropic_reading(body, 0, ""))["five_hour"]
        self.assertEqual(w["used_pct"], 22.0)

    def test_a_severity_that_is_not_text_is_unknown(self):
        """The metrics page looks severity up by value: a list or an object
        there raised on every scrape."""
        for sev in (["warning"], {"level": 2}, 2):
            with self.subTest(severity=sev):
                body = anthropic_body()
                body["limits"][1]["severity"] = sev
                reset({"name": ACCT_A, "ok": 1, "ts": 0.0, "good_ts": 0.0, "stale": 0,
                       "email": ACCT_A, "tier": "max", "http": 0, "err": "",
                       "reading": ex._anthropic_reading(body, 0, "max")})
                src = ex._reading_windows(ex._state[ACCT_A]["reading"])["seven_day"]["sources"][0]
                self.assertIsNone(src["severity"])
                self.assertIn('kind="weekly_all"', ex.render())

    def test_an_unknown_severity_has_no_sample(self):
        """Unknown is never 0: a severity Anthropic gave none of, or one not in
        SEVERITY, has no severity sample, where a known one has its value."""
        for sev, want in ((None, []), ("elevated", []), (["warning"], []),
                          ("normal", ["0"]), ("warning", ["1"]), ("critical", ["2"])):
            with self.subTest(severity=sev):
                body = anthropic_body()
                body["limits"][1]["severity"] = sev
                reset({"name": ACCT_A, "ok": 1, "ts": 0.0, "good_ts": 0.0, "stale": 0,
                       "email": ACCT_A, "tier": "max", "http": 0, "err": "",
                       "reading": ex._anthropic_reading(body, 0, "max")})
                got = [ln.rsplit(" ", 1)[1] for ln in ex.render().splitlines()
                       if ln.startswith("claude_usage_limit_severity{") and 'kind="weekly_all"' in ln]
                self.assertEqual(got, want)

    def test_no_extra_usage_is_no_credits_pot(self):
        r = anthropic_reading()
        self.assertIsNone(ex._reading_pot(r, ex.MONEY_POT))
        self.assertIsNone(ex._reading_pot(r, ex.ALLOWANCE_POT)["open"],
                          "Anthropic states no allowance gate of its own")

    def test_extra_usage_is_the_credits_pot(self):
        for extra, want in (({"is_enabled": True, "monthly_limit": 5000, "used_credits": 1200,
                              "utilization": 24.0}, True),
                            ({"is_enabled": False, "disabled_reason": "out_of_credits"}, False),
                            ({}, None)):
            with self.subTest(extra=extra):
                pot = ex._reading_pot(anthropic_reading(extra_usage=extra), ex.MONEY_POT)
                self.assertIs(pot["open"], want)
                self.assertEqual(pot["open_basis"], None if want is None else "exact")
                self.assertEqual(ex._reading_limit(pot, "balance")["basis"], "unknown")
        pot = ex._reading_pot(anthropic_reading(extra_usage={"is_enabled": True, "monthly_limit": 5000,
                                                             "used_credits": 1200}), ex.MONEY_POT)
        cap = ex._reading_limit(pot, "monthly_limit")
        self.assertEqual((cap["limit"], cap["used"], cap["unit"]), (5000.0, 1200.0, None),
                         "the cap's unit is not known (U7)")

    def test_poll_account_emits_the_reading(self):
        orig = psub._get
        self.addCleanup(setattr, psub, "_get", orig)
        psub._get = lambda path, token: (anthropic_body() if path == "/api/oauth/usage"
                                       else {"account": {"email_address": ACCT_A},
                                             "organization": {"organization_type": "claude_max"}})
        psub._creds[ACCT_A] = {"refresh_token": "r", "access_token": "tok",
                             "expires_at": time.time() + 9999}
        res = psub.poll_account(ACCT_A)
        self.assertEqual(res["ok"], 1, res["err"])
        self.assertNotIn("usage", res)
        self.assertEqual(res["reading"]["provider"], "anthropic")
        self.assertEqual(ex._reading_windows(res["reading"])["seven_day"]["used_pct"], 61.0)


def ledger_entry(i, charge=1.0, route=GPT_ROUTE, at=None, **over):
    e = {"id": f"e-{i}", "at": time.time() if at is None else at, "account": "gpt",
         "pot": "credits", "route": route, "model": "gpt-5.6", "charge": charge,
         "unit": "codex_credit", "basis": "exact", "speed": "standard",
         "usage": {"input_tokens": 1000, "cached_input_tokens": 0, "output_tokens": 100}}
    if charge is None:
        e.update(basis=None, reason="first_event_resend", usage=None)
    e.update(over)
    return e


def post_ledger(*entries, now=None):
    """ledger_accept, as a /reports batch of ledger events reaches it:
    -> ([(result, why)] per entry, seq)."""
    return ex.ledger_accept(list(entries), ROSTER, time.time() if now is None else now)


def month_start(t):
    return ex._budget_period(t)[0]


class TestLedgerAccept(_LogCapture, PayBase):
    """ledger_accept: validation, dedupe, the file, the aggregates (§1.7)."""

    def setUp(self):
        reset()

    def lines(self):
        with open(ex.LEDGER_FILE) as f:
            return [json.loads(ln) for ln in f]

    def test_accepts_and_appends(self):
        res, seq = post_ledger(ledger_entry(1, 2.5), ledger_entry(2, None))
        self.assertEqual((res, seq), ([("applied", None), ("applied", None)], 2))
        got = self.lines()
        self.assertEqual([e["id"] for e in got], ["e-1", "e-2"])
        self.assertEqual(set(got[0]), set(ex._LEDGER_KEYS), "every key written, absent ones null")
        self.assertIsNone(got[1]["charge"])

    def test_a_resend_counts_once(self):
        post_ledger(ledger_entry(1, 2.5))
        res, seq = post_ledger(ledger_entry(1, 2.5), ledger_entry(2, 1.0), ledger_entry(2, 1.0))
        self.assertEqual((results(res), seq), (["ignored", "applied", "ignored"], 2))
        self.assertEqual(len(self.lines()), 2)
        p = ex._ledger_payload(time.time(), ROSTER, ex._money)["pots"]["gpt/credits"]
        self.assertEqual(p["period_spend"], 3.5)

    def test_a_bad_entry_is_refused_with_its_reason(self):
        res, seq = post_ledger(ledger_entry(1), ledger_entry(2, -1.0))
        self.assertEqual(res, [("applied", None),
                               ("refused", "charge must be a finite number >= 0, "
                                           "or null for an uncounted hop")])
        self.assertEqual([e["id"] for e in self.lines()], ["e-1"], "the bad one is not written")
        self.assertEqual(seq, 1)

    def test_refusals(self):
        cases = [
            (ledger_entry(1, colour=1), "unknown key"),
            (dict(ledger_entry(1), id=""), "id must be"),
            (dict(ledger_entry(1), id="x" * 201), "id must be"),
            (dict(ledger_entry(1), at="now"), "at must be epoch seconds"),
            (dict(ledger_entry(1), account=ACCT_A), "not a pot whose charges the ledger counts"),
            (dict(ledger_entry(1), account="kimi"), "not a pot whose charges the ledger counts"),
            (dict(ledger_entry(1), account="nobody"), "not a pot whose charges the ledger counts"),
            (dict(ledger_entry(1), pot="allowance"), "not a pot whose charges the ledger counts"),
            (dict(ledger_entry(1), route=""), "route must be"),
            (dict(ledger_entry(1), unit="usd"), "unit must be 'codex_credit'"),
            (dict(ledger_entry(1), charge=float("nan")), "charge must be a finite number"),
            (dict(ledger_entry(1), charge=float("inf")), "charge must be a finite number"),
            (dict(ledger_entry(1), charge=True), "charge must be a finite number"),
            (dict(ledger_entry(1), charge="1"), "charge must be a finite number"),
            (dict(ledger_entry(1, None), reason=""), "carries its reason"),
            (dict(ledger_entry(1), basis="guess"), "basis must be one of"),
            (dict(ledger_entry(1), model=5), "model must be str"),
            (dict(ledger_entry(1), usage=[1]), "usage must be dict"),
            ("entry", "an entry is an object"),
        ]
        for e, needle in cases:
            with self.subTest(needle=needle, e=e):
                (res, why), = post_ledger(e)[0]
                self.assertEqual(res, "refused")
                self.assertIn(needle, why)
        self.assertFalse(os.path.exists(ex.LEDGER_FILE), "nothing written")

    def test_spend_is_per_period_and_route(self):
        now = time.time()
        last = month_start(now) - 86400
        post_ledger(ledger_entry(1, 4.0), ledger_entry(2, 1.0, route="fast/gpt/gpt-6.1-sol"),
                    ledger_entry(3, 100.0, at=last), ledger_entry(4, None))
        p = ex._ledger_payload(now, ROSTER, ex._money)
        self.assertEqual(p["period"], {"start": month_start(now), "end": ex._budget_period(now)[1]})
        g = p["pots"]["gpt/credits"]
        self.assertEqual((g["period_spend"], g["uncounted"]), (5.0, 1))
        self.assertEqual(g["routes"], {"fast/gpt/gpt-6.1-sol": 1.0, GPT_ROUTE: 4.0})
        self.assertEqual(ex._ledger_totals()[("gpt/credits", GPT_ROUTE)], 104.0,
                         "money_spent_total counts every period")

    def test_the_file_is_read_back_at_startup(self):
        post_ledger(ledger_entry(1, 4.0), ledger_entry(2, None))
        with open(ex.LEDGER_FILE, "a") as f:
            f.write("{not json\n")
            f.write(json.dumps({"at": "x"}) + "\n")
        with ex._ledger_lock:
            ex._ledger.update(seq=0, ids={}, spend={}, uncounted={}, unpriced={}, total={}, recon={},
                              unreadable=0, counted_at={})
        self.capture_log()
        ex._load_ledger()
        logged = self.logged
        p = ex._ledger_payload(time.time(), ROSTER, ex._money)
        self.assertEqual((p["seq"], p["unreadable_lines"]), (4, 2))
        self.assertEqual(p["pots"]["gpt/credits"]["period_spend"], 4.0)
        self.assertEqual(p["pots"]["gpt/credits"]["uncounted"], 1)
        self.assertTrue(any(":3 unreadable" in ln for ln in logged), logged)
        self.assertEqual(results(post_ledger(ledger_entry(1, 4.0))[0]), ["ignored"],
                         "ids read back still dedupe")
        self.assertEqual(post_ledger(ledger_entry(5, 1.0))[1], 5)

    def test_an_id_is_forgotten_only_once_its_resend_is_refused(self):
        """Dedupe keeps ids for this period and the last; an entry's `at` may
        reach no further back. So a resend after its id is forgotten is
        refused, and money_spent_total never counts one entry twice (nor
        goes down)."""
        now = time.time()
        last = month_start(month_start(now) - 1) + 10
        post_ledger(ledger_entry(1, 3.0, at=last), ledger_entry(2, 1.0, at=now), now=now)
        self.assertEqual(set(ex._ledger["ids"]), {"e-1", "e-2"})
        later = month_start(now) + 40 * 86400  # the next period: e-1's is two back
        with ex._ledger_lock:
            ex._ledger_prune(later)
        self.assertEqual(set(ex._ledger["ids"]), {"e-2"})
        total = sum(ex._ledger_totals().values())
        (res, why), = post_ledger(ledger_entry(1, 3.0, at=last), now=later)[0]
        self.assertEqual(res, "refused", why)
        self.assertIn("in this budget period or the last", why)
        self.assertEqual(sum(ex._ledger_totals().values()), total)
        res = post_ledger(ledger_entry(2, 1.0, at=now), now=later)[0]
        self.assertEqual(results(res), ["ignored"], res)
        self.assertEqual(sum(ex._ledger_totals().values()), total, "a resend in reach counts once")

    def restart(self):
        """What a brain restart does to the ledger: forget it, read the file."""
        with ex._ledger_lock:
            ex._ledger.update(seq=0, ids={}, spend={}, uncounted={}, unpriced={}, total={}, recon={},
                              unreadable=0, counted_at={})
        self.capture_log()
        ex._load_ledger()
        return ex._ledger_payload(time.time(), ROSTER, ex._money)

    def test_at_is_epoch_seconds_within_a_bound_of_now(self):
        now = time.time()
        last = month_start(month_start(now) - 1)
        for at, ok in ((now * 1000, False), (now + ex.LEDGER_AT_AHEAD_S + 1, False),
                       (last - 1, False), (-1.0, False),
                       (now + ex.LEDGER_AT_AHEAD_S - 1, True), (last, True)):
            with self.subTest(at=at):
                (res, why), = post_ledger(ledger_entry(f"{at}", at=at), now=now)[0]
                self.assertEqual(res, "applied" if ok else "refused", why)
                if not ok:
                    self.assertIn("at must be epoch seconds", why)

    def test_no_line_stops_the_brain_starting(self):
        # An `at` in milliseconds is past datetime's range: folded into a
        # budget period it raised at every start. Today's validation reads it
        # unreadable instead, as it does any line that is not an entry.
        post_ledger(ledger_entry(1, 4.0))
        with open(ex.LEDGER_FILE, "a") as f:
            for bad in (dict(ledger_entry(2), at=time.time() * 1000), dict(ledger_entry(3), at=1e300),
                        dict(ledger_entry(4), charge=-1.0), dict(ledger_entry(5), colour=1)):
                f.write(json.dumps(bad) + "\n")
            f.write("[" * 100000 + "\n")
            f.write("\xff\n")
        p = self.restart()
        self.assertEqual((p["seq"], p["unreadable_lines"]), (7, 6))
        self.assertEqual(p["pots"]["gpt/credits"]["period_spend"], 4.0)
        self.assertTrue(any(":2 unreadable (at must be epoch seconds" in ln for ln in self.logged),
                        self.logged)

    def test_a_failed_append_then_the_resend_counts_once(self):
        # The append wrote its lines and then failed (fsync), so the entries
        # came back `retry` and the proxy resends: in the file twice, counted
        # once, by id.
        post_ledger(ledger_entry(1, 1.0))
        fsync = ex.os.fsync
        def fail(fd):
            raise OSError(5, "Input/output error")
        ex.os.fsync = fail
        try:
            res, _ = post_ledger(ledger_entry(10, 5.0), ledger_entry(11, 7.0))
        finally:
            ex.os.fsync = fsync
        self.assertEqual(results(res), ["retry", "retry"])
        self.assertEqual(results(post_ledger(ledger_entry(10, 5.0), ledger_entry(11, 7.0))[0]),
                         ["applied", "applied"])
        before = ex._ledger_totals()
        self.assertEqual(len(self.lines()), 5, "the failed append's lines stay in the file")
        p = self.restart()
        self.assertEqual((p["pots"]["gpt/credits"]["period_spend"], p["unreadable_lines"]), (13.0, 0))
        self.assertEqual(ex._ledger_totals(), before, "money_spent_total never goes down, or up")
        self.assertEqual(results(post_ledger(ledger_entry(11, 7.0))[0]), ["ignored"],
                         "the ids read back")

    def test_a_partial_line_is_cut_before_the_next_append(self):
        # An append that failed part-way leaves a line with no newline. The
        # next append must not join its first line onto it (one unreadable
        # line hiding a counted charge), and a load drops it.
        post_ledger(ledger_entry(1, 1.0))
        with open(ex.LEDGER_FILE, "a") as f:
            f.write(json.dumps(ledger_entry(10, 5.0))[:40])
        p = self.restart()
        self.assertEqual((p["seq"], p["unreadable_lines"], p["pots"]["gpt/credits"]["period_spend"]),
                         (1, 0, 1.0))
        self.assertTrue(any(":2 has no newline" in ln for ln in self.logged), self.logged)
        self.capture_log()
        self.assertEqual(results(post_ledger(ledger_entry(10, 5.0), ledger_entry(11, 7.0))[0]),
                         ["applied", "applied"])
        self.assertTrue(any("cut 40 bytes" in ln for ln in self.logged), self.logged)
        self.assertEqual([e["id"] for e in self.lines()], ["e-1", "e-10", "e-11"])
        before = ex._ledger_totals()
        p = self.restart()
        self.assertEqual((p["unreadable_lines"], p["pots"]["gpt/credits"]["period_spend"]), (0, 13.0))
        self.assertEqual(ex._ledger_totals(), before)

    def test_a_complete_final_line_with_no_newline_is_dropped_too(self):
        # It came back `retry` (the newline was never written), so the proxy
        # resends it; counting it here as well would count it twice.
        post_ledger(ledger_entry(1, 1.0))
        with open(ex.LEDGER_FILE, "a") as f:
            f.write(json.dumps(ledger_entry(2, 5.0)))
        p = self.restart()
        self.assertEqual(p["pots"]["gpt/credits"]["period_spend"], 1.0)
        post_ledger(ledger_entry(2, 5.0))
        self.assertEqual([e["id"] for e in self.lines()], ["e-1", "e-2"])

    def test_cut_finds_a_line_boundary_beyond_one_read(self):
        post_ledger(ledger_entry(1, 1.0))
        with open(ex.LEDGER_FILE, "a") as f:
            f.write("x" * 200000)
        post_ledger(ledger_entry(2, 2.0))
        self.assertEqual([e["id"] for e in self.lines()], ["e-1", "e-2"])
        with open(ex.LEDGER_FILE, "w") as f:
            f.write("y" * 70000)
        post_ledger(ledger_entry(3, 3.0))
        self.assertEqual([e["id"] for e in self.lines()], ["e-3"])

    def test_reconciliation(self):
        t0 = time.time() - 100
        ex._ledger_note_balance("gpt/credits", 100.0, t0)
        post_ledger(ledger_entry(1, 5.0, at=t0 + 10), ledger_entry(2, 3.0, at=t0 - 50))
        ex._ledger_note_balance("gpt/credits", 90.0, t0 + 20)
        ex._ledger_note_balance("gpt/credits", 200.0, t0 + 30)   # a refill
        ex._ledger_note_balance("gpt/credits", 195.0, t0 + 40)
        ex._ledger_note_balance("gpt/credits", None, t0 + 50)    # unknown says nothing
        ex._ledger_note_balance("gpt/credits", 1.0, t0 + 5)      # older than the last: ignored
        rc = ex._ledger_payload(time.time(), ROSTER, ex._money)["pots"]["gpt/credits"]["reconciliation"]
        self.assertEqual(rc, {"since": round(t0), "balance_falls": 15.0, "ledger": 5.0,
                              "elsewhere_or_uncounted": 10.0})

    def test_only_countable_pots_are_listed(self):
        p = ex._ledger_payload(time.time(), ROSTER, ex._money)
        self.assertEqual(set(p["pots"]), {"gpt/credits"})
        g = p["pots"]["gpt/credits"]
        self.assertEqual(g, {"unit": "codex_credit", "period_spend": 0, "routes": {}, "uncounted": 0,
                             "unpriced": {}, "budget": None, "route_budgets": {},
                             "reconciliation": None})
        rates = p["rates"]["codex_credit"]
        self.assertEqual(rates["models"]["gpt-6-astra"], {"input": 250.0, "cached": 25.0, "output": 1250.0})
        self.assertEqual(rates["models"]["gpt-6.1-sol"], {"input": 50.0, "cached": 2.5, "output": 250.0})
        self.assertEqual(rates["speed_weights"], {"standard": 1.0, "fast": 2.0, "ultrafast": 6.0})
        self.assertEqual(rates["per_tokens"], 1_000_000)


class TestRecheckEndpoint(DocAPIBase, PayBase):
    """POST /recheck?account= (quota-model 50 §2.6, C7): key-guarded; polls
    the account at once and bumps its recheck_epoch in /preferred, which the
    proxy reads to drop the account's older refusal records."""

    def tearDown(self):
        DocAPIBase.tearDown(self)
        PayBase.tearDown(self)
        with ex._poll_health_lock:
            ex._recheck_epochs.clear()
        ex._poll_wakeup.clear()

    def test_key_guard(self):
        for key in (None, "nope"):
            self.assertEqual(self.req("POST", "/recheck?account=gpt", key=key)[0], 401)
        with ex._poll_health_lock:
            self.assertEqual(ex._recheck_epochs, {}, "a refused re-check bumps nothing")

    def test_account_required_and_known(self):
        self.assertEqual(self.req("POST", "/recheck")[0], 400)
        code, body, _ = self.req("POST", "/recheck?account=nobody@example.com")
        self.assertEqual((code, body["account"]), (404, "nobody@example.com"))
        with ex._poll_health_lock:
            self.assertEqual(ex._recheck_epochs, {})

    def test_polls_at_once_and_publishes_the_epoch(self):
        with ex._poll_health_lock:
            ex._poll_schedule["kimi"] = time.time() + 600
        ex._poll_wakeup.clear()
        before = time.time()
        code, body, _ = self.req("POST", "/recheck?account=kimi")
        self.assertEqual((code, body["account"]), (200, "kimi"))
        self.assertGreaterEqual(body["recheck_epoch"], before)
        with ex._poll_health_lock:
            self.assertNotIn("kimi", ex._poll_schedule, "due at the poller's next pass")
        self.assertTrue(ex._poll_wakeup.is_set(), "the poller wakes for it")
        _, p, _ = self.req("GET", "/preferred")
        self.assertEqual(p["accounts"]["kimi"]["recheck_epoch"], body["recheck_epoch"])
        self.assertIsNone(p["accounts"]["gpt"]["recheck_epoch"], "only that account")

    def test_epoch_never_moves_back(self):
        later = time.time() + 1000
        with ex._poll_health_lock:
            ex._recheck_epochs["kimi"] = later
        _, body, _ = self.req("POST", "/recheck?account=kimi")
        self.assertEqual(body["recheck_epoch"], later)


class TestBudgetGateLive(PayBase):
    """The brain's dial check reads its own ledger and the money document's
    budgets (§1.7, §1.9): the golden cases, on live state."""

    def setUp(self):
        g = gpt_on_credits(pct=100.0)
        self.reset_at = int(time.time()) + 86400
        g["raw"]["rate_limit"]["primary_window"]["reset_at"] = self.reset_at
        classes_reset(g, kimi_result(), cfg=pay_cfg(gpt=["gpt/credits"]))

    def check(self):
        ex.compute_preferred()
        return route_check(GPT_ROUTE, "gpt")

    def test_absent_budget_never_gates(self):
        post_ledger(ledger_entry(1, 1e6))
        self.assertEqual(self.check()["check"], "paid")

    def test_pot_budget(self):
        install_money({"pots": {"gpt/credits": {"budget": 10.0}}})
        post_ledger(ledger_entry(1, 9.0))
        self.assertEqual(self.check()["check"], "paid")
        post_ledger(ledger_entry(2, 1.0))
        c = self.check()
        self.assertEqual((c["check"], c["reason"]), ("no", "budget"))
        self.assertEqual(c["until"], min(self.reset_at, ex._budget_period(time.time())[1]),
                         "until the allowance reopens, no later than the period's end")
        v = class_verdicts()["gpt/gpt-5.6"]
        self.assertEqual(v["check"]["reason"], "budget")

    def test_uncounted_hops_never_reach_a_budget(self):
        install_money({"pots": {"gpt/credits": {"budget": 0.5}}})
        post_ledger(*[ledger_entry(i, None) for i in range(5)])
        self.assertEqual(self.check()["check"], "paid")

    def test_route_budget(self):
        install_money({"pots": {"gpt/credits": {"route_budgets": {GPT_ROUTE: 2.0}}}})
        post_ledger(ledger_entry(1, 5.0, route="fast/gpt/gpt-6.1-sol"))
        self.assertEqual(self.check()["check"], "paid", "another route's spend is not this route's")
        post_ledger(ledger_entry(2, 2.0))
        self.assertEqual(self.check()["reason"], "budget")

    def test_last_months_spend_is_not_this_months(self):
        install_money({"pots": {"gpt/credits": {"budget": 10.0}}})
        post_ledger(ledger_entry(1, 50.0, at=month_start(time.time()) - 60))
        self.assertEqual(self.check()["check"], "paid")

    def test_a_reached_budget_leaves_the_allowance(self):
        g = gpt_on_credits(pct=10.0)
        classes_reset(g, kimi_result(), cfg=pay_cfg(gpt=["gpt/credits"]))
        install_money({"pots": {"gpt/credits": {"budget": 1.0}}})
        post_ledger(ledger_entry(1, 5.0))
        self.assertEqual(self.check()["check"], "included")

    def test_an_unreadable_ledger_line_closes_every_budget_as_unknown(self):
        # It may hide spend, so no budget can say it has room while it is
        # there; a route with no budget is not touched.
        install_money({"pots": {"gpt/credits": {"budget": 10.0}}})
        post_ledger(ledger_entry(1, 1.0))
        with open(ex.LEDGER_FILE, "a") as f:
            f.write("{torn\n")
        with ex._ledger_lock:
            ex._ledger.update(seq=0, ids={}, spend={}, uncounted={}, unpriced={}, total={}, recon={},
                              unreadable=0, counted_at={})
        ex._load_ledger()
        c = self.check()
        self.assertEqual((c["check"], c["reason"], c["until"], c["basis"]),
                         ("no", "unknown", None, "ledger_unreadable"))
        install_money({})
        self.assertEqual(self.check()["check"], "paid")

    def test_a_lower_bound_entry_leaves_its_budgets_unknown(self):
        # Part of its cost has no price (an image with no image usage): it
        # counts what is known, and a budget it reaches cannot say it has
        # room for the rest of the period. A route budget elsewhere is not
        # reached by it.
        post_ledger(ledger_entry(1, 0.5, basis="lower_bound"))
        p = ex._ledger_payload(time.time(), ROSTER, ex._money)["pots"]["gpt/credits"]
        self.assertEqual((p["period_spend"], p["unpriced"]), (0.5, {GPT_ROUTE: 1}))
        install_money({"pots": {"gpt/credits": {"route_budgets": {"fast/gpt/gpt-6.1-sol": 10.0}}}})
        self.assertEqual(self.check()["check"], "paid")
        for money in ({"pots": {"gpt/credits": {"budget": 10.0}}},
                      {"pots": {"gpt/credits": {"route_budgets": {GPT_ROUTE: 10.0}}}}):
            install_money(money)
            c = self.check()
            self.assertEqual((c["check"], c["reason"], c["until"], c["basis"]),
                             ("no", "unknown", None, "unpriced"), money)
        # Last month's lower bound says nothing of this month.
        reset_ledger()
        post_ledger(ledger_entry(2, 0.5, basis="lower_bound", at=month_start(time.time()) - 60))
        self.assertEqual(self.check()["check"], "paid")

    def test_a_reached_budget_reads_reached_whatever_the_ledger_lacks(self):
        # Spend only grows, so a reached budget stays reached; only room is
        # what an undercounting ledger cannot vouch for.
        install_money({"pots": {"gpt/credits": {"budget": 1.0}}})
        post_ledger(ledger_entry(1, 5.0))
        with ex._ledger_lock:
            ex._ledger["unreadable"] = 1
        self.assertEqual(self.check()["reason"], "budget")

    def test_a_unit_with_no_rates_reads_unknown(self):
        # Every charge on it would enter uncounted, so the budget never closes.
        install_money({"pots": {"gpt/credits": {"budget": 10.0}}})
        rates = ex._LEDGER_RATES
        self.addCleanup(setattr, ex, "_LEDGER_RATES", rates)
        # Each table its charges need, chat and image alike.
        broken_all = [{}] + [{"codex_credit": dict(rates["codex_credit"], **{k: v})}
                             for k in ex._LEDGER_RATE_TABLES["codex_credit"] for v in (None, {} if k != "per_tokens" else 0)]
        for broken in broken_all:
            with self.subTest(rates=broken):
                ex._LEDGER_RATES = broken
                c = self.check()
                self.assertEqual((c["check"], c["reason"], c["until"], c["basis"]),
                                 ("no", "unknown", None, "no_rates"))

    def test_a_stale_ledger_reads_unknown_until_the_next_poll(self):
        ex.compute_preferred()
        with ex._plock:
            ctx = ex._policy_ctx(list(ex._state.values()), time.time())
        st = ctx["standings"]["gpt"]
        st = dict(st, next_poll_at=round(time.time()) + 240)
        now = time.time()
        led = dict(ex._ledger_payload(now, ROSTER, ex._money), as_of=now - ex.STALE_MAX - 1)
        r = {"id": GPT_ROUTE, "pay": ["gpt/credits"], "scope": "gpt-5.6"}
        c = ex.dial_check(r, "gpt", st, {"gpt/credits": {"budget": 10.0}}, now, ledger=led)
        self.assertEqual((c["check"], c["reason"], c["until"], c["basis"]),
                         ("no", "unknown", st["next_poll_at"], "unknown"))
        self.assertEqual(ex._until_basis(st, None, c["until"], c["reason"], now), "next_poll")


class MoneyDocBase(DocAPIBase, PayBase):
    """Real money and classes files, and the /money PUT."""

    def setUp(self):
        super().setUp()
        self.money_path = os.path.join(_TMP, f"money-{self.__class__.__name__}.json")
        self._old_money_file = ex.MONEY_FILE
        ex.MONEY_FILE = self.money_path
        for suffix in ("", ".history.jsonl"):
            try:
                os.remove(self.money_path + suffix)
            except FileNotFoundError:
                pass
        with ex._plock:
            ex._money, ex._money_doc, ex._money_warnings = ex._load_money(), {}, []

    def tearDown(self):
        ex.MONEY_FILE = self._old_money_file
        with ex._plock:
            ex._money_doc, ex._money_warnings = {}, []
        DocAPIBase.tearDown(self)
        PayBase.tearDown(self)

    def put(self, doc, rev=None, reason="test", key=KEY):
        rev = rev or self.req("GET", "/money")[1]["revision"]
        return self.req("PUT", "/money", {"doc": doc, "reason": reason}, key=key,
                        headers={"If-Match": rev, "X-Actor": "tester"})


class TestBudgetsAtPut(MoneyDocBase):
    """budget and route_budgets join the money document (§1.9), validated at PUT."""

    def setUp(self):
        super().setUp()
        self.install_classes(pay_cfg(gpt=["gpt/credits"]))
        reset(opus_acct(30), gpt_on_credits(), kimi_result())
        ex.compute_preferred()

    def test_a_budget_on_a_countable_pot_is_accepted_and_published(self):
        doc = {"pots": {"gpt/credits": {"budget": 50, "route_budgets": {GPT_ROUTE: 20}}}}
        code, p, _ = self.put(doc)
        self.assertEqual(code, 200, p)
        _, pref, _ = self.req("GET", "/preferred")
        pot = pref["pots"]["gpt/credits"]
        self.assertEqual((pot["budget"], pot["route_budgets"], pot["countable"]),
                         (50.0, {GPT_ROUTE: 20.0}, True))
        led = pref["ledger"]["pots"]["gpt/credits"]
        self.assertEqual((led["budget"], led["route_budgets"]), (50.0, {GPT_ROUTE: 20.0}))

    def test_refusals(self):
        cases = [
            ({"pots": {f"{ACCT_A}/credits": {"budget": 5}}}, "has none (§1.7)"),
            ({"pots": {"kimi/credits": {"budget": 5}}}, "has none (§1.7)"),
            ({"pots": {"gpt/credits": {"route_budgets": {"frontier/kimi/kimi-k3": 5}}}},
             "never draws this pot"),
            ({"pots": {"gpt/credits": {"route_budgets": {"lane:nope": 5}}}}, "unknown route"),
            ({"pots": {"gpt/credits": {"budget": -5}}}, "budget must be a number"),
        ]
        for doc, needle in cases:
            with self.subTest(doc=doc):
                code, p, _ = self.put(doc)
                self.assertEqual(code, 422, p)
                self.assertTrue(any(needle in e for e in p["errors"]), p["errors"])

    def test_a_budget_on_a_unit_the_ledger_has_no_rates_for_is_refused(self):
        rates = ex._LEDGER_RATES
        self.addCleanup(setattr, ex, "_LEDGER_RATES", rates)
        for broken in ({}, {"codex_credit": dict(rates["codex_credit"], images={})}):
            ex._LEDGER_RATES = broken
            for doc in ({"pots": {"gpt/credits": {"budget": 5}}},
                        {"pots": {"gpt/credits": {"route_budgets": {GPT_ROUTE: 5}}}}):
                code, p, _ = self.put(doc)
                self.assertEqual(code, 422, (broken, p))
                self.assertTrue(any("has none for codex_credit" in e for e in p["errors"]), p["errors"])

    def test_absent_budget_is_unset(self):
        self.assertEqual(self.put({})[0], 200)
        _, pref, _ = self.req("GET", "/preferred")
        self.assertIsNone(pref["pots"]["gpt/credits"]["budget"])
        self.assertIsNone(pref["ledger"]["pots"]["gpt/credits"]["budget"])

    def test_a_classes_change_that_orphans_a_route_budget_warns_dangling(self):
        self.assertEqual(self.put({"pots": {"gpt/credits": {"route_budgets": {GPT_ROUTE: 20}}}})[0], 200)
        errs, warns, _, _ = ex._doc_check("classes", pay_cfg(gpt=[]))
        self.assertEqual(errs, [])
        self.assertTrue(any(w.startswith("money: dangling:") and GPT_ROUTE in w for w in warns), warns)
        # Once live, the orphaned budget is shown dangling on the pot and binds nothing.
        self.install_classes(pay_cfg(gpt=[]))
        ex.compute_preferred()
        _, pref, _ = self.req("GET", "/preferred")
        self.assertTrue(any("dangling" in w for w in pref["pots"]["gpt/credits"]["warnings"]),
                        pref["pots"]["gpt/credits"])


class TestColdRoutesNameAPassingAccount(PayBase):
    """C2's open item: a bare lane or a pool where no passing account has a
    known effective still names one, as a member does."""

    def cold(self, name, credits):
        a = acct(name, 10)
        a["raw"]["seven_day"] = {}  # unknown
        a["raw"]["extra_usage"] = {"is_enabled": credits}
        return read(a)

    def test_cold_lane_names_the_passing_account(self):
        # A's credits are on and lane:default may not pay them: no (unknown).
        # B's are off: included, basis unknown, and no effective.
        classes_reset(self.cold(ACCT_A, True), self.cold(ACCT_B, False), cfg=pay_cfg())
        self.assertEqual(route_check("lane:default", ACCT_A)["check"], "no")
        self.assertEqual(route_check("lane:default", ACCT_B)["check"], "included")
        self.assertEqual(lane_account("default"), ACCT_B)
        with ex._plock:
            self.assertNotIn("effective", ex._preferred["lanes"]["default"],
                             "no number is invented for it")

    def test_cold_lane_ties_go_to_the_first_by_name(self):
        classes_reset(self.cold(ACCT_B, False), self.cold(ACCT_A, False), cfg=pay_cfg())
        self.assertEqual(lane_account("default"), ACCT_A)

    def test_a_lane_where_nothing_passes_names_nobody(self):
        classes_reset(self.cold(ACCT_A, True), self.cold(ACCT_B, True), cfg=pay_cfg())
        self.assertIsNone(lane_account("default"))

    def test_a_known_account_takes_over_from_a_cold_incumbent(self):
        classes_reset(self.cold(ACCT_A, False), self.cold(ACCT_B, False), cfg=pay_cfg())
        self.assertEqual(lane_account("default"), ACCT_A)
        b = acct(ACCT_B, 20)
        b["raw"]["extra_usage"] = {"is_enabled": False}
        with ex._lock:
            ex._state[ACCT_B] = read(b)
        ex.compute_preferred()
        self.assertEqual(lane_account("default"), ACCT_B)

    def test_cold_pool_names_the_passing_account(self):
        pool = []
        for name in ("gpt-2", "gpt-1"):
            g = gpt_result(name=name)
            g["raw"]["rate_limit"]["primary_window"]["used_percent"] = None  # no window read
            pool.append(read(g))
        classes_reset(*pool, cfg=pay_cfg())
        self.assertIsNone(ex._provider_effective(pool[0]))
        with ex._plock:
            self.assertEqual(ex._preferred["providers"]["gpt"]["account"], "gpt-1")


class TestC6Metrics(PayBase):
    """The new families §2.11 names; the existing ones stay (TestRenderMetrics
    and the families below)."""

    L = staticmethod(lambda t, m, **kw: TestRenderMetrics().line(t, m, **kw))

    def setUp(self):
        g = gpt_on_credits(pct=100.0)
        classes_reset(opus_spent(), g, kimi_result(), cfg=pay_cfg(opus=[POT_A], gpt=["gpt/credits"]))
        post_ledger(ledger_entry(1, 2.5), ledger_entry(2, 1.5), ledger_entry(3, None),
                    ledger_entry(4, 4.0, at=month_start(time.time()) - 60))
        self.t = ex.render()

    def test_help_and_type(self):
        for fam, typ in (("quota_pot_open", "gauge"), ("entitlement_active", "gauge"),
                         ("route_check", "gauge"), ("money_spent_total", "counter")):
            self.assertIn(f"# TYPE {fam} {typ}", self.t)

    def test_quota_pot_open(self):
        L, t = self.L, self.t
        self.assertEqual(L(t, "quota_pot_open", account="gpt", pot="allowance", basis="exact"), 0)
        self.assertEqual(L(t, "quota_pot_open", account="gpt", pot="credits", basis="derived"), 1)
        self.assertEqual(L(t, "quota_pot_open", account=ACCT_A, pot="credits", basis="exact"), 1)
        self.assertEqual(L(t, "quota_pot_open", account="kimi", pot="allowance", basis="derived"), 1)
        self.assertIsNone(L(t, "quota_pot_open", account="kimi", pot="credits"),
                          "unknown is absent, never 0")
        self.assertIsNone(L(t, "quota_pot_open", account=ACCT_B), "never measured")

    def test_entitlement_active(self):
        L, t = self.L, self.t
        self.assertEqual(L(t, "entitlement_active", account="gpt", pot="credits"), 1)
        self.assertEqual(L(t, "entitlement_active", account=ACCT_A, pot="allowance"), 1)
        self.assertIsNone(L(t, "entitlement_active", account="kimi", pot="credits"))

    def test_route_check(self):
        L, t = self.L, self.t
        self.assertEqual(L(t, "route_check", route=GPT_ROUTE, account="gpt", check="paid:gpt/credits"), 1)
        self.assertEqual(L(t, "route_check", route="frontier/lane:opus", account=ACCT_A,
                           check=f"paid:{POT_A}"), 1)
        self.assertEqual(L(t, "route_check", route="lane:opus", account=ACCT_B, check="no:unknown"), 1)
        n = sum(1 for ln in t.splitlines() if ln.startswith("route_check{"))
        with ex._lock:
            snap = list(ex._state.values())
        with ex._plock:
            checks = ex._policy_ctx(snap, time.time())["checks"]
        self.assertEqual(n, sum(len(c) for c in checks.values()), "one series per (route, account)")

    def test_money_spent_total(self):
        self.assertEqual(self.L(self.t, "money_spent_total", account="gpt", pot="credits",
                                route=GPT_ROUTE), 8.0, "every period, uncounted excluded")

    def test_existing_families_keep_their_series(self):
        L, t = self.L, self.t
        self.assertEqual(L(t, "money_balance", account="gpt", pot="credits", unit="codex_credit"), 250.0)
        self.assertEqual(L(t, "provider_account_quota_utilization_percent", provider="gpt",
                           account="gpt", window="seven_day"), 100.0)
        self.assertEqual(L(t, "claude_usage_window_utilization_percent", account=ACCT_A,
                           window="seven_day"), 10.0)
        self.assertEqual(L(t, "claude_usage_limit_percent", account=ACCT_A, kind="weekly_scoped",
                           scope_model="Opus"), 100.0)
        self.assertEqual(L(t, "claude_usage_extra_credits_enabled", account=ACCT_A), 1)


class TestReportedReadings(PayBase):
    """A reading wins a window when it is newer (§5): kept beside the polls,
    merged per window and per field inside the decision context."""

    def setUp(self):
        self.t0 = time.time() - 100  # the polls started 100 s ago
        reset(anth_polled(self.t0), gpt_on_credits(pct=40.0, good_age=100))
        ex.compute_preferred()

    def test_a_newer_reading_is_adopted_and_carries_its_own_age(self):
        at = self.t0 + 10
        self.assertEqual(post_reports(rep_reading(1, at, used=77.0))[0], [("applied", None)])
        st = live_ctx()["standings"][ACCT_A]
        w = st["allowance"]["windows"]["seven_day"]
        self.assertEqual((w["used_pct"], w["as_of"]), (77.0, at))
        self.assertEqual(st["allowance"]["windows"]["five_hour"]["as_of"], self.t0,
                         "a window nothing reported keeps the poll's age")
        self.assertEqual(st["as_of"], self.t0, "the standing's as_of stays the poll's")
        doc = ex._preferred_payload()
        self.assertEqual(doc["accounts"][ACCT_A]["allowance"]["windows"]["seven_day"]["as_of"], at)
        self.assertEqual(doc["weekly"][ACCT_A], 77.0, "the document reads the merge")

    def test_a_reading_up_to_a_point_below_agrees_and_moves_only_the_age(self):
        # The poll reads 30. A response a point below it is the same counter
        # rounded half a point lower (quota-model 10, "Rounding"): the poll's
        # use stands, and the reading moves only its age.
        at, week = self.t0 + 10, self.t0 + 3 * 86400
        self.assertEqual(post_reports(rep_reading(1, at, used=29.0, resets=week))[0], [("applied", None)])
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["as_of"]), (30.0, at))
        self.assertEqual(ex._preferred_payload()["weekly"][ACCT_A], 30.0)
        post_reports(rep_reading(2, at + 10, used=29.5))
        self.assertEqual(live_window("seven_day")["used_pct"], 30.0, "half a point below, no reset carried")
        post_reports(rep_reading(3, at + 20, used=31.0))
        self.assertEqual(live_window("seven_day")["used_pct"], 31.0, "a reading above is newer use")
        post_reports(rep_reading(4, at + 30, used=30.0))
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["as_of"]), (31.0, at + 30),
                         "a point below the report the brain holds agrees with it")

    def test_a_reading_further_below_or_of_another_period_is_newer_use(self):
        at = self.t0 + 10
        post_reports(rep_reading(1, at, used=28.5))
        self.assertEqual(live_window("seven_day")["used_pct"], 28.5, "more than a point below")
        reset(anth_polled(self.t0), gpt_on_credits(pct=40.0, good_age=100))
        ex.compute_preferred()
        post_reports(rep_reading(2, at, used=29.0, resets=self.t0 + 10 * 86400))
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["resets_at"]), (29.0, self.t0 + 10 * 86400),
                         "a point below, but of a later period")

    def test_an_older_reading_is_ignored(self):
        res, _ = post_reports(rep_reading(1, self.t0 - 5, used=77.0))
        self.assertEqual(res[0][0], "ignored")
        self.assertEqual(live_window("seven_day")["used_pct"], 30.0)
        post_reports(rep_reading(2, self.t0 + 20, used=60.0))
        res, _ = post_reports(rep_reading(3, self.t0 + 10, used=90.0))
        self.assertEqual(res[0][0], "ignored", "older than the report the brain holds")
        self.assertEqual(live_window("seven_day")["used_pct"], 60.0)
        self.assertEqual(post_reports(rep_reading(2, self.t0 + 20, used=60.0))[0][0][0], "ignored",
                         "the same reading again changes nothing")

    def test_per_field(self):
        at, r = self.t0 + 10, self.t0 + 9 * 86400
        post_reports(rep_reading(1, at, resets=r))
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["resets_at"], w["as_of"]), (30.0, r, self.t0),
                         "a reset alone moves the reset, not the use or its age")

    def test_a_reading_with_no_use_never_adds_a_window(self):
        reset(anth_polled(self.t0, five_hour=False))
        ex.compute_preferred()
        before = {lane: route_check(f"lane:{lane}", ACCT_A) for lane in ("opus", "fable")}
        self.assertEqual({c["check"] for c in before.values()}, {"included"})
        self.assertEqual({c["basis"] for c in before.values()}, {"fresh"})
        res, _ = post_reports(rep_reading(1, self.t0 + 10, window="five_hour",
                                          resets=self.t0 + 3 * 3600))
        self.assertEqual(res[0], ("ignored", "a reading that carries no use never adds a window"))
        self.assertIsNone(live_window("five_hour"))
        for lane, c in before.items():
            self.assertEqual(route_check(f"lane:{lane}", ACCT_A), c, "no route goes stale")
        post_reports(rep_reading(2, self.t0 + 10, window="five_hour", used=5.0))
        res, _ = post_reports(rep_reading(3, self.t0 + 20, window="five_hour",
                                          resets=self.t0 + 3 * 3600))
        self.assertEqual(res[0], ("applied", None), "a window a use added takes a reset")
        w = live_window("five_hour")
        self.assertEqual((w["used_pct"], w["as_of"], w["resets_at"]),
                         (5.0, self.t0 + 10, self.t0 + 3 * 3600))

    def test_a_poll_that_started_earlier_keeps_the_report(self):
        at = self.t0 + 50
        post_reports(rep_reading(1, at, used=77.0))
        poll_lands(anth_polled(at - 2, weekly=50.0))
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["as_of"]), (77.0, at))

    def test_a_later_poll_replaces_it(self):
        at = self.t0 + 10
        post_reports(rep_reading(1, at, used=77.0))
        poll_lands(anth_polled(at + 1, weekly=50.0))
        w = live_window("seven_day")
        self.assertEqual((w["used_pct"], w["as_of"]), (50.0, at + 1))
        self.assertEqual(ex._reported, {}, "dropped once a poll catches up")

    def test_five_hour_is_added_and_a_later_poll_leaving_it_out_removes_it(self):
        reset(anth_polled(self.t0, five_hour=False))
        at, r = self.t0 + 10, self.t0 + 4 * 3600
        self.assertIsNone(live_window("five_hour"))
        self.assertEqual(post_reports(rep_reading(1, at, window="five_hour", used=12.0, resets=r))[0],
                         [("applied", None)])
        w = live_window("five_hour")
        self.assertEqual((w["used_pct"], w["resets_at"], w["as_of"], w["period"], w["scope"]),
                         (12.0, r, at, "5h", None))
        post_reports(rep_reading(2, at, window="scoped:Fable", used=40.0))
        w = live_window("scoped:Fable")
        self.assertEqual((w["period"], w["scope"]), ("7d", "Fable"))
        poll_lands(anth_polled(at + 1, five_hour=False))
        self.assertIsNone(live_window("five_hour"), "a later poll replaces it by leaving it out")

    def test_an_account_never_measured_is_ignored(self):
        res, _ = post_reports(rep_reading(1, self.t0 + 10, used=77.0, account=ACCT_B))
        self.assertEqual(res[0], ("ignored", "an account the brain has never measured"))
        self.assertIsNone(live_ctx()["standings"][ACCT_B]["as_of"])
        self.assertEqual(ex._reported, {})

    def test_the_polls_own_facts_are_unchanged(self):
        before = copy.deepcopy(ex._state[ACCT_A])
        now = time.time()
        health = ex._unified_account_health(now)

        def gauges():
            # Both renders read the clock at `now`: a data age is now - good_ts, rounded to
            # 0.1 s, so two renders on the live clock differ whenever a tenth turns between them.
            with mock.patch.object(ex.time, "time", return_value=now):
                return sorted(l for l in ex.render().splitlines()
                              if l.startswith(("claude_usage_window_", "provider_quota_",
                                               "provider_account_quota_", "claude_usage_data_age",
                                               "claude_usage_lane_effective")))
        g0 = gauges()
        post_reports(rep_reading(1, self.t0 + 10, used=95.0),
                     rep_reading(2, self.t0 + 10, account="gpt", used=99.0))
        self.assertEqual(ex._state[ACCT_A], before, "_state is never written")
        self.assertEqual(live_window("seven_day")["used_pct"], 95.0)
        self.assertTrue(health["accounts"])
        self.assertEqual(ex._unified_account_health(now), health,
                         "account_health (good_ts, stale, age_s, the backoff) stays the poll's")
        self.assertTrue(g0)
        self.assertEqual(gauges(), g0, "the window gauges and ages stay the poll's")

    def test_credits(self):
        at = time.time() - 1
        res, _ = post_reports({"id": "c-1", "kind": "reading", "account": "gpt", "read_at": at,
                               "pot": "credits", "open": False, "balance": 3.5})
        self.assertEqual(res, [("applied", None)])
        cr = live_ctx()["standings"]["gpt"]["money"]["credits"]
        self.assertEqual((cr["open"], cr["balance"]), (False, 3.5))
        res, _ = post_reports({"id": "c-2", "kind": "reading", "account": "gpt", "read_at": at,
                               "pot": "credits", "open": True})
        self.assertEqual(res[0][0], "refused", "a response never says a pot is open")
        poll_lands(gpt_on_credits(pct=40.0))
        self.assertIs(live_ctx()["standings"]["gpt"]["money"]["credits"]["open"], True,
                      "a later poll replaces it")


def rep_limit(i, at, limit, until=None, cls="spent", ended=None, account=ACCT_A, **extra):
    """A refusal of a limit the brain has no window for (67 §9), as the
    proxy sends it; cls None leaves class out."""
    ev = {"id": f"l-{i}", "kind": "refusal", "account": account, "read_at": at, "window": None,
          "limit": limit, "until": until, "resets_at": None, "by_reading": False, "seen": None}
    if cls is not None:
        ev["class"] = cls
    if ended is not None:
        ev["ended"] = ended
    ev.update(extra)
    return ev


class TestRefusalsGolden(PayBase):
    """records_golden.json's refusal events (64 §5, 67 §9), as the proxy's
    encoder sends them: the applied ones, posted in order, are each
    applied, and the refused ones each refused."""

    def setUp(self):
        reset(acct(ACCT_A, 10.0), gpt_result(), kimi_result())
        ex.compute_preferred()
        with open(RECORDS_GOLDEN) as f:
            self.doc = json.load(f)["refusals"]
        self.now = max(c["event"]["read_at"] for c in self.doc["applied"]) + 1

    def test_applied(self):
        self.assertGreaterEqual(len(self.doc["applied"]), 4)
        for c in self.doc["applied"]:
            with self.subTest(c["name"]):
                ev = served_event(c["event"])
                self.assertLessEqual(set(ev), ex._REPORT_REFUSAL_KEYS)
                [(res, why)] = post_reports(ev, now=self.now)[0]
                self.assertEqual(res, "applied", why)
        evs = [c["event"] for c in self.doc["applied"]]
        self.assertTrue(all("class" in e for e in evs), "class is required (67 push 3c)")
        self.assertTrue(any(e.get("limit") for e in evs) and any(e.get("window") for e in evs),
                        "cases of a window and of a limit")

    def test_refused(self):
        # Each case is refused for its own reason, the golden's `why`, so a
        # case refused for an unrelated one fails.
        for c in self.doc["refused"]:
            with self.subTest(c["name"]):
                [(res, why)] = post_reports(served_event(c["event"]), now=self.now)[0]
                self.assertEqual((res, why), ("refused", c["why"]))
        self.assertEqual((ex._limit_holds, ex._holds), ({}, {}))


class TestReportsLedgerKind(PayBase):
    """The ledger kind: each entry on its own."""

    def setUp(self):
        reset(gpt_on_credits(pct=100.0))

    def ev(self, i, **over):
        return dict(ledger_entry(i, **over), kind="ledger")

    def lines(self):
        try:
            with open(ex.LEDGER_FILE) as f:
                return [json.loads(l) for l in f]
        except FileNotFoundError:
            return []

    def test_one_bad_entry_is_refused_alone(self):
        res, body = post_reports(self.ev(1), self.ev(2, charge=-1.0), self.ev(3))
        self.assertEqual([r for r, _ in res], ["applied", "refused", "applied"])
        self.assertIn("charge must be", res[1][1], "worded as ledger_accept words it")
        self.assertEqual(body["seq"], 2)
        self.assertEqual([e["id"] for e in self.lines()], ["e-1", "e-3"])
        self.assertNotIn("kind", self.lines()[0], "the entry's fields, unchanged")

    def test_validated_as_ledger_validates_it(self):
        far = time.time() + 120  # past the 60 s read_at rule, inside the ledger's own 300 s
        res, _ = post_reports(self.ev(1, at=far), self.ev(2, account="nobody"), self.ev(3, read_at=far))
        self.assertEqual(res[0], ("applied", None), "no read_at rule: the entry's at is its time")
        self.assertEqual(res[1], ("refused", ex._ledger_entry_problem(
            ledger_entry(2, account="nobody"), ex._account_roster(list(ex._state.values())), time.time())))
        self.assertEqual(res[2], ("applied", None), "a read_at, if ever sent, is stripped")

    def test_a_duplicate_is_ignored_and_seq_returned(self):
        self.assertEqual(post_reports(self.ev(1))[1]["seq"], 1)
        res, body = post_reports(self.ev(1), self.ev(1))
        self.assertEqual([r for r, _ in res], ["ignored", "ignored"])
        self.assertEqual(body["seq"], 1)
        res, body = post_reports(self.ev(2), self.ev(2))
        self.assertEqual([r for r, _ in res], ["applied", "ignored"], "within one batch too")
        self.assertEqual(body["seq"], 2)

    def test_an_append_failure_is_retry(self):
        with mock.patch.object(ex, "_ledger_append", side_effect=OSError("disk full")):
            res, body = post_reports(self.ev(1), rep_reading(9, time.time(), account="gpt", used=50.0))
        self.assertEqual(res[0][0], "retry")
        self.assertIn("disk full", res[0][1])
        self.assertEqual(res[1][0], "applied", "a reading beside it is unaffected")
        self.assertEqual(body["seq"], 0)
        self.assertEqual(ex._ledger["ids"], {})
        self.assertEqual(post_reports(self.ev(1))[0], [("applied", None)], "sent again, it lands")


class TestReportsEndpoint(_LogCapture, DocAPIBase, PayBase):
    """POST /reports over HTTP: key-guarded, one result per event."""

    def setUp(self):
        super().setUp()  # opus_acct (ACCT_A), gpt and kimi, polled now
        self.capture_log()

    def tearDown(self):
        DocAPIBase.tearDown(self)
        PayBase.tearDown(self)

    def post(self, body, key=KEY, raw=None):
        code, out, _ = self.req("POST", "/reports", body, key=key, raw=raw)
        return code, out

    def test_key_guard(self):
        body = {"events": [rep_reading(1, time.time(), used=50.0)]}
        self.assertEqual(self.post(body, key=None)[0], 401)
        self.assertEqual(self.post(body, key="nope")[0], 401)
        with mock.patch.object(ex, "SWITCH_KEY", ""):
            self.assertEqual(self.post(body)[0], 503)
        self.assertEqual(ex._reported, {})
        preflight = {"reporter": "deploy-preflight", "events": []}
        self.assertEqual(self.post(preflight, key=None)[0], 401, "the deploy preflight's probe")
        with mock.patch.object(ex, "SWITCH_KEY", ""):
            self.assertEqual(self.post(preflight, key=None)[0], 503)

    def test_ended_carries_the_refusals_fields(self):
        now = time.time()
        ev = rep_refusal(1, now, until=now + 3600)  # after setUp's poll
        self.assertEqual(self.post({"events": [ev]})[1]["results"][0]["result"], "applied")
        self.assertIn((ACCT_A, "seven_day"), ex._holds)
        self.assertEqual(self.post({"events": [dict(ev, id="f-2", read_at=now + 1, ended=True)]})[1]
                         ["results"][0]["result"], "applied")
        self.assertEqual(ex._holds, {})

    def test_body_shape(self):
        for body in ({}, {"events": {}}, {"events": [], "x": 1}, {"reporter": 5, "events": []},
                     {"events": [{}] * 501}, []):
            with self.subTest(body=body):
                self.assertEqual(self.post(body)[0], 400)
        self.assertEqual(self.post(None, raw=b"{nope")[0], 400)

    def test_empty_is_200(self):
        self.assertEqual(self.post({"events": []}), (200, {"results": [], "seq": 0}))
        self.assertEqual(self.post({"reporter": "nas", "events": []}), (200, {"results": [], "seq": 0}))

    def test_results_per_event(self):
        now = time.time()
        code, out = self.post({"reporter": "proxy-1", "events": [
            rep_reading(1, now, used=50.0, account="gpt"),
            dict(rep_reading(2, now), kind="weather"),
            rep_reading(3, now, account="nobody"),
            rep_reading(4, now, window="window:3h", account="gpt", used=1.0),
            rep_reading(41, now, window="scoped:Fable", account="gpt", used=1.0),
            rep_reading(5, now + 61, account="gpt", used=1.0),
            {"id": "s-1", "kind": "served", "account": "gpt", "read_at": now},
            rep_reading(6, now, account="gpt", used="lots"),
            dict(rep_reading(7, now, account="gpt", used=1.0), extra=1),
            "nope",
            rep_reading(8, now - 3600, account="gpt", used=1.0)]})
        self.assertEqual(code, 200)
        self.assertEqual([(r["id"], r["result"]) for r in out["results"]],
                         [("r-1", "applied"), ("r-2", "refused"), ("r-3", "refused"),
                          ("r-4", "refused"), ("r-41", "refused"), ("r-5", "refused"), ("s-1", "refused"),
                          ("r-6", "refused"), ("r-7", "refused"), (None, "refused"),
                          ("r-8", "ignored")])
        why = {r["id"]: r["why"] for r in out["results"]}
        self.assertIsNone(why["r-1"])
        self.assertIn("unknown kind", why["r-2"])
        self.assertIn("unknown account", why["r-3"])
        self.assertIn("unknown window", why["r-4"])
        self.assertIn("ahead of the brain's clock", why["r-5"])
        self.assertIn("missing key(s)", why["s-1"])
        self.assertEqual(out["seq"], 0)
        _, p, _ = self.req("GET", "/preferred")
        w = p["accounts"]["gpt"]["allowance"]["windows"]["seven_day"]
        self.assertEqual((w["used_pct"], w["as_of"]), (50.0, now), "one recompute, then published")
        self.assertTrue(any("reports from proxy-1" in l for l in self.logged), self.logged)

    def test_an_applied_ledger_entry_closes_a_reached_budget_at_once(self):
        g = gpt_on_credits(pct=100.0)
        g["raw"]["rate_limit"]["primary_window"]["reset_at"] = int(time.time()) + 86400
        classes_reset(g, kimi_result(), cfg=pay_cfg(gpt=["gpt/credits"]))
        install_money({"pots": {"gpt/credits": {"budget": 5.0}}})
        ex.compute_preferred()
        _, p, _ = self.req("GET", "/preferred")
        self.assertEqual(p["routes"][GPT_ROUTE]["checks"]["gpt"]["check"], "paid")
        code, out = self.post({"events": [dict(ledger_entry(1, 5.0), kind="ledger")]})
        self.assertEqual((code, out["results"][0]["result"], out["seq"]), (200, "applied", 1))
        _, p, _ = self.req("GET", "/preferred")
        c = p["routes"][GPT_ROUTE]["checks"]["gpt"]
        self.assertEqual((c["check"], c["reason"]), ("no", "budget"))
        self.assertEqual(p["ledger"]["pots"]["gpt/credits"]["period_spend"], 5.0)
        self.assertEqual(p["ledger"]["seq"], 1)

    def test_post_ledger_is_not_found(self):
        code, _, _ = self.req("POST", "/ledger", {"entries": [ledger_entry(1)]})
        self.assertEqual(code, 404)
        self.assertFalse(os.path.exists(ex.LEDGER_FILE))

    def test_read_at_inside_the_minute_ahead_is_taken(self):
        code, out = self.post({"events": [rep_reading(1, time.time() + 50, used=50.0,
                                                      account="gpt")]})
        self.assertEqual(out["results"][0]["result"], "applied")


# ---- served (quota-model 65 §7) ---------------------------------------------------

SERVED_GOLDEN = proxy_testdata("served_golden.json")


# The golden's accounts, by provider, as this harness names them.
SERVED_ACCOUNTS = {"anthropic-1": ACCT_A, "codex-1": "gpt", "kimi-1": "kimi"}


def served_golden():
    with open(SERVED_GOLDEN) as f:
        return json.load(f)


def served_event(ev):
    """A golden event with its account mapped to the harness's."""
    ev = copy.deepcopy(ev)
    ev["account"] = SERVED_ACCOUNTS.get(ev["account"], ev["account"])
    return ev


def served(i, model="claude-opus-5-5", speed="standard", boot="b1", seq=None, account=ACCT_A,
           complete=True, why=None, at=None, **tokens):
    """A served event; tokens default to an Anthropic split with nothing cached,
    its head at its read_at with no readings."""
    toks = {"input": 1, "cache_write_5m": 0, "cache_write_1h": 0, "cache_write_30m": None,
            "cache_write_12h": None, "cache_write": None, "cache_read": 0, "output": 1}
    toks.update(tokens)
    read_at = time.time() if at is None else at
    return {"id": f"s-{i}", "kind": "served", "account": account,
            "read_at": read_at, "head_at": read_at, "readings": {}, "boot": boot,
            "seq": i if seq is None else seq, "consumer": "laptop", "selection": model,
            "member": None, "model": model, "speed": speed, "paid": False, "tokens": toks,
            "complete": complete, "why": why, "output_max": None}


class TestServedGolden(PayBase):
    """The brain takes, prices and counts served_golden.json's cases, as the
    proxy's encoder emits them (65 §7)."""

    def setUp(self):
        reset(acct(ACCT_A, 10.0), gpt_result(), kimi_result())
        ex.compute_preferred()
        self.doc = served_golden()
        # The brain's clock: just after the golden's last read_at.
        self.now = max(c["event"]["read_at"] for c in self.doc["cases"]) + 1

    def post(self, *events, now=None):
        return post_reports(*events, now=self.now if now is None else now)[0]

    def test_cases(self):
        self.assertGreaterEqual(len(self.doc["cases"]), 14)
        missing, usd, unpriced = 0, {}, {}
        for c in self.doc["cases"]:
            with self.subTest(c["name"]):
                ev = served_event(c["event"])
                # Push 4 (66 §11): a case may carry output_max, until push 6 requires it.
                self.assertEqual(set(ev), ex._SERVED_KEYS,
                                 "the encoder's keys are the brain's")
                [(res, why)] = self.post(ev)
                self.assertEqual(res, c["result"], why)
                if res == "applied":
                    got, reason = ex._served_price(ev["model"], ev["speed"], ev["tokens"],
                                                   ev["complete"])
                    self.assertEqual(None if got is None else round(got, 6), c["api_usd"])
                    if c["api_usd"] is None:
                        self.assertTrue(reason.startswith(c["unpriced"]), reason)
                        k = (ev["account"], ev["model"], reason)
                        unpriced[k] = unpriced.get(k, 0) + 1
                    else:
                        self.assertIsNone(reason)
                        usd[ev["account"]] = usd.get(ev["account"], 0.0) + got
                missing = c.get("seq_missing_after", missing)
                self.assertEqual(ex._served["seq_missing"], missing)
        self.assertEqual({a: round(v, 6) for a, v in ex._served["usd"].items()},
                         {a: round(v, 6) for a, v in usd.items()})
        self.assertEqual(ex._served["unpriced"], unpriced)
        self.assertEqual(ex._served["events"], {ACCT_A: 14, "gpt": 7, "kimi": 2})
        self.assertEqual(ex._served["incomplete"], {ACCT_A: 3})
        self.assertEqual(ex._served["duplicates"], 1)

    def test_refused(self):
        base = next(c["event"] for c in self.doc["cases"]
                    if c["event"]["id"] == self.doc["refused_base"])
        self.assertGreaterEqual(len(self.doc["refused"]), 11)
        for i, r in enumerate(self.doc["refused"]):
            with self.subTest(r["name"]):
                ev = served_event(dict(base, id=f"refused-{i}"))
                ev.update({k: copy.deepcopy(v) for k, v in r.get("patch", {}).items()})
                for k in r.get("drop", ()):
                    del ev[k]
                if "read_at_ahead_s" in r:
                    ev["read_at"] = self.now + r["read_at_ahead_s"]
                [(res, why)] = self.post(ev)
                self.assertEqual(res, "refused")
                self.assertTrue(why)
        self.assertEqual(ex._served["events"], {}, "a refused event counts nothing")
        self.assertEqual(len(ex._served["ids"]), 0)
        self.assertEqual(self.post(served_event(base)), [("applied", None)],
                         "the base event itself is taken")


class TestServed(PayBase):
    """Pricing rules and memory bounds beyond the golden (65 §2, §7)."""

    def setUp(self):
        reset(acct(ACCT_A, 10.0), gpt_result(), kimi_result())
        ex.compute_preferred()

    @staticmethod
    def price(model, speed="standard", complete=True, **tokens):
        toks = dict.fromkeys(ex.SERVED_TOKEN_KINDS)
        toks.update(tokens)
        usd, why = ex._served_price(model, speed, toks, complete)
        return (round(usd, 6) if usd is not None else None), why

    def test_speeds(self):
        self.assertEqual(self.price("claude-opus-5-5", None, input=1000), (None, "speed unknown"))
        self.assertEqual(self.price("claude-sonnet-5-5", None, input=1000), (0.002, None),
                         "a model with no speed variants is priced as standard")
        self.assertEqual(self.price("claude-opus-5-5", "ultrafast", input=1),
                         (None, "no ultrafast price"))
        self.assertEqual(self.price("claude-sonnet-5", "fast", input=1), (None, "no fast price"))
        self.assertEqual(self.price("gpt-6.1-sol", "ultrafast", input=100000, output=1000000),
                         (61.2, None))

    def test_opus_fast_scales_its_cache_prices(self):
        """Anthropic: the caching multipliers apply on top of fast mode."""
        self.assertEqual(self.price("claude-opus-5-5", "fast", cache_write_5m=1000000,
                                    cache_write_1h=1000000, cache_read=1000000),
                         (10.0 + 16.0 + 0.4, None))

    def test_cache_write_kinds(self):
        self.assertEqual(self.price("gpt-6-astra", cache_write_30m=100000, cache_write_12h=100000),
                         (1.25 + 2.5, None), "OpenAI's split, each lifetime at its price")
        self.assertEqual(self.price("gpt-6.1-sol", "ultrafast", cache_write_30m=100000,
                                    cache_write_12h=100000), (1.5 + 3.0, None))
        self.assertEqual(self.price("gpt-6-astra", cache_write=10),
                         (None, "cache write split unknown"), "an OpenAI total, as an Anthropic one")
        self.assertEqual(self.price("gpt-6-astra", cache_write_5m=10), (None, "no cache_write_5m price"))
        self.assertEqual(self.price("claude-haiku-4-5", cache_write=10),
                         (None, "cache write split unknown"))
        self.assertEqual(self.price("claude-haiku-4-5", cache_write=0, input=1000), (0.001, None),
                         "a zero total is no write")

    def test_long_context_threshold(self):
        """Over 272K prompt tokens (input, cache reads and writes), every token
        pays the long-context prices; at 272K, the short ones."""
        self.assertEqual(self.price("gpt-6.1-sol", input=72000, cache_read=100000, cache_write_30m=100000,
                                    output=1000000),
                         (round((72000 * 2 + 100000 * 0.1 + 100000 * 2.5) / 1e6 + 10, 6), None))
        self.assertEqual(self.price("gpt-6.1-sol", input=72001, cache_read=100000, cache_write_30m=100000,
                                    output=1000000),
                         (round((72001 * 4 + 100000 * 0.2 + 100000 * 5) / 1e6 + 15, 6), None))
        self.assertEqual(self.price("gpt-6-astra", "fast", input=300000, output=1000000),
                         (12.0 + 150.0, None))
        # The lifetimes' writes count in the prompt: 272,001 with them, priced long.
        self.assertEqual(self.price("gpt-6.1-sol", input=72001, cache_write_30m=100000,
                                    cache_write_12h=100000),
                         (round((72001 * 4 + 100000 * 5 + 100000 * 10) / 1e6, 6), None))
        self.assertEqual(self.price("claude-opus-5-5", input=900000), (3.6, None),
                         "Anthropic: no long-context price")
        # Claude Haiku 5.5 over 100K, cache reads and writes counted (the
        # golden's srv-b2-4 and srv-b2-5 price both sides of it).
        self.assertEqual(self.price("claude-haiku-5-5", input=2, cache_read=100000, output=1000000),
                         (2.505001, None))
        self.assertEqual(self.price("claude-haiku-5-5", cache_read=100000, output=1000000),
                         (0.501, None))

    def test_the_tables_price_cache_writes_at_their_lifetimes_multipliers(self):
        """Each row prices every kind of its provider, and each cache write at
        its lifetime's multiple of the row's input price: Anthropic's 5m 1.25x
        and 1h 2x, OpenAI's 30m 1.25x and 12h 2.5x (10, 11)."""
        rows = [(m, sp, row) for m, speeds in ex.API_LIST_PRICES.items() for sp, row in speeds.items()]
        rows += [(m, sp + " long", row) for m, (_, speeds) in ex.API_LONG_CONTEXT.items()
                 for sp, row in speeds.items()]
        for m, sp, row in rows:
            with self.subTest(model=m, speed=sp):
                writes = ({"cache_write_5m": 1.25, "cache_write_1h": 2.0} if m.startswith("claude-")
                          else {"cache_write_30m": 1.25, "cache_write_12h": 2.5})
                self.assertEqual(set(row), {"input", "cache_read", "output"} | set(writes))
                for kind, mult in writes.items():
                    self.assertAlmostEqual(row[kind], row["input"] * mult, places=9)
        for m, (_, speeds) in ex.API_LONG_CONTEXT.items():
            self.assertEqual(set(speeds), set(ex.API_LIST_PRICES[m]), m)

    def test_every_model_named_has_a_price(self):
        for m in ("claude-fable-5-1", "claude-mythos-5-1", "claude-fable-5", "claude-mythos-5",
                  "claude-opus-5-5", "claude-opus-5", "claude-opus-4-8", "claude-opus-4-7",
                  "claude-opus-4-6", "claude-opus-4-5", "claude-opus-4-5-20251101",
                  "claude-sonnet-5-5", "claude-sonnet-5", "claude-sonnet-4-6", "claude-sonnet-4-5",
                  "claude-sonnet-4-5-20250929", "claude-haiku-5-5", "claude-haiku-4-5",
                  "claude-haiku-4-5-20251001", "gpt-6-astra", "gpt-6.1-sol"):
            self.assertIsNotNone(self.price(m, input=1)[0], m)
        for m in ("kimi-k3", "gpt-image-2"):
            self.assertEqual(self.price(m, input=1), (None, "no list price"))

    def test_fields(self):
        cases = {
            "seq": "seq must be", "boot": "boot must be", "tokens": "tokens must carry",
            "complete": "complete must be", "paid": "paid must be", "member": "member must be",
        }
        bad = {"seq": True, "boot": "", "tokens": {"input": 1}, "complete": None, "paid": "no",
               "member": 5}
        for i, (k, want) in enumerate(cases.items()):
            with self.subTest(k):
                [(res, why)], _ = post_reports(dict(served(i + 1), **{k: bad[k]}))
                self.assertEqual(res, "refused")
                self.assertIn(want, why)
        [(res, why)], _ = post_reports(served(10, input=1.5))
        self.assertEqual((res, "integer" in why), ("refused", True))
        [(res, why)], _ = post_reports(served(11, complete=False, why=""))
        self.assertEqual(res, "refused")

    def test_a_boot_first_heard_counts_no_gap(self):
        """The brain can't tell what a boot sent before it started from what was lost."""
        self.assertEqual(post_reports(served(1, boot="b9", seq=40))[0], [("applied", None)])
        self.assertEqual(ex._served["seq_missing"], 0)
        post_reports(served(2, boot="b9", seq=43))
        self.assertEqual(ex._served["seq_missing"], 2)
        post_reports(served(3, boot="b9", seq=41))
        self.assertEqual(ex._served["seq_missing"], 2, "a late seq takes nothing back, adds nothing")
        post_reports(served(4, boot="b9", seq=44))
        self.assertEqual(ex._served["seq_missing"], 2)

    def test_memory_is_bounded(self):
        with mock.patch.object(ex, "SERVED_IDS_MAX", 3), mock.patch.object(ex, "SERVED_BOOTS_MAX", 2):
            for i in range(1, 5):
                post_reports(served(i, boot=f"b{i}", seq=1))
            self.assertEqual(list(ex._served["ids"]), ["s-2", "s-3", "s-4"])
            self.assertEqual(list(ex._served["boots"]), ["b3", "b4"])
            self.assertEqual(post_reports(served(4, boot="b4", seq=1))[0][0][0], "ignored")
            self.assertEqual(post_reports(served(1, boot="b1", seq=1))[0][0][0], "applied",
                             "an id past the bound is taken again")
            post_reports(served(5, boot="b1", seq=9))
            self.assertEqual(ex._served["seq_missing"], 7, "b1 heard again: its seq from there")

    def test_exposition(self):
        post_reports(served(1, input=1000, output=1000),                       # 0.024
                     served(2, model="kimi-k3", account="kimi", cache_write_5m=None,
                            cache_write_1h=None),
                     served(3, complete=False, why="stream ended", output=None),
                     served(5, model="claude-opus-5-5", speed=None),
                     served(5, seq=6),                                           # resent id
                     served(6, model="gpt-6-astra", account="gpt", cache_write_5m=None,
                            cache_write_1h=None, input=100000, output=0))  # 1.0
        t = ex.render()
        L = TestRenderMetrics().line
        for fam in ("served_events_total", "served_api_dollars_total", "served_unpriced_total",
                    "served_incomplete_total", "served_seq_missing_total", "served_duplicates_total"):
            self.assertIn(f"# TYPE {fam} counter", t)
        self.assertEqual(L(t, "served_events_total", account=ACCT_A), 3)
        self.assertEqual(L(t, "served_events_total", account="kimi"), 1)
        self.assertEqual(L(t, "served_events_total", account="gpt"), 1)
        self.assertEqual(L(t, "served_api_dollars_total", account=ACCT_A), 0.024)
        self.assertEqual(L(t, "served_api_dollars_total", account="gpt"), 1.0)
        self.assertIsNone(L(t, "served_api_dollars_total", account="kimi"))
        self.assertEqual(L(t, "served_unpriced_total", account="kimi", model="kimi-k3",
                           reason="no list price"), 1, "an unpriced request names its model")
        self.assertEqual(L(t, "served_unpriced_total", account=ACCT_A, reason="incomplete"), 1)
        self.assertEqual(L(t, "served_unpriced_total", account=ACCT_A, reason="speed unknown"), 1)
        self.assertEqual(L(t, "served_incomplete_total", account=ACCT_A), 1)
        self.assertIn("\nserved_seq_missing_total 1\n", t, "seq 4 never came")
        self.assertIn("\nserved_duplicates_total 1\n", t)


class TestServedEndpoint(DocAPIBase, PayBase):
    """A post of only served events changes no decision, so it recomputes and
    publishes nothing (64 §2)."""

    def tearDown(self):
        DocAPIBase.tearDown(self)
        PayBase.tearDown(self)

    def test_a_served_only_post_publishes_nothing(self):
        ex.compute_preferred()
        ex._events_publish()  # the document for this state is the published one
        with mock.patch.object(ex, "compute_preferred") as recompute:
            code, out, _ = self.req("POST", "/reports", {"reporter": "nas", "events": [
                served(1, input=1000), served(2, model="kimi-k3", account="kimi")]})
            self.assertEqual(code, 200)
            self.assertEqual([r["result"] for r in out["results"]], ["applied", "applied"])
            recompute.assert_not_called()
        self.assertFalse(ex._events_publish(), "the same document: nothing published")
        self.assertEqual(ex._served["events"], {ACCT_A: 1, "kimi": 1})

    def test_decisive_counts_only_what_a_decision_reads(self):
        now = time.time()
        _, decisive = ex.reports_post([served(1)], "t", now)
        self.assertEqual(decisive, 0)
        _, decisive = ex.reports_post([served(2), rep_reading(1, now, used=50.0, account="gpt")],
                                      "t", now)
        self.assertEqual(decisive, 1)


# ---- sizing (quota-model 66) ------------------------------------------------------

SIZING_FIXTURE = os.path.join(BRAIN, "testdata",
                              "sizing_steps.json")


RESET_7D = 1794326400.0


class TestServedStep2Keys(PayBase):
    """66 §1: head_at and readings, required since 66 §11's push 3 and
    validated."""

    def setUp(self):
        reset(acct(ACCT_A, 10.0), gpt_result(), kimi_result())
        ex.compute_preferred()
        self.doc = served_golden()
        self.now = max(c["event"]["read_at"] for c in self.doc["cases"]) + 1

    def post(self, *events):
        return post_reports(*events, now=self.now)[0]

    def keyed(self, ev, readings=None, head_back=3.0):
        ev = served_event(ev)
        ev["head_at"] = ev["read_at"] - head_back
        ev["readings"] = {} if readings is None else readings
        return ev

    def test_the_new_golden_cases_applied(self):
        base = next(c["event"] for c in self.doc["cases"] if c["event"]["id"] == "srv-01")
        fable = next(c["event"] for c in self.doc["cases"] if c["event"]["model"] == "claude-fable-5-1")
        codex = next(c["event"] for c in self.doc["cases"] if c["event"]["account"] == "codex-1")
        kimi = next(c["event"] for c in self.doc["cases"] if c["event"]["account"] == "kimi-1")
        cases = {
            "Opus: its 7d reading": self.keyed(
                base, {"seven_day": {"used_pct": 15.0, "resets_at": 1794326400}}),
            "Fable: the 7d and the Fable cap": self.keyed(
                fable, {"seven_day": {"used_pct": 15.0, "resets_at": 1794326400},
                        "scoped:Fable": {"used_pct": 8.0, "resets_at": 1794326400}}),
            "codex: a whole-point reading": self.keyed(
                codex, {"seven_day": {"used_pct": 100, "resets_at": 1794326400}}),
            "Kimi: no rate-limit headers": self.keyed(kimi, {}),
            "incomplete, with its head": self.keyed(
                dict(base, id="srv-inc", complete=False, why="stream ended",
                     tokens=dict(base["tokens"], output=None)),
                {"seven_day": {"used_pct": 15.0, "resets_at": None}}),
            "a reading with only its reset": self.keyed(
                dict(base, id="srv-reset"), {"seven_day": {"resets_at": 1794326400}}),
        }
        for name, ev in cases.items():
            with self.subTest(name):
                self.assertEqual(self.post(ev), [("applied", None)])
        self.assertEqual(ex._served["incomplete"], {ACCT_A: 1})

    def test_the_new_golden_cases_refused(self):
        base = next(c["event"] for c in self.doc["cases"] if c["event"]["id"] == "srv-01")
        ok = {"seven_day": {"used_pct": 15.0, "resets_at": 1794326400}}
        cases = {
            "head_at after read_at": dict(self.keyed(base, ok), head_at=base["read_at"] + 0.5),
            "head_at not a number": dict(self.keyed(base, ok), head_at="20:05:51.630"),
            "head_at null": dict(self.keyed(base, ok), head_at=None),
            "head_at in milliseconds": dict(self.keyed(base, ok), head_at=1793739997500.0),
            "readings not an object": dict(self.keyed(base, ok), readings=[ok]),
            "a reading field the brain doesn't know": self.keyed(
                base, {"seven_day": {"status": "allowed"}}),
            "used_pct negative": self.keyed(base, {"seven_day": {"used_pct": -1, "resets_at": None}}),
            "used_pct a string": self.keyed(base, {"seven_day": {"used_pct": "15", "resets_at": None}}),
            "resets_at not epoch seconds": self.keyed(
                base, {"seven_day": {"used_pct": 15.0, "resets_at": "2026-11-10T00:00:00Z"}}),
            "resets_at in milliseconds": self.keyed(
                base, {"seven_day": {"used_pct": 15.0, "resets_at": 1794326400000}}),
            "a window with neither field": self.keyed(base, {"seven_day": {}}),
            "not a window id": self.keyed(base, {"7d": {"used_pct": 15.0, "resets_at": None}}),
        }
        for i, (name, ev) in enumerate(cases.items()):
            with self.subTest(name):
                ev = dict(ev, id=f"bad-{i}")
                [(res, why)] = self.post(ev)
                self.assertEqual(res, "refused")
                self.assertTrue(why)
        self.assertEqual(ex._served["events"], {}, "a refused event counts nothing")
        self.assertEqual(ex._sizing["accounts"], {}, "nor reaches sizing")

    def test_an_event_without_the_keys_is_refused(self):
        """Neither key, or only one: refused as any missing key is (66 §11 push 3)."""
        base = next(c["event"] for c in self.doc["cases"] if c["event"]["id"] == "srv-01")
        ok = {"seven_day": {"used_pct": 15.0, "resets_at": 1794326400}}
        cases = {"neither": ("head_at", "readings"), "head_at only": ("readings",),
                 "readings only": ("head_at",)}
        for i, (name, gone) in enumerate(cases.items()):
            with self.subTest(name):
                ev = {k: v for k, v in self.keyed(base, ok).items() if k not in gone}
                [(res, why)] = self.post(dict(ev, id=f"bare-{i}"))
                self.assertEqual(res, "refused")
                self.assertIn("missing key", why)
                for k in gone:
                    self.assertIn(k, why)
        self.assertEqual(ex._served["events"], {}, "a refused event counts nothing")
        self.assertEqual(ex._sizing["accounts"], {}, "nor reaches sizing")

    def test_an_unknown_key_is_still_refused(self):
        base = next(c["event"] for c in self.doc["cases"] if c["event"]["id"] == "srv-01")
        [(res, why)] = self.post(dict(self.keyed(base), head_time=1.0))
        self.assertEqual(res, "refused")
        self.assertIn("unknown key", why)


class TestServedOutputMax(PayBase):
    """66 §1: output_max, a hung-up request's bound, required since 66 §11's
    push 6 and validated."""

    WHY_LEFT = "the client left before the response ended"

    def setUp(self):
        reset(acct(ACCT_A, 10.0), gpt_result(), kimi_result())
        ex.compute_preferred()
        self.addCleanup(reset_sizing)
        self.doc = served_golden()
        self.now = max(c["event"]["read_at"] for c in self.doc["cases"]) + 1
        self.base = next(c["event"] for c in self.doc["cases"] if c["event"]["id"] == "srv-01")

    def post(self, *events):
        return post_reports(*events, now=self.now)[0]

    def hung(self, i, **kw):
        """The golden's Opus event, hung up after its start: no output count."""
        ev = served_event(dict(self.base, id=f"srv-hung-{i}", seq=100 + i, complete=False,
                               why=self.WHY_LEFT, tokens=dict(self.base["tokens"], output=None)))
        ev.update(kw)
        return ev

    def test_applied(self):
        cases = {
            "incomplete, bounded": self.hung(1, output_max=32000),
            "incomplete, a bound of 0": self.hung(2, output_max=0),
            "incomplete, null": self.hung(3, output_max=None),
            "complete, null": served_event(dict(self.base, output_max=None)),
        }
        for name, ev in cases.items():
            with self.subTest(name):
                self.assertEqual(self.post(ev), [("applied", None)])
        self.assertEqual(ex._served["incomplete"], {ACCT_A: 3})

    def test_refused(self):
        cases = {
            "output_max on a complete event": served_event(dict(self.base, output_max=4096)),
            "output_max without an input count": self.hung(
                1, output_max=4096, tokens=dict(self.base["tokens"], output=None, input=None)),
            "output_max negative": self.hung(2, output_max=-1),
            "output_max with an output count": self.hung(
                3, output_max=4096, tokens=dict(self.base["tokens"])),
            "output_max not an integer": self.hung(4, output_max=4096.5),
            "output_max a string": self.hung(5, output_max="4096"),
            "output_max a boolean": self.hung(6, output_max=True),
        }
        for name, ev in cases.items():
            with self.subTest(name):
                [(res, why)] = self.post(ev)
                self.assertEqual(res, "refused")
                self.assertTrue(why.startswith("output_max"), why)
        self.assertEqual(ex._served["events"], {}, "a refused event counts nothing")

    def test_the_key_is_required(self):
        """Since 66 §11's push 6 an event without output_max is refused like
        any missing key."""
        ev = self.hung(1)
        del ev["output_max"]
        self.assertEqual(self.post(ev), [("refused", "missing key(s) ['output_max']")])

    def test_a_bounded_event_stays_unpriced(self):
        """The bound is sizing's only (66 §4): the event counts as unpriced,
        reason incomplete, and adds no API dollars."""
        self.assertEqual(self.post(self.hung(1, output_max=32000)), [("applied", None)])
        self.assertEqual(ex._served["usd"], {})
        self.assertEqual(ex._served["unpriced"], {(ACCT_A, "claude-opus-5-5", "incomplete"): 1})

    def test_the_bound(self):
        """The middle: the known tokens and half the bound as output, at the
        model's row; the span: the whole bound at its output price."""
        toks = dict.fromkeys(ex.SERVED_TOKEN_KINDS)
        toks.update(input=1000, cache_write_5m=0, cache_write_1h=2000, cache_read=10000)
        usd, span, speed, tier, mid = ex._served_bound("claude-opus-5-5", "standard", toks, 3001)
        self.assertEqual((speed, tier, mid["output"]), ("standard", "standard", 1500.5))
        self.assertAlmostEqual(usd, (1000 * 4.0 + 2000 * 8.0 + 10000 * 0.2 + 1500.5 * 20.0) / 1e6)
        self.assertAlmostEqual(span, 3001 * 20.0 / 1e6)
        self.assertIsNone(ex._served_bound("claude-unknown-9", "standard", toks, 3001),
                          "a model the table can't price stays unbounded")
        self.assertIsNone(ex._served_bound("claude-opus-5-5", None, toks, 3001))


class SizingBase(PayBase):
    """Served events with their heads, posted as the proxy posts them."""

    T0 = 1793740000.0

    def setUp(self):
        reset(acct(ACCT_A, 10.0), gpt_result(), kimi_result())
        ex.compute_preferred()
        self.n = 0
        self.seqs = {}
        self.logged = []
        orig = ex.log
        self.addCleanup(setattr, ex, "log", orig)
        ex.log = self.logged.append
        self.addCleanup(reset_sizing)

    def ev(self, head, end, used=None, usd=0.0, window="seven_day", model="claude-opus-5-5",
           account=ACCT_A, reset=RESET_7D, readings=None, boot="b1", seq=None, **kw):
        """A served event ending at `end` whose head at `head` read `used` on
        `window`, costing `usd` at the model's input price."""
        self.n += 1
        if seq is None:
            seq = self.seqs[boot] = self.seqs.get(boot, 0) + 1
        price = ex.API_LIST_PRICES.get(model, {}).get("standard", {}).get("input", 4.0)
        toks = {"input": int(round(usd * 1e6 / price)), "output": 0}
        if model.startswith("gpt"):
            toks.update(cache_write_5m=None, cache_write_1h=None)
        toks.update(kw.pop("tokens", {}))
        e = served(self.n, model=model, account=account, boot=boot, seq=seq, at=end, **toks)
        e.update(kw)
        e["head_at"] = head
        if readings is None:
            readings = {} if used is None else {window: {"used_pct": used, "resets_at": reset}}
        e["readings"] = readings
        return e

    def feed(self, *events, now=None, reporter="test-host"):
        out = post_reports(*events, now=(max(e["read_at"] for e in events) + 1) if now is None
                           else now, reporter=reporter)[0]
        self.assertEqual([r for r, _ in out], ["applied"] * len(events), out)
        return out

    def win(self, window="seven_day", account=ACCT_A):
        return ex._sizing["windows"][(account, window)]

    def samples(self, window="seven_day", account=ACCT_A):
        return [(s["p"], s["q"], round(s["usd"], 6)) for s in self.win(window, account)["samples"]]

    def counts(self, window="seven_day", account=ACCT_A):
        return self.win(window, account)["counts"]

    def walk(self, steps, q0=10.0, t0=None, gap=300.0, window="seven_day", model="claude-opus-5-5",
             account=ACCT_A, reset=RESET_7D, close=True):
        """Anchor at q0, a first transition (no step: nothing said when the
        meter reached q0), then one step per (points, [usd of each event]
        [, model]) in `steps`, gap seconds apart, an event's usd a (usd,
        model) of its own model where the step mixes; with close, an event
        past the last boundary's settle. Each boundary's head ends a second
        after it, in the next step; before a step with no events it is a
        long request that ends after that step, so the step is silent. ->
        (events in end order, their seq too, and the boundary times)."""
        t = self.T0 if t0 is None else t0
        mk = functools.partial(self.ev, window=window, account=account, reset=reset)
        evs = [mk(t, t + 1, used=q0, model=model)]
        q, T = q0 + 1, t + 100
        evs.append(mk(T, T + 1, used=q, model=model))
        bounds = [T]
        for step in steps:
            points, usds, m = (tuple(step) + (model,))[:3]
            if not usds:
                evs[-1]["read_at"] = T + gap + 2
            for k, u in enumerate(usds):
                u, um = u if isinstance(u, tuple) else (u, m)
                end = T + 10 + k * (gap - 20) / max(len(usds), 1)
                evs.append(mk(end - 2, end, used=q, usd=u, model=um))
            T, q = T + gap, q + points
            evs.append(mk(T, T + 1, used=q, model=model))
            bounds.append(T)
        if close:
            evs.append(mk(T + ex.SIZING_SETTLE_S + 1, T + ex.SIZING_SETTLE_S + 2, used=q,
                          model=model))
        return self.in_end_order(evs), bounds

    @staticmethod
    def in_end_order(evs):
        """The events by read_at, each boot's seq following it, as a proxy
        numbers what it sends."""
        evs = sorted(evs, key=lambda e: e["read_at"])
        firsts = {}
        for e in evs:
            firsts.setdefault(e["boot"], []).append(e["seq"])
        for b in firsts:
            firsts[b].sort()
        for e in evs:
            e["seq"] = firsts[e["boot"]].pop(0)
        return evs


def _fixture_events(doc, base):
    def ep(hms):
        return datetime.fromisoformat(f"{doc['date']}T{hms}+00:00").timestamp()
    out = []
    for e in doc["events"]:
        toks = dict.fromkeys(ex.SERVED_TOKEN_KINDS)
        toks.update(e["tokens"])
        out.append({"id": f"fx-{e['seq']}", "kind": "served", "account": ACCT_A,
                    "read_at": ep(e["end"]), "head_at": ep(e["head"]),
                    "readings": {"seven_day": {"used_pct": float(e["seven_day"]),
                                               "resets_at": doc["resets_at"]}},
                    "boot": "fx", "seq": e["seq"], "consumer": "laptop",
                    "selection": doc["model"], "member": "lane:opus", "model": doc["model"],
                    "speed": "standard", "paid": False, "tokens": toks, "complete": True,
                    "why": None, "output_max": None})
    return out, ep


class TestSizingFixture(SizingBase):
    """testdata/sizing_steps.json: the recorded transitions (66 §2, §12)."""

    def setUp(self):
        super().setUp()
        with open(SIZING_FIXTURE) as f:
            self.doc = json.load(f)
        self.events, self.ep = _fixture_events(self.doc, self.T0)

    def run_in(self, events, check=None):
        reset_sizing()
        with ex._served_lock:
            ex._served = ex._served_fresh()
        for i, ev in enumerate(events, 1):
            # The proxy numbers what it sends in the order it sends it.
            self.feed(dict(ev, seq=i), now=ev["read_at"] + 1)
            if check:
                check(ev)
        w = self.win()
        return ({b["q"]: b["t"] for b in w["pending"]}, dict(w["anchor"]), self.samples(),
                dict(w["counts"]))

    def expected(self):
        exp = self.doc["expect"]
        return ([(s["p"], s["q"], s["usd"]) for s in exp["samples"]],
                {"q": exp["anchor"]["q"], "t": self.ep(exp["anchor"]["t"])}, exp["steps_total"])

    def test_end_order_and_head_order_agree(self):
        samples, anchor, counts = self.expected()
        by_end = sorted(self.events, key=lambda e: e["read_at"])
        by_head = sorted(self.events, key=lambda e: e["head_at"])
        self.assertNotEqual([e["id"] for e in by_end], [e["id"] for e in by_head])
        for order in (by_end, by_head):
            pending, a, got, c = self.run_in(order)
            self.assertEqual(got, samples)
            self.assertEqual((a["q"], round(a["t"], 3)), (anchor["q"], round(anchor["t"], 3)))
            self.assertEqual(pending, {})
            self.assertEqual(c, counts)
        w = self.win()
        size, basis, spread, _ = ex._sizing_estimate(w["samples"])
        self.assertEqual(basis, "learned", spread)

    def test_the_late_head_moves_the_boundary_earlier(self):
        """The 20:05 case: the first 0.10 in end order has head 20:05:54.851;
        an event ending 7 s later has head 20:05:51.630, and T_10 moves."""
        after = {self.ep(k): self.ep(v)
                 for k, v in self.doc["expect"]["boundary_10_after"].items()}
        seen = {}

        def check(ev):
            if ev["read_at"] in after:
                w = self.win()
                b = [x["t"] for x in w["pending"] if x["q"] == 10.0]
                seen[ev["read_at"]] = b[0] if b else (w["anchor"]["t"] if w["anchor"]["q"] == 10 else None)
        self.run_in(sorted(self.events, key=lambda e: e["read_at"]), check)
        self.assertEqual({k: round(v, 3) for k, v in seen.items()},
                         {k: round(v, 3) for k, v in after.items()})

    def test_a_step_closes_only_at_its_settle_in_data_time(self):
        cut = self.ep(next(iter(self.doc["expect"]["samples_before"])))
        n_before = next(iter(self.doc["expect"]["samples_before"].values()))
        early = [e for e in self.events if e["read_at"] < cut]
        late = [e for e in self.events if e["read_at"] >= cut]
        # One replayed batch, as after the brain was unreachable, and a clock
        # far past every event: the data's time decides, not the brain's.
        self.feed(*early, now=late[-1]["read_at"] + 1)
        self.assertEqual(len(self.samples()), n_before)
        self.assertEqual([b["q"] for b in self.win()["pending"]], [13.0], "T_13 waits")
        self.feed(*late)
        self.assertEqual(self.samples(), self.expected()[0])

    def test_the_log_line(self):
        self.run_in(sorted(self.events, key=lambda e: e["read_at"]))
        lines = [l for l in self.logged if l.startswith("sizing: ")]
        self.assertIn(f"sizing: {ACCT_A} seven_day 12→13 at 2026-11-03T20:50:20.788Z $1.6144 "
                      "over 14 events: sample", lines)


class TestSizingBoundaries(SizingBase):
    """Heads, boundaries and steps (66 §2)."""

    def test_heads_that_change_nothing(self):
        evs, (T11,) = self.walk([])
        self.feed(*evs)
        w = self.win()
        self.assertEqual((w["anchor"]["q"], w["anchor"]["t"], w["pending"]), (11.0, T11, []))
        before = copy.deepcopy((w["anchor"], w["pending"]))
        t = T11 + 700
        self.feed(self.ev(t, t + 1, used=10.0),                # a point below the anchor
                  self.ev(T11 - 10, t + 2, used=12.0),         # above it, its head before it
                  self.ev(t + 3, t + 4, used=11.0))            # the anchor's own reading
        self.assertEqual((w["anchor"], w["pending"]), before)

    def test_the_first_transition_is_no_step(self):
        evs, (T11,) = self.walk([], close=True)
        self.feed(*evs)
        w = self.win()
        self.assertEqual((w["anchor"]["q"], w["anchor"]["t"], w["anchor"]["fresh"]), (11.0, T11, False))
        self.assertEqual(w["counts"], {}, "nothing said when the meter reached 10")
        self.assertEqual(w["samples"], [])

    def test_a_late_event_recuts_the_step(self):
        """T_12 moves earlier, and the work ending between the two times goes
        to the next step."""
        evs, (T11,) = self.walk([], close=False)
        self.feed(*evs)
        t = T11 + 300
        self.feed(self.ev(T11 + 50, T11 + 60, used=11.0, usd=1.0),
                  self.ev(t, t + 2, used=12.0),
                  self.ev(T11 + 230, T11 + 280, used=11.0, usd=1.0))   # ends before t
        self.assertEqual([b["t"] for b in self.win()["pending"]], [T11, t])
        self.feed(self.ev(t - 100, t + 5, used=12.0))                # an earlier head of 12
        self.assertEqual([b["t"] for b in self.win()["pending"]], [T11, t - 100])
        self.feed(self.ev(t + 700, t + 701, used=12.0))
        self.assertEqual(self.samples(), [(11.0, 12.0, 1.0)], "the 1.0 ending at t-20 is the next step's")

    def test_a_boundary_below_a_lower_one_is_reordered(self):
        evs, (T11,) = self.walk([], close=False)
        self.feed(*evs)
        self.feed(self.ev(T11 + 300, T11 + 301, used=12.0),
                  self.ev(T11 + 200, T11 + 400, used=13.0))
        w = self.win()
        self.assertEqual(w["counts"], {"reordered": 1})
        self.assertEqual((w["anchor"]["q"], w["anchor"]["t"], w["anchor"]["fresh"], w["pending"]),
                         (13.0, T11 + 200, True, []))
        self.assertTrue(any(l.endswith(": reordered") for l in self.logged))

    def test_a_multi_point_step_is_one_sample(self):
        evs, _ = self.walk([(3, [1.0, 1.0, 1.0])])
        self.feed(*evs)
        [s] = self.win()["samples"]
        self.assertEqual((s["p"], s["q"], s["points"], s["usd"]), (11.0, 14.0, 3.0, 3.0))
        self.assertEqual(s["usd"] / s["points"], 1.0)
        size, basis, _, _ = ex._sizing_estimate(self.win()["samples"])
        self.assertEqual((size, basis), (100.0, "few_steps"), "one step, not three points")
        self.assertEqual(self.counts(), {"sample": 1})

    def test_a_head_between_two_boundaries_splits_the_step(self):
        evs, (T11,) = self.walk([], close=False)
        self.feed(*evs)
        self.feed(self.ev(T11 + 300, T11 + 301, used=13.0))
        self.feed(self.ev(T11 + 100, T11 + 350, used=12.0))
        self.assertEqual([(b["q"], b["t"]) for b in self.win()["pending"]],
                         [(11.0, T11), (12.0, T11 + 100), (13.0, T11 + 300)])

    def test_three_steps_learn(self):
        evs, _ = self.walk([(1, [1.0, 1.0]), (1, [1.0, 1.1]), (1, [0.9, 1.0])])
        self.feed(*evs)
        size, basis, spread, _ = ex._sizing_estimate(self.win()["samples"])
        self.assertEqual(basis, "learned")
        self.assertAlmostEqual(size, 200.0)

    def test_a_step_past_100_is_no_sample(self):
        evs, _ = self.walk([(1, [1.0]), (1, [1.0])], q0=98.0)
        self.feed(*evs)
        self.assertEqual(self.samples(), [(99.0, 100.0, 1.0)])
        self.assertEqual(self.counts(), {"sample": 1, "over_limit": 1})
        self.assertIsNone(ex._sizing_hold_from(self.win()), "a window at its allowance holds nothing")
        self.assertEqual(len(ex._sizing["accounts"][ACCT_A]["buf"]), 1, "the last, in its grace")

    def test_a_reset_reanchors(self):
        evs, (T11, T12) = self.walk([(1, [1.0])], gap=700, close=False)
        self.feed(*evs)
        t = T12 + 100
        self.feed(self.ev(t, t + 1, used=0.0, reset=RESET_7D + 604800))
        w = self.win()
        self.assertEqual(w["counts"], {"period_change": 1}, "the open step was not settled")
        self.assertEqual((w["anchor"]["q"], w["anchor"]["reset"], w["anchor"]["fresh"]),
                         (0.0, RESET_7D + 604800, True))

    def test_a_codex_fall_without_a_reset_reanchors(self):
        evs, (T11, T12) = self.walk([(1, [1.0])], account="gpt", model="gpt-6.1-sol",
                                    reset=None, close=False)
        self.feed(*evs)
        t = T12 + ex.SIZING_SETTLE_S + 5
        self.feed(self.ev(t, t + 1, used=3.0, account="gpt", model="gpt-6.1-sol", reset=None))
        w = self.win(account="gpt")
        self.assertEqual(w["counts"], {"sample": 1}, "late enough: the open step closed first")
        self.assertEqual((w["anchor"]["q"], w["anchor"]["fresh"]), (3.0, True))

    def test_the_step_to_100_closes_on_the_next_periods_head(self):
        """Anthropic's 429 is the only response reading 100: its refusal is
        the boundary, and the next period's first head closes the step when
        it is late enough (66 §2)."""
        for late, want in ((True, {"sample": 1}), (False, {"period_change": 1})):
            with self.subTest(late=late):
                reset_sizing()
                with ex._reports_lock:
                    ex._holds.clear()
                evs, (T99,) = self.walk([], q0=98.0, close=False)
                evs.append(self.ev(T99 + 50, T99 + 60, used=99.0, usd=2.0))
                evs.append(self.ev(T99 + 650, T99 + 660, used=99.0, usd=3.0))
                self.feed(*evs)
                tr = T99 + 720
                self.assertEqual(post_reports(rep_refusal(self.n, tr, resets=RESET_7D),
                                              now=tr + 1)[0], [("applied", None)])
                self.assertEqual([b["q"] for b in self.win()["pending"]], [100.0])
                self.assertEqual(self.win()["anchor"]["q"], 99.0)
                t = tr + (ex.SIZING_SETTLE_S + 1 if late else 30)
                self.feed(self.ev(t, t + 1, used=0.0, reset=RESET_7D + 604800))
                self.assertEqual(self.counts(), want)
                if late:
                    self.assertEqual(self.samples(), [(99.0, 100.0, 5.0)])


class TestSizingOutcomes(SizingBase):
    """Each unclean outcome of 66 §4, one case each."""

    def one_step(self, extra=(), usd=(1.0, 1.0), **walk):
        """Anchor, first transition, one step with `extra` events ending inside it."""
        evs, bounds = self.walk([(1, list(usd))], **walk)
        self.feed(*self.in_end_order(evs + list(extra)))
        return bounds

    def outcome(self):
        return dict(self.counts())

    def test_clean_is_a_sample(self):
        self.one_step()
        self.assertEqual(self.outcome(), {"sample": 1})

    def test_unpriced(self):
        T = self.T0 + 100
        self.one_step([self.ev(T + 150, T + 160, model="claude-opus-9")])
        self.assertEqual(self.outcome(), {"unpriced": 1})

    def test_incomplete(self):
        T = self.T0 + 100
        self.one_step([self.ev(T + 150, T + 160, complete=False, why="stream ended")])
        self.assertEqual(self.outcome(), {"incomplete": 1})

    def test_paid(self):
        T = self.T0 + 100
        self.one_step([self.ev(T + 150, T + 160, usd=5.0, paid=True)])
        self.assertEqual(self.outcome(), {"paid": 1})

    def test_seq_gap(self):
        evs, bounds = self.walk([(1, [1.0, 1.0])])
        T11 = bounds[0]
        for e in evs:
            if e["read_at"] > T11 + 150:
                e["seq"] += 2                  # two events lost on the way
        self.feed(*evs)
        self.assertEqual(self.outcome(), {"seq_gap": 1})

    def test_proxy_restart(self):
        """The proxy restarted inside the step: b1's last event, then b2's
        first; what was in flight never sent anything."""
        evs, bounds = self.walk([(1, [1.0, 1.0])])
        T11 = bounds[0]
        for e in evs:
            if e["read_at"] > T11 + 150:
                e["boot"] = "b2"
        self.feed(*self.in_end_order(evs))
        self.assertEqual(self.outcome(), {"proxy_restart": 1})

    def test_two_proxies_at_once_are_no_restart(self):
        T = self.T0 + 100
        evs, bounds = self.walk([(1, [1.0, 1.0])], close=False)
        other = [self.ev(t - 1, t, boot="b2") for t in (self.T0 + 5, T + 50, T + 250, T + 320)]
        for e in sorted(evs + other, key=lambda e: e["read_at"]):
            self.feed(e, reporter="p2" if e["boot"] == "b2" else "p1")
        T12 = bounds[-1]
        self.feed(self.ev(T12 + 700, T12 + 701, used=12.0), reporter="p1")
        self.feed(self.ev(T12 + 700, T12 + 702, used=12.0, boot="b2"), reporter="p2")
        self.assertEqual(self.outcome(), {"sample": 1})

    def test_one_reporters_restart_while_another_serves(self):
        """p2 restarts (b2, then b3) while p1 serves throughout: the step
        holding b2's last event and b3's first is proxy_restart (66 §4 per
        reporter). A restart found after its step closed takes the sample
        back."""
        evs, bounds = self.walk([(1, [1.0, 1.0])] * 5, close=False)
        T11, T15, T16 = bounds[0], bounds[4], bounds[5]
        p2 = [self.ev(T11 + 30, T11 + 40, boot="b2"),     # p2's first boot: no restart
              self.ev(T15 + 30, T15 + 40, boot="b3")]     # p2 restarted
        for e in sorted(evs + p2, key=lambda e: e["read_at"]):
            self.feed(e, reporter="p2" if e["boot"] != "b1" else "p1")
        self.feed(self.ev(T16 + 700, T16 + 701, used=16.0), reporter="p1")
        # b2's last event (T11+40) was in the step 11→12, closed as a sample
        # before p2 came back; b3's first (T15+40) is in the step 15→16.
        self.assertEqual(self.outcome(), {"sample": 4, "proxy_restart": 1})
        self.assertEqual(self.win()["retracted"], {"proxy_restart": 1})
        self.assertEqual([(s["p"], s["q"]) for s in self.win()["samples"]],
                         [(12.0, 13.0), (13.0, 14.0), (14.0, 15.0)])
        rg = self.win()["range"]
        self.assertEqual((rg["points"], [st["lo"] for st in rg["steps"]]),
                         (3.0, [bounds[1], bounds[2], bounds[3]]), "11→12 left the range")

    def test_a_restart_found_after_a_silent_step_takes_it_out_of_the_range(self):
        """p2 serves another account; its old boot's last event falls in a
        silent step of this window. A restart of p2 found after that step
        closed takes the step out of the range: points and silent points."""
        evs, bounds = self.walk([(1, []), (1, [1.0]), (1, [1.0])], close=False)
        T11, T12, T13, T14 = bounds
        mk = functools.partial(self.ev, account="kimi", model="kimi-k3", readings={})
        p2 = [mk(T11 + 30, T11 + 40, boot="b2"), mk(T14 + 5, T14 + 10, boot="b3")]
        for e in sorted(evs + p2, key=lambda e: e["read_at"]):
            self.feed(e, reporter="p2" if e["boot"] != "b1" else "p1")
        self.assertEqual(self.counts(), {"silent": 1}, "11→12 closed silent before p2 came back")
        self.feed(self.ev(T14 + 700, T14 + 701, used=14.0), reporter="p1")
        self.assertEqual(self.counts(), {"silent": 1, "sample": 2})
        rg = self.win()["range"]
        self.assertEqual(([st["lo"] for st in rg["steps"]], rg["points"], rg["silent"]),
                         ([T12, T13], 2.0, 0.0))

    def test_a_graceful_brain_restart_is_no_proxy_restart(self):
        evs, bounds = self.walk([(1, [1.0, 1.0])] * 2, close=False)
        T11, T12, T13 = bounds
        for e in evs:
            self.feed(e, reporter="p1")
        self.feed(self.ev(T11 + 30, T11 + 40, boot="b2"), reporter="p2")
        ex._sizing_shutdown()
        reset_sizing_memory()
        ex._sizing_load()
        self.feed(self.ev(T12 + 30, T12 + 40, boot="b2"), reporter="p2")
        self.feed(self.ev(T13 + 700, T13 + 701, used=13.0), reporter="p1")
        self.assertEqual(self.outcome(), {"sample": 2})

    def test_overflow(self):
        with mock.patch.object(ex, "SIZING_EVENTS_MAX", 4):
            T = self.T0 + 100
            self.one_step([self.ev(T + 150 + k, T + 151 + k) for k in range(5)])
        self.assertEqual(self.outcome(), {"overflow": 1})
        self.assertGreater(ex._sizing["accounts"][ACCT_A]["dropped"], 0)

    def test_silent(self):
        self.one_step(usd=())
        self.assertEqual(self.outcome(), {"silent": 1})
        self.assertEqual(self.win()["samples"], [], "a silent step measures nothing of the window")
        self.assertEqual(self.win()["range"]["silent"], 1.0)

    def test_fuzz(self):
        """Work straddling a boundary past a quarter of the step's."""
        evs, bounds = self.walk([(1, [1.0, 1.0])], close=False)
        T11, T12 = bounds
        evs.append(self.ev(T11 - 30, T11 + 20, used=10.0, usd=1.0))  # its head before T_11
        self.feed(*self.in_end_order(evs))
        self.feed(self.ev(T12 + 700, T12 + 701, used=12.0))
        self.assertEqual(self.outcome(), {"fuzz": 1})
        self.assertEqual(self.win()["range"]["work"], 3.0, "its work still counts outside")

    def test_brain_restart(self):
        evs, bounds = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        with ex._sizing_lock:
            ex._sizing_save()                           # the samples' write; no open state
        ex._sizing_load()
        self.assertIn((ACCT_A, "seven_day"), ex._sizing["restarted"])
        T12 = bounds[-1]
        self.feed(self.ev(T12 + 50, T12 + 51, used=12.0),
                  self.ev(T12 + 300, T12 + 301, used=13.0),
                  self.ev(T12 + 1000, T12 + 1001, used=13.0))
        self.assertEqual(self.outcome(), {"brain_restart": 1})
        self.assertEqual((self.win()["anchor"]["q"], self.win()["anchor"]["fresh"]), (13.0, False),
                         "the next transition anchors again")

    def test_over_limit(self):
        evs, _ = self.walk([(1, [1.0])], q0=100.0, account="gpt", model="gpt-6.1-sol")
        self.feed(*evs)
        self.assertEqual(self.counts(account="gpt"), {"over_limit": 1})

    def test_cheap_is_a_sample(self):
        evs, _ = self.walk([(1, [1.0])] * 3 + [(1, [0.5])])
        self.feed(*evs)
        self.assertEqual(self.counts(), {"sample": 4, "cheap": 1})
        self.assertTrue(self.win()["samples"][-1]["cheap"])
        self.assertTrue(any(l.endswith("sample (cheap)") for l in self.logged))


class TestSizingBound(SizingBase):
    """66 §4: a hung-up request counts at the middle of its range when the
    step's spans are within SIZING_UNSEEN of its work."""

    OUT = ex.API_LIST_PRICES["claude-opus-5-5"]["standard"]["output"] / 1e6  # $ per token

    def hung(self, head, end, usd=0.1, output_max=1000, **kw):
        """A hung-up Opus request: its input worth `usd`, no output count."""
        return self.ev(head, end, usd=usd, complete=False,
                       why="the client left before the response ended",
                       output_max=output_max, tokens={"output": None}, **kw)

    def one_step(self, *extra):
        evs, bounds = self.walk([(1, [1.0, 1.0])])
        self.feed(*self.in_end_order(evs + list(extra)))
        return bounds

    def test_a_small_span_is_a_sample_at_the_middle(self):
        T = self.T0 + 100
        self.one_step(self.hung(T + 150, T + 160))     # spans $0.02 of $2.11
        mid = 0.1 + 500 * self.OUT
        self.assertEqual(self.counts(), {"sample": 1, "bounded": 1})
        [smp] = self.win()["samples"]
        self.assertAlmostEqual(smp["usd"], 2.0 + mid)
        self.assertTrue(smp["bounded"])
        self.assertEqual(smp["by"]["claude-opus-5-5|standard|standard"]["tokens"]["output"], 500.0,
                         "the token totals carry the half-limit output")
        self.assertAlmostEqual(ex._sizing_reprice(copy.deepcopy(smp))["usd"], 2.0 + mid,
                               msg="a re-price keeps it at the middle")
        self.assertAlmostEqual(self.win()["range"]["work"], 2.0 + mid)
        [line] = [l for l in self.logged if l.startswith("sizing:") and "11→12" in l]
        self.assertIn(": sample; 1 hung-up, spans $0.0200 against 5% of the work", line)

    def test_a_large_span_is_incomplete(self):
        T = self.T0 + 100
        self.one_step(self.hung(T + 150, T + 160, output_max=10000))  # $0.20 of $2.20
        self.assertEqual(self.counts(), {"incomplete": 1})
        self.assertEqual(self.win()["samples"], [])
        rg = self.win()["range"]
        self.assertEqual((rg["points"], rg["steps"]), (0.0, []),
                         "its work is unknown: it never enters the range")
        self.assertTrue(any("incomplete; 1 hung-up, spans $0.2000" in l for l in self.logged))

    def test_the_spans_add_up(self):
        """Each span is under 5% of the work; together they are over it."""
        T = self.T0 + 100
        self.one_step(*(self.hung(T + 150 + k, T + 160 + k, output_max=3000) for k in range(2)))
        self.assertEqual(self.counts(), {"incomplete": 1})

    def test_an_incomplete_event_without_a_bound_is_incomplete(self):
        T = self.T0 + 100
        for kw in (dict(output_max=None), dict(model="claude-unknown-9")):
            with self.subTest(kw):
                reset_sizing()
                self.one_step(self.hung(T + 150, T + 160, **kw))
                self.assertEqual(self.counts(), {"incomplete": 1})

    def test_a_paid_hung_up_request_closes_its_step_as_paid(self):
        """A paid request's work is the account's own spend, not the meter's:
        its span is not the meter's either, and the step closes as paid."""
        T = self.T0 + 100
        self.one_step(self.hung(T + 150, T + 160, output_max=10000, paid=True))
        self.assertEqual(self.counts(), {"paid": 1})
        self.assertFalse(any("hung-up" in l for l in self.logged))

    def test_a_bounded_entry_round_trips_with_its_span(self):
        evs, (T11, T12) = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        self.feed(self.ev(T12 + 40, T12 + 45, used=12.0, usd=1.0),
                  self.hung(T12 + 50, T12 + 60))
        ex._sizing_shutdown()
        with open(ex.SIZING_FILE) as f:
            saved = json.load(f)
        reset_sizing_memory()
        ex._sizing_load()
        [e] = [e for e in ex._sizing["accounts"][ACCT_A]["buf"] if not e["complete"]]
        self.assertAlmostEqual(e["span"], 1000 * self.OUT)
        self.feed(self.ev(T12 + 300, T12 + 301, used=13.0),
                  self.ev(T12 + 1000, T12 + 1001, used=13.0))
        self.assertEqual(self.counts(), {"sample": 2, "bounded": 1})
        self.assertAlmostEqual(self.win()["samples"][-1]["usd"], 1.1 + 500 * self.OUT)
        # The same file can't be used with the span gone, or with a complete
        # that is not a bool: it is set aside.
        def no_span(e):
            e.pop("span", None)

        def complete_null(e):
            if not e["complete"]:
                e["complete"] = None
        for bad in (no_span, complete_null):
            with self.subTest(bad.__name__):
                doc = copy.deepcopy(saved)
                for e in doc["open"]["accounts"][ACCT_A]["buf"]:
                    bad(e)
                reset_sizing()
                with open(ex.SIZING_FILE, "w") as f:
                    json.dump(doc, f)
                ex._sizing_load()
                self.assertEqual(ex._sizing["windows"], {})
                self.assertIsNotNone(ex._sizing_gate["set_aside"])

    def test_sizing_names_the_constant(self):
        self.assertEqual(ex.sizing_payload()["constants"]["unseen"], 0.05)


class TestSizingScopes(SizingBase):
    """66 §3: a scoped window takes its model's work only; 66 §8: the buffer."""

    def test_scoped_and_unscoped_cuts(self):
        t = self.T0
        R = RESET_7D

        def both(head, end, sd, fb, usd=0.0, model="claude-fable-5-1"):
            return self.ev(head, end, usd=usd, model=model,
                           readings={"seven_day": {"used_pct": sd, "resets_at": R},
                                     "scoped:Fable": {"used_pct": fb, "resets_at": R}})
        evs = [both(t, t + 1, 10.0, 5.0), both(t + 100, t + 101, 11.0, 6.0),
               both(t + 150, t + 160, 11.0, 6.0, usd=2.0),
               self.ev(t + 200, t + 210, used=11.0, usd=3.0),             # Opus
               self.ev(t + 220, t + 230, used=11.0, usd=1.0, model="claude-sonnet-5-5"),
               both(t + 400, t + 401, 12.0, 7.0),
               both(t + 1100, t + 1101, 12.0, 7.0)]
        self.feed(*evs)
        self.assertEqual(self.samples(), [(11.0, 12.0, 6.0)])
        self.assertEqual(self.samples("scoped:Fable"), [(6.0, 7.0, 2.0)])
        held = ex._sizing["accounts"][ACCT_A]["buf"]
        self.assertTrue(all(e["r"] > t + 400 for e in held), "closed past by every window")

    def test_an_event_leaves_once_no_window_needs_it(self):
        t = self.T0
        R = RESET_7D
        fb = {"scoped:Fable": {"used_pct": 6.0, "resets_at": R}}
        self.feed(self.ev(t, t + 1, readings={"seven_day": {"used_pct": 10.0, "resets_at": R},
                                              **fb}, model="claude-fable-5-1"),
                  self.ev(t + 100, t + 101, used=11.0),
                  self.ev(t + 150, t + 160, used=11.0, usd=1.0),                # Opus
                  self.ev(t + 170, t + 180, readings=fb, model="claude-fable-5-1", usd=1.0),
                  self.ev(t + 300, t + 301, used=12.0),
                  self.ev(t + 1000, t + 1001, used=12.0))
        rs = [e["r"] for e in ex._sizing["accounts"][ACCT_A]["buf"]]
        self.assertNotIn(t + 160, rs, "Opus work: seven_day closed past it")
        self.assertIn(t + 180, rs, "Fable work: scoped:Fable's open step still needs it")

    def test_overflow_marks_only_the_windows_that_needed_it(self):
        t = self.T0
        R = RESET_7D
        fb = {"scoped:Fable": {"used_pct": 6.0, "resets_at": R}}
        with mock.patch.object(ex, "SIZING_EVENTS_MAX", 4):
            self.feed(self.ev(t, t + 1, used=10.0),                                 # Opus
                      *[self.ev(t + 10 + k, t + 11 + k, usd=0.1) for k in range(3)],
                      self.ev(t + 50, t + 51, readings=fb, model="claude-fable-5-1"))
        self.assertIsNotNone(self.win()["overflow_to"])
        self.assertIsNone(self.win("scoped:Fable")["overflow_to"])

    def test_a_late_event_retracts_its_steps_sample(self):
        """A reporter behind another by more than the settle: its event in a
        closed step takes the sample back (`late`) and its work joins the
        measured range; one no window can use is counted late."""
        evs, (T11, T12) = self.walk([(1, [1.0])])
        self.feed(*evs, reporter="p1")
        self.assertEqual(self.win()["range"]["work"], 1.0)
        self.feed(self.ev(T11 + 5, T11 + 10, usd=0.5, boot="b2"), reporter="p2")
        self.assertEqual(self.samples(), [])
        self.assertEqual(self.counts(), {"sample": 1}, "a step is counted once, at its close")
        self.assertEqual(self.win()["retracted"], {"late": 1})
        self.assertEqual(self.win()["range"]["work"], 1.5)
        self.assertEqual(self.win()["range"]["steps"][0]["work"], 1.5)
        self.assertEqual(ex._sizing["accounts"][ACCT_A]["late"], 0)
        self.assertTrue(any(l.endswith("late (sample retracted)") for l in self.logged))
        self.assertIn(f'sizing_samples_retracted_total{{account="{ACCT_A}",window="seven_day",'
                      'outcome="late"} 1', "\n".join(ex._sizing_metric_lines()))
        # Before the first transition: no step, no range; no window can use it.
        self.feed(self.ev(self.T0 + 40, self.T0 + 50, usd=0.5, boot="b2"), reporter="p2")
        self.assertEqual(ex._sizing["accounts"][ACCT_A]["late"], 1)

    def test_a_late_event_unsilences_its_step(self):
        evs, (T11, T12) = self.walk([(1, [])])
        self.feed(*evs, reporter="p1")
        self.assertEqual(self.counts(), {"silent": 1})
        self.assertEqual(self.win()["range"]["silent"], 1.0)
        self.feed(self.ev(T11 + 5, T11 + 10, usd=0.5, boot="b2"), reporter="p2")
        self.assertEqual((self.win()["range"]["silent"], self.win()["range"]["work"]), (0.0, 0.5))

    def test_a_late_event_without_priced_work_takes_its_step_out_of_the_range(self):
        """The review's probe: a second reporter's late unpriced event leaves
        its step's work unknown, so the step leaves the measured range (66
        §6); so do a paid and an incomplete one, and a hung-up one with its
        bound: whether the closed step's spans stay within SIZING_UNSEEN is
        not kept (66 §4)."""
        late = {"unpriced": dict(model="claude-unknown-9"),
                "paid": dict(usd=0.5, paid=True),
                "incomplete": dict(usd=0.5, complete=False, why="cut"),
                "hung-up": dict(usd=0.1, complete=False, output_max=1000, tokens={"output": None},
                                why="the client left before the response ended")}
        for name, kw in late.items():
            with self.subTest(name):
                reset_sizing()
                evs, (T11, T12, T13) = self.walk([(1, [1.0]), (1, [1.0])])
                self.feed(*evs, reporter="p1")
                rg = self.win()["range"]
                self.assertEqual((rg["points"], rg["work"], len(rg["steps"])), (2.0, 2.0, 2))
                self.feed(self.ev(T11 + 5, T11 + 10, boot=f"b-{name}", **kw), reporter="p2")
                rg = self.win()["range"]
                self.assertEqual((rg["points"], rg["work"], len(rg["steps"])), (1.0, 1.0, 1))
                self.assertEqual([(st["lo"], st["hi"]) for st in rg["steps"]], [(T12, T13)])
                self.assertEqual(sum(p["usd"] for p in rg["by"].values()), 1.0)
                self.assertEqual(self.samples(), [(12.0, 13.0, 1.0)])
                self.assertEqual(self.win()["retracted"], {"late": 1})

    def test_the_range_keeps_its_newest_steps_whole(self):
        """Past SIZING_RANGE_STEPS_MAX a step is final in the range: a late
        event without priced work no longer takes it out."""
        with mock.patch.object(ex, "SIZING_RANGE_STEPS_MAX", 2):
            evs, (T11, *_) = self.walk([(1, [1.0])] * 3)
            self.feed(*evs, reporter="p1")
            rg = self.win()["range"]
            self.assertEqual((rg["points"], len(rg["steps"])), (3.0, 2))
            self.feed(self.ev(T11 + 5, T11 + 10, model="claude-unknown-9", boot="b2"), reporter="p2")
            self.assertEqual(self.win()["range"]["points"], 3.0)

    def test_a_window_whose_heads_stop_lets_go_at_its_reset(self):
        """A header no longer sent (66 §8): once data time is the settle past
        the window's reset with no head of the next period, the steps due by
        then close as usual (a boundary before the reset is always due), and
        the window lets go of its anchor and holds nothing."""
        R = self.T0 + 2000
        evs, bounds = self.walk([(1, [1.0])] * 2, reset=R, close=False)
        T13 = bounds[-1]
        self.feed(*evs)
        self.feed(self.ev(T13 + 20, T13 + 30, usd=1.0, readings={}))
        self.assertIsNotNone(self.win()["anchor"])
        self.assertTrue(ex._sizing["accounts"][ACCT_A]["buf"])
        self.feed(self.ev(R + 100, R + 110, readings={}))
        self.assertIsNotNone(self.win()["anchor"], "not before the settle past the reset")
        t = R + ex.SIZING_SETTLE_S + 1
        self.feed(self.ev(t, t + 1, readings={}))
        self.assertEqual(self.counts(), {"sample": 2})
        self.assertIsNone(self.win()["anchor"])
        self.assertIsNone(ex._sizing_hold_from(self.win()))
        t2 = t + 11 + ex.SIZING_SETTLE_S
        self.feed(self.ev(t + 10, t2, readings={}))
        self.assertEqual([e["r"] for e in ex._sizing["accounts"][ACCT_A]["buf"]], [t2],
                         "nothing held for it: only the newest event's grace")
        self.assertTrue(any("lets go of its open step" in l for l in self.logged))

    def test_a_resend_of_a_buffered_event_is_dropped(self):
        evs, (T11,) = self.walk([], close=False)
        self.feed(*evs)
        e = self.ev(T11 + 50, T11 + 60, usd=1.0)
        self.feed(e)
        with ex._served_lock:
            ex._served = ex._served_fresh()        # a restart forgot the ids ...
        self.feed(dict(e, id="s-again"))           # ... and the proxy sends it again
        self.assertEqual(sum(1 for x in ex._sizing["accounts"][ACCT_A]["buf"]
                             if x["seq"] == e["seq"]), 1)

    def test_the_gap_bound(self):
        with mock.patch.object(ex, "SIZING_GAPS_MAX", 3):
            for k in range(6):
                self.feed(self.ev(self.T0 + k, self.T0 + k + 0.5, seq=10 * k + 1))
        self.assertEqual(len(ex._sizing["gaps"]), 3)

    def test_steps_outside_the_proxy_window_need_no_events(self):
        """Without readings (Kimi, or a head with no rate-limit headers) the
        buffer keeps only the settle's grace."""
        for k in range(5):
            self.feed(self.ev(self.T0 + k * 400, self.T0 + k * 400 + 1, usd=1.0))
        rs = [e["r"] for e in ex._sizing["accounts"][ACCT_A]["buf"]]
        self.assertEqual(rs, [self.T0 + 1201, self.T0 + 1601])
        self.assertEqual(ex._sizing["windows"], {})


class TestSizingEstimator(SizingBase):
    """66 §5."""

    def test_bases(self):
        S = lambda v: {"usd": v, "points": 1.0}  # noqa: E731
        self.assertEqual(ex._sizing_estimate([])[:2], (None, "none"))
        self.assertEqual(ex._sizing_estimate([S(1.0)])[1], "few_steps")
        self.assertEqual(ex._sizing_estimate([S(1.0), S(1.1)])[1], "few_steps")
        self.assertEqual(ex._sizing_estimate([S(1.0), S(1.1), S(0.95)])[1], "learned")
        self.assertEqual(ex._sizing_estimate([S(1.0), S(2.0), S(0.4)])[1], "disagree")

    def test_basis_through_steps_and_the_disagree_line(self):
        evs, bounds = self.walk([(1, [1.0]), (1, [3.0]), (1, [0.3])])
        cut = [e for e in evs if e["read_at"] <= bounds[3] + 1]
        self.feed(*cut)
        self.assertEqual(self.win()["basis"], "few_steps")
        self.feed(*evs[len(cut):])
        self.assertEqual(self.win()["basis"], "disagree")
        self.assertTrue(any("samples disagree" in l for l in self.logged))

    def test_a_majority_of_cheap_steps_moves_the_median_down(self):
        evs, _ = self.walk([(1, [1.0])] * 3 + [(1, [0.5])] * 4)
        self.feed(*evs)
        size, basis, _, _ = ex._sizing_estimate(self.win()["samples"])
        self.assertEqual(size, 50.0)
        self.assertEqual(len(self.win()["samples"]), 7, "a downward shift retires nothing")

    def test_an_upward_shift_retires_the_older_samples(self):
        evs, _ = self.walk([(1, [1.0])] * 4 + [(1, [2.0])] * 3)
        self.feed(*evs)
        self.assertEqual([s["usd"] for s in self.win()["samples"]], [2.0, 2.0, 2.0])
        self.assertTrue(any(l.endswith("samples retired: the window grew") for l in self.logged))
        self.assertEqual(ex._sizing_estimate(self.win()["samples"])[:2], (200.0, "learned"))

    def test_a_downward_shift_retires_nothing(self):
        evs, _ = self.walk([(1, [2.0])] * 4 + [(1, [1.0])] * 3)
        self.feed(*evs)
        self.assertEqual([s["usd"] for s in self.win()["samples"]], [2.0] * 4 + [1.0] * 3)
        self.assertFalse(any("retired" in l for l in self.logged))

    def test_kept_samples_are_bounded_by_count_and_age(self):
        s = [{"usd": 1.0, "points": 1.0, "to": float(t)} for t in range(30)]
        self.assertEqual(len(ex._sizing_kept(s, 30.0)), ex.SIZING_SAMPLES_KEEP)
        self.assertEqual(ex._sizing_kept(s, 29.0 + ex.SIZING_SAMPLE_AGE_S), [s[-1]])

    def test_a_price_change_reprices(self):
        """Samples keep tokens by (model, speed, tier); a start re-prices them,
        the long-context tier at its own row, and drops what the table no
        longer prices."""
        t = self.T0
        R = RESET_7D
        rd = lambda q: {"seven_day": {"used_pct": q, "resets_at": R}}  # noqa: E731
        big = {"input": 150000, "output": 0}                            # past Haiku 5.5's 100K
        self.feed(self.ev(t, t + 1, readings=rd(10.0)), self.ev(t + 100, t + 101, readings=rd(11.0)),
                  self.ev(t + 150, t + 160, model="claude-haiku-5-5", tokens=big, readings={}),
                  self.ev(t + 170, t + 180, model="claude-sonnet-5-5", tokens={"input": 1000000},
                          readings={}),
                  self.ev(t + 400, t + 401, readings=rd(12.0)),
                  self.ev(t + 1100, t + 1101, readings=rd(12.0)))
        [s] = self.win()["samples"]
        self.assertEqual(set(s["by"]), {"claude-haiku-5-5|standard|long",
                                        "claude-sonnet-5-5|standard|standard",
                                        "claude-opus-5-5|standard|standard"})
        self.assertAlmostEqual(s["usd"], 0.075 + 2.0)
        with ex._sizing_lock:
            ex._sizing_save()
        long_rows = {"standard": dict(ex.API_LONG_CONTEXT["claude-haiku-5-5"][1]["standard"],
                                      input=1.0)}
        with mock.patch.dict(ex.API_LONG_CONTEXT,
                             {"claude-haiku-5-5": (100000, long_rows)}):
            ex._sizing_load()
            [s] = self.win()["samples"]
            self.assertAlmostEqual(s["usd"], 0.15 + 2.0)
            self.assertAlmostEqual(s["by"]["claude-haiku-5-5|standard|long"]["usd"], 0.15)
        with ex._sizing_lock:
            ex._sizing_save()
        with mock.patch.dict(ex.API_LIST_PRICES):
            del ex.API_LIST_PRICES["claude-sonnet-5-5"]
            ex._sizing_load()
            self.assertEqual(self.win()["samples"], [])
            self.assertTrue(any("1 dropped" in l for l in self.logged))

    def test_per_model_medians(self):
        evs, _ = self.walk([(1, [1.0])] * 3 + [(1, [2.0], "claude-fable-5-1")] * 2)
        self.feed(*evs)
        view = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]
        self.assertEqual(view["per_model"],
                         {"claude-fable-5-1": {"median_per_point": 2.0, "samples": 2},
                          "claude-opus-5-5": {"median_per_point": 1.0, "samples": 3}})


class TestSizingOutside(SizingBase):
    """66 §6: the signed sum over the measured range."""

    def view(self, window="seven_day", account=ACCT_A):
        return ex.sizing_payload()["accounts"][account]["windows"][window]["outside"]

    def test_none_before_a_step(self):
        self.assertIsNone(ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]["outside"])

    def test_near_zero_on_proxy_only_use(self):
        evs, _ = self.walk([(1, [1.0])] * 4)
        self.feed(*evs)
        o = self.view()
        self.assertEqual((o["measured_points"], o["outside_points"], o["outside_pct"]), (4.0, 0.0, 0.0))

    def test_fuzz_across_one_boundary_cancels(self):
        """Work pushed across a boundary leaves one step and enters the next:
        the sum is the same."""
        evs, bounds = self.walk([(1, [1.0])] * 3 + [(1, [0.6]), (1, [1.4])])
        self.feed(*evs)
        o = self.view()
        self.assertEqual(o["measured_points"], 5.0)
        self.assertAlmostEqual(o["outside_points"], 0.0)

    def test_a_silent_step_counts_its_points(self):
        evs, _ = self.walk([(1, [1.0])] * 3 + [(2, [])])
        self.feed(*evs)
        o = self.view()
        self.assertEqual((o["measured_points"], o["outside_points"]), (5.0, 2.0))
        self.assertEqual(o["outside_pct"], 40.0)

    def test_silent_points_alone_while_not_learned(self):
        evs, _ = self.walk([(1, [1.0]), (1, [])])
        self.feed(*evs)
        o = self.view()
        self.assertEqual((o["measured_points"], o["outside_points"]), (2.0, 1.0))

    def test_the_floor_applies_to_the_total(self):
        evs, _ = self.walk([(1, [1.0])] * 3 + [(1, [3.0]), (1, [])])
        self.feed(*evs)
        o = self.view()
        self.assertEqual(o["outside_points"], 0.0, "-2 + 1 floors at 0, once")

    def test_the_range_restarts_on_a_period_change(self):
        evs, bounds = self.walk([(1, [])])
        self.feed(*evs)
        self.assertEqual(self.view()["measured_points"], 1.0)
        R2 = RESET_7D + 604800
        evs, _ = self.walk([(1, [1.0])], t0=bounds[-1] + 5000, q0=0.0, reset=R2)
        self.feed(*evs)
        o = self.view()
        self.assertEqual((o["period"], o["measured_points"], o["outside_points"]), (R2, 1.0, 0.0))


class TestSizingPoll(SizingBase):
    """66 §7: Kimi's limits are poll-sized, its pool without a reset."""

    def poll(self, t, remaining, weekly=2.0):
        res = kimi_result(ts=t, total_remaining=remaining, weekly=weekly)
        ex._sizing_poll("kimi", res)

    def test_modes(self):
        self.assertEqual(ex._sizing_mode("anthropic", "seven_day"), "header")
        self.assertEqual(ex._sizing_mode("anthropic", "scoped:Fable"), "header")
        self.assertIsNone(ex._sizing_mode("anthropic", "five_hour"))
        self.assertEqual(ex._sizing_mode("gpt", "five_hour"), "header")
        self.assertEqual(ex._sizing_mode("gpt", "window:3h"), "header")
        self.assertEqual(ex._sizing_mode("kimi", "seven_day"), "poll")
        self.assertEqual(ex._sizing_mode("kimi", "total_quota", "cycle_pool"), "poll")
        self.assertIsNone(ex._sizing_mode("runway", "seven_day"))

    def test_an_anthropic_five_hour_reading_is_not_sized(self):
        self.feed(self.ev(self.T0, self.T0 + 1,
                          readings={"five_hour": {"used_pct": 40.0, "resets_at": RESET_7D}}))
        self.assertNotIn((ACCT_A, "five_hour"), ex._sizing["windows"])

    def test_the_pool_steps_without_a_reset(self):
        P = ex.POLL_INTERVAL
        t = self.T0
        price = {"standard": {"input": 1.0, "cache_write_5m": 1.0, "cache_write_1h": 1.0,
                              "cache_read": 0.1, "output": 4.0}}
        with mock.patch.dict(ex.API_LIST_PRICES, {"kimi-k3": price}):
            self.poll(t, 90.0)              # 10 % used: the anchor
            self.poll(t + P, 89.0)          # 11 %: the first transition
            for k in range(1, 10):
                self.feed(self.ev(t + P + k * 120 - 2, t + P + k * 120, model="kimi-k3",
                                  account="kimi", usd=0.5, readings={}))
            for k in (2, 3, 4):
                self.poll(t + k * P, 89.0)
            self.poll(t + 5 * P, 88.0)      # 12 %
            self.poll(t + 6 * P, 88.0)      # the next poll closes the step
            self.poll(t + 7 * P, 99.5)      # 0.5 %: a fall of more than a point, a new period
        w = self.win("total_quota", "kimi")
        self.assertEqual(w["mode"], "poll")
        self.assertIsNone(w["anchor"]["reset"])
        self.assertEqual([(s["p"], s["q"], s["usd"]) for s in w["samples"]], [(11.0, 12.0, 4.5)])
        self.assertEqual((w["anchor"]["q"], w["anchor"]["fresh"]), (0.5, True))
        self.assertIsNone(w["range"]["first"], "a new period, a new range")

    def test_a_limit_the_polls_stop_carrying_lets_go_at_its_reset(self):
        t = self.T0
        R = t + 1000
        with ex._sizing_lock:
            ex._sizing_acct("kimi", "kimi")
            w = ex._sizing_win("kimi", "window:gone", "poll")
            ex._sizing_head("kimi", "window:gone", w, t, 10.0, R, t)
        self.poll(R + 100, 90.0)
        self.assertIsNotNone(w["anchor"])
        self.poll(R + ex.SIZING_SETTLE_S + 1, 90.0)
        self.assertIsNone(w["anchor"])
        self.assertIsNone(ex._sizing_hold_from(w))

    def test_the_poll_fuzz(self):
        """Work that ended in the poll before a boundary is on either side."""
        P = ex.POLL_INTERVAL
        t = self.T0
        price = {"standard": {"input": 1.0, "cache_write_5m": 1.0, "cache_write_1h": 1.0,
                              "cache_read": 0.1, "output": 4.0}}
        with mock.patch.dict(ex.API_LIST_PRICES, {"kimi-k3": price}):
            self.poll(t, 90.0)
            self.poll(t + P, 89.0)
            self.feed(self.ev(t + 2 * P - 20, t + 2 * P - 10, model="kimi-k3", account="kimi",
                              usd=1.0, readings={}))
            self.poll(t + 2 * P, 88.0)      # 12 %, a step of one poll: all its work is fuzz
            self.poll(t + 3 * P, 88.0)
        self.assertEqual(self.counts("total_quota", "kimi"), {"fuzz": 1})

    def test_unpriced_kimi_today(self):
        P = ex.POLL_INTERVAL
        t = self.T0
        self.poll(t, 90.0)
        self.poll(t + P, 89.0)
        self.feed(self.ev(t + P + 100, t + P + 110, model="kimi-k3", account="kimi", readings={}))
        self.poll(t + 2 * P, 88.0)
        self.poll(t + 3 * P, 88.0)
        self.assertEqual(self.counts("total_quota", "kimi"), {"unpriced": 1})


class TestSizingPersistence(SizingBase):
    """66 §8: samples when they change; the open state at a graceful shutdown."""

    def test_samples_round_trip(self):
        evs, _ = self.walk([(1, [1.0])] * 3)
        self.feed(*evs)
        with open(ex.SIZING_FILE) as f:
            doc = json.load(f)
        self.assertEqual(doc["v"], 1)
        self.assertNotIn("open", doc, "the open state is not written as samples change")
        before = copy.deepcopy(self.win()["samples"])
        ex._sizing_load()
        self.assertEqual(self.win()["samples"], before)
        self.assertEqual(self.win()["basis"], "learned")

    def test_a_graceful_shutdown_resumes_the_open_step(self):
        evs, bounds = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        T12 = bounds[-1]
        self.feed(self.ev(T12 + 50, T12 + 60, used=12.0, usd=1.5))
        before = copy.deepcopy((self.win()["anchor"], self.win()["pending"]))
        ex._sizing_shutdown()
        reset_sizing_memory()
        ex._sizing_load()
        self.assertEqual((self.win()["anchor"], self.win()["pending"]), before)
        self.assertEqual(ex._sizing["restarted"], set())
        with open(ex.SIZING_FILE) as f:
            self.assertNotIn("open", json.load(f), "good for this start only")
        self.feed(self.ev(T12 + 300, T12 + 301, used=13.0),
                  self.ev(T12 + 1000, T12 + 1001, used=13.0))
        self.assertEqual(self.samples(), [(11.0, 12.0, 1.0), (12.0, 13.0, 1.5)])
        self.assertNotIn("brain_restart", self.counts())

    def test_a_start_without_it_marks_the_open_step(self):
        evs, bounds = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        ex._sizing_shutdown()
        reset_sizing_memory()
        ex._sizing_load()                      # the open state is used once ...
        reset_sizing_memory()
        ex._sizing_load()                      # ... so a second start is a crash's
        self.assertIn((ACCT_A, "seven_day"), ex._sizing["restarted"])
        self.assertTrue(any("no open state" in l for l in self.logged))

    def test_sigterm_saves_and_exits(self):
        self.addCleanup(setattr, ex, "_sigterm_seen", False)
        evs, _ = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        with self.assertRaises(SystemExit):
            ex._sigterm(15, None)
        with open(ex.SIZING_FILE) as f:
            self.assertIn("open", json.load(f))

    def test_a_missing_or_unusable_file(self):
        """A missing file starts empty; an unusable one (not JSON, another
        version, a version-1 file with a field missing or of the wrong type)
        is set aside as .bad-<UTC time>, never over an earlier one, before
        anything is written; sizing starts empty and /sizing names it."""
        ex._sizing_load()
        self.assertEqual(ex._sizing["windows"], {})
        evs, _ = self.walk([(1, [1.0])] * 2)
        self.feed(*evs)
        with open(ex.SIZING_FILE) as f:
            good = json.load(f)
        smp = good["windows"][ACCT_A]["seven_day"]["samples"][0]
        bad_open = dict(good, open={"accounts": {ACCT_A: {"data_t": 1.0, "buf": [{"r": 1.0}]}}})
        cases = {
            "not json": "{",
            "another version": '{"v": 9}',
            "a sample without its tokens": json.dumps(
                {"v": 1, "windows": {ACCT_A: {"seven_day": {"mode": "header",
                                                            "samples": [{"usd": 1.0}]}}}}),
            "a sample of no points": json.dumps(copy.deepcopy(good) | {"windows": {ACCT_A: {
                "seven_day": {"mode": "header", "samples": [dict(smp, points=0)], "range": None}}}}),
            "a range without its steps": json.dumps({"v": 1, "windows": {ACCT_A: {"seven_day": dict(
                good["windows"][ACCT_A]["seven_day"],
                range={k: v for k, v in good["windows"][ACCT_A]["seven_day"]["range"].items()
                       if k != "steps"})}}}),
            "a buffered event cut short": json.dumps(bad_open),
            "a list for windows": '{"v": 1, "windows": []}',
        }
        for k, (name, text) in enumerate(cases.items()):
            with self.subTest(name):
                reset_sizing_memory()
                self.logged.clear()
                with open(ex.SIZING_FILE, "w") as f:
                    f.write(text)
                ex._sizing_load()
                self.assertEqual(ex._sizing["windows"], {})
                self.assertFalse(os.path.exists(ex.SIZING_FILE))
                bad = ex._sizing_gate["set_aside"]
                self.assertRegex(bad, r"\.bad-\d{8}T\d{6}Z(-\d+)?$")
                self.assertEqual(len(sizing_bad_files()), k + 1, "never over an earlier one")
                with open(bad) as f:
                    self.assertEqual(f.read(), text)
                self.assertEqual(len([l for l in self.logged if "set aside as" in l]), 1)
                self.assertEqual(ex.sizing_payload()["file"], {
                    "path": ex.SIZING_FILE, "frozen": False, "frozen_why": None, "set_aside": bad})
                # Sizing goes on, and its next save leaves the set-aside file alone.
                self.feed(*self.walk([(1, [1.0])], t0=self.T0 + 10000)[0])
                with open(bad) as f:
                    self.assertEqual(f.read(), text)
                os.remove(ex.SIZING_FILE)

    def test_a_file_that_cannot_be_read_is_left_and_frozen(self):
        """An I/O error reading a file that may be good: it stays where it
        is, sizing starts empty, nothing writes over it, /sizing says why,
        and the shutdown says the open state was not saved."""
        evs, _ = self.walk([(1, [1.0])] * 2)
        self.feed(*evs)
        with open(ex.SIZING_FILE) as f:
            good = f.read()
        reset_sizing_memory()
        real_open = open

        def deny(path, *a, **k):
            if path == ex.SIZING_FILE:
                raise PermissionError(13, "Permission denied")
            return real_open(path, *a, **k)
        with mock.patch("builtins.open", deny):
            ex._sizing_load()
        self.assertEqual(sizing_bad_files(), [])
        self.assertEqual(ex._sizing["windows"], {})
        f = ex.sizing_payload()["file"]
        self.assertTrue(f["frozen"])
        self.assertIn("Permission denied", f["frozen_why"])
        self.assertIsNone(f["set_aside"])
        self.feed(*self.walk([(1, [1.0])], t0=self.T0 + 10000)[0])
        ex._sizing_shutdown()
        with open(ex.SIZING_FILE) as fh:
            self.assertEqual(fh.read(), good)
        self.assertTrue(any(l.startswith("sizing: open state not saved: sizing is frozen (cannot read")
                            for l in self.logged))
        self.assertFalse(any("open state saved" in l for l in self.logged))

    def test_a_missing_key_never_passes_as_null(self):
        evs, _ = self.walk([(1, [1.0])] * 2)
        self.feed(*evs)
        with open(ex.SIZING_FILE) as f:
            doc = json.load(f)
        for key in ("q", "reset"):
            with self.subTest(key):
                d = copy.deepcopy(doc)
                del d["windows"][ACCT_A]["seven_day"]["range"][key]
                self.assertEqual(ex._sizing_doc_problem(d), f"windows.{ACCT_A}.seven_day.range")

    def test_a_file_that_cannot_be_set_aside_is_never_written_over(self):
        with open(ex.SIZING_FILE, "w") as f:
            f.write("{")
        with mock.patch.object(ex.os, "replace", side_effect=OSError("read-only")):
            ex._sizing_load()
        self.assertIn("not set aside (read-only)", ex.sizing_payload()["file"]["frozen_why"])
        self.feed(*self.walk([(1, [1.0])])[0])
        with open(ex.SIZING_FILE) as f:
            self.assertEqual(f.read(), "{")

    def test_the_range_is_repriced_at_load(self):
        evs, _ = self.walk([(1, [1.0])] * 2)
        self.feed(*evs)
        self.assertAlmostEqual(self.win()["range"]["work"], 2.0)
        row = dict(ex.API_LIST_PRICES["claude-opus-5-5"]["standard"])
        row["input"] *= 2
        with mock.patch.dict(ex.API_LIST_PRICES, {"claude-opus-5-5": {"standard": row}}):
            ex._sizing_load()
            self.assertAlmostEqual(self.win()["range"]["work"], 4.0)
            self.assertEqual([round(s["usd"], 6) for s in self.win()["samples"]], [2.0, 2.0])
        with mock.patch.dict(ex.API_LIST_PRICES):
            del ex.API_LIST_PRICES["claude-opus-5-5"]
            ex._sizing_load()
            self.assertIsNone(self.win()["range"])
            self.assertTrue(any("1 measured range(s) started again" in l for l in self.logged))

    def test_a_post_after_the_shutdown_save_is_answered_retry(self):
        """The shutdown save closes sizing: a served event after it is
        answered retry and leaves no trace, so its resend is applied by the
        next brain; refusals and polls change nothing; no save writes the
        file again."""
        evs, bounds = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        T12 = bounds[-1]
        ex._sizing_shutdown()
        with open(ex.SIZING_FILE) as f:
            saved = f.read()
        self.assertIn('"open"', saved)
        pending = copy.deepcopy(self.win()["pending"])
        counted = copy.deepcopy((ex._served["events"], ex._served["usd"], list(ex._served["ids"])))
        late = self.ev(T12 + 700, T12 + 701, used=12.0, usd=1.0)   # would close the step
        out = post_reports(late, now=T12 + 702)[0]
        self.assertEqual(out, [("retry", "the brain is shutting down")])
        self.assertEqual((ex._served["events"], ex._served["usd"], list(ex._served["ids"])), counted)
        refusal = rep_refusal(self.n + 1, T12 + 710, resets=RESET_7D)
        self.assertEqual(post_reports(refusal, now=T12 + 711)[0],
                         [("retry", "the brain is shutting down")])
        self.assertEqual(self.win()["pending"], pending)
        with ex._reports_lock:
            self.assertEqual(ex._holds, {}, "nothing of the refusal is kept")
        with ex._sizing_lock:
            ex._sizing_save()
        with open(ex.SIZING_FILE) as f:
            self.assertEqual(f.read(), saved, "the open state stays on the file")
        reset_sizing_memory()                    # the next brain
        ex._sizing_load()
        self.assertEqual(post_reports(late, now=T12 + 702)[0], [("applied", None)])
        self.assertEqual(self.samples(), [(11.0, 12.0, 1.0)])
        self.assertEqual(post_reports(refusal, now=T12 + 711)[0], [("applied", None)])

    def test_the_shutdown_save_waits_for_events_in_flight(self):
        evs, _ = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        self.assertTrue(ex._sizing_enter())      # a served event past the gate
        done = threading.Event()
        th = threading.Thread(target=lambda: (ex._sizing_shutdown(), done.set()))
        th.start()
        self.assertFalse(done.wait(0.2), "the save waits for it")
        self.assertFalse(ex._sizing_enter(), "and lets no new one in")
        ex._sizing_leave()
        self.assertTrue(done.wait(2))
        th.join()

    def test_a_refusal_leaves_data_time_alone(self):
        """A refusal is a head, not data time (66 §2): posted ahead of a
        batch's served events, it must not settle steps by itself."""
        evs, bounds = self.walk([(1, [1.0])], close=False)
        self.feed(*evs)
        T12 = bounds[-1]
        data_t = ex._sizing["accounts"][ACCT_A]["data_t"]
        tr = T12 + 2000
        self.assertEqual(post_reports(rep_refusal(self.n + 1, tr, used=12.0, resets=RESET_7D),
                                      now=tr + 1)[0], [("applied", None)])
        self.assertEqual(ex._sizing["accounts"][ACCT_A]["data_t"], data_t)
        self.assertEqual(self.counts(), {}, "the step 11→12 is still open")
        self.feed(self.ev(T12 + 30, T12 + 40, usd=0.5))
        self.assertEqual(self.counts(), {})

    def test_a_second_sigterm_returns_at_once(self):
        """Landing inside the first handler, which may hold _sizing_lock, a
        second SIGTERM takes no lock and raises nothing."""
        self.addCleanup(setattr, ex, "_sigterm_seen", False)
        ex._sigterm_seen = True
        out = []
        with ex._sizing_lock:
            th = threading.Thread(target=lambda: out.append(ex._sigterm(15, None)))
            th.start()
            th.join(2)
            self.assertFalse(th.is_alive())
        self.assertEqual(out, [None])

    def test_sigterm_in_a_real_process(self):
        """The exporter started on an empty temp dir (no accounts, no creds,
        and no network: urlopen refuses) with an unusable sizing file sets it
        aside and serves; a SIGTERM saves sizing and exits 0."""
        d = tempfile.mkdtemp(prefix="sizing-sigterm-")
        self.addCleanup(shutil.rmtree, d, True)
        with socket.socket() as so:
            so.bind(("127.0.0.1", 0))
            port = so.getsockname()[1]
        env = {k: v for k, v in os.environ.items() if not k.startswith(("SWITCH_", "KIMI_", "OPENAI_",
                                                                        "RUNWAY_", "CLAUDE_"))}
        env.update(PORT=str(port), POLL_INTERVAL="3600",
                   **{k: os.path.join(d, k.lower()) for k in (
                       "CLAUDE_ACCOUNTS_FILE", "KIMI_CREDS_FILE", "OPENAI_CODEX_CREDS_FILE",
                       "RUNWAY_CREDS_FILE", "SWITCH_CLASSES_FILE", "SWITCH_CONSUMERS_FILE",
                       "SWITCH_HARNESS_FILE", "SWITCH_HARNESS_KEY_FILE", "SWITCH_LANES_FILE",
                       "SWITCH_LEDGER_FILE", "SWITCH_MONEY_FILE", "SWITCH_SIZING_FILE",
                       "SWITCH_STATE_FILE")})
        sizing = env["SWITCH_SIZING_FILE"]
        with open(sizing, "w") as f:
            f.write('{"v": 1, "windows": {"a": {"seven_day": {"mode": "header", '
                    '"samples": [{"usd": 1.0}]}}}}')
        code = ("import sys, urllib.request; sys.path.insert(0, sys.argv[1]); "
                "import claude_usage_exporter as ex\n"
                "def no_net(*a, **k): raise OSError('no network in this test')\n"
                "ex.urlopen = urllib.request.urlopen = no_net\n"
                "ex.main()")
        proc = subprocess.Popen([sys.executable, "-c", code, os.path.dirname(ex.__file__)],
                                env=env, cwd=d, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True)
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        body = None
        for _ in range(100):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/sizing", timeout=1) as r:
                    body = json.load(r)
                break
            except OSError:
                if proc.poll() is not None:
                    break
                time.sleep(0.1)
        self.assertIsNotNone(body, proc.stdout.read() if proc.poll() is not None else "no answer")
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/preferred", timeout=2) as r:
            self.assertEqual(r.status, 200)
        self.assertEqual(len([n for n in os.listdir(d) if n.startswith("switch_sizing_file.bad-")]), 1)
        self.assertTrue(body["file"]["set_aside"].startswith(sizing + ".bad-"))
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=10)
        self.assertEqual(proc.returncode, 0, out[-2000:])
        self.assertIn("set aside as", out)
        self.assertIn("sizing: open state saved", out)
        with open(sizing) as f:
            self.assertEqual(json.load(f)["open"]["accounts"], {})


SIZING_WEIGHTS_FIXTURE = os.path.join(BRAIN, "testdata",
                                      "sizing_weights.json")


OPUS, FABLE = "claude-opus-5-5", "claude-fable-5-1"


def weight_rows(window, n, rho, pi=10.0, noise=0.02, seed=1, lo=0.05, hi=0.85, fixed=None):
    """Made-up rows of the weights' fit [(window, points, {model: list
    dollars})]: n steps of a point, Fable's share of a step's list dollars
    stepping from lo to hi and the rest Opus's, but for `fixed` {model:
    (share, true weight)} at one share of every step. A step's dollars are
    the window's size `pi` in Opus dollars at the true weights (Fable's
    `rho`), within a relative noise drawn from `seed`."""
    r = random.Random(seed)
    fixed = fixed or {}
    out = []
    for k in range(n):
        f = lo + (hi - lo) * k / max(n - 1, 1)
        rest = 1 - f - sum(s for s, _ in fixed.values())
        d = (pi * (1 + noise * r.uniform(-1, 1))
             / (rest + f * rho + sum(s * w for s, w in fixed.values())))
        u = {OPUS: d * rest, FABLE: d * f}
        u.update((m, d * s) for m, (s, _) in fixed.items())
        out.append((window, 1.0, u))
    return out


def weight_samples(rows, t0=0.0):
    """Rows as kept samples, five minutes apart."""
    return [{"from": t0 + 300 * i, "to": t0 + 300 * (i + 1), "p": float(i), "q": i + p,
             "points": p, "usd": sum(u.values()),
             "by": {f"{m}|standard|standard": {"usd": v} for m, v in u.items()}}
            for i, (_, p, u) in enumerate(rows)]


class TestSizingWeights(SizingBase):
    """67 §3, §4: a weight per model, learned per provider from the kept
    samples, and the windows' sizes read at the weights."""

    W = (ACCT_A, "seven_day")
    # A dollar of Opus a point, Fable weighing 2: $3 of Opus and $1 of Fable.
    MIX = [(1, [(0.8, OPUS), (0.1, FABLE)]), (1, [(0.5, OPUS), (0.25, FABLE)]),
           (1, [(0.2, OPUS), (0.4, FABLE)]), (1, [(0.6, OPUS), (0.2, FABLE)]),
           (1, [(0.9, OPUS), (0.05, FABLE)])]

    def fit(self, provider="anthropic"):
        return ex._sizing["weights"][provider]

    def fixture_rows(self):
        with open(SIZING_WEIGHTS_FIXTURE) as f:
            doc = json.load(f)
        return [((ACCT_A, wid), s["points"], s["usd"]) for wid, ss in doc["windows"].items()
                for s in ss]

    def test_two_models_in_a_varying_mix(self):
        rows = weight_rows(self.W, 12, 2.0)
        f = ex._sizing_fit(rows)
        m = f["models"]
        self.assertEqual(f["reference"], OPUS)
        self.assertGreater(m[OPUS]["dollars"], m[FABLE]["dollars"], "the most dollars")
        self.assertEqual((m[FABLE]["basis"], m[FABLE]["steps"]), ("weighed", 12))
        self.assertLess(m[FABLE]["rel_error"], 0.02)
        self.assertAlmostEqual(m[FABLE]["ratio"], 2.0, delta=2 * m[FABLE]["rel_error"] * 2.0)
        self.assertEqual(f["ratios"], {FABLE: m[FABLE]["ratio"]})
        self.assertEqual(m[OPUS], {"ratio": 1.0, "rel_error": None, "basis": "reference",
                                   "dollars": m[OPUS]["dollars"], "steps": 12})
        self.assertGreater(ex._sizing_estimate(weight_samples(rows))[2], 0.1)
        self.assertLess(ex._sizing_estimate(weight_samples(rows), f["ratios"])[2], 0.02,
                        "the mix leaves the spread; the noise stays")
        # Mostly Fable: Fable is the reference, and Opus weighs half of it.
        g = ex._sizing_fit(weight_rows(self.W, 12, 2.0, lo=0.5, hi=0.95))
        self.assertEqual(g["reference"], FABLE)
        self.assertAlmostEqual(g["ratios"][OPUS], 0.5, delta=2 * g["models"][OPUS]["rel_error"] * 0.5)

    def test_a_model_at_one_share_of_every_step_is_not_weighed(self):
        """Its dollars are the window's own, so the size takes up any weight:
        it is the one not determined, though it has more dollars than Fable,
        whose weight it would otherwise take down with it."""
        S5 = "claude-sonnet-5"
        f = ex._sizing_fit(weight_rows(self.W, 12, 2.0, lo=0.02, hi=0.25, fixed={S5: (0.3, 1.3)}))
        m = f["models"]
        self.assertGreater(m[S5]["dollars"], m[FABLE]["dollars"])
        self.assertEqual((m[S5]["basis"], m[S5]["ratio"], m[S5]["rel_error"]), ("unweighed", 1.0, None))
        self.assertEqual(m[FABLE]["basis"], "weighed")
        self.assertEqual(set(f["ratios"]), {FABLE})

    def test_a_model_at_a_fixed_share_of_the_reference_bends_no_weight(self):
        """Sonnet at exactly half of Opus's dollars in every step, Fable
        varying: Sonnet's weight and Fable's can't be told apart, Sonnet
        (more dollars) is fitted first, so Fable is the one not determined;
        alone, Sonnet's weight comes out below 0 and fails. Nothing is
        weighed and the window reads at list prices, as step 2 did: no
        wrong weight. Off exact by 2 %, Fable is weighed against Opus and
        Sonnet together (Sonnet carried at 1): 2.3 x 1.5 / 1.3."""
        S5 = "claude-sonnet-5"

        def rows(jitter):
            r = random.Random(3)
            out = []
            for k in range(20):
                o = r.uniform(1, 3)
                u = {OPUS: o, S5: 0.5 * o * (1 + jitter * r.uniform(-1, 1)),
                     FABLE: r.uniform(0.05, 0.3) * o}
                x = u[OPUS] + 0.6 * u[S5] + 2.3 * u[FABLE]
                out.append((self.W, x / 10 * (1 + 0.01 * r.uniform(-1, 1)), u))
            return out

        m = ex._sizing_fit(rows(0.0))["models"]
        self.assertGreater(m[S5]["dollars"], m[FABLE]["dollars"])
        self.assertEqual({k: (m[k]["basis"], m[k]["ratio"], m[k]["rel_error"]) for k in (S5, FABLE)},
                         {S5: ("unweighed", 1.0, None), FABLE: ("unweighed", 1.0, None)})
        f = ex._sizing_fit(rows(0.02))
        self.assertEqual(set(f["ratios"]), {FABLE})
        self.assertAlmostEqual(f["ratios"][FABLE], 2.3 * 1.5 / 1.3, delta=0.05)

    def test_candidates(self):
        """A candidate holds 1 % of a sample's dollars in 3 samples; one that
        fails the bound goes back to 1, its error shown, and Fable still
        passes."""
        H, S55, S5 = "claude-haiku-5-5", "claude-sonnet-5-5", "claude-sonnet-5"
        rows = weight_rows(self.W, 12, 2.0, noise=0.03, fixed={H: (0.005, 1.0)})
        for i in (2, 7):
            rows[i][2][S55] = 0.5            # 5 %, in two samples
        for i in (2, 5, 8):
            rows[i][2][S5] = 0.15            # 1.5 %, in three, lost in the noise
            rows[i][2][OPUS] -= 0.15
        m = ex._sizing_fit(rows)["models"]
        self.assertEqual((m[H]["basis"], m[H]["steps"], m[H]["rel_error"]), ("unweighed", 0, None))
        self.assertEqual((m[S55]["basis"], m[S55]["steps"], m[S55]["rel_error"]), ("unweighed", 2, None))
        self.assertEqual((m[S5]["basis"], m[S5]["steps"], m[S5]["ratio"]), ("unweighed", 3, 1.0))
        self.assertGreater(m[S5]["rel_error"], ex.SIZING_WEIGHT_AGREE)
        self.assertEqual(m[FABLE]["basis"], "weighed")

    def test_a_window_of_one_model_pins_only_its_own_size(self):
        rows = weight_rows(self.W, 12, 2.0, lo=0.05, hi=0.6)
        solo = [((ACCT_A, "scoped:Fable"), 1.0, {FABLE: 5.0 * (1 + 0.02 * (k % 3 - 1))})
                for k in range(4)]
        f, g = ex._sizing_fit(rows), ex._sizing_fit(rows + solo)
        self.assertEqual(g["reference"], OPUS)
        self.assertEqual(g["ratios"], f["ratios"])
        self.assertEqual((g["fit"]["windows"], g["fit"]["mixed"], g["models"][FABLE]["steps"]),
                         (2, 1, 16))
        # A provider of one model: nothing to weigh.
        h = ex._sizing_fit(solo)
        self.assertEqual((h["reference"], h["ratios"], h["fit"]["mixed"], h["fit"]["trimmed"]),
                         (FABLE, {}, 0, 0))

    def test_cheap_rows_leave_the_weights_and_stay_in_the_median(self):
        """Two steps of outside use, more points than their work: cheap at
        the fitted weights, so left out of them. Left in, they make Fable's
        weight many times less certain."""
        rows = [(k, p * (1.6 if i in (3, 9) else 1.0), u)
                for i, (k, p, u) in enumerate(weight_rows(self.W, 12, 2.0, noise=0.005))]
        f = ex._sizing_fit(rows)
        self.assertEqual(f["fit"]["trimmed"], 2)
        self.assertAlmostEqual(f["ratios"][FABLE], 2.0, delta=0.01)
        with mock.patch.object(ex, "SIZING_CHEAP", 1.0):      # nothing is cheap
            kept = ex._sizing_fit(rows)
        self.assertEqual(kept["fit"]["trimmed"], 0)
        self.assertGreater(kept["models"][FABLE]["rel_error"], 10 * f["models"][FABLE]["rel_error"])
        x = [sum(v * f["ratios"].get(m, 1.0) for m, v in u.items()) / p for _, p, u in rows]
        self.assertEqual(ex._sizing_estimate(weight_samples(rows), f["ratios"])[3], ex._median(x))

    def test_trimmed_steps_stay_in_the_windows_median(self):
        """Two steps of outside use, cheap at the weights: left out of the
        weights' fit, kept in the window's samples and its median (66 §5).
        The mix's steps are scaled a few percent either way, so the median
        with the two differs from the median without them."""
        scale = (1.0, 0.96, 1.02, 1.04, 0.98)
        mix = [(1, [(u * f, m) for u, m in usds]) for (_, usds), f in zip(self.MIX, scale)]
        outside = [(1, [(0.3, OPUS), (0.1, FABLE)]), (1, [(0.1, OPUS), (0.2, FABLE)])]
        evs, _ = self.walk(mix[:2] + outside[:1] + mix[2:4] + outside[1:] + mix[4:])
        self.feed(*evs)
        fit = self.fit()
        self.assertEqual((fit["fit"]["samples"], fit["fit"]["trimmed"]), (7, 2))
        self.assertAlmostEqual(fit["ratios"][FABLE], 2.0, delta=0.1)
        v = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]
        per = [s["weighted"] / s["points"] for s in v["samples"]]
        kept = [x for x in per if x >= (1 - ex.SIZING_CHEAP) * ex._median(per)]
        self.assertEqual((len(per), len(kept)), (7, 5))
        self.assertAlmostEqual(v["median_per_point"], ex._median(per), places=9)
        self.assertGreater(abs(ex._median(kept) - ex._median(per)), 0.005)

    def test_the_reference_moves_no_model_size(self):
        """The fit at Fable's reference is the same fit: Opus weighs one over
        Fable's weight with the same error, and every model size is the
        same."""
        rows = weight_rows(self.W, 12, 2.0) + [((ACCT_A, "scoped:Fable"), 1.0, {FABLE: 1.0 + 0.05 * k})
                                               for k in range(4)]
        f, g = ex._sizing_fit(rows), ex._sizing_fit(rows, ref=FABLE)
        self.assertEqual((f["reference"], g["reference"]), (OPUS, FABLE))
        self.assertAlmostEqual(g["ratios"][OPUS] * f["ratios"][FABLE], 1.0, places=8)
        self.assertAlmostEqual(g["models"][OPUS]["rel_error"], f["models"][FABLE]["rel_error"], places=8)
        for wid in ("seven_day", "scoped:Fable"):
            s = weight_samples([r for r in rows if r[0][1] == wid])
            for m in (OPUS, FABLE):
                a = ex._sizing_estimate(s, f["ratios"])[0] / f["models"][m]["ratio"]
                b = ex._sizing_estimate(s, g["ratios"])[0] / g["models"][m]["ratio"]
                self.assertAlmostEqual(a / b, 1.0, places=8, msg=(wid, m))

    def test_the_reference_follows_the_dollars_and_moves_no_model_size(self):
        """Fable-heavy steps take Fable past Opus's dollars: Fable becomes the
        reference, Opus weighs a half and the reference dollars halve; the
        model sizes stay where they were."""
        evs, bounds = self.walk(self.MIX + [(1, [(0.1, OPUS), (0.45, FABLE)])] * 6)
        split = bounds[5] + ex.SIZING_SETTLE_S + 1          # the mix's steps closed, no other
        self.feed(*[e for e in evs if e["read_at"] <= split])
        v = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]
        self.assertEqual((self.fit()["reference"], self.fit()["fit"]["samples"]), (OPUS, 5))
        self.assertEqual([round(x, 6) for x in (v["sizes"][OPUS], v["sizes"][FABLE], v["size_ref"])],
                         [100.0, 50.0, 100.0])
        self.feed(*[e for e in evs if e["read_at"] > split])
        fit = self.fit()
        self.assertEqual((fit["reference"], fit["fit"]["samples"]), (FABLE, 11))
        self.assertGreater(fit["models"][FABLE]["dollars"], fit["models"][OPUS]["dollars"])
        self.assertAlmostEqual(fit["ratios"][OPUS], 0.5, places=6)
        v = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]
        self.assertEqual([round(x, 6) for x in (v["sizes"][OPUS], v["sizes"][FABLE], v["size_ref"])],
                         [100.0, 50.0, 50.0])
        self.assertIn(f"sizing: anthropic weights: reference {FABLE}; {OPUS} weighed 0.500 (error 0.0%)",
                      self.logged)

    def test_a_scoped_windows_dollars_count_once(self):
        """A Fable request is in seven_day and in scoped:Fable both. The
        dollars that pick the reference are those of the windows that bind
        every model, so $2.50 more of Fable in scoped:Fable ($3.50 in all,
        against $3 of Opus) leaves Opus the reference."""
        evs, _ = self.walk(self.MIX)
        solo, _ = self.walk([(1, [0.5])] * 5, t0=self.T0 + 20000, window="scoped:Fable", model=FABLE)
        self.feed(*evs, *solo)
        fit = self.fit()
        self.assertEqual((fit["reference"], fit["fit"]["samples"]), (OPUS, 10))
        self.assertAlmostEqual(fit["models"][OPUS]["dollars"], 3.0, places=6)
        self.assertAlmostEqual(fit["models"][FABLE]["dollars"], 1.0, places=6)

    def test_the_fit_is_deterministic_and_settles_within_its_cap(self):
        rows = self.fixture_rows()
        f = ex._sizing_fit(rows)
        self.assertEqual(ex._sizing_fit(copy.deepcopy(rows)), f)
        steps, real = [], ex._sizing_fit_weights

        def spy(*a):
            out = real(*a)
            steps.append(out[2])
            return out
        with mock.patch.object(ex, "_sizing_fit_weights", spy):
            ex._sizing_fit(rows)
        self.assertTrue(steps and max(steps) < 10, steps)
        del steps[:]
        with mock.patch.object(ex, "_sizing_fit_weights", spy), \
                mock.patch.object(ex, "SIZING_WEIGHT_ITERS", 1):
            ex._sizing_fit(rows)
        self.assertEqual(max(steps), 1, "the cap holds")

    def test_the_live_samples(self):
        """testdata/sizing_weights.json (67 §1, §3): Fable's
        heaviest step looks cheap at list prices and is not at the weight,
        so nothing leaves the weight's fit and it comes out at 2.31."""
        rows = self.fixture_rows()
        f = ex._sizing_fit(rows)
        m = f["models"]
        self.assertEqual((f["reference"], set(f["ratios"])), (OPUS, {FABLE}))
        self.assertAlmostEqual(m[FABLE]["ratio"], 2.311, delta=0.005)
        self.assertLess(m[FABLE]["rel_error"], 0.05)
        self.assertEqual((m["claude-sonnet-5"]["basis"], m["claude-sonnet-5"]["ratio"]), ("unweighed", 1.0))
        self.assertGreater(m["claude-sonnet-5"]["rel_error"], ex.SIZING_WEIGHT_AGREE, "a candidate that fails")
        for other in ("claude-haiku-5-5", "claude-sonnet-5-5"):
            self.assertEqual((m[other]["basis"], m[other]["steps"]), ("unweighed", 0))
        self.assertEqual({k: f["fit"][k] for k in ("samples", "trimmed", "windows", "mixed")},
                         {"samples": 41, "trimmed": 0, "windows": 2, "mixed": 1})
        self.assertLess(f["fit"]["max_residual_points"], 0.06)
        seven = [r for r in rows if r[0][1] == "seven_day"]
        listed = [sum(u.values()) / p for _, p, u in seven]
        self.assertTrue(any(v < (1 - ex.SIZING_CHEAP) * ex._median(listed) for v in listed))
        self.assertGreater(ex._sizing_estimate(weight_samples(seven))[2], 0.1)
        size, basis, spread, _ = ex._sizing_estimate(weight_samples(seven), f["ratios"])
        self.assertEqual(basis, "learned")
        self.assertLess(spread, 0.02)
        self.assertAlmostEqual(size / 100, 30.97, delta=0.05, msg="Opus a point")
        self.assertAlmostEqual(size / 100 / f["ratios"][FABLE], 13.40, delta=0.05, msg="Fable a point")
        fable = weight_samples([r for r in rows if r[0][1] == "scoped:Fable"])
        self.assertAlmostEqual(ex._sizing_estimate(fable, f["ratios"])[0] / f["ratios"][FABLE] / 100,
                               6.45, delta=0.01)

    def test_one_model_reads_as_before(self):
        """No weighed model: the sizes are step 2's list-dollar median."""
        evs, _ = self.walk([(1, [1.0])] * 3 + [(1, [1.1])])
        self.feed(*evs)
        v = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]
        self.assertEqual((v["size"], v["size_ref"], v["sizes"]), (100.0, 100.0, {OPUS: 100.0}))
        self.assertEqual((v["unweighed_share"], v["own_weights"], v["weights_disagree"]), (0.0, {}, []))
        self.assertEqual((self.fit()["reference"], self.fit()["ratios"]), (OPUS, {}))
        self.assertFalse(any("weighted" in line for line in self.logged))

    def test_cheap_is_judged_at_the_weights(self):
        """A Fable-heavy step: $0.55 of list dollars for a point, a dollar of
        Opus at the weight. The step line shows both."""
        heavy = (1, [(0.1, OPUS), (0.45, FABLE)])
        evs, _ = self.walk(self.MIX + [heavy])
        self.feed(*evs)
        self.assertEqual(self.counts(), {"sample": 6})
        [line] = [l for l in self.logged if " $0.5500 " in l]
        self.assertIn(" $0.5500 (weighted $1.0000, unweighed 0%) over ", line)
        self.assertTrue(line.endswith(": sample"), line)
        # With nothing weighed the same step is cheap at list prices.
        reset_sizing()
        with mock.patch.object(ex, "SIZING_WEIGHT_STEPS_MIN", 99):
            evs, _ = self.walk(self.MIX + [heavy])
            self.feed(*evs)
        self.assertEqual(self.counts(), {"sample": 6, "cheap": 1})
        steps = [l for l in self.logged if " seven_day 16→17 " in l]
        self.assertTrue(steps[-1].endswith(" $0.5500 over 3 events: sample (cheap)"), steps)

    def test_sizing_and_metrics_publish_the_weights(self):
        evs, _ = self.walk(self.MIX)
        solo, _ = self.walk([(1, [0.3]), (1, [0.31]), (1, [0.29])], t0=self.T0 + 20000,
                            window="scoped:Fable", model=FABLE)
        self.feed(*evs, *solo)
        doc = ex.sizing_payload()
        wt = doc["weights"]["anthropic"]
        self.assertEqual(set(wt), {"reference", "models", "fit"})
        self.assertEqual((wt["reference"], wt["models"][FABLE]["basis"]), (OPUS, "weighed"))
        self.assertAlmostEqual(wt["models"][FABLE]["ratio"], 2.0, places=6)
        self.assertEqual((wt["fit"]["windows"], wt["fit"]["mixed"]), (2, 1))
        self.assertEqual({k: doc["constants"][k] for k in ("weight_agree", "weight_share",
                                                           "weight_steps_min", "weight_iters",
                                                           "weight_z")},
                         {"weight_agree": 0.25, "weight_share": 0.01, "weight_steps_min": 3,
                          "weight_iters": 200, "weight_z": 2.0})
        sd = doc["accounts"][ACCT_A]["windows"]["seven_day"]
        self.assertAlmostEqual(sd["size_ref"], 100.0, places=6)
        self.assertAlmostEqual(sd["size"], 80.0, places=6, msg="list dollars of its mix, $3 to $1")
        self.assertEqual(set(sd["sizes"]), {OPUS, FABLE})
        self.assertAlmostEqual(sd["sizes"][OPUS], 100.0, places=6)
        self.assertAlmostEqual(sd["sizes"][FABLE], 50.0, places=6)
        self.assertAlmostEqual(sd["median_per_point"], 1.0, places=6)
        self.assertAlmostEqual(sd["newest_per_point"], 1.0, places=6)
        self.assertEqual((sd["unweighed_share"], set(sd["own_weights"]), sd["weights_disagree"]),
                         (0.0, {FABLE}, []))
        for s in sd["samples"]:
            self.assertAlmostEqual(s["weighted"], 1.0, places=6)
        self.assertAlmostEqual(sd["outside"]["work_usd"], 4.0, places=6)
        self.assertAlmostEqual(sd["outside"]["work_weighted"], 5.0, places=6)
        self.assertAlmostEqual(sd["outside"]["outside_points"], 0.0, places=6)
        fb = doc["accounts"][ACCT_A]["windows"]["scoped:Fable"]
        self.assertEqual(set(fb["sizes"]), {FABLE}, "Opus does not run on it")
        self.assertAlmostEqual(fb["sizes"][FABLE], 30.0, places=6)
        self.assertAlmostEqual(fb["size"], 30.0, places=6)
        self.assertEqual(fb["own_weights"], {})
        metrics = "\n".join(ex._sizing_metric_lines())
        for line in (f'sizing_weight{{provider="anthropic",model="{OPUS}"}} 1.0',
                     f'sizing_weight{{provider="anthropic",model="{FABLE}"}} 2.0',
                     f'sizing_size_api_usd{{account="{ACCT_A}",window="seven_day"}} 80.0',
                     f'sizing_model_size_api_usd{{account="{ACCT_A}",window="seven_day",model="{FABLE}"}} 50.0',
                     f'sizing_model_size_api_usd{{account="{ACCT_A}",window="scoped:Fable",model="{FABLE}"}} 30.0',
                     f'sizing_weights_disagree{{account="{ACCT_A}",window="seven_day",model="{FABLE}"}} 0'):
            self.assertIn(line, metrics)
        self.assertNotIn(f'window="scoped:Fable",model="{OPUS}"', metrics)

    def test_the_per_window_check(self):
        """A window whose own fit is more than SIZING_WEIGHT_AGREE off the
        shared one is flagged, counted and logged as it starts and ends; one
        whose own fit is undetermined is not listed."""
        seven = weight_rows((ACCT_A, "seven_day"), 20, 2.0, seed=2)
        five = weight_rows((ACCT_A, "five_hour"), 6, 4.0, pi=2.0, seed=3)
        month = [((ACCT_A, "monthly"), 1.0, {OPUS: 3.0 * (1 + 0.01 * (k % 2)),
                                             FABLE: 1.0 * (1 + 0.01 * (k % 2))}) for k in range(5)]

        def put(*windows):
            with ex._sizing_lock:
                ex._sizing_acct(ACCT_A, "anthropic")
                for rows in windows:
                    ex._sizing_win(ACCT_A, rows[0][0][1], "header")["samples"] = \
                        weight_samples(rows, self.T0)
                ex._sizing_refit("anthropic")

        put(seven, five, month)
        fit = self.fit()
        shared, own = fit["models"][FABLE]["ratio"], fit["own"][(ACCT_A, "five_hour")][FABLE]["ratio"]
        self.assertAlmostEqual(own, 4.0, delta=0.1)
        self.assertGreater(abs(own - shared), ex.SIZING_WEIGHT_AGREE * shared)
        self.assertEqual(fit["disagree"], {(ACCT_A, "five_hour"): [FABLE]})
        self.assertEqual(set(fit["own"]), {(ACCT_A, "seven_day"), (ACCT_A, "five_hour")})
        view = ex.sizing_payload()["accounts"][ACCT_A]["windows"]
        self.assertEqual((view["five_hour"]["weights_disagree"], view["seven_day"]["weights_disagree"],
                          view["monthly"]["own_weights"]), ([FABLE], [], {}))
        metrics = "\n".join(ex._sizing_metric_lines())
        lab = f'account="{ACCT_A}",window="%s",model="{FABLE}"'
        self.assertIn(f"sizing_weights_disagree{{{lab % 'five_hour'}}} 1", metrics)
        self.assertIn(f"sizing_weights_disagree{{{lab % 'seven_day'}}} 0", metrics)
        self.assertNotIn(f"sizing_weights_disagree{{{lab % 'monthly'}}}", metrics)
        said = [l for l in self.logged if " weights disagree" in l or " agree again" in l]
        self.assertEqual(said, [f"sizing: {ACCT_A} five_hour weights disagree: {FABLE} {own:.3f} here, "
                                f"{shared:.3f} shared"])
        put()                                       # no change: nothing more said
        self.assertEqual(len([l for l in self.logged if " weights " in l and "agree" in l]), 1)
        put(weight_rows((ACCT_A, "five_hour"), 6, 2.0, pi=2.0, seed=3))
        self.assertEqual(self.fit()["disagree"], {})
        self.assertIn(f"sizing: {ACCT_A} five_hour weights agree again", self.logged)

    def test_a_window_apart_by_its_noise_alone_is_not_flagged(self):
        """The own weight must be past SIZING_WEIGHT_AGREE of the shared one
        and past SIZING_WEIGHT_Z of its own standard error (67 §3)."""
        apart = ex._sizing_apart
        self.assertFalse(apart({"ratio": 2.6, "rel_error": 0.13}, 2.0), "0.6 apart, within 2 x 0.34")
        self.assertTrue(apart({"ratio": 2.6, "rel_error": 0.05}, 2.0), "0.6 apart, past 2 x 0.13")
        self.assertFalse(apart({"ratio": 2.4, "rel_error": 0.01}, 2.0), "within a quarter of 2")
        self.assertTrue(apart({"ratio": 1.4, "rel_error": 0.01}, 2.0))

    def test_a_model_the_shared_fit_does_not_weigh_is_not_compared(self):
        """Sonnet is determined in a quiet window and lost in a noisy one's
        noise, so the shared fit leaves it at 1. That 1 is no weight: the
        quiet window's own 0.6 is published and not flagged."""
        S5 = "claude-sonnet-5"
        r = random.Random(1)

        def window(acct, noise):
            out = []
            for k in range(24):
                f, s = r.uniform(0.05, 0.6), r.uniform(0.01, 0.3)
                u = {OPUS: 1 - f - s, FABLE: f, S5: s}
                x = u[OPUS] + 2.3 * f + 0.6 * s
                out.append(((acct, "seven_day"), x / 10 * (1 + noise * r.uniform(-1, 1)), u))
            return out

        with ex._sizing_lock:
            for a, rows in ((ACCT_A, window(ACCT_A, 0.01)), (ACCT_B, window(ACCT_B, 0.30))):
                ex._sizing_acct(a, "anthropic")
                ex._sizing["accounts"][a]["data_t"] = self.T0 + 1e5
                ex._sizing_win(a, "seven_day", "header")["samples"] = weight_samples(rows, self.T0)
            ex._sizing_refit("anthropic")
        fit = self.fit()
        self.assertEqual(fit["models"][S5]["basis"], "unweighed")
        self.assertAlmostEqual(fit["own"][(ACCT_A, "seven_day")][S5]["ratio"], 0.6, delta=0.05)
        self.assertEqual(fit["disagree"], {})
        self.assertFalse(any(" weights disagree" in l for l in self.logged))

    def test_a_restart_found_in_two_windows_refits_once(self):
        """A reporter's restart takes back the samples its old boot's last
        event fell in, here in seven_day and scoped:Fable: the weights are
        fitted again once, as the event that found it ends, without them."""
        evs, bounds = self.walk(self.MIX, close=False)
        for e in evs:
            if "seven_day" in e["readings"]:
                e["readings"]["scoped:Fable"] = dict(e["readings"]["seven_day"])
        T11, T16 = bounds[0], bounds[-1]
        # scoped:Fable moves with seven_day's points here, so its dollars a
        # point swing with the mix: no growth retire, every sample kept.
        with mock.patch.object(ex, "SIZING_SHIFT", 99.0):
            for e in sorted(evs + [self.ev(T11 + 30, T11 + 40, boot="b2")], key=lambda e: e["read_at"]):
                self.feed(e, reporter="p2" if e["boot"] == "b2" else "p1")
            self.feed(self.ev(T16 + 700, T16 + 701), reporter="p1")
        self.assertEqual(self.fit()["fit"]["samples"], 10)
        with mock.patch.object(ex, "_sizing_refit", wraps=ex._sizing_refit) as refit:
            self.feed(self.ev(T16 + 800, T16 + 801, boot="b3"), reporter="p2")
        self.assertEqual(refit.call_count, 1)
        self.assertEqual([self.win(w)["retracted"] for w in ("seven_day", "scoped:Fable")],
                         [{"proxy_restart": 1}] * 2)
        self.assertEqual(self.fit()["fit"]["samples"], 8)
        self.assertAlmostEqual(self.fit()["ratios"][FABLE], 2.0, places=6)

    def test_a_late_event_takes_its_sample_out_of_the_weights(self):
        evs, bounds = self.walk(self.MIX)
        self.feed(*evs, reporter="p1")
        self.assertEqual(self.fit()["fit"]["samples"], 5)
        self.feed(self.ev(bounds[0] + 5, bounds[0] + 10, usd=0.5, boot="b2"), reporter="p2")
        self.assertEqual(self.win()["retracted"], {"late": 1})
        self.assertEqual(self.fit()["fit"]["samples"], 4)

    def test_a_mix_change_retires_nothing(self):
        """66 §5's growth retire reads weighted dollars. Fable-heavy steps and
        then Opus alone, a dollar of Opus a point throughout: at list prices
        the Opus steps look half as dear again and retire the rest."""
        heavy = [(1, [(0.2, OPUS), (0.4, FABLE)]), (1, [(0.4, OPUS), (0.3, FABLE)]),
                 (1, [(0.1, OPUS), (0.45, FABLE)]), (1, [(0.6, OPUS), (0.2, FABLE)])]
        evs, _ = self.walk(heavy + [(1, [1.0])] * 3)
        self.feed(*evs)
        self.assertEqual(len(self.win()["samples"]), 7)
        self.assertFalse(any("retired" in l for l in self.logged))
        reset_sizing()
        with mock.patch.object(ex, "SIZING_WEIGHT_STEPS_MIN", 99):
            evs, _ = self.walk(heavy + [(1, [1.0])] * 3)
            self.feed(*evs)
        self.assertEqual(len(self.win()["samples"]), 3)
        self.assertTrue(any(l.endswith("samples retired: the window grew") for l in self.logged))

    def test_a_reprice_refits_and_a_dropped_model_leaves(self):
        S55 = "claude-sonnet-5-5"
        evs, _ = self.walk(self.MIX + [(1, [(0.3, OPUS), (0.3, FABLE), (0.1, S55)])])
        self.feed(*evs)
        self.assertAlmostEqual(self.fit()["ratios"][FABLE], 2.0, places=6)
        self.assertEqual(self.fit()["models"][S55]["basis"], "unweighed")
        with ex._sizing_lock:
            ex._sizing_save(open_state=True)
        doubled = {"standard": {k: 2 * v for k, v in ex.API_LIST_PRICES[FABLE]["standard"].items()}}
        with mock.patch.dict(ex.API_LIST_PRICES, {FABLE: doubled}):
            ex._sizing_load()
            self.assertAlmostEqual(self.fit()["ratios"][FABLE], 1.0, places=6)
            with ex._sizing_lock:
                ex._sizing_save(open_state=True)
        with mock.patch.dict(ex.API_LIST_PRICES):
            del ex.API_LIST_PRICES[S55]
            ex._sizing_load()
            self.assertTrue(any("1 dropped" in l for l in self.logged))
            self.assertNotIn(S55, self.fit()["models"])
            self.assertAlmostEqual(self.fit()["ratios"][FABLE], 2.0, places=6)

    def test_the_file_keeps_no_weights_and_a_start_refits(self):
        evs, _ = self.walk(self.MIX)
        self.feed(*evs)
        before = copy.deepcopy(self.fit())
        with ex._sizing_lock:
            ex._sizing_save(open_state=True)
        with open(ex.SIZING_FILE) as f:
            doc = json.load(f)
        self.assertEqual(doc["v"], 1)
        self.assertNotIn("weights", doc)
        self.assertNotIn("ratio", json.dumps(doc))
        del self.logged[:]
        ex._sizing_load()
        self.assertAlmostEqual(self.fit()["ratios"][FABLE], before["ratios"][FABLE], places=9)
        self.assertIn(f"sizing: anthropic weights: reference {OPUS}; {FABLE} weighed 2.000 (error 0.0%)",
                      self.logged)
        # A start without the open state knows no provider until the
        # account's next event, which fits its weights.
        with ex._sizing_lock:
            ex._sizing_save()
        ex._sizing_load()
        self.assertEqual(ex._sizing["weights"], {})
        last = max(e["read_at"] for e in evs)
        self.feed(self.ev(last + 10, last + 11, used=16.0, boot="b2"))
        self.assertAlmostEqual(self.fit()["ratios"][FABLE], before["ratios"][FABLE], places=9)


def reset_sizing_memory():
    """A new process: no sizing state in memory, the file left as it is."""
    with ex._sizing_lock:
        ex._sizing = ex._sizing_fresh()
        ex._sizing_gate.update(closing=False, inflight=0, frozen=None, set_aside=None)


class TestSizingPublication(HTTPBase, SizingBase):
    """66 §9: /metrics and GET /sizing, a pure read; nothing in /preferred."""

    def setUp(self):
        SizingBase.setUp(self)

    def tearDown(self):
        SizingBase.tearDown(self)

    def test_sizing_lists_every_measured_account(self):
        code, p = self.get("/sizing")
        self.assertEqual(code, 200)
        a = p["accounts"]
        self.assertEqual(a[ACCT_A]["windows"]["seven_day"]["size_basis"], "none")
        self.assertEqual(a[ACCT_A]["windows"]["seven_day"]["size_mode"], "header")
        self.assertNotIn("five_hour", a[ACCT_A]["windows"])
        self.assertEqual(a["gpt"]["windows"]["seven_day"]["size_mode"], "header")
        self.assertEqual(a["kimi"]["windows"]["total_quota"]["size_mode"], "poll")
        self.assertEqual(p["constants"]["settle_s"], 600)

    def test_sizing_and_metrics_render(self):
        evs, _ = self.walk([(1, [1.0])] * 3 + [(1, [])])
        self.feed(*evs)
        _, before = self.get("/preferred")
        code, p = self.get("/sizing")
        v = p["accounts"][ACCT_A]["windows"]["seven_day"]
        self.assertEqual((v["size"], v["size_basis"], v["samples_n"]), (100.0, "learned", 3))
        self.assertEqual(v["steps"], {"sample": 3, "silent": 1})
        self.assertEqual(v["outside"]["outside_points"], 1.0)
        self.assertEqual(v["open"]["anchor"]["q"], 15.0)
        self.assertEqual(v["samples"][0]["by"]["claude-opus-5-5|standard|standard"]["usd"], 1.0)
        t = ex.render()
        L = lambda name, **lab: [l for l in t.splitlines()  # noqa: E731
                                 if l.startswith(name + "{") and all(f'{k}="{v}"' in l
                                                                     for k, v in lab.items())]
        lab = {"account": ACCT_A, "window": "seven_day"}
        self.assertEqual(L("sizing_size_api_usd", **lab)[0].split()[-1], "100.0")
        self.assertEqual(L("sizing_samples", **lab)[0].split()[-1], "3")
        self.assertEqual(L("sizing_step_api_usd", **lab)[0].split()[-1], "1.0")
        self.assertEqual(L("sizing_spread", **lab)[0].split()[-1], "0.0")
        self.assertEqual(L("sizing_steps_total", outcome="silent", **lab)[0].split()[-1], "1")
        self.assertEqual(L("sizing_outside_points", **lab)[0].split()[-1], "1.0")
        self.assertEqual(L("sizing_measured_points", **lab)[0].split()[-1], "4.0")
        self.assertEqual(L("sizing_outside_pct", **lab)[0].split()[-1], "25.0")
        self.assertEqual(L("sizing_events_dropped_total", account=ACCT_A)[0].split()[-1], "0")
        self.assertEqual(L("sizing_events_late_total", account=ACCT_A)[0].split()[-1], "0")
        self.assertEqual(L("sizing_size_api_usd", account="gpt"), [], "present only while learned")
        self.assertEqual(L("sizing_samples", account="gpt", window="seven_day")[0].split()[-1], "0")
        for fam in ("sizing_size_api_usd", "sizing_steps_total", "sizing_outside_pct"):
            self.assertIn(f"# TYPE {fam} ", t)
        _, after = self.get("/preferred")
        for k in ("accounts", "routes", "classes", "lanes"):
            self.assertEqual(after[k], before[k])
        self.assertEqual(after["fleet"], before["fleet"], "a read of /sizing moves nothing")

    def test_sizing_is_a_pure_read(self):
        self.assertIn("/sizing", ex._PURE_READS)
        with mock.patch.object(ex, "_events_mark") as mark:
            self.get("/sizing")
        mark.assert_not_called()


class TestFleetAsks(unittest.TestCase):
    """68 §3.2: what only the owner can fix, per selection over the accounts
    it reaches, and on the proxy."""

    NOW = 1793740000

    def figure(self, health=None, limits=None, recheck_ago=None):
        gpt = {"provider": "gpt", "windows": {
            "seven_day": {"period_s": WEEK, "used": 10, "resets_in": 100000}}}
        if recheck_ago is not None:
            gpt["recheck_ago"] = recheck_ago
        return fleet_case_figure(room_case(
            {"sub-A": anth(seven=10), "sub-B": anth(seven=10), "gpt": gpt},
            classes=[fable_class("anth"),
                     {"class": "g", "members": [{"kind": "provider", "provider": "gpt",
                                                 "model": "gpt-6.1-sol", "priority": 1,
                                                 "stop_at": None}]}],
            health=health, limits=limits))

    def test_auth_on_the_selections_that_reach_the_account(self):
        f = self.figure(health={"sub-B": {"kind": "auth", "sign_in": "oauth"},
                                "sub-A": {"kind": "none", "sign_in": "oauth"}})
        ask = {"account": "sub-B", "kind": "auth", "sign_in": "oauth", "model": None, "until": None}
        self.assertEqual(f["selections"]["anth"]["asks"], [ask])
        self.assertEqual(f["selections"]["lane:opus"]["asks"], [ask])
        self.assertEqual(f["selections"]["g"]["asks"], [], "g cannot reach sub-B")
        self.assertEqual(f["proxy"]["asks"], [ask])

    def test_no_ask_from_transient_or_rate_limit(self):
        f = self.figure(health={"sub-A": {"kind": "transient", "sign_in": "oauth"},
                                "gpt": {"kind": "rate_limit", "sign_in": "oauth"}})
        self.assertEqual(f["proxy"]["asks"], [])
        self.assertEqual(f["selections"]["g"]["asks"], [])

    def test_not_entitled_with_its_model_and_until(self):
        f = self.figure(health={"gpt": {"kind": "auth", "sign_in": "oauth"}},
                        limits={"gpt": [{"limit": "model:gpt-6.1-sol", "class": "not_entitled",
                                         "until_in": 21600},
                                        {"limit": "pool:credits", "class": "spent",
                                         "until_in": 3600}]})
        self.assertEqual(f["selections"]["g"]["asks"], [
            {"account": "gpt", "kind": "auth", "sign_in": "oauth", "model": None, "until": None},
            {"account": "gpt", "kind": "not_entitled", "sign_in": "oauth", "model": "gpt-6.1-sol",
             "until": self.NOW + 21600}], "auth first; a spent limit raises none")
        self.assertEqual(f["selections"]["anth"]["asks"], [])

    def test_not_entitled_only_where_its_limit_binds(self):
        # A Fable model's ask: on the class and the Fable lane, whose
        # sessions dial Fable; not on the other lanes, which never do. The
        # proxy lists it; an auth ask stays on every selection reaching it.
        f = self.figure(health={"sub-A": {"kind": "auth", "sign_in": "oauth"}},
                        limits={"sub-A": [{"limit": "model:claude-fable-5-1",
                                           "class": "not_entitled", "until_in": 600}]})
        kinds = {k: [x["kind"] for x in v["asks"]] for k, v in f["selections"].items()}
        self.assertEqual(kinds["anth"], ["auth", "not_entitled"])
        self.assertEqual(kinds["lane:fable"], ["auth", "not_entitled"])
        for lane in ("lane:opus", "lane:sonnet", "lane:haiku", "lane:default"):
            self.assertEqual(kinds[lane], ["auth"], lane)
        self.assertEqual([x["kind"] for x in f["proxy"]["asks"]], ["auth", "not_entitled"])
        # A pool binds every selection.
        f = self.figure(limits={"sub-A": [{"limit": "pool:p", "class": "not_entitled",
                                           "until_in": 600}]})
        self.assertEqual(len(f["selections"]["lane:haiku"]["asks"]), 1)

    def test_auth_sorts_first_across_accounts(self):
        f = self.figure(health={"sub-B": {"kind": "auth", "sign_in": "oauth"}},
                        limits={"sub-A": [{"limit": "model:claude-fable-5-1",
                                           "class": "not_entitled", "until_in": 600}]})
        self.assertEqual([(x["kind"], x["account"]) for x in f["selections"]["anth"]["asks"]],
                         [("auth", "sub-B"), ("not_entitled", "sub-A")])
        self.assertEqual([(x["kind"], x["account"]) for x in f["proxy"]["asks"]],
                         [("auth", "sub-B"), ("not_entitled", "sub-A")])

    def test_not_entitled_ends_at_its_until_and_at_a_recheck(self):
        lim = {"gpt": [{"limit": "model:gpt-6.1-sol", "class": "not_entitled", "until_in": -1}]}
        self.assertEqual(self.figure(limits=lim)["proxy"]["asks"], [], "past its until")
        lim = {"gpt": [{"limit": "model:gpt-6.1-sol", "class": "not_entitled", "until_in": 600,
                        "refused_ago": 120}]}
        self.assertEqual(len(self.figure(limits=lim)["proxy"]["asks"]), 1)
        self.assertEqual(self.figure(limits=lim, recheck_ago=60)["proxy"]["asks"], [],
                         "the owner's re-check after the refusal")

    def test_sign_in_from_the_providers_auth_kind(self):
        rows = {"accounts": [
            {"name": "max-a", "provider": "anthropic", "last_error_kind": "auth"},
            {"name": "gpt", "provider": "gpt", "last_error_kind": "none"},
            {"name": "kimi", "provider": "kimi", "last_error_kind": "auth"}]}
        self.assertEqual(ex._fleet_health(rows), {
            "max-a": {"kind": "auth", "sign_in": "oauth"},
            "gpt": {"kind": "none", "sign_in": "oauth"},
            "kimi": {"kind": "auth", "sign_in": "key"}})
        self.assertNotIn("auth", ex.PROVIDERS["local"], "local has no row to raise an ask")


class TestFleetAsksOnTheDocument(SizingBase):
    """68 §3.2: /preferred reads account_health before _plock for its asks;
    /metrics reads none, and takes no lock a token refresh holds across its
    network call."""

    def test_the_document_carries_an_auth_ask(self):
        real_health = ex._unified_account_health

        def health(now):
            self.assertFalse(ex._plock.locked(), "account_health is read before _plock")
            return real_health(now)
        with ex._lock:
            ex._state[ACCT_A] = dict(ex._state[ACCT_A], ok=0, http=401)
        with mock.patch.object(ex, "_unified_account_health", side_effect=health):
            doc = ex._preferred_payload()
        self.assertIn({"account": ACCT_A, "kind": "auth", "sign_in": "oauth", "model": None,
                       "until": None}, doc["fleet"]["proxy"]["asks"])
        self.assertEqual(doc["account_health"]["worst"], "auth_dead", "the document's own rows")

    def test_metrics_read_no_account_health(self):
        # account_health takes _refresh_lock, which a token refresh holds
        # across its network call; the gauges read no asks, so /metrics
        # never asks for it, and its fleet gauges are the same.
        before = ex.render()
        with mock.patch.object(ex, "_unified_account_health",
                               side_effect=AssertionError("/metrics read account_health")):
            after = ex.render()
        fleet = lambda text: [l for l in text.splitlines() if l.startswith("fleet_")]  # noqa: E731
        self.assertTrue(fleet(before))
        self.assertEqual(fleet(after), fleet(before))


class TestFleetDocument(SizingBase):
    """The fleet in the brain's document and on /metrics (67 §8)."""

    def view(self):
        return fleet_view({ACCT_A: {"seven_day": (1000.0, {"claude-opus-5-5": 1000.0})}})

    def test_document_carries_fleet(self):
        with mock.patch.object(ex, "_sizing_fleet_view", return_value=self.view()):
            f = ex._preferred_payload()["fleet"]
        self.assertEqual(f["unit"], "api_usd")
        self.assertEqual(set(f), {"unit", "proxy", "selections", "accounts"})
        self.assertIn("lane:opus", f["selections"])
        a = f["selections"]["lane:opus"]["accounts"][ACCT_A]
        self.assertEqual((a["capacity"], a["room"], a["valued"]), (1000.0, 900.0, "claude-opus-5-5"))
        self.assertEqual(f["accounts"][ACCT_A]["limits"], [], "no limit held")
        listed = {e["account"]: e for e in f["proxy"]["listed"]}
        self.assertEqual(listed[ACCT_B], {"account": ACCT_B, "why": "unmeasured", "kind": "unread",
                                          "window": None, "left_pct": None, "refused": None,
                                          "resting": False, "resting_until": None})
        self.assertEqual((listed["kimi"]["why"], listed["kimi"]["kind"]),
                         ("total_quota not sized yet (none)", "learning"))

    def test_the_view_is_taken_outside_plock(self):
        # _sizing_lock is never held around _plock (67 §8).
        def view():
            self.assertFalse(ex._plock.locked(), "the sizing view is taken before _plock")
            return self.view()
        with mock.patch.object(ex, "_sizing_fleet_view", side_effect=view) as v:
            ex._preferred_payload()
            ex.render()
        self.assertEqual(v.call_count, 2)

    def test_the_view_reuses_the_sizing_numbers(self):
        evs, _ = self.walk([(1, [1.0])] * 3)
        self.feed(*evs)
        w = ex._sizing_fleet_view()["windows"][ACCT_A]["seven_day"]
        s = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]
        self.assertEqual((w["basis"], w["mix"], w["sizes"], w["samples"]),
                         (s["size_basis"], s["size"], s["sizes"], s["samples_n"]))
        self.assertEqual(w["basis"], "learned")
        f = ex._preferred_payload()["fleet"]
        a = f["selections"]["lane:opus"]["accounts"][ACCT_A]
        self.assertEqual((a["capacity"], a["room"]), (100.0, 90.0), "$1 a point, 10 of 100 used")

    def test_a_hold_zeroes_room_and_keeps_capacity(self):
        t = time.time()
        out, _ = post_reports(rep_refusal(1, t - 5, until=t + 3600, by_reading=False))
        self.assertEqual(out, [("applied", None)])
        with mock.patch.object(ex, "_sizing_fleet_view", return_value=self.view()):
            f = ex._preferred_payload()["fleet"]
        a = f["selections"]["lane:opus"]["accounts"][ACCT_A]
        self.assertEqual((a["capacity"], a["room"], a["windows"]["seven_day"]["used"]),
                         (1000.0, 0.0, 100.0))
        self.assertEqual(a["refused"]["why"], "spent")
        self.assertEqual(f["accounts"][ACCT_A]["left_pct"], 0.0)

    def test_a_limit_zeroes_the_bound_room_through_reports(self):
        # 67 §9: a model limit reported by the proxy refuses the selections
        # its lane scope binds, capacity kept, and moves nothing the dial
        # check reads (§13 Q6).
        t = time.time()
        before = live_ctx()["checks"]
        out, _ = post_reports(rep_limit(1, t - 5, "model:claude-opus-4-7", until=t + 1800))
        self.assertEqual(out, [("applied", None)])
        self.assertEqual(live_ctx()["checks"], before, "the dial check is unchanged")
        with mock.patch.object(ex, "_sizing_fleet_view", return_value=self.view()):
            f = ex._preferred_payload()["fleet"]
        a = f["selections"]["lane:opus"]["accounts"][ACCT_A]
        self.assertEqual((a["capacity"], a["room"], a["refused"]),
                         (1000.0, 0.0, {"why": "spent", "limit": "model:claude-opus-4-7",
                                        "until": t + 1800}))
        self.assertIsNone(f["selections"]["lane:fable"]["accounts"][ACCT_A]["refused"],
                          "another lane is not bound")
        self.assertEqual(f["accounts"][ACCT_A]["limits"],
                         [{"limit": "model:claude-opus-4-7", "class": "spent", "until": t + 1800}])
        self.assertEqual((f["accounts"][ACCT_A]["room"], f["accounts"][ACCT_A]["refused"]),
                         (900.0, None), "a model limit does not bind the whole proxy")

    def test_two_renders_a_minute_apart(self):
        # fleet flips only by a rule (a reset passing, a reading going
        # stale), as the routes' checks do; a minute alone moves nothing (64 §2).
        t = time.time()
        with mock.patch.object(ex, "_sizing_fleet_view", return_value=self.view()):
            with mock.patch.object(ex.time, "time", return_value=t):
                a = ex._preferred_payload()
            with mock.patch.object(ex.time, "time", return_value=t + 60):
                b = ex._preferred_payload()
        self.assertEqual(json.dumps(a["fleet"]), json.dumps(b["fleet"]))
        self.assertEqual(ex._preferred_digest(a), ex._preferred_digest(b))

    def test_a_sample_close_marks_the_document(self):
        # 67 §8: a sample close marks the event stream after _sizing_after
        # saves; a served event that closes nothing does not.
        evs, _ = self.walk([(1, [1.0])] * 3)
        with mock.patch.object(ex, "_events_mark") as mark:
            self.feed(evs[0])
            mark.assert_not_called()
            self.feed(*evs[1:])
            mark.assert_called()
            self.assertTrue(os.path.exists(ex.SIZING_FILE), "saved before the mark")

    def test_a_new_document_follows_a_sample_close(self):
        ex._events_publish()
        self.assertFalse(ex._events_publish(), "nothing changed")
        evs, _ = self.walk([(1, [1.0])] * 3)
        self.feed(*evs)
        self.assertTrue(ex._events_publish(), "the fleet moved with the sizes")
        with ex._events_cond:
            doc = json.loads(ex._events_pub["data"])
        self.assertEqual(doc["fleet"]["accounts"][ACCT_A]["size_basis"], "learned")

    def test_a_late_event_in_a_silent_step_moves_no_fleet(self):
        # The review's probe: a late served event with no readings whose work
        # falls in a silent step moves /sizing's outside share, never fleet
        # (64 §2, 67 §8): fleet carries no outside_pct.
        evs, bounds = self.walk([(1, [1.0])] * 3 + [(1, [])])
        self.feed(*evs, reporter="p1")
        ex._events_publish()
        before = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]["outside"]["outside_pct"]
        self.feed(self.ev(bounds[3] + 5, bounds[3] + 10, usd=0.5, boot="b2"), reporter="p2")
        after = ex.sizing_payload()["accounts"][ACCT_A]["windows"]["seven_day"]["outside"]["outside_pct"]
        self.assertNotEqual(before, after, "the probe reaches the silent step")
        self.assertFalse(ex._events_publish())
        self.assertNotIn("outside_pct", json.dumps(ex._preferred_payload()["fleet"]))

    def test_an_account_that_learns_its_provider_resizes(self):
        # A start without the open state keeps samples but no provider; the
        # account's next event fits its weights, which moves its sizes.
        evs, _ = self.walk([(1, [1.0])] * 3)
        self.feed(*evs)
        with ex._sizing_lock:
            ex._sizing_save()
        reset_sizing_memory()
        with mock.patch.object(ex, "_events_mark") as mark:
            ex._sizing_load()
        mark.assert_called_once_with()
        self.assertIsNone((ex._sizing["accounts"].get(ACCT_A) or {}).get("provider"))
        with ex._sizing_lock:
            ex._sizing.pop("resized", None)
            ex._sizing_acct(ACCT_A, "anthropic")
            self.assertTrue(ex._sizing.pop("resized", False))
            ex._sizing_acct(ACCT_A, "anthropic")
            self.assertNotIn("resized", ex._sizing, "once known, no refit")

    def test_the_gauges_render(self):
        with mock.patch.object(ex, "_sizing_fleet_view", return_value=self.view()):
            t = ex.render()
        L = lambda fam, **lab: [l for l in t.splitlines()  # noqa: E731
                                if l.startswith(fam + "{") and all(f'{k}="{v}"' in l
                                                                   for k, v in lab.items())]
        self.assertEqual(L("fleet_capacity_api_usd", level="proxy", name="proxy")[0].split()[-1],
                         "1000.0")
        self.assertEqual(L("fleet_room_api_usd", level="selection", name="lane:opus")[0].split()[-1],
                         "900.0")
        self.assertEqual(L("fleet_left_pct", level="account", name=ACCT_A)[0].split()[-1], "90.0")
        # A listed account: its own left_pct, no dollars (67 §13 Q7).
        self.assertEqual(L("fleet_capacity_api_usd", level="account", name="gpt"), [])
        self.assertEqual(len(L("fleet_left_pct", level="account", name="gpt")), 1)
        self.assertEqual(L("fleet_left_pct", level="account", name=ACCT_B), [], "unmeasured")
        for fam in ("fleet_capacity_api_usd", "fleet_room_api_usd", "fleet_left_pct"):
            self.assertIn(f"# TYPE {fam} gauge", t)
        self.assertIn("For an account, its main window's own (cap - used) / cap", t)


class TestLocalRoutes(PayBase):
    """The local class on live state (#135): one account, `local`; a measured
    reading reads included with no money; never measured reads no (unknown)."""

    def test_route_accounts_and_check(self):
        classes_reset(opus_acct(30, resets=True), local_result(), cfg=[local_class_cfg()])
        r = ex._preferred_payload()["routes"]["local/local/local-gemma4-12b"]
        self.assertEqual((r["provider"], r["accounts"], r["pay"]), ("local", ["local"], []))
        self.assertEqual(route_check("local/local/local-gemma4-12b", "local")["check"], "included")
        st = ex._preferred_payload()["accounts"]["local"]
        self.assertEqual((st["kind"], st["allowance"]["open"], st["money"]["credits"]["open"]),
                         ("subscription", True, False))

    def test_members_read_ok_and_usable(self):
        # A fresh, ok poll of an unmetered account is the whole answer: no
        # number is missing, so each member reads ok/usable, not no_quota_data.
        classes_reset(opus_acct(30, resets=True), local_result(), cfg=[local_class_cfg()])
        members = ex._preferred_payload()["classes"]["local"]["members"]
        self.assertEqual([(m["id"], m["status"], m["usable"], m["targets"]) for m in members],
                         [("local/local-gemma4-12b", "ok", True, {"local": None}),
                          ("local/local-gpt-oss-20b", "ok", True, {"local": None}),
                          ("local/local-qwen3-coder-30b", "ok", True, {"local": None})])

    def test_stale_members_read_stale(self):
        classes_reset(opus_acct(30, resets=True), local_result(good_age=ex.STALE_MAX + 100),
                      cfg=[local_class_cfg()])
        m = ex._preferred_payload()["classes"]["local"]["members"][0]
        self.assertEqual((m["status"], m["usable"]), ("stale", False))

    def test_unmetered_is_locals_alone(self):
        # Only a reading marked unmetered skips the number: every other
        # provider's reading lacks the mark, and an open allowance with no
        # windows that is not marked still reads no_quota_data.
        self.assertTrue(ex._unmetered(local_result()))
        for r in (kimi_result(), gpt_result(), runway_result()):
            self.assertFalse(ex._unmetered(r), r["provider"])
        unmarked = local_result()
        unmarked["reading"]["pots"][0].pop("unmetered")
        classes_reset(opus_acct(30, resets=True), cfg=[local_class_cfg()])
        with ex._lock:
            ex._state["local"] = unmarked
        m = ex._preferred_payload()["classes"]["local"]["members"][0]
        self.assertEqual((m["status"], m["usable"]), ("no_quota_data", False))

    def test_bind_lands_on_gemma(self):
        classes_reset(opus_acct(30, resets=True), local_result(), cfg=[local_class_cfg()])
        code, p = self.bind(cname="local")
        self.assertEqual(code, 200)
        self.assertEqual((p["route"], p["check"]["check"]),
                         ("local/local/local-gemma4-12b", "included"))

    def test_never_measured_is_unknown(self):
        classes_reset(opus_acct(30, resets=True), cfg=[local_class_cfg()])
        with mock.patch.object(ex, "_provider_poll_targets",
                               return_value={"local": ("local", None)}):
            ex.compute_preferred()
            c = route_check("local/local/local-gemma4-12b", "local")
        self.assertEqual((c["check"], c["reason"]), ("no", "unknown"))


class TestAnthropicApiProvider(AuthHTTP):
    """The anthropic-api driver: static keys enrolled on /auth, polled with a
    free model list, read as an open unmetered allowance, serving the lanes
    beside the account loop's, and dispensed by /api-key."""

    KEY = "test-api-key-one"

    def setUp(self):
        super().setUp()
        d = tempfile.mkdtemp(dir=_TMP)
        self.addCleanup(setattr, papi, "ANTHROPIC_API_CREDS_FILE", papi.ANTHROPIC_API_CREDS_FILE)
        papi.ANTHROPIC_API_CREDS_FILE = os.path.join(d, "anthropic-api.json")
        self.addCleanup(setattr, papi, "_keys", None)
        papi._keys = None
        self.seen = []
        orig = papi._get_models
        self.addCleanup(setattr, papi, "_get_models", orig)
        papi._get_models = lambda key: self.seen.append(key) or {"data": []}
        reset(acct(ACCT_A, 30), acct(ACCT_B, 45))

    def enroll(self, name, key=KEY):
        code, d = self.post("/auth/anthropic-api/begin", {}, key="test-switch-key")
        self.assertEqual(code, 200, d)
        return self.post("/auth/anthropic-api/complete",
                         {"state": d["state"], "name": name, "api_key": key}, key="test-switch-key")

    def poll_into_state(self, name):
        res = papi.poll(None, account=name)
        res["good_ts"] = res["ts"]
        with ex._lock:
            ex._state[name] = res
        return res

    def test_enrolment_validates_then_stores_the_key_owner_only(self):
        code, d = self.enroll("team-a")
        self.assertEqual((code, d["account"], d["reauthorized"]), (200, "team-a", False))
        self.assertEqual(self.seen, [self.KEY], "validated with one model list")
        with open(papi.ANTHROPIC_API_CREDS_FILE) as f:
            self.assertEqual(json.load(f), [{"name": "team-a", "api_key": self.KEY}])
        self.assertEqual(os.stat(papi.ANTHROPIC_API_CREDS_FILE).st_mode & 0o777, 0o600)
        code, d = self.enroll("team-a", key="test-api-key-two")
        self.assertEqual((code, d["reauthorized"]), (200, True), "a name enrolled again is re-keyed")
        self.assertEqual(papi._load(), {"team-a": "test-api-key-two"})

    def test_a_refused_key_saves_nothing(self):
        def refuse(key):
            raise urllib.error.HTTPError(papi.ANTHROPIC_API_BASE + "/v1/models", 401, "no", {}, None)
        papi._get_models = refuse
        code, d = self.enroll("team-a")
        self.assertEqual(code, 422)
        self.assertIn("HTTP 401", d["error"])
        self.assertNotIn(self.KEY, d["error"])
        self.assertFalse(os.path.exists(papi.ANTHROPIC_API_CREDS_FILE))

    def test_a_name_another_account_holds_is_refused(self):
        for name in (ACCT_A, "kimi", "bad name"):
            code, d = self.enroll(name)
            self.assertEqual(code, 422, name)
        self.assertFalse(os.path.exists(papi.ANTHROPIC_API_CREDS_FILE))

    def test_the_auth_page_lists_accounts_by_fingerprint_never_the_key(self):
        self.enroll("team-a")
        code, d = self.get("/auth/providers", key="test-switch-key")
        rows = d["providers"]["anthropic-api"]["accounts"]
        self.assertEqual([r["name"] for r in rows], ["team-a"])
        self.assertEqual(rows[0]["key_fingerprint"], ex._key_fingerprint(self.KEY))
        self.assertNotIn(self.KEY, json.dumps(d))

    def test_the_poll_reads_an_open_unmetered_allowance(self):
        self.enroll("team-a")
        res = papi.poll(None, account="team-a")
        self.assertEqual((res["ok"], res["kind"], res["provider"]), (1, "provider", "anthropic-api"))
        self.assertEqual(res["reading"]["kind"], "api")
        self.assertTrue(ex._unmetered(res))
        def refuse(key):
            raise urllib.error.HTTPError(papi.ANTHROPIC_API_BASE + "/v1/models", 401, "no", {}, None)
        papi._get_models = refuse
        res = papi.poll(None, account="team-a")
        self.assertEqual((res["ok"], res["http"], ex._classify_poll_error(res)), (0, 401, "auth"))

    def test_its_accounts_serve_every_lane_as_api_accounts(self):
        self.enroll("team-a")
        self.poll_into_state("team-a")
        ex.compute_preferred()
        p = ex._preferred_payload()
        self.assertEqual(p["accounts"]["team-a"]["kind"], "api")
        self.assertNotIn("anthropic-api", p["providers"], "no pool: its accounts are lane accounts")
        for ln in ex._lane_names:
            route = p["routes"][f"lane:{ln}"]
            self.assertIn("team-a", route["accounts"], ln)
            self.assertEqual(route["checks"]["team-a"]["check"], "included", ln)

    def test_api_key_dispenses_its_key_under_the_switch_key(self):
        self.enroll("team-a")
        self.assertEqual(self.get("/api-key?account=team-a")[0], 401)
        code, d = self.get("/api-key?account=team-a", key="test-switch-key")
        self.assertEqual((code, d), (200, {"account": "team-a", "api_key": self.KEY}))

    def test_no_keys_no_rows(self):
        """Without a creds file the provider adds nothing a payload carries."""
        self.assertFalse(ex.PROVIDERS["anthropic-api"]["enabled"]())
        self.assertNotIn("anthropic-api", {pid for pid, _ in ex._provider_poll_targets().values()})
        self.assertEqual(ex.PROVIDERS["anthropic-api"]["auth"]["status"](time.time()), [])


class TestReauthPageKeyed(HTTPBase):
    """/reauth is keyed like /auth: without the switch key, the key shell."""

    def test_without_the_key_the_shell_asks_for_it(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/reauth")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req, timeout=5)
        body = cm.exception.read().decode()
        self.assertEqual(cm.exception.code, 401)
        self.assertIn("This page needs the switch key.", body)
        self.assertNotIn("Re-authorize", body)



# ---- the prepaid notices (notices.py) -------------------------------------------

import notices  # noqa: E402

BRIDGE = "http://bridge.test/alert"
N_T0 = 1_800_000_000.0


class FakeBridge:
    """The alert bridge: records each post and answers from `replies`, a list
    of (status, body dict) or an exception, the last one repeating."""

    def __init__(self, *replies):
        self.replies = list(replies) or [(200, {"telegram": True, "ha": True})]
        self.posts = []

    def __call__(self, url, body):
        self.posts.append((url, body))
        r = self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]
        if isinstance(r, Exception):
            raise r
        return r[0], json.dumps(r[1]).encode()

    def kinds(self):
        return [b["alerts"][0]["labels"]["alertname"] for _, b in self.posts]


def nview(balance=None, read_at=None, dry=False, **over):
    v = {"name": "Acme", "serves": "image generation", "billing_url": "https://pay.example/acme",
         "dry": dry, "balance": balance, "read_at": read_at, "unit": "acme_credit",
         "resets_at": None, "classes": [{"class": "images", "covered_by": "other/model-x"},
                                        {"class": "images-acme", "covered_by": None}]}
    v.update(over)
    return v


class NoticesBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="notices-")
        self.path = os.path.join(self.dir, "notices.json")
        self.bridge = FakeBridge()
        self.logs = []
        self.env = mock.patch.dict(os.environ, {"SWITCH_BRIDGE_URL": BRIDGE})
        self.env.start()
        self.n = self.engine()

    def tearDown(self):
        self.env.stop()
        shutil.rmtree(self.dir, ignore_errors=True)

    def engine(self):
        return notices.Notices(self.path, post=self.bridge, log=self.logs.append)

    def tick(self, at, **v):
        """A tick at `at`, then the worker's pass at the same moment."""
        self.n.tick({"acme": nview(**v)}, at)
        self.n.deliver(at)
        return self.rec()

    def rec(self):
        return self.n._accounts["acme"]

    def doc(self, **v):
        return self.n.payload({"acme": nview(**v)})["accounts"]["acme"]


class TestNoticesCycle(NoticesBase):
    """ok -> low -> dry -> ok, each message once, and nothing on the way back."""

    def test_ok_low_dry_ok(self):
        H = 3600
        self.assertEqual(self.tick(N_T0, balance=1000, read_at=N_T0)["state"], "ok")
        self.tick(N_T0 + 12 * H, balance=800, read_at=N_T0 + 12 * H)
        rec = self.tick(N_T0 + 24 * H, balance=600, read_at=N_T0 + 24 * H)
        self.assertEqual(rec["state"], "ok", "400 a day, 600 left: more than a day")
        self.assertEqual(self.bridge.posts, [])
        rec = self.tick(N_T0 + 36 * H, balance=380, read_at=N_T0 + 36 * H)
        self.assertEqual((rec["state"], rec["since"], rec["warned_at"]),
                         ("low", N_T0 + 36 * H, N_T0 + 36 * H))
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow"])
        pace = self.doc(balance=380)["pace"]
        self.assertEqual(pace, {"per_day": 413.33, "unit": "acme_credit", "days_left": 0.92})
        self.tick(N_T0 + 37 * H, balance=300, read_at=N_T0 + 37 * H)
        self.assertEqual(len(self.bridge.posts), 1, "one warning per cycle")
        rec = self.tick(N_T0 + 40 * H, balance=0, read_at=N_T0 + 40 * H, dry=True)
        self.assertEqual((rec["state"], rec["dry_sent_at"]), ("dry", N_T0 + 40 * H))
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow", "PrepaidAccountDry"])
        rec = self.tick(N_T0 + 41 * H, balance=5000, read_at=N_T0 + 41 * H)
        self.assertEqual((rec["state"], rec["warned_at"], rec["end_at"]), ("ok", None, N_T0 + 41 * H))
        self.assertEqual(len(self.bridge.posts), 2, "no it's-back message")
        doc = self.doc(balance=5000)
        self.assertEqual([e["kind"] for e in doc["sent"]], ["low", "dry"])
        self.assertTrue(all(e["ok"] and e["telegram"] and e["ha"] for e in doc["sent"]))
        labels = self.bridge.posts[1][1]["alerts"][0]["labels"]
        self.assertEqual(labels, {"alertname": "PrepaidAccountDry", "alertroute": "prepaid",
                                  "delivery": "critical", "account": "acme"})
        self.assertEqual(self.bridge.posts[0][0], BRIDGE)

    def test_a_recharge_after_a_warning_is_followed_by_no_ran_dry(self):
        H = 3600
        for i, b in enumerate((1000, 600, 300)):
            self.tick(N_T0 + i * 18 * H, balance=b, read_at=N_T0 + i * 18 * H)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow"])
        rec = self.tick(N_T0 + 40 * H, balance=2000, read_at=N_T0 + 40 * H)
        self.assertEqual((rec["state"], rec["warned_at"], rec["pending"]), ("ok", None, None))
        for i in range(1, 30):
            at = N_T0 + 40 * H + i * 300
            self.tick(at, balance=2000 - i, read_at=at)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow"], "no ran-dry, no second warning")

    def test_a_rise_clears_a_warning_still_due(self):
        self.bridge.replies = [(502, {"telegram": False, "ha": False})]
        H = 3600
        for i, b in enumerate((1000, 600, 300)):
            self.tick(N_T0 + i * 18 * H, balance=b, read_at=N_T0 + i * 18 * H)
        self.assertIsNotNone(self.rec()["pending"])
        rec = self.tick(N_T0 + 40 * H, balance=2000, read_at=N_T0 + 40 * H)
        self.assertIsNone(rec["pending"])
        n = len(self.bridge.posts)
        self.tick(N_T0 + 41 * H, balance=2000, read_at=N_T0 + 41 * H)
        self.assertEqual(len(self.bridge.posts), n, "nothing due after the rise")

    def test_fewer_than_24h_of_samples_warn_nothing(self):
        H = 3600
        for i, b in enumerate((1000, 500, 100, 10)):
            rec = self.tick(N_T0 + i * 6 * H, balance=b, read_at=N_T0 + i * 6 * H)
        self.assertEqual(rec["state"], "ok")
        self.assertIsNone(self.doc(balance=10)["pace"])
        self.assertEqual(self.bridge.posts, [])

    def test_no_burn_is_no_pace(self):
        for i in range(4):
            self.tick(N_T0 + i * notices.DAY, balance=5, read_at=N_T0 + i * notices.DAY)
        self.assertIsNone(self.doc(balance=5)["pace"])
        self.assertEqual(len(self.rec()["samples"]), 4, "one sample a day while it holds")


class TestNoticesReminders(NoticesBase):
    def test_a_reminder_exactly_every_24h_while_dry(self):
        dry_at = N_T0 + 7
        self.tick(dry_at, dry=True)
        self.assertEqual(self.rec()["dry_sent_at"], dry_at)
        at = dry_at
        while at < dry_at + 3 * notices.DAY + 3600:
            at += 300
            self.tick(at, dry=True)
        sent = self.doc(dry=True)["sent"]
        self.assertEqual([(e["kind"], e["at"] - dry_at) for e in sent],
                         [("dry", 0), ("reminder", notices.DAY), ("reminder", 2 * notices.DAY),
                          ("reminder", 3 * notices.DAY)])
        self.assertEqual(self.rec()["last_reminder_at"], dry_at + 3 * notices.DAY)
        body = self.bridge.posts[1][1]["alerts"][0]
        self.assertEqual(body["labels"]["alertname"], "PrepaidAccountDryReminder")
        self.assertEqual(body["annotations"]["title"], "Acme still needs payment")
        self.assertIn("Image generation stopped since Jan 15 08:00 UTC.",
                      body["annotations"]["description"])

    def test_a_brain_down_for_days_sends_one_reminder(self):
        self.tick(N_T0, dry=True)
        self.tick(N_T0 + 3.5 * notices.DAY, dry=True)
        self.tick(N_T0 + 3.6 * notices.DAY, dry=True)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry", "PrepaidAccountDryReminder"])
        self.assertEqual(self.rec()["last_reminder_at"], N_T0 + 3 * notices.DAY)
        self.tick(N_T0 + 4 * notices.DAY, dry=True)
        self.assertEqual(len(self.bridge.posts), 3, "the schedule holds to the ran-dry message")

    def test_leaving_dry_stops_the_reminders(self):
        self.tick(N_T0, dry=True)
        self.tick(N_T0 + 100, dry=False, recheck_at=N_T0 + 50)
        self.tick(N_T0 + 2 * notices.DAY, dry=False, recheck_at=N_T0 + 50)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"])


class TestNoticesDryRule(NoticesBase):
    """One rule of dry for every prepaid pot: a check reading no, or a shut
    report, starts it; only money showing (a rise, a counted charge after the
    start, the owner's re-check) ends it; and at most one ran-dry or reminder
    message a day, however it flaps."""
    H = 3600

    def shut(self, at, shut_at, balance=500, read_at=None, **v):
        """A tick of a readable pot with credits left that the proxy reported
        shut at shut_at ("not enough credits for this job")."""
        return self.tick(at, balance=balance, read_at=read_at or N_T0, shut_at=shut_at, **v)

    def test_a_shut_with_credits_left_holds_until_a_rise(self):
        self.tick(N_T0, balance=500, read_at=N_T0)
        rec = self.shut(N_T0 + 60, N_T0 + 50)
        self.assertEqual((rec["state"], rec["dry_at"]), ("dry", N_T0 + 50))
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"])
        for i in range(1, 6):  # polls reading the same 500, no rise
            at = N_T0 + i * self.H
            rec = self.tick(at, balance=500, read_at=at, shut_at=N_T0 + 50)
        self.assertEqual(rec["state"], "dry", "a positive balance without a rise is no money shown")
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"], "no new message")
        rec = self.tick(N_T0 + 6 * self.H, balance=480, read_at=N_T0 + 6 * self.H, shut_at=N_T0 + 50)
        self.assertEqual(rec["state"], "dry", "a fall is no money shown either")
        rec = self.tick(N_T0 + 7 * self.H, balance=5000, read_at=N_T0 + 7 * self.H, shut_at=N_T0 + 50)
        self.assertEqual((rec["state"], rec["end_at"]), ("ok", N_T0 + 7 * self.H))
        rec = self.tick(N_T0 + 8 * self.H, balance=5000, read_at=N_T0 + 8 * self.H, shut_at=N_T0 + 50)
        self.assertEqual(rec["state"], "ok", "the resent shut, older than the rise, starts nothing")
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"])

    def test_a_charge_after_the_shut_ends_it(self):
        self.tick(N_T0, balance=500, read_at=N_T0)
        self.shut(N_T0 + 60, N_T0 + 50, charged_at=N_T0 - 10)
        rec = self.shut(N_T0 + 120, N_T0 + 50, charged_at=N_T0 + 40)
        self.assertEqual(rec["state"], "dry", "a charge before the shut is no money shown")
        rec = self.shut(N_T0 + 180, N_T0 + 50, charged_at=N_T0 + 170)
        self.assertEqual((rec["state"], rec["end_at"]), ("ok", N_T0 + 170))
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"], "no it's-back message")

    def test_a_dry_check_ends_at_the_owners_recheck(self):
        rec = self.tick(N_T0, dry=True, read_at=N_T0 - 30)
        self.assertEqual((rec["state"], rec["dry_at"]), ("dry", N_T0 - 30))
        rec = self.tick(N_T0 + 60, dry=True, read_at=N_T0 - 30, recheck_at=N_T0 + 55)
        self.assertEqual(rec["state"], "ok")
        rec = self.tick(N_T0 + 90, dry=True, read_at=N_T0 - 30, recheck_at=N_T0 + 55)
        self.assertEqual(rec["state"], "ok", "the check on the reading before the re-check starts nothing")
        rec = self.tick(N_T0 + 400, dry=True, read_at=N_T0 + 390, recheck_at=N_T0 + 55)
        self.assertEqual(rec["state"], "dry", "a reading after it, still no money, starts a dry")

    def test_a_second_dry_within_a_day_keeps_the_first_messages_schedule(self):
        H = self.H
        self.tick(N_T0, balance=500, read_at=N_T0)
        self.shut(N_T0 + 60, N_T0 + 60)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"])
        rec = self.shut(N_T0 + H, N_T0 + 60, charged_at=N_T0 + H - 1)
        self.assertEqual(rec["state"], "ok")
        rec = self.shut(N_T0 + 5 * H, N_T0 + 5 * H)  # dry again, 5 h after the message
        self.assertEqual(rec["state"], "dry")
        self.tick(N_T0 + 23 * H, balance=500, read_at=N_T0, shut_at=N_T0 + 5 * H)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"], "no second ran-dry")
        rec = self.tick(N_T0 + 60 + notices.DAY, balance=500, read_at=N_T0, shut_at=N_T0 + 5 * H)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry", "PrepaidAccountDryReminder"])
        self.assertEqual(rec["last_reminder_at"], N_T0 + 60 + notices.DAY,
                         "the reminder 24 h after the first message")
        # Ended, then dry again more than a day after the last message: its own ran-dry.
        self.shut(N_T0 + 30 * H, N_T0 + 5 * H, charged_at=N_T0 + 30 * H)
        later = N_T0 + 60 + 2 * notices.DAY + H
        rec = self.shut(later, later)
        self.assertEqual(rec["state"], "dry")
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry", "PrepaidAccountDryReminder",
                                               "PrepaidAccountDry"])


class TestNoticesRestart(NoticesBase):
    def test_a_restart_replays_the_file_and_resends_nothing(self):
        self.tick(N_T0, dry=True)
        self.assertEqual(len(self.bridge.posts), 1)
        with open(self.path) as f:
            self.assertEqual(json.load(f)["accounts"]["acme"]["state"], "dry")
        self.n = self.engine()  # the brain restarted
        self.tick(N_T0 + 600, dry=True)
        self.assertEqual(len(self.bridge.posts), 1, "never resent")
        self.assertEqual([e["kind"] for e in self.doc(dry=True)["sent"]], ["dry"])
        self.n = self.engine()
        self.tick(N_T0 + notices.DAY, dry=True)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry", "PrepaidAccountDryReminder"],
                         "the reminder keeps the ran-dry message's schedule across a restart")

    def test_an_unreadable_file_starts_empty_and_says_so(self):
        with open(self.path, "w") as f:
            f.write("{not json")
        self.assertEqual(self.n.load(), 0)
        self.assertTrue(any("cannot read" in m for m in self.logs), self.logs)


class TestNoticesDelivery(NoticesBase):
    def test_any_channel_delivered_is_sent_each_channel_recorded(self):
        # Telegram down, the push channel up: sent once, never re-pushed. The real bridge's
        # reply also carries ok/critical/delivered booleans; they are not channels.
        self.bridge.replies = [(200, {"telegram": False, "ha": True, "ok": True, "critical": True, "delivered": True})]
        rec = self.tick(N_T0, dry=True)
        e = self.doc(dry=True)["sent"][0]
        self.assertEqual((e["ok"], e["telegram"], e["ha"], e["channels"], e["detail"], e["tries"]),
                         (True, False, True, {"telegram": False, "ha": True}, "telegram not delivered", 1))
        self.assertEqual((rec["dry_sent_at"], rec["pending"]), (N_T0, None))
        for i in range(1, 30):
            self.tick(N_T0 + i * 60, dry=True)
        self.assertEqual(len(self.bridge.posts), 1)

    def test_a_total_failure_is_retried_under_one_id_with_backoff(self):
        self.bridge.replies = [(502, {"telegram": False, "ha": False}),
                               (200, {"telegram": False, "ha": False}),
                               urllib.error.URLError("refused"),
                               (200, {}),
                               (200, {"telegram": True, "ha": False})]
        posts_at = []
        at = N_T0
        while len(self.bridge.posts) < 5 and at < N_T0 + 3 * 3600:
            n = len(self.bridge.posts)
            self.tick(at, dry=True)
            if len(self.bridge.posts) > n:
                posts_at.append(at - N_T0)
            at += 15
        self.assertEqual(posts_at, [0, 60, 180, 420, 900], "1 min, doubling")
        sent = self.doc(dry=True)["sent"]
        self.assertEqual(len(sent), 1, "one notice, its tries recorded on it")
        self.assertEqual((sent[0]["ok"], sent[0]["tries"], sent[0]["detail"]), (True, 5, "ha not delivered"))
        self.assertEqual(self.rec()["dry_sent_at"], N_T0 + 900, "marked sent only once delivered")
        self.assertEqual(sum("not delivered (" in m for m in self.logs), 1, "one log line, not one a try")

    def test_the_backoff_caps_at_30_minutes(self):
        self.assertEqual([notices._retry_after(k) for k in (1, 2, 3, 5, 6, 7, 20)],
                         [60, 120, 240, 960, 1800, 1800, 1800])

    def test_what_each_answer_is(self):
        o = notices.outcome
        self.assertEqual(o(200, b'{"telegram": true, "ha": true}'), (True, {"telegram": True, "ha": True}, None))
        self.assertEqual(o(200, b'{"telegram": false, "ha": false}')[::2], (False, "no channel delivered"))
        self.assertEqual(o(200, b"not json")[::2], (False, "no channel delivered"))
        self.assertEqual(o(503, b'{"telegram": true}')[::2], (False, "bridge HTTP 503"))

    def test_a_hanging_bridge_never_holds_the_tick(self):
        release = threading.Event()

        def hang(url, body):
            release.wait(10)
            return 200, b'{"telegram": true}'
        self.n.post = hang
        self.n.start()
        try:
            t0 = time.monotonic()
            for i in range(5):
                self.n.tick({"acme": nview(dry=True)}, N_T0 + i)
                self.n.kick()
            self.assertLess(time.monotonic() - t0, 1.0)
        finally:
            release.set()
            self.n.stop()

    def test_a_502_is_a_failure(self):
        self.bridge.replies = [(502, {"telegram": False, "ha": False})]
        self.tick(N_T0, dry=True)
        e = self.doc(dry=True)["sent"][0]
        self.assertEqual((e["ok"], e["detail"]), (False, "bridge HTTP 502"))
        self.assertIsNone(self.rec()["dry_sent_at"])

    def test_no_bridge_address_is_off_and_says_why(self):
        with mock.patch.dict(os.environ, {"SWITCH_BRIDGE_URL": ""}):
            self.tick(N_T0, dry=True)
            doc = self.n.payload({"acme": nview(dry=True)})
        self.assertEqual((doc["enabled"], doc["why"]), (False, "no bridge address"))
        self.assertEqual(doc["accounts"]["acme"]["state"], "dry")
        self.assertEqual(self.bridge.posts, [])

    def test_the_default_address_when_unset(self):
        old = notices._bridge_default
        try:
            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("SWITCH_BRIDGE_URL", None)
                notices.set_bridge_default("")
                self.assertEqual(notices.bridge_url(), "")
                notices.set_bridge_default("http://edition.test/alert")
                self.assertEqual(notices.bridge_url(), "http://edition.test/alert")
        finally:
            notices.set_bridge_default(old)

    def test_send_test_marks_the_message_and_records_nothing(self):
        doc = {"notices": {"accounts": {"acme": {"name": "Acme", "serves": "image generation",
                                                 "billing_url": "https://pay.example/acme",
                                                 "classes": []}}}}
        with mock.patch("urllib.request.urlopen") as uo:
            uo.return_value.__enter__.return_value.read.return_value = json.dumps(doc).encode()
            out = notices.send_test("acme", post=self.bridge)
        self.assertEqual(out, {"ok": True, "telegram": True, "ha": True,
                               "channels": {"telegram": True, "ha": True}, "detail": None})
        ann = self.bridge.posts[0][1]["alerts"][0]["annotations"]
        self.assertEqual(ann["title"], "[test] Acme ran dry: needs payment")
        self.assertFalse(os.path.exists(self.path))


class TestNoticesWords(unittest.TestCase):
    def test_the_class_coverage_text(self):
        v = nview()
        _, _, desc = notices.message("dry", "acme", v)
        self.assertEqual(desc, "Image generation stopped. Class images still has other/model-x; "
                               "images-acme has nothing else. Top up: https://pay.example/acme")
        _, _, desc = notices.message("dry", "acme", nview(classes=[], serves="music generation",
                                                          resets_at=N_T0, billing_url=None))
        self.assertEqual(desc, "Music generation stopped. Nothing else covers it. "
                               "Credits reset Jan 15.")
        title, _, desc = notices.message(
            "low", "acme", nview(balance=1240, pace={"per_day": 1100.0, "span_days": 7}))
        self.assertEqual(title, "Acme: about a day of credit left")
        self.assertEqual(desc, "1,240 credits left; the last 7 days spent about 1,100 a day. "
                               "Image generation stops when it runs out; class images still has "
                               "other/model-x; images-acme has nothing else. "
                               "Top up: https://pay.example/acme")
        title, _, _ = notices.message("dry", "acme", nview(name=None))
        self.assertEqual(title, "acme ran dry: needs payment", "no name: the account id")


class TestNoticesInTheDocument(PayBase):
    """/preferred `notices` from the core: off in the tests (no bridge), an
    api account with topup off listed, one whose pot no dial draws (the
    Anthropic API's, entitlement none) not."""

    def test_shape(self):
        classes_reset(opus_acct(30, resets=True))
        doc = ex._preferred_payload()["notices"]
        self.assertEqual(doc, {"enabled": False, "why": "no bridge address", "accounts": {}})

    def test_an_api_account_whose_pot_no_dial_draws_is_not_prepaid(self):
        with mock.patch.object(papi, "_load", return_value={"key-a": "sk-test"}):
            now = time.time()
            api = read({"name": "key-a", "kind": "provider", "provider": "anthropic-api", "ok": 1,
                        "ts": now, "good_ts": now, "stale": 0, "http": 0, "err": "",
                        "retry_after": None, "raw": {}})
            classes_reset(opus_acct(30, resets=True), api)
            self.assertEqual(ex._preferred_payload()["accounts"]["key-a"]["kind"], "api")
            self.assertEqual(ex._preferred_payload()["notices"]["accounts"], {})


class TestNoticesRecharge(NoticesBase):
    """A recharge leaves more than a day's burn (or is any rise while the
    pace is unknown); a smaller rise ends neither a warning's cycle nor a dry."""

    def burn(self, days=3, per_day=100, start=None):
        """Samples falling per_day a day for `days` days, from start (default
        days * per_day + 90, so they end at 90). -> the last sample's time."""
        start = days * per_day + 90 if start is None else start
        for d in range(days + 1):
            at = N_T0 + d * notices.DAY
            self.tick(at, balance=start - d * per_day, read_at=at)
        return N_T0 + days * notices.DAY

    def test_a_wobble_near_zero_warns_once(self):
        t = self.burn()  # 100 a day, 90 left: low
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow"])
        for i, b in enumerate((95, 85, 92), 1):
            rec = self.tick(t + i * 3600, balance=b, read_at=t + i * 3600)
            self.assertEqual(rec["state"], "low")
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow"], "one warning")
        rec = self.tick(t + 5 * 3600, balance=1000, read_at=t + 5 * 3600)
        self.assertEqual((rec["state"], rec["warned_at"]), ("ok", None), "a recharge ends the cycle")

    def test_a_refund_after_a_ran_dry_ends_nothing(self):
        t = self.burn(start=390)  # ends at 90, warned
        rec = self.tick(t + 3600, balance=0, read_at=t + 3600, dry=True)
        self.assertEqual(rec["state"], "dry")
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow", "PrepaidAccountDry"])
        rec = self.tick(t + 7200, balance=5, read_at=t + 7200, dry=False)
        self.assertEqual(rec["state"], "dry", "a 5-credit refund is no recharge")
        self.tick(t + 3 * 3600, balance=5, read_at=t + 3 * 3600)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountLow", "PrepaidAccountDry"], "no warning")

    def test_any_rise_is_a_recharge_while_the_pace_is_unknown(self):
        self.tick(N_T0, balance=0, read_at=N_T0, dry=True)
        rec = self.tick(N_T0 + 60, balance=5, read_at=N_T0 + 60)
        self.assertEqual(rec["state"], "ok")

    def test_no_warning_within_a_day_of_a_ran_dry(self):
        t = self.burn(start=500)  # 100 a day, ends at 200: ok
        self.tick(t + 60, balance=200, read_at=t, shut_at=t + 30)  # a shut with credits left
        self.tick(t + 120, balance=200, read_at=t, charged_at=t + 90)  # a charge: ok again
        rec = self.tick(t + 3600, balance=90, read_at=t + 3600)
        self.assertEqual(rec["state"], "low")
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry"], "no warning within 24 h of it")
        self.tick(t + 60 + notices.DAY, balance=90, read_at=t + 3600)
        self.assertEqual(self.bridge.kinds(), ["PrepaidAccountDry", "PrepaidAccountLow"])

    def test_no_warning_when_the_pot_resets_first(self):
        t = self.burn()  # 90 left at 100 a day: 0.9 days
        self.n.reset()
        self.bridge.posts.clear()
        for d in range(4):
            at = N_T0 + d * notices.DAY
            rec = self.tick(at, balance=390 - d * 100, read_at=at, resets_at=at + 12 * 3600)
        self.assertEqual(rec["state"], "ok", "the credits reset in 12 h, before the 0.9 days")
        self.assertEqual(self.bridge.posts, [])
        rec = self.tick(t + 60, balance=90, read_at=t, resets_at=t + 2 * notices.DAY)
        self.assertEqual(rec["state"], "low", "a reset after it runs out does not help")


if __name__ == "__main__":
    unittest.main(verbosity=2)
