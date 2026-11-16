"""The local-model driver (shared): one account, `local`, an OpenAI-compatible
llama-server router on the LAN. Enabled only while LOCAL_UPSTREAM is set."""
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


# A llama-server router (#135): the `local` class's one account. Its address is
# the deployment's (LOCAL_UPSTREAM, declared beside the proxy's in compose), and
# the provider is enabled only while it is set. GET /models checks each declared
# member exists and pins its context_window, without loading a model.
# LOCAL_PLAN_LABEL names the machine in the reading's plan.
LOCAL_UPSTREAM = os.environ.get("LOCAL_UPSTREAM", "").rstrip("/")
LOCAL_PLAN_LABEL = os.environ.get("LOCAL_PLAN_LABEL", "local model server")


# A local model server (#135): a machine, not a plan. Its allowance is open while the
# poll succeeds (the reading's age is the dial check's, as for every account);
# it has no windows and no money pot to draw.

def _local_declared():
    """{model: context_window} of every local member the class document declares."""
    out = {}
    for c in core._classes:
        for m in c.get("members") or []:
            if m.get("kind") == "provider" and m.get("provider") == "local":
                out[m["model"]] = m.get("context_window")
    return out


def _local_models(upstream):
    """Router GET /models only: inventory/presets, never an autoloading request."""
    req = urllib.request.Request(upstream + "/models", headers={"Accept": "application/json"})
    with core.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _local_context(model):
    """(context, basis) without loading a model (llama-server b12110).

    A running child's meta.n_ctx is its actual per-slot window. Before first
    load status.args is empty, but status.preset is the merged, canonical INI
    (including router CLI overrides). Check its explicit ctx-size; never mistake
    context_length (the GGUF training window) for the configured context.
    This proves configuration, not allocation/fit/per-slot size before a load.
    """
    meta = model.get("meta") or {}
    if meta.get("n_ctx") is not None:
        return meta["n_ctx"], "serves context"
    status = model.get("status") or {}
    preset = status.get("preset")
    if isinstance(preset, str):
        for line in preset.splitlines():
            match = re.fullmatch(r"\s*ctx-size\s*=\s*(\d+)\s*", line)
            if match:
                return int(match[1]) or None, "configures context"
    # args is populated after a spawn and persists across unload. b12110's
    # common_preset::to_args emits canonical long flags.
    args = status.get("args") or []
    for i, arg in enumerate(args[:-1]):
        if arg == "--ctx-size":
            try:
                return int(args[i + 1]) or None, "configures context"
            except (ValueError, TypeError):
                break
    return None, "configures context"


def _local_check(models, declared):
    """First missing member/window mismatch from non-loading GET /models.

    Unloaded presets must pin ctx-size: an auto-sized context cannot be verified
    without a load. A loaded model additionally exposes its real slot window.
    """
    held = {}
    for model in (models or {}).get("data") or []:
        if not isinstance(model, dict):
            continue
        for name in [model.get("id"), *(model.get("aliases") or [])]:
            if isinstance(name, str):
                held[name] = model
    for name, window in sorted(declared.items()):
        if name not in held:
            return f"{name} is declared but llama-server does not hold it"
        got, basis = _local_context(held[name])
        if got != window:
            return (f"{name}: llama-server {basis} {got if got is not None else 'unset'}, "
                    f"the class declares context_window {window}")
    return ""


def _local_reading(as_of):
    """The local server's §1.5 reading: an open allowance with no windows, marked
    unmetered (nothing limits it but the machine being up, which the poll
    itself measures), and no money pot it can draw."""
    credits = {"id": core.MONEY_POT, "type": "money", "draw": 2, "unit": None, "unit_usd": None,
               "entitlement": {"state": "none", "basis": "derived"},
               "open": False, "open_basis": "derived", "limits": []}
    return {"provider": "local", "kind": "subscription", "as_of": as_of,
            "source": "GET /models", "ok": True,
            "plan": {"label": LOCAL_PLAN_LABEL, "basis": "derived"},
            "pots": [core._allowance_pot([], open_=True, open_basis="derived", unmetered=True), credits],
            "beside": [], "models": {}}


def poll_local(prev=None, account=None):  # noqa: ARG001 — uniform pool dispatch (#71)
    """Return one local-server poll result (never raises), shaped as poll_kimi's.
    Fails when llama-server is unreachable, lacks a declared member, or its
    non-loading inventory cannot confirm that member's declared context_window."""
    out = {"name": "local", "kind": "provider", "provider": "local",
           "ok": 0, "ts": time.time(), "good_ts": None, "stale": 0,
           "http": 0, "err": "", "retry_after": None, "reading": None}
    upstream = LOCAL_UPSTREAM
    if not upstream:
        out["err"] = "LOCAL_UPSTREAM is not set"
        return out
    declared = _local_declared()
    try:
        err = _local_check(_local_models(upstream), declared)
    except urllib.error.HTTPError as e:
        out["http"] = e.code
        out["err"] = f"llama-server HTTP {e.code}"
        return out
    except urllib.error.URLError as e:
        if isinstance(e.reason, ConnectionRefusedError):
            out["err"] = f"{upstream}: connection refused — check that llama-server is running there"
        else:
            out["err"] = f"{upstream}: {e.reason}"
        return out
    except Exception as e:  # noqa: BLE001
        out["err"] = f"llama-server {type(e).__name__}: {e}"
        return out
    if err:
        out["err"] = err
        return out
    out["reading"] = _local_reading(out["ts"])
    out["ok"] = 1
    return out


# No `auth` record: nothing to enroll, LOCAL_UPSTREAM is a LAN address, and the
# proxy strips every credential on the hop.
register("local", {"poll": poll_local, "enabled": lambda: bool(LOCAL_UPSTREAM)})
