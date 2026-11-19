"""The Anthropic API driver (shared): accounts that are static API keys, each
billed per token by its own console organization. A key is enrolled through
the /auth endpoints (a name and the key) on the keyring (providers/keyring.py), kept in
the tenant's anthropic-api.json, and dispensed to the routing proxy by GET
/api-key?account=. Its accounts serve the lanes beside the account loop's
(`lanes`): the proxy sends a lane's request on one with x-api-key.

There is no usage endpoint to poll for a key's spend, so the reading is an
open, unmetered allowance, like the local model's: the poll (a free GET
/v1/models) proves the key still works, and that is the whole answer."""
import json
import os
import urllib.request

import claude_usage_exporter as core
from providers import keyring

PROVIDER = "anthropic-api"
ANTHROPIC_API_CREDS_FILE = "anthropic-api.json"  # in each tenant's dir
ANTHROPIC_API_BASE = os.environ.get("ANTHROPIC_API_BASE", "https://api.anthropic.com").rstrip("/")
ANTHROPIC_VERSION = "2023-06-01"
POT = "balance"


def _get_models(key):
    """GET /v1/models: free, and refused with 401 for a key that no longer works."""
    req = urllib.request.Request(
        ANTHROPIC_API_BASE + "/v1/models",
        headers={"x-api-key": key, "anthropic-version": ANTHROPIC_VERSION,
                 "Accept": "application/json"})
    with core.key_urlopen(req, timeout=15) as resp:
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


def _probe(key):
    """Enrolment's probe: the model list answers 200 only to a working key."""
    _get_models(key)
    return None


KEYS = keyring.register(
    PROVIDER, "Anthropic API", ANTHROPIC_API_CREDS_FILE,
    # The model list answering proves the key, and that is the whole reading.
    lambda key: _get_models(key), lambda _raw, as_of: (_reading(as_of), ""), _probe,
    "Name the account and paste its API key. The key is validated with a model list before "
    "anything is saved; a name already enrolled has its key replaced.",
    "Static API keys, held by this brain alone: the routing proxy is dispensed each one by "
    "GET /api-key.",
    what="models",
    api_pot=POT,
    # Its accounts serve the lanes beside the account loop's (_routes).
    lanes=True)
