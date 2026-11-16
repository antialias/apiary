"""The brain's driver registry: what each provider module adds to the core.

The core (claude_usage_exporter.py) knows providers only through the tables
below. A driver module imports the core, defines its poller and authority, and
calls register() once at import; an entry point (brain.py, brain_household.py)
imports the drivers it ships, so the set of providers is the set of imports.
The tables are filled in import order, and that order is the one every payload
lists providers in.

register(pid, spec) takes the PROVIDERS row and these keys beside it:
    api_pot   the money pot of an api-kind provider's account (§1.2): marks the
              provider api, which /api-key dispenses and the dial check walks
    ledger    {"unit", "tables", "rates"}: the provider's pots are countable
              in that unit (§1.7), whose charges need those rate tables;
              rates, when given, are published as ledger.rates[unit]
    modality  {model: "image"}: what a model of this provider makes, when it
              is not chat (_model_modality)
Row keys the prepaid notices read (notices.py), each optional (null when
absent), declared by the driver: name (the provider's name in words, "Acme"),
serves (what stops when its money runs out, "image generation"), billing
(the page where the owner pays, a URL).
Row keys the core reads: poll(prev, account=), enabled(), accounts(),
auth {kind, label, custody, enroll, status, begin, complete}, api_key(account),
lanes (its accounts serve the lanes, as the account loop's do),
account_loop {start, load, names, poll, generation, expires_at} (the one
provider whose accounts the account loop polls), sizing(wid, kind).

register_route(method, path, handler) adds an HTTP route the core's dispatch
does not know: handler(request_handler, parsed_url) answers it.
"""

PROVIDERS = {}
_API_POTS = {}
_COUNTABLE_UNITS = {}
_LEDGER_RATES = {}
_LEDGER_RATE_TABLES = {}
_MODEL_MODALITY = {}
ROUTES = {}


def register(pid, spec):
    """Add provider `pid`. A second registration of one id is a wiring fault."""
    if pid in PROVIDERS:
        raise ValueError(f"provider {pid!r} is registered twice")
    row = dict(spec)
    pot = row.pop("api_pot", None)
    ledger = row.pop("ledger", None)
    modality = row.pop("modality", None)
    if pot:
        _API_POTS[pid] = pot
    if ledger:
        unit = ledger["unit"]
        _COUNTABLE_UNITS[pid] = unit
        _LEDGER_RATE_TABLES[unit] = tuple(ledger.get("tables") or ())
        if ledger.get("rates"):
            _LEDGER_RATES[unit] = ledger["rates"]
    for model, kind in (modality or {}).items():
        _MODEL_MODALITY[(pid, model)] = kind
    PROVIDERS[pid] = row
    return row


def register_route(method, path, handler):
    """Serve `method path` with handler(request_handler, parsed_url)."""
    key = (method.upper(), path.rstrip("/"))
    if key in ROUTES:
        raise ValueError(f"route {key} is registered twice")
    ROUTES[key] = handler
