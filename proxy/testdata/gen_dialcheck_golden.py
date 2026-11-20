"""Generate the dial-check golden fixtures shared by the brain and the proxy."""
import copy, json, sys
from datetime import datetime, timezone

NOW = 1_800_000_000
H, D = 3600, 86400

def win(used, resets, period, scope=None, basis="exact", gate=None):
    w = {"used_pct": used, "resets_at": resets, "period": period, "scope": scope,
         "reset_basis": basis}
    if gate is not None:
        w["open"] = gate  # a window's own gate (a codex additional limit's allowed)
    return w

def aged(st, ages):
    """Each window read at the standing's as_of unless ages says otherwise
    (a response's headers refresh the windows they carry, §1.4)."""
    for name, w in st["allowance"]["windows"].items():
        w["as_of"] = (ages or {}).get(name, st["as_of"])
    return st

def event(eid, label, windows, trigger="owner", now=True, gives_back=None):
    """An allowance event in the standing (quota-model 69 §2.2), as much of it
    as the dial check's options read: its trigger, the provider's usable.now,
    and its effects: a refill of each window named, and with gives_back
    ({share_of, points}) a give_back on seven_day."""
    effects = [{"window": w, "effect": "refill", "basis": "stated"} for w in windows]
    if gives_back is not None:
        effects.append({"window": "seven_day", "effect": "give_back", "basis": "inferred",
                        **gives_back})
    return {"id": eid, "program": eid.split(":")[0], "label": label, "trigger": trigger,
            "effects": effects, "anchor": "kept",
            "usable": {"now": now, "why_not": None if now else "unavailable"}}

def with_events(st, events, state="read", why=None):
    st["allowance"]["events"] = events
    st["allowance"]["events_basis"] = {"state": state, "why": why}
    return st

CREDIT = event("codex_reset:credit-1", "Full reset (Monthly)", ["seven_day"])
PAID = event("codex_paid_reset", "Immediate reset (paid)", ["seven_day"], trigger="owner_pays")
FULL_RESET = event("cedar_ember:full-reset", "One full limit reset", ["seven_day", "scoped:Fable"])
FABLE_RESET = event("cedar_ember:fable-reset", "One Fable reset", ["scoped:Fable"])

def anth(five=40.0, seven=60.0, fable=50.0, credits=False, as_of=NOW - 60,
         five_reset=NOW + H, seven_reset=NOW + 3 * D, fable_reset=NOW + 2 * D, money=True, ages=None,
         events=None):
    st = {"provider": "anthropic", "as_of": as_of, "next_poll_at": NOW + 240,
          "allowance": {"open": None, "windows": {
              "five_hour": win(five, five_reset, "5h"),
              "seven_day": win(seven, seven_reset, "7d"),
              "scoped:Fable": win(fable, fable_reset, "7d", "Fable")}},
          "money": {}}
    if money:
        st["money"] = {"credits": {"open": credits, "balance": None, "unit": None}}
    if events is not None:
        with_events(st, events)
    return aged(st, ages)

def codex(used=30.0, allowed=True, credits=True, as_of=NOW - 60, reset=NOW + 4 * D, balance=120.0,
          ages=None, events=None):
    st = aged({"provider": "gpt", "as_of": as_of, "next_poll_at": NOW + 240,
               "allowance": {"open": allowed,
                             "windows": {"seven_day": win(used, reset, "7d", basis="early_possible")}},
               "money": {"credits": {"open": credits, "balance": balance, "unit": "codex_credit"}}},
              ages)
    if events is not None:
        with_events(st, events)
    return st

def runway(balance=500.0, used=10.0, limit=50.0, as_of=NOW - 60, windows=None):
    """An api account (§1.2): no allowance, its money pot `balance` with the
    model's rolling count (13 §2)."""
    return aged({"provider": "runway", "kind": "api", "as_of": as_of, "next_poll_at": NOW + 240,
                 "allowance": {"open": None, "windows": windows or {}},
                 "money": {"balance": {"open": None if balance is None else balance > 0, "balance": balance,
                                       "unit": "runway_credit", "limits": {
                                           "rolling_count:gpt_image_2": {
                                               "kind": "rolling_count", "scope": "gpt_image_2", "used": used,
                                               "limit": limit, "window_s": 86400, "counts": "output"}}}}},
                None)

def kimi(used=20.0, as_of=NOW - 60):
    return aged({"provider": "kimi", "as_of": as_of, "next_poll_at": NOW + 240,
                 "allowance": {"open": None, "windows": {"five_hour": win(used, NOW + 2 * H, "5h")}},
                 "money": {"credits": {"open": None, "balance": None, "unit": None}}}, None)

def local(as_of=NOW - 60):
    """The LAN box (#135): an allowance open while the poll succeeds, no
    windows, no money pot it can draw."""
    return aged({"provider": "local", "as_of": as_of, "next_poll_at": NOW + 240,
                 "allowance": {"open": True if as_of is not None else None, "windows": {}},
                 "money": {"credits": {"open": False if as_of is not None else None,
                                       "balance": None, "unit": None}}}, None)

def route(rid, pay=(), stop_at=None, scope=None):
    return {"id": rid, "pay": list(pay), "stop_at": stop_at, "scope": scope}

def chk(check, pot=None, reason=None, until=None, basis="fresh"):
    return {"check": check, "pot": pot, "reason": reason, "until": until, "basis": basis}

LANE_OPUS = "frontier/lane:opus"
LANE_FABLE = "frontier/lane:fable"
GPT = "capable/gpt/gpt-6.1-sol"
cases = []
def case(name, r, account, standing, want, pots=None, ledger=None, request=None):
    """request is the dial's {max_cost, outputs} (null: the brain's own
    check, which has none: unknown cost, one output)."""
    cases.append({"name": name, "route": r, "account": account, "standing": standing,
                  "pots": pots or {}, "ledger": ledger, "request": request, "want": want})

# The budget period is the UTC calendar month holding NOW (§1.9).
_d = datetime.fromtimestamp(NOW, timezone.utc)
MONTH = int(datetime(_d.year, _d.month, 1, tzinfo=timezone.utc).timestamp())
MONTH_END = int(datetime(_d.year + (_d.month == 12), _d.month % 12 + 1, 1, tzinfo=timezone.utc).timestamp())
LAST_MONTH = int(datetime(_d.year - (_d.month == 1), (_d.month - 2) % 12 + 1, 1, tzinfo=timezone.utc).timestamp())

RATES = {"codex_credit": {"per_tokens": 1000000,
                          "models": {"gpt-6.1-sol": {"input": 50.0, "cached": 2.5, "output": 250.0}},
                          "images": {"gpt-image-2": {"image": {"input": 200.0, "cached": 50.0, "output": 750.0},
                                                     "text": {"input": 125.0, "cached": 31.25, "output": 250.0}}},
                          "speed_weights": {"standard": 1.0}}}
# The tables each unit's charges need (C8): a Runway task's charge is its own
# cost.credits, so runway_credit needs none.
RATE_TABLES = {"codex_credit": ["per_tokens", "models", "speed_weights", "images"], "runway_credit": []}

def ledger(spend=0.0, routes=None, as_of=NOW - 30, start=MONTH, pot="gpt/credits", rates=RATES,
           unreadable=0, others=None, unpriced=None, unit="codex_credit", tables=RATE_TABLES,
           reserved=None, routes_reserved=None):
    """The /preferred ledger block's fields the check reads (§1.7). reserved
    and routes_reserved are the proxy's own: what dials in flight hold."""
    pots = {pot: {"unit": unit, "period_spend": spend, "routes": routes or {},
                  "unpriced": unpriced or {}}}
    if reserved is not None:
        pots[pot]["reserved"] = reserved
    if routes_reserved is not None:
        pots[pot]["routes_reserved"] = routes_reserved
    pots.update(others or {})
    return {"period": {"start": start, "end": MONTH_END if start == MONTH else MONTH},
            "as_of": as_of, "unreadable_lines": unreadable, "rates": rates, "rate_tables": tables,
            "pots": pots}

# --- Anthropic allowance ---
case("anthropic fresh and open is included", route(LANE_OPUS), "personal", anth(), chk("included"))
case("anthropic 5h spent, credits off: spent until the 5h reset",
     route(LANE_OPUS), "personal", anth(five=100.0), chk("no", reason="spent", until=NOW + H))
case("anthropic 5h and 7d spent: the allowance reopens when the later one resets",
     route(LANE_OPUS), "personal", anth(five=100.0, seven=100.0), chk("no", reason="spent", until=NOW + 3 * D))
case("anthropic Fable cap spent closes the Fable lane",
     route(LANE_FABLE, scope="Fable"), "personal", anth(fable=100.0), chk("no", reason="spent", until=NOW + 2 * D))
case("anthropic Fable cap spent leaves opus open",
     route(LANE_OPUS, scope="Opus"), "personal", anth(fable=100.0), chk("included"))
case("anthropic Fable cap binds a lane whose scope differs only in case",
     route(LANE_FABLE, scope="fable"), "personal", anth(fable=100.0), chk("no", reason="spent", until=NOW + 2 * D))
case("a spent window whose reset has passed is open",
     route(LANE_OPUS), "personal", anth(five=100.0, five_reset=NOW - 10), chk("included"))
case("a window at 96 percent still holds allowance",
     route(LANE_OPUS), "personal", anth(five=96.0), chk("included"))
case("anthropic unknown percent with credits off is used: unknown basis",
     route(LANE_OPUS), "personal", anth(five=None), chk("included", basis="unknown"))

# --- Anthropic stale and never measured (62 B1) ---
case("anthropic 2h stale with credits off is used at its last value",
     route(LANE_OPUS), "personal", anth(as_of=NOW - 2 * H), chk("included", basis="stale"))
case("anthropic 2h stale and spent at its last value is still spent",
     route(LANE_OPUS), "personal", anth(as_of=NOW - 2 * H, five=100.0),
     chk("no", reason="spent", until=NOW + H, basis="stale"))
case("anthropic stale with credits on and no pay: unknown until the next poll",
     route(LANE_OPUS), "personal", anth(as_of=NOW - 2 * H, credits=True),
     chk("no", reason="unknown", until=NOW + 240, basis="stale"))
case("anthropic stale with credits on and pay listing them is used",
     route(LANE_OPUS, pay=["personal/credits"]), "personal", anth(as_of=NOW - 2 * H, credits=True),
     chk("included", basis="stale"))
case("anthropic stale with the credits state unknown and no pay: unknown",
     route(LANE_OPUS), "personal", anth(as_of=NOW - 2 * H, credits=None),
     chk("no", reason="unknown", until=NOW + 240, basis="stale"))
case("a fresh reading with the credits state unknown (no extra_usage) is included",
     route(LANE_OPUS), "personal", anth(credits=None), chk("included"))
case("anthropic stale with no money pot reported is used",
     route(LANE_OPUS), "personal", anth(as_of=NOW - 2 * H, money=False), chk("included", basis="stale"))
case("exactly STALE_MAX old is still fresh",
     route(LANE_OPUS), "personal", anth(as_of=NOW - 1800, credits=True), chk("included"))
# Staleness is per limit (§1.4): the limits that bind the route, each by
# when it was read. A response refreshes the windows it carries; an opus
# reply carries no Fable cap.
FRESH_OPUS = {"five_hour": NOW - 60, "seven_day": NOW - 60, "scoped:Fable": NOW - 2 * H}
case("a stale Fable cap leaves a route it does not bind fresh, credits on",
     route(LANE_OPUS, scope="Opus"), "personal", anth(credits=True, ages=FRESH_OPUS), chk("included"))
case("a stale Fable cap blocks the Fable lane where money is at risk",
     route(LANE_FABLE, scope="Fable"), "personal", anth(credits=True, ages=FRESH_OPUS),
     chk("no", reason="unknown", until=NOW + 240, basis="stale"))
case("a stale Fable cap with credits off is used at its last value",
     route(LANE_FABLE, scope="Fable"), "personal", anth(credits=False, ages=FRESH_OPUS),
     chk("included", basis="stale"))
case("an old poll whose binding windows a response refreshed is fresh",
     route(LANE_OPUS, scope="Opus"), "personal",
     anth(as_of=NOW - 2 * H, credits=True, ages={"five_hour": NOW - 60, "seven_day": NOW - 60}),
     chk("included"))
case("a codex gate is read with the poll: fresh windows do not freshen it",
     route(GPT), "gpt", codex(as_of=NOW - 2 * H, ages={"seven_day": NOW - 60}),
     chk("no", reason="unknown", until=NOW + 240, basis="stale"))
case("never measured: unknown until the first reading",
     route(LANE_OPUS), "personal", anth(as_of=None), chk("no", reason="unknown", until=NOW + 240, basis="unknown"))

# --- Anthropic money ---
case("anthropic spent with credits on and pay []: no (pay) until the allowance resets",
     route(LANE_OPUS), "personal", anth(five=100.0, credits=True), chk("no", reason="pay", until=NOW + H))
case("anthropic spent with credits on and pay listing them: paid",
     route(LANE_OPUS, pay=["personal/credits"]), "personal", anth(five=100.0, credits=True),
     chk("paid", pot="personal/credits"))
case("pay naming another account's pot does not pay on this one",
     route(LANE_OPUS, pay=["work/credits"]), "personal", anth(five=100.0, credits=True),
     chk("no", reason="pay", until=NOW + H))
case("anthropic spent with the credits state unknown and pay listing them: paid, unknown basis",
     route(LANE_OPUS, pay=["personal/credits"]), "personal", anth(five=100.0, credits=None),
     chk("paid", pot="personal/credits", basis="unknown"))

# --- Codex (C1 fields) ---
case("codex open is included", route(GPT), "gpt", codex(), chk("included"))
case("codex closed, credits open, pay []: no (pay) until the reset",
     route(GPT), "gpt", codex(used=100.0, allowed=False), chk("no", reason="pay", until=NOW + 4 * D))
case("codex closed, credits open, pay [gpt/credits]: paid",
     route(GPT, pay=["gpt/credits"]), "gpt", codex(used=100.0, allowed=False), chk("paid", pot="gpt/credits"))
case("codex gate closed below 100 percent is closed",
     route(GPT), "gpt", codex(used=80.0, allowed=False, credits=False), chk("no", reason="spent", until=NOW + 4 * D))
case("codex closed, credits closed, topup off: spent until the allowance reset",
     route(GPT, pay=["gpt/credits"]), "gpt", codex(used=100.0, allowed=False, credits=False),
     chk("no", reason="spent", until=NOW + 4 * D), pots={"gpt/credits": {"topup": "off"}})
case("codex closed with no reset, credits closed, topup off: retry at the next poll",
     route(GPT, pay=["gpt/credits"]), "gpt", codex(used=100.0, allowed=False, credits=False, reset=None),
     chk("no", reason="spent", until=NOW + 240), pots={"gpt/credits": {"topup": "off"}})
case("anthropic spent with no reset known and credits off: retry at the next poll",
     route(LANE_OPUS), "personal", anth(five=100.0, five_reset=None), chk("no", reason="spent", until=NOW + 240))
case("codex closed with no reset, credits closed, topup on: until the next poll",
     route(GPT, pay=["gpt/credits"]), "gpt", codex(used=100.0, allowed=False, credits=False, reset=None),
     chk("no", reason="spent", until=NOW + 240), pots={"gpt/credits": {"topup": "on"}})
case("codex closed, credits closed, topup on: the earlier of reset and refill",
     route(GPT), "gpt", codex(used=100.0, allowed=False, credits=False),
     chk("no", reason="spent", until=NOW + 240), pots={"gpt/credits": {"topup": "on"}})
case("codex closed with no reset, credits open, pay []: until paid",
     route(GPT), "gpt", codex(used=100.0, allowed=False, reset=None),
     chk("no", reason="pay", until="until_paid"))
case("codex closed whose reset has passed is open",
     route(GPT), "gpt", codex(used=100.0, allowed=False, reset=NOW - 5), chk("included"))
case("codex 2h stale, credits open, pay []: unknown until the next poll",
     route(GPT), "gpt", codex(as_of=NOW - 2 * H), chk("no", reason="unknown", until=NOW + 240, basis="stale"))
case("codex 2h stale, pay [gpt/credits]: used at its last value",
     route(GPT, pay=["gpt/credits"]), "gpt", codex(as_of=NOW - 2 * H), chk("included", basis="stale"))
case("codex 2h stale with credits closed is used at its last value",
     route(GPT), "gpt", codex(as_of=NOW - 2 * H, credits=False), chk("included", basis="stale"))

# --- the local class (#135): a LAN box's llama-server, measured by its poll ---
LOCAL = "local/local/local-gemma4-12b"
case("local measured, no windows, no money: included",
     route(LOCAL), "local", local(), chk("included"))
case("local 2h stale (the PC went away after a good poll): used at its last reading",
     route(LOCAL), "local", local(as_of=NOW - 2 * H), chk("included", basis="stale"))
case("local never measured: unknown until the first reading",
     route(LOCAL), "local", local(as_of=None), chk("no", reason="unknown", until=NOW + 240, basis="unknown"))

# --- the image route (C4): the codex image model in class `images`, at
# pay [gpt/credits]. Same account, same checks as chat
# (account grain): only the route differs. ---
IMG = "images/gpt/gpt-image-2"
case("image: codex open is included",
     route(IMG, pay=["gpt/credits"]), "gpt", codex(), chk("included"))
case("image: codex closed, credits open: paid from gpt/credits",
     route(IMG, pay=["gpt/credits"]), "gpt", codex(used=100.0, allowed=False), chk("paid", pot="gpt/credits"))
case("image: codex closed, credits closed: spent until the reset",
     route(IMG, pay=["gpt/credits"]), "gpt", codex(used=100.0, allowed=False, credits=False),
     chk("no", reason="spent", until=NOW + 4 * D))
case("image: codex closed, credits state unknown, pay listing them: paid, unknown basis (as for chat)",
     route(IMG, pay=["gpt/credits"]), "gpt", codex(used=100.0, allowed=False, credits=None),
     chk("paid", pot="gpt/credits", basis="unknown"))
case("image: codex never measured: unknown until the next poll",
     route(IMG, pay=["gpt/credits"]), "gpt", codex(as_of=None), chk("no", reason="unknown", until=NOW + 240, basis="unknown"))
case("image: codex closed, credits open, pay []: no (pay), never paid",
     route(IMG), "gpt", codex(used=100.0, allowed=False), chk("no", reason="pay", until=NOW + 4 * D))

# --- an unattributed model record (§2.6), as the proxy's routeCheck reads it: a
# spent window that binds the one dial, with no period and no scope. PR11 may
# re-admit only the `no (spent)` it makes (§2.2 row 14): never into money. ---
def model_record(st, until=NOW + 2 * H, refused=NOW - 30):
    st["allowance"]["windows"]["model:m1"] = {"used_pct": 100.0, "resets_at": until, "period": None,
                                              "scope": None, "as_of": refused, "reset_basis": "record"}
    return st

case("a model record on codex with its gate open, credits open, pay []: no (pay), never re-admitted",
     route(GPT), "gpt", model_record(codex(used=60.0)), chk("no", reason="pay", until=NOW + 2 * H))
case("a model record on codex with credits closed: no (spent) until the record ends",
     route(GPT), "gpt", model_record(codex(used=60.0, credits=False)), chk("no", reason="spent", until=NOW + 2 * H))
case("a model record on codex, credits open, pay [gpt/credits]: paid",
     route(GPT, pay=["gpt/credits"]), "gpt", model_record(codex(used=60.0)), chk("paid", pot="gpt/credits"))
case("a model record on anthropic with the credits state unknown, pay []: no (pay)",
     route(LANE_OPUS), "personal", model_record(anth(credits=None)), chk("no", reason="pay", until=NOW + 2 * H))
case("a model record on anthropic with credits off: no (spent)",
     route(LANE_OPUS), "personal", model_record(anth()), chk("no", reason="spent", until=NOW + 2 * H))
case("a model record on an account with no money pot: no (spent)",
     route(LANE_OPUS), "personal", model_record(anth(money=False)), chk("no", reason="spent", until=NOW + 2 * H))

# --- stop_at (agreed decision 3) ---
S760 = {"window": "7d", "level": 60}
case("stop_at 7d/60 at 60: no (stop_at) until the window resets",
     route(GPT, stop_at=S760), "gpt", codex(used=60.0), chk("no", reason="stop_at", until=NOW + 4 * D))
case("stop_at 7d/60 whose window has reset since the reading: included",
     route(GPT, stop_at=S760), "gpt", codex(used=70.0, reset=NOW - 5), chk("included"))
case("stop_at 7d/60 below the level: included",
     route(GPT, stop_at=S760), "gpt", codex(used=59.9), chk("included"))
case("stop_at 7d/60 leaves another route on the account included",
     route("fast/gpt/gpt-6.1-sol"), "gpt", codex(used=60.0), chk("included"))
case("stop_at 7d/60 at 100 with pay []: no (pay)",
     route(GPT, stop_at=S760), "gpt", codex(used=100.0, allowed=False), chk("no", reason="pay", until=NOW + 4 * D))
case("stop_at 7d/60 at 100 with pay [gpt/credits]: paid",
     route(GPT, pay=["gpt/credits"], stop_at=S760), "gpt", codex(used=100.0, allowed=False),
     chk("paid", pot="gpt/credits"))
case("stop_at level 0: never included",
     route(GPT, stop_at={"window": "7d", "level": 0}), "gpt", codex(used=0.0),
     chk("no", reason="stop_at", until=NOW + 4 * D))
case("stop_at level 0 holds past its window's reset, until the next reading",
     route(GPT, stop_at={"window": "7d", "level": 0}), "gpt", codex(used=5.0, reset=NOW - 10),
     chk("no", reason="stop_at", until=NOW + 240))
case("stop_at level 0 on effective holds past the reset too",
     route(GPT, stop_at={"window": "effective", "level": 0}), "gpt", codex(used=5.0, reset=NOW - 10),
     chk("no", reason="stop_at", until=NOW + 240))
case("stop_at level 100: as if unset",
     route(GPT, stop_at={"window": "7d", "level": 100}), "gpt", codex(used=99.0), chk("included"))
case("stop_at on a scope binds the scoped window",
     route(LANE_FABLE, scope="Fable", stop_at={"window": "Fable", "level": 60}), "personal", anth(fable=70.0),
     chk("no", reason="stop_at", until=NOW + 2 * D))
case("stop_at effective takes the highest binding window",
     route(LANE_OPUS, stop_at={"window": "effective", "level": 60}), "personal", anth(five=40.0, seven=65.0),
     chk("no", reason="stop_at", until=NOW + 3 * D))
case("stop_at effective breaks a tie by window name: five_hour before seven_day",
     route(LANE_OPUS, stop_at={"window": "effective", "level": 60}), "personal", anth(five=70.0, seven=70.0),
     chk("no", reason="stop_at", until=NOW + H))
case("stop_at effective ignores another lane's scoped window",
     route(LANE_OPUS, scope="Opus", stop_at={"window": "effective", "level": 60}), "personal",
     anth(five=40.0, seven=50.0, fable=90.0), chk("included"))
case("stop_at on a window the account does not report has no effect",
     route(GPT, stop_at={"window": "5h", "level": 10}), "gpt", codex(used=50.0), chk("included"))
case("stop_at on a stale reading uses the last value",
     route(LANE_OPUS, stop_at={"window": "7d", "level": 50}), "personal", anth(as_of=NOW - 2 * H, seven=55.0),
     chk("no", reason="stop_at", until=NOW + 3 * D, basis="stale"))

# --- Kimi: Extra Usage has no API ---
case("kimi fresh and open is included", route("agent/kimi/kimi-k3"), "kimi", kimi(), chk("included"))
case("kimi spent with Extra Usage unknown and pay []: no (pay)",
     route("agent/kimi/kimi-k3"), "kimi", kimi(used=100.0), chk("no", reason="pay", until=NOW + 2 * H))
case("kimi stale with Extra Usage unknown and pay []: unknown",
     route("agent/kimi/kimi-k3"), "kimi", kimi(as_of=NOW - 2 * H),
     chk("no", reason="unknown", until=NOW + 240, basis="stale"))

# --- a window's own gate and a provider member's model scope (C6) ---
SOL = "gpt-6.1-sol"
def codex_model_limit(gate=False, used=40.0, reset=NOW + D, scope=SOL, **kw):
    st = codex(**kw)
    st["allowance"]["windows"][f"scoped:{scope}:7d"] = win(used, reset, "7d", scope, "early_possible", gate)
    return aged(st, None)

case("a codex per-model limit closed by its own gate closes its model's route",
     route(GPT, scope=SOL), "gpt", codex_model_limit(credits=False),
     chk("no", reason="spent", until=NOW + D))
case("a codex per-model limit closed by its own gate, credits open, pay []: no (pay)",
     route(GPT, scope=SOL), "gpt", codex_model_limit(), chk("no", reason="pay", until=NOW + D))
case("a codex per-model limit binds no other model's route",
     route("capable/gpt/gpt-6-astra", scope="gpt-6-astra"), "gpt", codex_model_limit(credits=False),
     chk("included"))
case("a codex per-model limit with its gate open below 100 is open",
     route(GPT, scope=SOL), "gpt", codex_model_limit(gate=True), chk("included"))
case("a per-model gate closed whose reset has passed is open",
     route(GPT, scope=SOL), "gpt", codex_model_limit(reset=NOW - 5, credits=False), chk("included"))
case("an unscoped window of another length binds every route",
     route(GPT, scope=SOL), "gpt",
     aged(dict(codex(credits=False), allowance={"open": True, "windows": {
         "seven_day": win(30.0, NOW + 4 * D, "7d", basis="early_possible"),
         "window:1d": win(100.0, NOW + H, "1d", basis="early_possible")}}), None),
     chk("no", reason="spent", until=NOW + H))

# --- the budget gate (§1.7, §1.9) ---
PAID_GPT = dict(r=route(GPT, pay=["gpt/credits"]), account="gpt")
SPENT_GPT = lambda **kw: codex(used=100.0, allowed=False, **kw)
B100 = {"gpt/credits": {"topup": "off", "budget": 100.0}}
case("no budget and no ledger: paid", PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"))
case("no budget: the ledger's spend does not matter", PAID_GPT["r"], "gpt", SPENT_GPT(),
     chk("paid", pot="gpt/credits"), ledger=ledger(spend=1e6))
case("a budget with room: paid", PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots=B100, ledger=ledger(spend=99.5))
case("a budget reached: no (budget) until the allowance reopens",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="budget", until=NOW + 4 * D),
     pots=B100, ledger=ledger(spend=100.0))
case("a budget reached with no reset known: until the period ends",
     PAID_GPT["r"], "gpt", SPENT_GPT(reset=None), chk("no", reason="budget", until=MONTH_END),
     pots=B100, ledger=ledger(spend=120.0))
case("a budget reached, the reset after the period's end: until the period ends",
     PAID_GPT["r"], "gpt", SPENT_GPT(reset=MONTH_END + 3 * D), chk("no", reason="budget", until=MONTH_END),
     pots=B100, ledger=ledger(spend=120.0))
case("a budget of 0 closes the pot to our routes",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="budget", until=NOW + 4 * D),
     pots={"gpt/credits": {"topup": "off", "budget": 0.0}}, ledger=ledger())
case("a route budget reached closes that route, the pot budget aside",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="budget", until=NOW + 4 * D),
     pots={"gpt/credits": {"topup": "off", "budget": 100.0, "route_budgets": {GPT: 10.0}}},
     ledger=ledger(spend=20.0, routes={GPT: 10.0, "fast/gpt/gpt-6.1-sol": 10.0}))
case("another route's budget reached leaves this route paid",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots={"gpt/credits": {"topup": "off", "route_budgets": {"fast/gpt/gpt-6.1-sol": 5.0}}},
     ledger=ledger(spend=20.0, routes={GPT: 15.0, "fast/gpt/gpt-6.1-sol": 5.0}))
case("a route budget with no spend on the route: paid",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots={"gpt/credits": {"topup": "off", "route_budgets": {GPT: 5.0}}},
     ledger=ledger(spend=50.0, routes={"fast/gpt/gpt-6.1-sol": 50.0}))
case("a budget with no ledger cannot say it has room: unknown until the next poll",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", until=NOW + 240, basis="unknown"),
     pots=B100)
case("a budget with a ledger older than STALE_MAX: unknown until the next poll",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", until=NOW + 240, basis="unknown"),
     pots=B100, ledger=ledger(spend=1.0, as_of=NOW - 2 * H))
case("a budget reached on a ledger older than STALE_MAX stays reached",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="budget", until=NOW + 4 * D),
     pots=B100, ledger=ledger(spend=100.0, as_of=NOW - 2 * H))
case("a ledger file with an unreadable line may hide spend: unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="ledger_unreadable"),
     pots=B100, ledger=ledger(spend=1.0, unreadable=1))
case("a budget reached stays reached beside an unreadable line",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="budget", until=NOW + 4 * D),
     pots=B100, ledger=ledger(spend=100.0, unreadable=1))
case("no rates for the pot's unit: every charge would enter uncounted, unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="no_rates"),
     pots=B100, ledger=ledger(spend=1.0, rates={}))
case("no speed weights for the pot's unit: unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="no_rates"),
     pots=B100, ledger=ledger(spend=1.0, rates={"codex_credit": dict(RATES["codex_credit"], speed_weights={})}))
case("no image rates for the pot's unit: an image charge would enter uncounted, unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="no_rates"),
     pots=B100, ledger=ledger(spend=1.0, rates={"codex_credit": dict(RATES["codex_credit"], images={})}))
case("a lower_bound entry on the pot leaves its budget unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="unpriced"),
     pots=B100, ledger=ledger(spend=1.0, routes={"fast/gpt/gpt-6.1-sol": 1.0},
                              unpriced={"fast/gpt/gpt-6.1-sol": 1}))
case("a lower_bound entry on this route leaves its route budget unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="unpriced"),
     pots={"gpt/credits": {"topup": "off", "route_budgets": {GPT: 10.0}}},
     ledger=ledger(spend=1.0, routes={GPT: 1.0}, unpriced={GPT: 1}))
case("a lower_bound entry on another route leaves this route budget paid",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots={"gpt/credits": {"topup": "off", "route_budgets": {GPT: 10.0}}},
     ledger=ledger(spend=1.0, routes={"fast/gpt/gpt-6.1-sol": 1.0}, unpriced={"fast/gpt/gpt-6.1-sol": 1}))
case("last month's lower_bound entry says nothing of this month",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots=B100, ledger=ledger(spend=1.0, start=LAST_MONTH, unpriced={GPT: 1}))
case("an unreadable line or no rates leave a route with no budget paid",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     ledger=ledger(spend=1.0, unreadable=1, rates={}))
case("a ledger of last month has spent nothing of this one",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots=B100, ledger=ledger(spend=500.0, start=LAST_MONTH))
case("another pot's spend is not this pot's",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots=B100, ledger=ledger(others={"other/credits": {"unit": "codex_credit", "period_spend": 500.0,
                                                        "routes": {GPT: 500.0}}}))
case("a ledger that lists no unit for the pot counts none of its charges: unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="no_rates"),
     pots=B100, ledger=ledger(spend=500.0, pot="other/credits"))
case("a budget reached does not touch an open allowance",
     PAID_GPT["r"], "gpt", codex(), chk("included"), pots=B100, ledger=ledger(spend=500.0))
case("a budget reached on a stale reading keeps the reading's basis",
     PAID_GPT["r"], "gpt", SPENT_GPT(as_of=NOW - 2 * H),
     chk("no", reason="budget", until=NOW + 4 * D, basis="stale"), pots=B100, ledger=ledger(spend=100.0))
case("a budget reached on an unpaid route is still no (pay)",
     route(GPT), "gpt", SPENT_GPT(), chk("no", reason="pay", until=NOW + 4 * D),
     pots=B100, ledger=ledger(spend=100.0))
case("the published rule decides: a unit whose rule needs no speed weights is complete without them",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("paid", pot="gpt/credits"),
     pots=B100, ledger=ledger(spend=1.0, rates={"codex_credit": dict(RATES["codex_credit"], speed_weights={})},
                              tables=dict(RATE_TABLES, codex_credit=["per_tokens", "models", "images"])))
case("a ledger with no rule for the pot's unit: unknown",
     PAID_GPT["r"], "gpt", SPENT_GPT(), chk("no", reason="unknown", basis="no_rates"),
     pots=B100, ledger=ledger(spend=1.0, tables={"runway_credit": []}))

# --- an api account (§1.2, C8): Runway's balance, every dial money ---
RWY = "images/runway/gpt_image_2"
RW_POT = "runway/balance"
def rw_route(pay=(RW_POT,)):
    return route(RWY, pay=pay, scope="gpt_image_2")
def rw_ledger(spend=0.0, **kw):
    return ledger(spend=spend, pot=RW_POT, unit="runway_credit", **kw)
def req(max_cost, outputs=1):
    return {"max_cost": max_cost, "outputs": outputs}
TOPUP_ON = {RW_POT: {"topup": "on"}}
TOPUP_OFF = {RW_POT: {"topup": "off"}}
case("runway with pay []: no (pay) until paid, whatever the balance",
     rw_route(pay=()), "runway", runway(), chk("no", reason="pay", until="until_paid"), request=req(5))
case("runway funded and paid: paid on its balance", rw_route(), "runway", runway(),
     chk("paid", pot=RW_POT), request=req(5))
case("runway funded, the brain's own check (no request): paid", rw_route(), "runway", runway(),
     chk("paid", pot=RW_POT))
case("runway never measured: unknown until the next poll",
     rw_route(), "runway", {"provider": "runway", "kind": "api", "as_of": None, "next_poll_at": NOW + 240,
                            "allowance": {"open": None, "windows": {}},
                            "money": {"balance": {"open": None, "balance": None, "unit": None}}},
     chk("no", reason="unknown", until=NOW + 240, basis="unknown"), request=req(5))
case("runway's balance unread: paid, basis unknown (the refusal is the worst it costs)",
     rw_route(), "runway", runway(balance=None), chk("paid", pot=RW_POT, basis="unknown"), request=req(5))
case("runway on a stale reading: paid on the last balance, basis stale",
     rw_route(), "runway", runway(as_of=NOW - 2 * H), chk("paid", pot=RW_POT, basis="stale"), request=req(5))
case("runway at a zero balance, topup off: no (balance) until paid",
     rw_route(), "runway", runway(balance=0.0), chk("no", reason="balance", until="until_paid"),
     pots=TOPUP_OFF, request=req(5))
case("runway at a zero balance, no setting: topup is off",
     rw_route(), "runway", runway(balance=0.0), chk("no", reason="balance", until="until_paid"), request=req(5))
case("runway at a zero balance, topup on: no (balance) until the hourly refill after the reading",
     rw_route(), "runway", runway(balance=0.0), chk("no", reason="balance", until=NOW - 60 + H),
     pots=TOPUP_ON, request=req(5))
case("runway at a zero balance, topup on, the refill hour passed: the next poll",
     rw_route(), "runway", runway(balance=0.0, as_of=NOW - 2 * H),
     chk("no", reason="balance", until=NOW + 240, basis="stale"), pots=TOPUP_ON, request=req(5))
case("runway with pay [] at a zero balance is no (pay), not no (balance)",
     rw_route(pay=()), "runway", runway(balance=0.0), chk("no", reason="pay", until="until_paid"),
     pots=TOPUP_ON, request=req(5))
case("runway's balance below the request's max_cost: no (balance)",
     rw_route(), "runway", runway(balance=15.0), chk("no", reason="balance", until="until_paid"),
     request=req(20))
case("runway's balance exactly the request's max_cost: paid",
     rw_route(), "runway", runway(balance=20.0), chk("paid", pot=RW_POT), request=req(20))
case("runway's balance above 0, the cost unknown: paid",
     rw_route(), "runway", runway(balance=0.5), chk("paid", pot=RW_POT), request=req(None))
case("runway's rolling count full: no (spent) until the next poll (a rolling count names no reset)",
     rw_route(), "runway", runway(used=50.0), chk("no", reason="spent", until=NOW + 240), request=req(5))
case("runway's rolling count one short, two outputs asked: no (spent)",
     rw_route(), "runway", runway(used=49.0), chk("no", reason="spent", until=NOW + 240), request=req(5, 2))
case("runway's rolling count one short, one output asked: paid",
     rw_route(), "runway", runway(used=49.0), chk("paid", pot=RW_POT), request=req(5, 1))
case("runway's rolling count unread: paid",
     rw_route(), "runway", runway(used=None), chk("paid", pot=RW_POT), request=req(5))
case("a rolling count of another model does not bind this route",
     route("images/runway/other", pay=[RW_POT], scope="other"), "runway", runway(used=50.0),
     chk("paid", pot=RW_POT), request=req(5))
case("a closed window on runway (a refusal record's): no (spent) until it reopens",
     rw_route(), "runway",
     runway(windows={"scoped:gpt_image_2": win(100.0, NOW + H, "1h", "gpt_image_2", gate=False)}),
     chk("no", reason="spent", until=NOW + H), request=req(5))
case("runway's budget: spend + reserved + max_cost at the budget is paid",
     rw_route(), "runway", runway(), chk("paid", pot=RW_POT),
     pots={RW_POT: {"topup": "off", "budget": 100.0}}, ledger=rw_ledger(70.0, reserved=10.0), request=req(20))
case("runway's budget: spend + reserved + max_cost over the budget is no (budget) to the period's end",
     rw_route(), "runway", runway(), chk("no", reason="budget", until=MONTH_END),
     pots={RW_POT: {"topup": "off", "budget": 100.0}}, ledger=rw_ledger(70.0, reserved=10.0), request=req(21))
case("runway's budget with the cost unknown: reached at spend + reserved",
     rw_route(), "runway", runway(), chk("no", reason="budget", until=MONTH_END),
     pots={RW_POT: {"topup": "off", "budget": 100.0}}, ledger=rw_ledger(90.0, reserved=10.0),
     request=req(None))
case("runway's route budget counts what this route has reserved",
     rw_route(), "runway", runway(), chk("no", reason="budget", until=MONTH_END),
     pots={RW_POT: {"topup": "off", "route_budgets": {RWY: 10.0}}},
     ledger=rw_ledger(5.0, routes={RWY: 5.0}, routes_reserved={RWY: 4.0}), request=req(2))
case("another route's reservation is not this route's",
     rw_route(), "runway", runway(), chk("paid", pot=RW_POT),
     pots={RW_POT: {"topup": "off", "route_budgets": {RWY: 10.0}}},
     ledger=rw_ledger(5.0, routes={RWY: 5.0}, routes_reserved={"images/runway/other": 9.0}), request=req(2))
case("last month's reservation still holds: a dial in flight has not settled",
     rw_route(), "runway", runway(), chk("no", reason="budget", until=MONTH_END),
     pots={RW_POT: {"topup": "off", "budget": 100.0}},
     ledger=rw_ledger(500.0, start=LAST_MONTH, reserved=95.0), request=req(10))
case("runway_credit needs no rate table: a budget with room is paid on a ledger with no runway rates",
     rw_route(), "runway", runway(), chk("paid", pot=RW_POT),
     pots={RW_POT: {"topup": "off", "budget": 100.0}}, ledger=rw_ledger(10.0), request=req(5))
case("a ledger with no rule for runway_credit: unknown",
     rw_route(), "runway", runway(), chk("no", reason="unknown", basis="no_rates"),
     pots={RW_POT: {"topup": "off", "budget": 100.0}},
     ledger=rw_ledger(10.0, tables={"codex_credit": RATE_TABLES["codex_credit"]}), request=req(5))

def v(name, checks, want):
    return {"name": name, "checks": checks, "want": want}

verdicts = [
    v("included beats paid and no", [chk("no", reason="spent", until=NOW + 100), chk("paid", pot="personal/credits"),
                                     chk("included")], 2),
    v("fresh beats stale within included", [chk("included", basis="stale"), chk("included")], 1),
    v("fresh beats unknown within included", [chk("included", basis="unknown"), chk("included")], 1),
    v("paid beats no", [chk("no", reason="pay", until=NOW + 50), chk("paid", pot="gpt/credits")], 1),
    v("the earliest until wins among no", [chk("no", reason="spent", until="until_paid"),
                                           chk("no", reason="spent", until=NOW + 50),
                                           chk("no", reason="spent", until=NOW + 10),
                                           chk("no", reason="unknown", until=None)], 2),
    v("until_paid beats an unknown time", [chk("no", reason="spent", until=None),
                                           chk("no", reason="spent", until="until_paid")], 1),
    v("the first wins a tie", [chk("included"), chk("included")], 0),
]

def p(pot, priority):
    return {"pot": pot, "priority": priority}

paid_orders = [
    {"name": "listed pots in list order, unlisted after by priority then pot id",
     "pay_order": ["gpt/credits", "personal/credits"],
     "candidates": [p("personal/credits", 1), p("work/credits", 0), p("gpt/credits", 2), p("alt/credits", 0),
                    p("kimi/credits", 3)],
     "want": ["gpt/credits", "personal/credits", "alt/credits", "work/credits", "kimi/credits"]},
    {"name": "an empty pay_order ranks by member priority, then pot id",
     "pay_order": [],
     "candidates": [p("personal/credits", 2), p("work/credits", 1), p("gpt/credits", 1)],
     "want": ["gpt/credits", "work/credits", "personal/credits"]},
]

# --- the unavailable reply (§2.8, C3): a class's member routes, every
# account's standing; each evaluator runs its own dial check per (route,
# account) and builds the body from those checks. ---
unavailable = []
def ucase(name, routes, standings, want, pots=None, ledger=None, request=None, given=None):
    # given: {route: {account: check}}, a check neither evaluator makes from a
    # standing (the proxy's `no (blocked)`, from a not_entitled record on the
    # model), used as it stands in place of the evaluator's own.
    unavailable.append({"name": name, "routes": routes, "standings": standings, "pots": pots or {},
                        "ledger": ledger, "request": request, "given": given or {}, "want": want})

def urt(rid, accounts, pay=(), scope=None, stop_at=None):
    r = route(rid, pay=pay, stop_at=stop_at, scope=scope)
    r["accounts"] = list(accounts)
    return r

def acc(account, why, until, window, basis="fresh", check="no"):
    return {"account": account, "check": check, "why": why, "until": until, "basis": basis, "window": window}

def body(members, at, at_basis, options, retryable=None):
    # A reply with a time is retryable; one without says which it is.
    return {"members": members, "available_at": at, "available_basis": at_basis,
            "retry_after_s": None if at is None else at - NOW,
            "retryable": at is not None if retryable is None else retryable, "options": options}

def opt(kind, rid, account, pot, action):
    return {"kind": kind, "route": rid, "account": account, "pot": pot, "action": action}

def ev_opt(rid, account, ev, fee=""):
    return {"kind": "event", "route": rid, "account": account, "pot": None, "event": ev["id"],
            "action": f"{ev['label']} on {account}{fee}"}

FABLE_LANE = "c/lane:fable"
C_GPT = "c/gpt/gpt-6.1-sol"
ANTH3 = ["maxplan", "ghost", "personal"]
FABLE_RESETS = {"maxplan": NOW + 2 * D, "ghost": NOW + 3 * D, "personal": NOW + 5 * D}
def fable_spent():
    return {a: anth(fable=100.0, fable_reset=t, credits=False) for a, t in FABLE_RESETS.items()}
def fable_member():
    return {"route": FABLE_LANE, "accounts": [acc(a, "spent", FABLE_RESETS[a], "scoped:Fable") for a in ANTH3]}
def enable_opts():
    return [opt("enable", FABLE_LANE, a, a + "/credits",
                f"enable usage credits on {a}, and add {a}/credits to {FABLE_LANE}'s pay") for a in ANTH3]
GPT_CLOSED = codex(used=100.0, allowed=False, events=[PAID])

ucase("the §2.8 example: Fable spent everywhere, codex closed with credits open and pay []",
      [urt(FABLE_LANE, ANTH3, scope="Fable"), urt(C_GPT, ["gpt"])],
      dict(fable_spent(), gpt=GPT_CLOSED),
      body([fable_member(), {"route": C_GPT, "accounts": [acc("gpt", "pay", NOW + 4 * D, "seven_day")]}],
           NOW + 2 * D, "exact",
           enable_opts() + [opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay"),
                            ev_opt(C_GPT, "gpt", PAID, ", for a fee")]))
ucase("the Fable lane alone gives the same reply without the codex line",
      [urt(FABLE_LANE, ANTH3, scope="Fable")], fable_spent(),
      body([fable_member()], NOW + 2 * D, "exact", enable_opts()))
ucase("codex alone: its reset may come early",
      [urt(C_GPT, ["gpt"])], {"gpt": codex(used=100.0, allowed=False)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "pay", NOW + 4 * D, "seven_day")]}], NOW + 4 * D,
           "early_possible", [opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay")]))
ucase("a spent member and one never measured: Retry-After from the next poll",
      [urt("c/lane:opus", ["personal"]), urt(C_GPT, ["gpt"])],
      {"personal": anth(five=100.0, seven=100.0), "gpt": codex(as_of=None)},
      body([{"route": "c/lane:opus", "accounts": [acc("personal", "spent", NOW + 3 * D, "seven_day")]},
            {"route": C_GPT, "accounts": [acc("gpt", "unknown", NOW + 240, None, basis="unknown")]}],
           NOW + 240, "next_poll",
           [opt("enable", "c/lane:opus", "personal", "personal/credits",
                "enable usage credits on personal, and add personal/credits to c/lane:opus's pay"),
            opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay")]))
ucase("codex closed with no reset known, credits open, pay []: until paid, no time, non-retryable",
      [urt(C_GPT, ["gpt"])], {"gpt": codex(used=100.0, allowed=False, reset=None)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "pay", "until_paid", None)]}], None, None,
           [opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay")],
           retryable=False))
ucase("until paid on every route: still the non-retryable money reply",
      [urt(C_GPT, ["gpt"]), urt("c/gpt/gpt-5.6", ["gpt"])],
      {"gpt": codex(used=100.0, allowed=False, reset=None)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "pay", "until_paid", None)]},
            {"route": "c/gpt/gpt-5.6", "accounts": [acc("gpt", "pay", "until_paid", None)]}], None, None,
           [opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay"),
            opt("permit", "c/gpt/gpt-5.6", "gpt", "gpt/credits", "add gpt/credits to c/gpt/gpt-5.6's pay")],
           retryable=False))
# A `no (unknown)` is never the money reply, whatever its time: only a reading
# can tell, and one may come with no one acting. With no time it states none.
def unpolled(st):
    st["as_of"], st["next_poll_at"] = None, None
    return st
ucase("an unknown with no time: retryable, and no time stated",
      [urt(C_GPT, ["gpt"])], {"gpt": unpolled(codex())},
      body([{"route": C_GPT, "accounts": [acc("gpt", "unknown", None, None, basis="unknown")]}], None, None,
           [opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay")],
           retryable=True))
ucase("an unknown beside an until paid: the unknown keeps it retryable",
      [urt(C_GPT, ["gpt"]), urt("c/lane:opus", ["personal"])],
      {"gpt": codex(used=100.0, allowed=False, reset=None), "personal": unpolled(anth(credits=True))},
      body([{"route": C_GPT, "accounts": [acc("gpt", "pay", "until_paid", None)]},
            {"route": "c/lane:opus", "accounts": [acc("personal", "unknown", None, None, basis="unknown")]}],
           None, None,
           [opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay"),
            opt("permit", "c/lane:opus", "personal", "personal/credits", "add personal/credits to c/lane:opus's pay")],
           retryable=True))
ucase("credits closed with topup off: the allowance reset, and money to add",
      [urt(C_GPT, ["gpt"], pay=["gpt/credits"])], {"gpt": codex(used=100.0, allowed=False, credits=False)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "spent", NOW + 4 * D, "seven_day")]}], NOW + 4 * D,
           "early_possible", [opt("fund", C_GPT, "gpt", "gpt/credits", "add money to gpt/credits")]),
      pots={"gpt/credits": {"topup": "off"}})
ucase("credits closed with topup on: the refill at the next poll comes first",
      [urt(C_GPT, ["gpt"], pay=["gpt/credits"])], {"gpt": codex(used=100.0, allowed=False, credits=False)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "spent", NOW + 240, None)]}], NOW + 240,
           "next_poll", [opt("fund", C_GPT, "gpt", "gpt/credits", "add money to gpt/credits")]),
      pots={"gpt/credits": {"topup": "on"}})
ucase("stop_at holds the route until its window resets; no money option",
      [urt(C_GPT, ["gpt"], stop_at={"window": "7d", "level": 60})], {"gpt": codex(used=70.0)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "stop_at", NOW + 4 * D, "seven_day")]}], NOW + 4 * D,
           "early_possible", []))
# The allowance's events (quota-model 69 §6.2): on a spent, pay or budget
# refusal, each event the owner could use now whose effects name a window the
# route's scope binds, or any window while the account's own gate is closed.
GPT_SPENT = [{"route": C_GPT, "accounts": [acc("gpt", "spent", NOW + 4 * D, "seven_day")]}]
FUND_GPT = opt("fund", C_GPT, "gpt", "gpt/credits", f"add money to gpt/credits, and add gpt/credits to {C_GPT}'s pay")
ucase("a usable event is offered, and a paid one with its fee",
      [urt(C_GPT, ["gpt"])],
      {"gpt": codex(used=100.0, allowed=False, credits=False, events=[CREDIT, PAID])},
      body(GPT_SPENT, NOW + 4 * D, "early_possible",
           [FUND_GPT, ev_opt(C_GPT, "gpt", CREDIT), ev_opt(C_GPT, "gpt", PAID, ", for a fee")]))
ucase("an unusable event is not offered",
      [urt(C_GPT, ["gpt"])],
      {"gpt": codex(used=100.0, allowed=False, credits=False,
                    events=[event("codex_reset:credit-1", "Full reset (Monthly)", ["seven_day"], now=False)])},
      body(GPT_SPENT, NOW + 4 * D, "early_possible", [FUND_GPT]))
ucase("events unread: no option",
      [urt(C_GPT, ["gpt"])],
      {"gpt": with_events(codex(used=100.0, allowed=False, credits=False), [], "unread", "HTTP 401")},
      body(GPT_SPENT, NOW + 4 * D, "early_possible", [FUND_GPT]))
ucase("an event on another window's scope is not offered: the weekly's reset is, the Fable cap's is not",
      [urt(LANE_OPUS, ["maxplan"], scope="Opus")],
      {"maxplan": anth(seven=100.0, events=[FULL_RESET, FABLE_RESET])},
      body([{"route": LANE_OPUS, "accounts": [acc("maxplan", "spent", NOW + 3 * D, "seven_day")]}],
           NOW + 3 * D, "exact",
           [opt("enable", LANE_OPUS, "maxplan", "maxplan/credits",
                f"enable usage credits on maxplan, and add maxplan/credits to {LANE_OPUS}'s pay"),
            ev_opt(LANE_OPUS, "maxplan", FULL_RESET)]))
# An event is offered only where it gives use back to the window that refused
# the route: a give_back of 0 points, or of none stated, gives an Opus lane
# walled on the weekly nothing; one of more than 0 does.
OPUS_WALLED = [{"route": LANE_OPUS, "accounts": [acc("maxplan", "spent", NOW + 3 * D, "seven_day")]}]
ENABLE_OPUS = opt("enable", LANE_OPUS, "maxplan", "maxplan/credits",
                  f"enable usage credits on maxplan, and add maxplan/credits to {LANE_OPUS}'s pay")
def fable_back(points):
    return event("cedar_ember:fable-reset", "One Fable reset", ["scoped:Fable"],
                 gives_back={"share_of": "scoped:Fable", "points": points})
ucase("a Fable reset whose share of the weekly is 0 is not offered to an Opus lane walled on the weekly",
      [urt(LANE_OPUS, ["maxplan"], scope="Opus")],
      {"maxplan": anth(seven=100.0, events=[fable_back(0)])},
      body(OPUS_WALLED, NOW + 3 * D, "exact", [ENABLE_OPUS]))
ucase("a Fable reset whose share is not stated is not offered to an Opus lane walled on the weekly",
      [urt(LANE_OPUS, ["maxplan"], scope="Opus")],
      {"maxplan": anth(seven=100.0, events=[fable_back(None)])},
      body(OPUS_WALLED, NOW + 3 * D, "exact", [ENABLE_OPUS]))
ucase("a Fable reset with a share of the weekly above 0 is offered to an Opus lane walled on the weekly",
      [urt(LANE_OPUS, ["maxplan"], scope="Opus")],
      {"maxplan": anth(seven=100.0, events=[fable_back(25)])},
      body(OPUS_WALLED, NOW + 3 * D, "exact", [ENABLE_OPUS, ev_opt(LANE_OPUS, "maxplan", fable_back(25))]))
ucase("a lane refused by its scoped cap is not offered a reset that refills only the weekly",
      [urt(FABLE_LANE, ["maxplan"], scope="Fable")],
      {"maxplan": anth(fable=100.0, events=[event("cedar_ember:weekly-reset", "One weekly reset",
                                                  ["seven_day"])])},
      body([{"route": FABLE_LANE, "accounts": [acc("maxplan", "spent", NOW + 2 * D, "scoped:Fable")]}],
           NOW + 2 * D, "exact",
           [opt("enable", FABLE_LANE, "maxplan", "maxplan/credits",
                f"enable usage credits on maxplan, and add maxplan/credits to {FABLE_LANE}'s pay")]))
ucase("the Fable cap's reset is offered to the Fable lane it binds",
      [urt(FABLE_LANE, ["maxplan"], scope="Fable")],
      {"maxplan": anth(fable=100.0, events=[FABLE_RESET])},
      body([{"route": FABLE_LANE, "accounts": [acc("maxplan", "spent", NOW + 2 * D, "scoped:Fable")]}],
           NOW + 2 * D, "exact",
           [opt("enable", FABLE_LANE, "maxplan", "maxplan/credits",
                f"enable usage credits on maxplan, and add maxplan/credits to {FABLE_LANE}'s pay"),
            ev_opt(FABLE_LANE, "maxplan", FABLE_RESET)]))
ucase("a model record's end is a record time",
      [urt(C_GPT, ["gpt"])], {"gpt": model_record(codex(used=60.0, credits=False))},
      body([{"route": C_GPT, "accounts": [acc("gpt", "spent", NOW + 2 * H, "model:m1")]}], NOW + 2 * H,
           "record", [opt("fund", C_GPT, "gpt", "gpt/credits",
                          f"add money to gpt/credits, and add gpt/credits to {C_GPT}'s pay")]))
ucase("an included account is listed with its check and gives no time",
      [urt("c/lane:opus", ["personal", "work"])], {"personal": anth(five=100.0), "work": anth()},
      body([{"route": "c/lane:opus", "accounts": [acc("personal", "spent", NOW + H, "five_hour"),
                                                  acc("work", None, None, None, check="included")]}],
           NOW + H, "exact",
           [opt("enable", "c/lane:opus", "personal", "personal/credits",
                "enable usage credits on personal, and add personal/credits to c/lane:opus's pay")]))
# §5.1's brain gap: the brain has been down past stale_max, so every reading
# is stale and the next poll it promised has passed. Credits open and pay []
# make the check no (unknown) until that passed time: the reply states no
# time (until null), never one in the past, and stays retryable: the brain
# coming back is a reading, and no one need act for it.
BRAIN_GONE = NOW - 7200
def gone(st):
    st["next_poll_at"] = BRAIN_GONE + 300
    return st
ucase("the brain down past stale_max: an unknown whose time has passed, retryable with no time",
      [urt(C_GPT, ["gpt"])], {"gpt": gone(codex(as_of=BRAIN_GONE))},
      body([{"route": C_GPT, "accounts": [acc("gpt", "unknown", None, None, basis="stale")]}], None, None,
           [opt("permit", C_GPT, "gpt", "gpt/credits", f"add gpt/credits to {C_GPT}'s pay")],
           retryable=True))
ucase("the brain down past stale_max, credits closed: a spent whose next poll has passed is retryable too",
      [urt(C_GPT, ["gpt"], pay=["gpt/credits"])],
      {"gpt": gone(codex(used=100.0, allowed=False, credits=False, reset=None, as_of=BRAIN_GONE))},
      body([{"route": C_GPT, "accounts": [acc("gpt", "spent", None, None, basis="stale")]}], None, None,
           [opt("fund", C_GPT, "gpt", "gpt/credits", "add money to gpt/credits")], retryable=True),
      pots={"gpt/credits": {"topup": "off"}})
ucase("a passed next poll gives no time; another account's reset still does",
      [urt("c/lane:opus", ["personal", "work"])],
      {"personal": gone(anth(as_of=BRAIN_GONE, credits=True)), "work": anth(five=100.0)},
      body([{"route": "c/lane:opus", "accounts": [acc("personal", "unknown", None, None, basis="stale"),
                                                  acc("work", "spent", NOW + H, "five_hour")]}],
           NOW + H, "exact",
           [opt("permit", "c/lane:opus", "personal", "personal/credits",
                "add personal/credits to c/lane:opus's pay"),
            opt("enable", "c/lane:opus", "work", "work/credits",
                "enable usage credits on work, and add work/credits to c/lane:opus's pay")]))
ucase("no route has an account: no check, so no body; configuration, the typed 503 (§2.8 item 6)",
      [urt(C_GPT, [], pay=["gpt/credits"])], {}, None)
ucase("two routes, neither with an account: still no body",
      [urt(FABLE_LANE, [], scope="Fable"), urt(C_GPT, [])], {}, None)
ucase("a route with no account beside a spent one is listed empty; the body is the spent one's",
      [urt(C_GPT, []), urt("c/lane:opus", ["personal"])], {"personal": anth(five=100.0)},
      body([{"route": C_GPT, "accounts": []},
            {"route": "c/lane:opus", "accounts": [acc("personal", "spent", NOW + H, "five_hour")]}],
           NOW + H, "exact",
           [opt("enable", "c/lane:opus", "personal", "personal/credits",
                "enable usage credits on personal, and add personal/credits to c/lane:opus's pay")]))
ucase("images, codex closed (62 M2), pay []: retryable to the codex reset, with the permit",
      [urt(IMG, ["gpt"])], {"gpt": codex(used=100.0, allowed=False)},
      body([{"route": IMG, "accounts": [acc("gpt", "pay", NOW + 4 * D, "seven_day")]}], NOW + 4 * D,
           "early_possible", [opt("permit", IMG, "gpt", "gpt/credits", f"add gpt/credits to {IMG}'s pay")]))
ucase("image: codex closed, credits closed: the reset, and money to add",
      [urt(IMG, ["gpt"], pay=["gpt/credits"])], {"gpt": codex(used=100.0, allowed=False, credits=False)},
      body([{"route": IMG, "accounts": [acc("gpt", "spent", NOW + 4 * D, "seven_day")]}], NOW + 4 * D,
           "early_possible", [opt("fund", IMG, "gpt", "gpt/credits", "add money to gpt/credits")]),
      pots={"gpt/credits": {"topup": "off"}})
ucase("images with no codex account: no body, the typed 503",
      [urt(IMG, [], pay=["gpt/credits"])], {}, None)

# C6's budget `no` (§1.9): Retry-After is the allowance reset ahead of the money,
# else the budget period's end (exact: 00:00Z on the next 1st); the owner's
# option is raising each budget the route has reached, beside the reset options.
ucase("a budget reached: the allowance reset, the option to raise it, and the reset",
      [urt(C_GPT, ["gpt"], pay=["gpt/credits"])],
      {"gpt": codex(used=100.0, allowed=False, events=[PAID])},
      body([{"route": C_GPT, "accounts": [acc("gpt", "budget", NOW + 4 * D, "seven_day")]}], NOW + 4 * D,
           "early_possible", [opt("budget", C_GPT, "gpt", "gpt/credits", "raise gpt/credits's budget"),
                              ev_opt(C_GPT, "gpt", PAID, ", for a fee")]),
      pots=B100, ledger=ledger(spend=100.0))
ucase("a budget reached with no reset known: Retry-After the period's end, exact",
      [urt(C_GPT, ["gpt"], pay=["gpt/credits"])], {"gpt": codex(used=100.0, allowed=False, reset=None)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "budget", MONTH_END, None)]}], MONTH_END, "exact",
           [opt("budget", C_GPT, "gpt", "gpt/credits", "raise gpt/credits's budget")]),
      pots=B100, ledger=ledger(spend=100.0))
ucase("a pot budget and the route's budget both reached: an option for each",
      [urt(C_GPT, ["gpt"], pay=["gpt/credits"])], {"gpt": codex(used=100.0, allowed=False, reset=None)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "budget", MONTH_END, None)]}], MONTH_END, "exact",
           [opt("budget", C_GPT, "gpt", "gpt/credits", "raise gpt/credits's budget"),
            opt("budget", C_GPT, "gpt", "gpt/credits", f"raise {C_GPT}'s route budget on gpt/credits")]),
      pots={"gpt/credits": {"topup": "off", "budget": 100.0, "route_budgets": {C_GPT: 10.0}}},
      ledger=ledger(spend=100.0, routes={C_GPT: 10.0}))
ucase("only the route's budget reached: the option names it alone",
      [urt(C_GPT, ["gpt"], pay=["gpt/credits"])], {"gpt": codex(used=100.0, allowed=False, reset=None)},
      body([{"route": C_GPT, "accounts": [acc("gpt", "budget", MONTH_END, None)]}], MONTH_END, "exact",
           [opt("budget", C_GPT, "gpt", "gpt/credits", f"raise {C_GPT}'s route budget on gpt/credits")]),
      pots={"gpt/credits": {"topup": "off", "budget": 100.0, "route_budgets": {C_GPT: 10.0}}},
      ledger=ledger(spend=40.0, routes={C_GPT: 10.0}))

# C8: Runway, the images class's second member (priority 2).
RW_C = "images/runway/gpt_image_2"
def rw_urt(pay=(RW_POT,)):
    return urt(RW_C, ["runway"], pay=pay, scope="gpt_image_2")
ucase("images, codex closed, runway pay []: the codex reset, a permit for each",
      [urt(IMG, ["gpt"]), rw_urt(pay=())],
      {"gpt": codex(used=100.0, allowed=False), "runway": runway()},
      body([{"route": IMG, "accounts": [acc("gpt", "pay", NOW + 4 * D, "seven_day")]},
            {"route": RW_C, "accounts": [acc("runway", "pay", "until_paid", None)]}], NOW + 4 * D,
           "early_possible", [opt("permit", IMG, "gpt", "gpt/credits", f"add gpt/credits to {IMG}'s pay"),
                              opt("permit", RW_C, "runway", RW_POT, f"add {RW_POT} to {RW_C}'s pay")]),
      request=req(5))
ucase("runway alone at a zero balance, topup off: not retryable, add money",
      [rw_urt()], {"runway": runway(balance=0.0)},
      body([{"route": RW_C, "accounts": [acc("runway", "balance", "until_paid", None)]}], None, None,
           [opt("fund", RW_C, "runway", RW_POT, f"add money to {RW_POT}")]),
      pots=TOPUP_OFF, request=req(5))
ucase("runway alone at a zero balance, topup on: retryable at the hourly refill, derived",
      [rw_urt()], {"runway": runway(balance=0.0)},
      body([{"route": RW_C, "accounts": [acc("runway", "balance", NOW - 60 + H, None)]}], NOW - 60 + H,
           "derived", [opt("fund", RW_C, "runway", RW_POT, f"add money to {RW_POT}")]),
      pots=TOPUP_ON, request=req(5))
ucase("runway with pay [] at a zero balance: money and the permit together",
      [rw_urt(pay=())], {"runway": runway(balance=0.0)},
      body([{"route": RW_C, "accounts": [acc("runway", "pay", "until_paid", None)]}], None, None,
           [opt("fund", RW_C, "runway", RW_POT, f"add money to {RW_POT}, and add {RW_POT} to {RW_C}'s pay")]),
      request=req(5))
ucase("runway's budget reached with this request's cost: raise it, to the period's end",
      [rw_urt()], {"runway": runway()},
      body([{"route": RW_C, "accounts": [acc("runway", "budget", MONTH_END, None)]}], MONTH_END, "exact",
           [opt("budget", RW_C, "runway", RW_POT, f"raise {RW_POT}'s budget")]),
      pots={RW_POT: {"topup": "off", "budget": 100.0}}, ledger=rw_ledger(95.0), request=req(10))

# A blocked member (§2.8, D5): the plan refuses the model, so the proxy reads
# its not_entitled record as no (blocked). It gives no time and no option,
# and money behind it is not offered: alone it is the non-retryable reply.
BLOCKED = chk("no", reason="blocked", until=None)
ucase("images, gpt blocked alone: no time, no option, not retryable",
      [urt(IMG, ["gpt"], pay=["gpt/credits"])], {"gpt": codex(used=10.0)},
      body([{"route": IMG, "accounts": [acc("gpt", "blocked", None, None)]}], None, None, [],
           retryable=False),
      given={IMG: {"gpt": BLOCKED}}, request=req(None))
ucase("images, gpt blocked beside runway out of money: until paid, add money to runway only",
      [urt(IMG, ["gpt"], pay=["gpt/credits"]), rw_urt()],
      {"gpt": codex(used=10.0), "runway": runway(balance=0.0)},
      body([{"route": IMG, "accounts": [acc("gpt", "blocked", None, None)]},
            {"route": RW_C, "accounts": [acc("runway", "balance", "until_paid", None)]}], None, None,
           [opt("fund", RW_C, "runway", RW_POT, f"add money to {RW_POT}")], retryable=False),
      pots=TOPUP_OFF, given={IMG: {"gpt": BLOCKED}}, request=req(5))
ucase("images, gpt blocked beside runway's refill: the refill's time, retryable",
      [urt(IMG, ["gpt"], pay=["gpt/credits"]), rw_urt()],
      {"gpt": codex(used=10.0), "runway": runway(balance=0.0)},
      body([{"route": IMG, "accounts": [acc("gpt", "blocked", None, None)]},
            {"route": RW_C, "accounts": [acc("runway", "balance", NOW - 60 + H, None)]}], NOW - 60 + H,
           "derived", [opt("fund", RW_C, "runway", RW_POT, f"add money to {RW_POT}")]),
      pots=TOPUP_ON, given={IMG: {"gpt": BLOCKED}}, request=req(5))

ucase("images, gpt blocked over a spent window with a usable reset: blocked wins, no time, no option",
      [urt(IMG, ["gpt"], pay=["gpt/credits"])],
      {"gpt": codex(used=100.0, allowed=False, credits=False, events=[CREDIT])},
      body([{"route": IMG, "accounts": [acc("gpt", "blocked", None, None)]}], None, None, [],
           retryable=False),
      given={IMG: {"gpt": BLOCKED}}, request=req(None))

doc = {"_comment": ("Dial-check golden fixtures (quota-model 50 §1.6, §5.1). Generated; both "
                    "evaluators read this file: the brain (test_exporter.py DialCheckGolden) and the "
                    "proxy (dialcheck_test.go). Times are epoch seconds; `until` is a time, "
                    "\"until_paid\" or null. `ledger` is the /preferred ledger block's period, "
                    "as_of and per-pot spend (null: none read)."),
       "now": NOW, "stale_max_s": 1800, "cases": cases, "verdicts": verdicts, "paid_orders": paid_orders,
       "unavailable": unavailable}
json.dump(doc, sys.stdout, indent=1)
sys.stdout.write("\n")
