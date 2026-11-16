"""Generate the window-record golden fixtures shared by the brain and the proxy
(quota-model 64 §5, "A refusal holds the window at 100").

`cases`: a refusal as POST /reports carries it (`record`: window, read_at,
until, resets_at, by_reading, seen), then one or more `steps`, each the
account's standing as /preferred publishes it and the instant it is judged
at; a step's `want` is whether the record ends there and, when it holds,
until when (null: until a reading ends it). A record that holds carries its
until into the next step. The proxy's record_golden_test.go runs each step
through pruneRecords, the brain's test_exporter.py (RecordsGoldenTest)
through _hold_judge.

`windows`: a live record and the window it closes, mapped to the window it
makes (used_pct, resets_at, as_of, reset_basis): the proxy's recordWindow and
recordBasis, the brain's _hold_window.

`refusals`: refusal events as POST /reports takes them (67 §9): `applied`,
posted to one brain in order, each applied; `refused`, each refused.

Regenerate with
    python3 testdata/gen_records_golden.py > testdata/records_golden.json
"""
import json

NOW = 1_800_000_000
H, D = 3600, 86400
SLACK = 600  # recResetSlack
WIN = "seven_day"


def win(used, resets, as_of, gate=None, period="7d", scope=None):
    w = {"used_pct": used, "resets_at": resets, "as_of": as_of, "period": period, "scope": scope,
         "reset_basis": "exact" if resets is not None else None}
    if gate is not None:
        w["open"] = gate  # a window's own gate (a codex additional limit's allowed)
    return w


def standing(windows=None, as_of=NOW - 60, gate=None, recheck=None):
    return {"provider": "anthropic", "kind": "subscription", "as_of": as_of,
            "next_poll_at": NOW + 240, "recheck_epoch": recheck,
            "allowance": {"open": gate, "windows": windows or {}},
            "money": {"credits": {"open": None, "balance": None, "unit": None}}}


def record(refused=NOW - 120, until=None, by_reading=True, seen_used=100.0,
           seen_resets=NOW + 3 * D, window=WIN, resets=None):
    return {"window": window, "read_at": refused, "until": until, "resets_at": resets,
            "by_reading": by_reading, "seen": {"used_pct": seen_used, "resets_at": seen_resets}}


cases = []
windows = []


def step(st, ends, until=None, now=NOW):
    return {"standing": st, "now": now, "want": {"ends": ends, "until": None if ends else until}}


def case(name, rec, st, ends, until=None, now=NOW):
    cases.append({"name": name, "record": rec, "steps": [step(st, ends, until, now)]})


def window_case(name, rec, w, want, now=NOW):
    windows.append({"name": name, "record": rec, "window": w, "now": now, "want": want})


REF = NOW - 120          # the refusal
OLD = NOW - 300          # a reading from before it
NEW = NOW - 30           # a reading after it
R7 = NOW + 3 * D         # the refused period's reset
R7_NEXT = R7 + 7 * D     # the next period's

# By time.
case("named until ahead, no newer reading: holds until it",
     record(until=R7), standing({WIN: win(100.0, R7, OLD)}), False, R7)
case("named until passed: ends",
     record(until=NOW - 1), standing({WIN: win(100.0, R7, OLD)}), True)
case("until exactly now: ends (live only before it)",
     record(until=NOW), standing({WIN: win(100.0, R7, OLD)}), True)
case("unattributed, until ahead, newer reading open and changed: only time ends it",
     record(until=NOW + H, by_reading=False), standing({WIN: win(10.0, R7_NEXT, NEW)}), False, NOW + H)
case("unattributed, until passed: ends",
     record(until=NOW - 5, by_reading=False), standing({WIN: win(100.0, R7, NEW)}), True)

# By reading: only a reading newer than the refusal judges it.
case("no until, the reading predates the refusal: holds by reading",
     record(), standing({WIN: win(10.0, R7_NEXT, OLD)}), False, None)
case("no until, the reading is as old as the refusal: not newer, holds",
     record(), standing({WIN: win(10.0, R7_NEXT, REF)}), False, None)
case("no until, the window is not in the reading: holds",
     record(), standing({"five_hour": win(10.0, NOW + H, NEW, period="5h")}), False, None)
case("an account never measured: holds",
     record(), standing({}, as_of=None), False, None)

# Open and changed ends it.
case("newer reading, a new period (reset past the slack) and open: ends",
     record(), standing({WIN: win(0.0, R7_NEXT, NEW)}), True)
case("newer reading, the same period at a lower use: ends",
     record(seen_used=100.0), standing({WIN: win(60.0, R7, NEW)}), True)
case("newer reading, a lower use and a reset that has passed: ends",
     record(seen_used=100.0, seen_resets=NOW - 10), standing({WIN: win(5.0, NOW - 10, NEW)}), True)
case("nothing seen at the refusal, newer reading open with a reset ahead: ends",
     record(seen_used=None, seen_resets=None), standing({WIN: win(40.0, R7, NEW)}), True)
case("named until ahead, newer reading open and changed: the reading ends it first",
     record(until=R7), standing({WIN: win(0.0, R7_NEXT, NEW)}), True)

# The same period at the same or a higher use does not; a closed window learns its reset.
case("newer reading, same period at the same use, closed: holds, learns the reset",
     record(), standing({WIN: win(100.0, R7, NEW)}), False, R7)
case("newer reading, same period, open at the same use: no change, holds, learns the reset",
     record(seen_used=98.0), standing({WIN: win(98.0, R7, NEW)}), False, R7)
case("newer reading, a higher use, reset moved inside the slack: holds, learns the reset",
     record(seen_used=97.0), standing({WIN: win(99.0, R7 + SLACK - 1, NEW)}), False, R7 + SLACK - 1)
case("newer reading, reset moved just past the slack, open: a new period, ends",
     record(seen_used=97.0), standing({WIN: win(99.0, R7 + SLACK + 1, NEW)}), True)
case("newer reading, a new period but at 100: closed, holds, learns the new reset",
     record(), standing({WIN: win(100.0, R7_NEXT, NEW)}), False, R7_NEXT)
case("newer reading at 100 with its reset passed: no new period, holds with no until",
     record(seen_resets=NOW - 10), standing({WIN: win(100.0, NOW - 10, NEW)}), False, None)
case("named until ahead, newer reading closed in a new period: keeps its own until",
     record(until=R7), standing({WIN: win(100.0, R7_NEXT, NEW)}), False, R7)
case("newer reading open and changed, the account's gate shut: holds",
     record(), standing({WIN: win(0.0, R7_NEXT, NEW)}, gate=False), False, R7_NEXT)
case("newer reading at a lower use, the window's own gate shut: closed, holds",
     record(), standing({WIN: win(30.0, R7, NEW, gate=False)}), False, R7)

# A point below 100 (quota-model 10, "Rounding": every 2xx at the window's last
# point reads 99, the refusal 100). Judged against the use seen at the refusal,
# never against the hold's 100: a refusal that carried no utilization saw the
# 98 held before it, and 99 is no lower use. A use at most a point below the
# one seen, in the same period (useAgrees: resets within the slack, or either
# unknown), is the seen reading again: a refusal seen at 100 holds against a
# 99 read after it, which is what a brain restarted during the hold publishes
# when its resync brings the 99 (64 §8). More than a point below, or a new
# period, still ends it.
case("a point below 100, seen at 98 (no utilization on the refusal): holds, learns the reset",
     record(seen_used=98.0, seen_resets=R7), standing({WIN: win(99.0, R7, NEW)}), False, R7)
case("a 99 read after a refusal seen at 100: a point below, the same reading, holds, learns the reset",
     record(seen_used=100.0, seen_resets=R7), standing({WIN: win(99.0, R7, NEW)}), False, R7)
case("a 99 with no reset read after a refusal seen at 100: the same period, holds",
     record(seen_used=100.0, seen_resets=R7), standing({WIN: win(99.0, None, NEW)}), False, None)
case("a 98 read after a refusal seen at 100, the same period: more than a point below, ends",
     record(seen_used=100.0, seen_resets=R7), standing({WIN: win(98.0, R7, NEW)}), True)
case("a 99 read after a refusal seen at 100, a new period: ends",
     record(seen_used=100.0, seen_resets=R7), standing({WIN: win(99.0, R7_NEXT, NEW)}), True)
case("a 99 read before a refusal seen at 100: not newer, holds",
     record(seen_used=100.0, seen_resets=R7), standing({WIN: win(99.0, R7, OLD)}), False, None)

# The owner's re-check.
case("a re-check after the refusal: ends",
     record(until=R7), standing({WIN: win(100.0, R7, OLD)}, recheck=REF + 1), True)
case("a re-check before the refusal: holds",
     record(until=R7), standing({WIN: win(100.0, R7, OLD)}, recheck=REF - 1), False, R7)

case("a re-check exactly at the refusal: holds (only a later one ends it)",
     record(until=R7), standing({WIN: win(100.0, R7, OLD)}, recheck=REF), False, R7)

# A learned until: from the first newer reading, kept after it, ended by time.
cases.append({"name": "two steps: learns the reset, keeps it past a later reading, ends at it",
              "record": record(),
              "steps": [step(standing({WIN: win(100.0, R7, NEW)}), False, R7),
                        step(standing({WIN: win(100.0, R7_NEXT, NEW + 60)}, as_of=NEW + 60),
                             False, R7, now=NOW + 120),
                        step(standing({WIN: win(100.0, R7_NEXT, NEW + 60)}, as_of=NEW + 60),
                             True, now=R7)]})

# A scoped window keeps to its own id.
case("a scoped window: judged on its own reading",
     record(window="scoped:Fable"),
     standing({"scoped:Fable": win(20.0, R7_NEXT, NEW, scope="Fable"), WIN: win(100.0, R7, NEW)}), True)

# The window a live record makes (recordWindow, recordBasis).
FIVE = "five_hour"
R5 = NOW + 2 * H  # a named reset inside the clamp [30 min, 6 h]


def held(used, resets, as_of, basis):
    return {"used_pct": used, "resets_at": resets, "as_of": as_of, "reset_basis": basis}


window_case("a named reset inside the clamp: ends at it, exact",
            record(window=FIVE, until=R5, resets=R5, by_reading=False),
            win(80.0, R5, OLD, period="5h"), held(100.0, R5, REF, "exact"))
window_case("a named reset beyond the clamp: resets at the named reset, exact",
            record(window=FIVE, until=NOW + 6 * H, resets=NOW + 3 * D, by_reading=False),
            win(80.0, R5, OLD, period="5h"), held(100.0, NOW + 3 * D, REF, "exact"))
window_case("no named reset: ends at its until, record",
            record(window=FIVE, until=NOW + 30 * 60, by_reading=False),
            win(80.0, R5, OLD, period="5h"), held(100.0, NOW + 30 * 60, REF, "record"))
_codex = win(100.0, R7, OLD)
_codex["reset_basis"] = "early_possible"
window_case("a window's own basis stands when the record's is exact",
            record(until=R7, resets=R7), _codex, held(100.0, R7, REF, "early_possible"))
window_case("a window's own basis gives way to record",
            record(until=NOW + 30 * 60, by_reading=False), dict(_codex),
            held(100.0, NOW + 30 * 60, REF, "record"))
window_case("by reading, no until, the reading's reset ahead: kept",
            record(), win(100.0, R7, OLD), held(100.0, R7, REF, "exact"))
window_case("by reading, no until, the reading's reset passed: cleared, record",
            record(), win(100.0, NOW - 10, OLD), held(100.0, None, REF, "record"))
window_case("a window read after the refusal keeps its own as_of",
            record(until=R7, resets=R7), win(100.0, R7, NEW), held(100.0, R7, NEW, "exact"))
window_case("a window the reading lacks: made at 100 from nothing",
            record(window=FIVE, until=NOW + 30 * 60, by_reading=False),
            win(None, None, None, period="5h"), held(100.0, NOW + 30 * 60, REF, "record"))

# The refusal events as POST /reports takes them (64 §5, 67 §9): a window's,
# and a limit the brain has no window for (model:<m>, pool:<name>), with its
# class, which the brain requires since 67 push 3c. `applied` go to one brain
# in order, at NOW, and each is applied; `refused` each alone, and each is
# refused. Accounts are named by provider,
# as in served_golden.json: the brain's test maps anthropic-1, codex-1 and
# kimi-1 to its own.
RREAD = NOW - 120


def refusal_ev(i, account, read_at=RREAD, **kw):
    ev = {"id": f"ref-{i:02d}", "kind": "refusal", "account": account, "read_at": read_at}
    ev.update(kw)
    return ev


refusals = {"applied": [], "refused": []}


def applied(name, ev, rec=None):
    """An applied case; rec, when given, is the proxy's record the event is
    encoded from (reportedAs, refusalEvent), and the proxy's test encodes it
    and expects exactly `event`: account, limit (the record's key), lim_kind,
    class, refused_at, until and resets (null for none), attributed, seen and
    ended_at (null when not ended)."""
    c = {"name": name, "event": ev}
    if rec is not None:
        c["record"] = rec
    refusals["applied"].append(c)


def refused(name, ev, why):
    """A refused case; why is the brain's reason, exactly."""
    refusals["refused"].append({"name": name, "event": ev, "why": why})


def proxy_record(account, key, lim_kind, cls, refused_at, until=None, resets=None, attributed=False,
                 seen=None, ended_at=None):
    return {"account": account, "limit": key, "lim_kind": lim_kind, "class": cls,
            "refused_at": refused_at, "until": until, "resets": resets, "attributed": attributed,
            "seen": seen or {"used_pct": None, "resets_at": None}, "ended_at": ended_at}


refused("a window refusal as the proxy sent it before 67: no limit, no class",
        refusal_ev(1, "anthropic-1", window=WIN, until=None, resets_at=None, by_reading=True,
                   seen={"used_pct": 100.0, "resets_at": NOW + 3 * D}),
        "class is required: spent, or not_entitled on a limit")
applied("a window refusal with class spent and limit null",
        refusal_ev(2, "anthropic-1", read_at=RREAD + 10, window=WIN, limit=None, **{"class": "spent"},
                   until=None, resets_at=None, by_reading=True,
                   seen={"used_pct": 100.0, "resets_at": NOW + 3 * D}),
        proxy_record("anthropic-1", WIN, "window", "spent", RREAD + 10, attributed=True,
                     seen={"used_pct": 100.0, "resets_at": NOW + 3 * D}))
refused("a limit with no class",
        refusal_ev(3, "anthropic-1", window=None, limit="model:claude-sonnet-5",
                   until=NOW + 30 * 60, resets_at=None, by_reading=False, seen=None),
        "class is required: spent, or not_entitled on a limit")
applied("a model limit with its class",
        refusal_ev(4, "anthropic-1", window=None, limit="model:claude-haiku-5", **{"class": "spent"},
                   until=NOW + 30 * 60, resets_at=None, by_reading=False, seen=None),
        proxy_record("anthropic-1", "model:claude-haiku-5", "model", "spent", RREAD,
                     until=NOW + 30 * 60))

# 67 §9's cases. A model limit: an Anthropic 429 whose rejected window no
# alias names, with the reset it named inside the clamp.
MREAD, MUNTIL, MRESET = RREAD + 20, RREAD + 20 + 2 * H, RREAD + 20 + 2 * H
applied("a model limit",
        refusal_ev(5, "anthropic-1", read_at=MREAD, window=None, limit="model:claude-opus-4-1",
                   **{"class": "spent"}, until=MUNTIL, resets_at=MRESET, by_reading=False, seen=None),
        proxy_record("anthropic-1", "model:claude-opus-4-1", "model", "spent", MREAD, until=MUNTIL,
                     resets=MRESET))
applied("a pool limit",
        refusal_ev(6, "kimi-1", window=None, limit="pool:total_quota", **{"class": "spent"},
                   until=RREAD + 30 * 60, resets_at=None, by_reading=False, seen=None),
        proxy_record("kimi-1", "total_quota", "pool", "spent", RREAD, until=RREAD + 30 * 60))
applied("a not_entitled model",
        refusal_ev(7, "codex-1", window=None, limit="model:gpt-6.1-sol", **{"class": "not_entitled"},
                   until=RREAD + 6 * H, resets_at=None, by_reading=False, seen=None),
        proxy_record("codex-1", "model:gpt-6.1-sol", "model", "not_entitled", RREAD,
                     until=RREAD + 6 * H))
applied("a model limit ended",
        refusal_ev(8, "anthropic-1", read_at=MREAD + 40, window=None, limit="model:claude-opus-4-1",
                   **{"class": "spent"}, until=MUNTIL, resets_at=MRESET, by_reading=False, seen=None,
                   ended=True),
        proxy_record("anthropic-1", "model:claude-opus-4-1", "model", "spent", MREAD, until=MUNTIL,
                     resets=MRESET, ended_at=MREAD + 40))

refused("both window and limit",
        refusal_ev(20, "anthropic-1", window=WIN, limit="model:claude-opus-4-1", **{"class": "spent"},
                   until=RREAD + 30 * 60, resets_at=None, by_reading=False, seen=None),
        "a refusal names exactly one of window and limit")
refused("neither",
        refusal_ev(21, "anthropic-1", window=None, limit=None, **{"class": "spent"},
                   until=RREAD + 30 * 60, resets_at=None, by_reading=False, seen=None),
        "a refusal names exactly one of window and limit")
refused("a limit with no until",
        refusal_ev(22, "anthropic-1", window=None, limit="model:claude-opus-4-1", **{"class": "spent"},
                   until=None, resets_at=None, by_reading=False, seen=None),
        "a limit's refusal carries its until: no reading can end it")
refused("an unknown limit prefix",
        refusal_ev(23, "anthropic-1", window=None, limit="scope:Sonnet", **{"class": "spent"},
                   until=RREAD + 30 * 60, resets_at=None, by_reading=False, seen=None),
        "unknown limit 'scope:Sonnet': a limit is model:<model> or pool:<name>")

print(json.dumps({"reset_slack_s": SLACK, "cases": cases, "windows": windows,
                  "refusals": refusals}, indent=1))
