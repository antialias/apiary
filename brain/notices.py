"""Prepaid "needs payment" notices (shared): what the owner is told when a
prepaid account runs out of money, and what Scry shows of it.

A prepaid account is an `api` account whose money pot has `topup` off in the
money document: nothing refills it but the owner. Which accounts those are,
what each one serves and where it is paid are declarations (the money
document; a driver's `name`, `serves` and `billing`), so an account added
later joins with no code here. The core builds one view of each at every
compute_preferred (core._notices_views) and calls Notices.tick() with it;
this module keeps what it has seen and sent, decides what is due, and
delivers it.

  ran dry   one message when the account goes dry. It names the account,
            what stops working, whether another member of each of its
            classes still covers it, and where to pay.
  reminder  while it stays dry, one more every 24 h after the ran-dry
            message, on that message's schedule.
  low       for an account whose balance is read: one warning when the
            balance would run out within about a day at the last 7 days'
            pace, unless the pot's own reset comes first. One per cycle: a
            recharge (a purchase, a plan's cycle reset) ends the cycle,
            clears the warning and anything due, so a purchase after a
            warning is followed by no ran-dry message. None within 24 h of a
            ran-dry or reminder message.

A recharge is a rise in the polled balance that leaves more than a day's
burn, or any rise while the pace is unknown. A smaller rise (a refund near
zero) ends nothing, so a balance that wobbles near zero does not flap.

One rule of dry for every prepaid pot. It starts when the pot's own check
reads `no (balance, until_paid)` on a reading newer than the last dry's end,
or when the proxy reports the pot shut (a view's shut_at) after that end; a
shut counts on a pot the poll reads too (a "not enough credits for this
job" refusal with credits left). It ends only when money shows: a recharge,
a counted ledger entry on the pot newer than the dry's start (it served a
paid request), or the owner's re-check after the start. A poll that reads a
balance with no recharge does not end it. A dry that starts within
24 h of the account's last ran-dry or reminder message sends no ran-dry: it
keeps that message's reminder schedule, so an account that flaps is told at
most once a day.

There is no "it's back" message and no quiet hours: a message goes when its
event happens, and its times are in UTC.

The queue. Beside the prepaid rules, an edition's own code may hand this
sender one message to deliver (queue(id, kind, account, title, summary, desc,
labels)): its words and its labels are the caller's, and it goes through the
same bridge, worker, retry rule and file as the prepaid notices. The id is the
caller's and names one message: queueing an id already queued or sent does
nothing, so a caller that queues again after a restart sends nothing twice. A
queued message on a tenant whose notices are off (no bridge, or not bridged)
is recorded as not sent, with the reason, and never posted. tick() never
touches the queue, and the queue never touches the prepaid records.

Delivery is the proxy's: Alertmanager-webhook JSON posted to the alert bridge
(SWITCH_BRIDGE_URL), labels {alertname, alertroute: prepaid, delivery:
critical, account} (a queued message's labels are its caller's), so each goes
to chat and a phone push. The tick only
decides; one worker thread posts (start(), kick()), so a slow bridge never
holds compute_preferred or /reports. A message counts as delivered on a 2xx
whose body says any channel delivered (telegram or ha true), each channel's
result recorded; a total failure (not a 2xx, or no channel true) stays due
and is tried again under the same id, RETRY_FIRST_S after the first try,
doubling to RETRY_MAX_S. With no bridge address the notices are off and
/preferred says so (enabled false, why).

The state is a JSON file (SWITCH_NOTICES_FILE), written atomically on each
change and read at start, so a restart neither forgets nor resends:

  {"version": 1, "seq": n, "accounts": {account: {state: ok|low|dry, since,
   warned_at, dry_sent_at, last_reminder_at, dry_at (the last dry's start),
   end_at (what ended it, at), samples: [[at, balance]...],
   sent: [<= SENT_MAX notices, oldest first], pending: {id, kind, slot, words,
   tries, next_at} | null}},
   "queued": {"pending": [{id, kind, account, words, labels, queued_at, tries,
   next_at}...], "sent": [<= SENT_MAX, oldest first]}}

It is not fsynced, as the brain's own state file (_save_state) is not: a
power cut can lose the last change, and with it at most one resend.

/preferred `notices` (payload()) is the contract Scry reads: per prepaid
account its state, since, name, serves, billing_url, pace, classes and sent;
and `queued`, the queue's sent ring (each message's id, kind, account, at,
ok, channels, tries, detail), so a page can say whether one was delivered.
"""
import json
import math
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

DAY = 86400.0
WINDOW_S = 7 * DAY      # the pace is the falls of the last 7 days
SENT_MAX = 50           # the sent ring
RETRY_FIRST_S = 60.0    # a failed delivery is tried again after this,
RETRY_MAX_S = 1800.0    # doubling, at most this
WORKER_PERIOD_S = 30.0  # the worker's pass when nothing kicks it
POST_TIMEOUT_S = 5.0    # the bridge's answer
KINDS = ("low", "dry", "reminder")
ALERTNAMES = {"low": "PrepaidAccountLow", "dry": "PrepaidAccountDry",
              "reminder": "PrepaidAccountDryReminder"}
NO_BRIDGE = "no bridge address"
NOT_LAN = "not a LAN tenant: the bridge is the household's"

# The bridge address when SWITCH_BRIDGE_URL is not in the environment: none
# here. An edition that has a bridge declares it (set_bridge_default). The
# variable set but empty turns the notices off whatever the default.
_bridge_default = ""


def set_bridge_default(url):
    """Declare the edition's bridge address (household code)."""
    global _bridge_default
    _bridge_default = url or ""


def bridge_url():
    """The address notices post to, read at each call: SWITCH_BRIDGE_URL when
    it is in the environment (empty: off), else the edition's default."""
    if "SWITCH_BRIDGE_URL" in os.environ:
        return os.environ["SWITCH_BRIDGE_URL"].strip()
    return _bridge_default


def _http_post(url, body):
    """POST body as JSON -> (status, reply bytes). An HTTP error status is an
    answer, not an exception; no answer at all raises."""
    req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=POST_TIMEOUT_S) as resp:
            return resp.status, resp.read(65536)
    except urllib.error.HTTPError as e:
        return e.code, e.read(65536)


def _new_record():
    return {"state": "ok", "since": None, "warned_at": None, "dry_sent_at": None,
            "last_reminder_at": None, "dry_at": None, "end_at": None, "samples": [], "sent": [],
            "pending": None}


# ---- the pace ------------------------------------------------------------------

def pace(samples, balance, unit=None):
    """{per_day, unit, days_left} from the samples' falls over their span (at
    most WINDOW_S), or None: fewer than 24 h of samples, a burn of 0, or no
    balance read."""
    if balance is None or len(samples) < 2:
        return None
    span = samples[-1][0] - samples[0][0]
    if span < DAY:
        return None
    falls = sum(max(0.0, a[1] - b[1]) for a, b in zip(samples, samples[1:]))
    if falls <= 0:
        return None
    per_day = falls / (min(span, WINDOW_S) / DAY)
    return {"per_day": round(per_day, 2), "unit": unit,
            "days_left": round(max(balance, 0.0) / per_day, 2)}


def _sample(rec, balance, at):
    """Keep the polled balance read at `at`: on a change, and once a day while
    it holds. -> True when it is a rise. Samples older than WINDOW_S before the
    newest go."""
    if balance is None or at is None:
        return False
    s = rec["samples"]
    last = s[-1] if s else None
    if last is not None and at <= last[0]:
        return False
    if last is not None and balance == last[1] and at - last[0] < DAY:
        return False
    s.append([at, balance])
    floor = at - WINDOW_S
    while len(s) > 1 and s[0][0] < floor:
        s.pop(0)
    return last is not None and balance > last[1]


# ---- the words -----------------------------------------------------------------

def _num(v):
    return f"{v:,.0f}" if abs(v) >= 10 else f"{v:,.1f}".rstrip("0").rstrip(".")


def _unit_words(unit):
    """acme_credit -> credits, acme_input_token -> tokens."""
    if not unit:
        return "units"
    return unit.rsplit("_", 1)[-1] + "s"


def _utc(t, with_time=True):
    d = datetime.fromtimestamp(t, timezone.utc)
    s = f"{d:%b} {d.day}"
    return f"{s} {d:%H:%M} UTC" if with_time else s


def _serves(v):
    s = v.get("serves") or "Its requests"
    return s[:1].upper() + s[1:]


def _coverage(classes):
    """'Class images still has other/model-x; images-acme has nothing
    else', or 'nothing else covers it' with no class."""
    if not classes:
        return "Nothing else covers it"
    parts = []
    for i, c in enumerate(classes):
        name = ("Class " if i == 0 else "") + c["class"]
        parts.append(f"{name} still has {c['covered_by']}" if c.get("covered_by")
                     else f"{name} has nothing else")
    return "; ".join(parts)


def message(kind, account, v, since=None, test=False):
    """(title, summary, description) of one notice on view v."""
    name = v.get("name") or account
    serves = _serves(v)
    pay = f" Top up: {v['billing_url']}" if v.get("billing_url") else ""
    if kind == "low":
        p = v.get("pace") or {}
        unit = _unit_words(v.get("unit"))
        days = p.get("span_days") or 7
        title = f"{name}: about a day of credit left"
        summary = f"{serves} stops within about a day."
        desc = (f"{_num(v['balance'])} {unit} left; the last {days} day{'s' if days != 1 else ''} "
                f"spent about {_num(p.get('per_day') or 0)} a day. {serves} stops when it runs "
                f"out; {_coverage(v.get('classes'))[:1].lower()}{_coverage(v.get('classes'))[1:]}."
                + pay)
    elif kind == "dry":
        title = f"{name} ran dry: needs payment"
        summary = f"{serves} stopped."
        desc = f"{serves} stopped. {_coverage(v.get('classes'))}."
        if v.get("resets_at"):
            desc += f" Credits reset {_utc(v['resets_at'], with_time=False)}."
        desc += pay
    else:
        title = f"{name} still needs payment"
        summary = f"{serves} is still stopped."
        desc = f"{serves} stopped" + (f" since {_utc(since)}" if since else "") + "." + pay
    if test:
        title, summary = "[test] " + title, "[test] " + summary
    return title, summary, desc


def alert_body(kind, account, title, summary, desc, labels=None):
    """The bridge's Alertmanager-webhook JSON, as the proxy posts it. labels:
    a queued message's own (its caller's); else the prepaid kind's."""
    if labels is None:
        labels = {"alertname": ALERTNAMES[kind], "alertroute": "prepaid",
                  "delivery": "critical", "account": account}
    return {"status": "firing", "alerts": [{
        "status": "firing", "labels": dict(labels),
        "annotations": {"title": title, "summary": summary, "description": desc}}]}


def outcome(status, reply):
    """(ok, channels, detail) of one bridge answer. channels is each channel's
    result as the bridge says it ({telegram: true, ha: false}); ok on a 2xx
    where any channel delivered. detail names what failed, or None."""
    try:
        r = json.loads(reply or b"{}")
    except ValueError:
        r = {}
    r = r if isinstance(r, dict) else {}
    # Only the delivery channels: the bridge's reply also carries ok/critical/delivered booleans.
    channels = {k: r[k] for k in ("telegram", "ha") if isinstance(r.get(k), bool)}
    if not 200 <= status < 300:
        return False, channels, f"bridge HTTP {status}"
    if not any(channels.values()):
        return False, channels, "no channel delivered"
    failed = sorted(k for k, v in channels.items() if not v)
    return True, channels, (", ".join(failed) + " not delivered") if failed else None


def _new_queue():
    return {"pending": [], "sent": []}


QUEUE_LABELS = ("alertname", "alertroute", "delivery", "account")


def _retry_after(tries):
    """Seconds before the next try after `tries` failed ones: RETRY_FIRST_S,
    doubling, at most RETRY_MAX_S."""
    return min(RETRY_FIRST_S * 2 ** max(0, tries - 1), RETRY_MAX_S)


# ---- the engine ----------------------------------------------------------------

class Notices:
    """The notices' state and delivery. tick() folds the views in and decides
    what is due, quickly, in the caller's path; deliver() posts what is due,
    on the worker thread (start(), kick()), never in the caller's path.
    payload() reads. post(url, body) -> (status, reply bytes) is the bridge (a
    test's fake); log(msg) the brain's log. bridged says whether this
    tenant's notices reach the bridge at all (the LAN tenant's do; another
    tenant's are recorded and never posted). store, when given, reads and
    writes the file (the core's sealed store); else plain JSON."""

    def __init__(self, path, post=None, log=print, bridged=True, store=None):
        self.path = path
        self.post = post or _http_post
        self.log = log
        self.bridged = bridged
        self.store = store
        self._lock = threading.Lock()          # the state, held briefly
        self._deliver_lock = threading.Lock()  # one delivery pass at a time
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._worker = None
        self._accounts = None
        self._queued = None
        self._seq = 0

    # -- the file --

    def _load_locked(self):
        if self._accounts is not None:
            return
        self._accounts, self._queued, self._seq = {}, _new_queue(), 0
        try:
            doc = self._read()
        except FileNotFoundError:
            return
        except Exception as e:  # noqa: BLE001 — OSError, ValueError, CredStoreError
            self.log(f"WARN notices: cannot read {self.path} ({e}); starting with nothing sent")
            return
        if doc is None:
            return
        if not isinstance(doc, dict) or not isinstance(doc.get("accounts"), dict):
            self.log(f"WARN notices: {self.path} is not a notices file; starting with nothing sent")
            return
        self._seq = int(doc.get("seq") or 0)
        for acct, rec in doc["accounts"].items():
            if isinstance(rec, dict):
                self._accounts[acct] = dict(_new_record(), **rec)
        q = doc.get("queued")
        if isinstance(q, dict):
            self._queued = {k: [e for e in (q.get(k) or []) if isinstance(e, dict) and e.get("id")]
                            for k in ("pending", "sent")}

    def _read(self):
        if self.store is not None:
            return self.store.read_json(self.path)
        with open(self.path) as f:
            return json.load(f)

    def _save_locked(self):
        doc = {"version": 1, "seq": self._seq, "accounts": self._accounts,
               "queued": self._queued}
        try:
            if self.store is not None:
                self.store.write_json(self.path, doc, sort_keys=True)
                return
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(doc, f, sort_keys=True)
            os.replace(tmp, self.path)
        except OSError as e:
            self.log(f"WARN notices: could not write {self.path}: {e}")

    def url(self):
        """The bridge this tenant's notices post to: the edition's, when the
        tenant is bridged; "" otherwise."""
        return bridge_url() if self.bridged else ""

    def load(self):
        """Read the file now (main(); otherwise the first use reads it)."""
        with self._lock:
            self._load_locked()
            return len(self._accounts)

    def reset(self):
        """Forget everything and remove the file (tests)."""
        with self._lock:
            self._accounts, self._queued, self._seq = {}, _new_queue(), 0
            for p in (self.path, self.path + ".tmp"):
                try:
                    os.remove(p)
                except FileNotFoundError:
                    pass

    # -- the tick --

    def _observe(self, acct, rec, v, now):
        """Fold one view into the record: the balance sample, a recharge, the
        state. -> True when the record changed."""
        changed = False
        n = len(rec["samples"])
        last = rec["samples"][-1][:] if n else None
        rose = _sample(rec, v.get("balance"), v.get("read_at"))
        if len(rec["samples"]) != n or (rec["samples"] and rec["samples"][-1] != last):
            changed = True
        p = pace(rec["samples"], v.get("balance"), v.get("unit"))
        # A recharge is a rise that leaves more than a day's burn, or any rise
        # while the pace is unknown; a smaller one (a refund) ends nothing.
        recharge = rose and (p is None or v["balance"] > p["per_day"])
        if recharge and (rec["warned_at"] is not None or rec["pending"] is not None):
            # A recharge ends the cycle: the warning and anything due.
            rec.update(warned_at=None, pending=None)
            changed = True
        dry = rec["state"] == "dry"
        if dry:
            end = self._dry_end(rec, v, recharge, now)
            if end is not None:
                rec["end_at"], dry = end, False
                changed = True
        else:
            start = self._dry_start(rec, v, now)
            if start is not None:
                rec["dry_at"], dry = start, True
                if not self._told_within_a_day(rec, now):
                    # A new episode: its own ran-dry. Within 24 h of the last
                    # message it keeps that message's reminder schedule.
                    rec.update(dry_sent_at=None, last_reminder_at=None)
                changed = True
        if dry:
            state = "dry"
        elif (p is not None and 0 < v["balance"] <= p["per_day"]
              and not (v.get("resets_at") and v["resets_at"] < now + p["days_left"] * DAY)):
            # Low, unless the pot's own reset comes before it would run out.
            state = "low"
        else:
            state = "ok"
        if state != rec["state"]:
            self.log(f"notices: {acct} {rec['state']} -> {state}")
            rec["state"], rec["since"] = state, now
            changed = True
        return changed

    @staticmethod
    def _told_within_a_day(rec, now):
        """Was a ran-dry or reminder message sent in the last 24 h?"""
        last = max(rec["dry_sent_at"] or 0, rec["last_reminder_at"] or 0)
        return bool(last) and now - last < DAY

    @staticmethod
    def _dry_start(rec, v, now):
        """When a dry starts on this view, or None: the proxy's shut report,
        or the pot's check reading no, on evidence newer than the last dry's
        end (a resent report or a stale reading starts none). The check's
        evidence is the view's dry_at (the report it reads), else the reading."""
        end = rec.get("end_at")
        shut = v.get("shut_at")
        if shut is not None and (end is None or shut > end):
            return shut
        if v.get("dry"):
            at = v.get("dry_at") or v.get("read_at") or now
            if end is None or at > end:
                return at
        return None

    @staticmethod
    def _dry_end(rec, v, recharge, now):
        """What ends the dry, at, or None: money shows. A recharge of the
        polled balance, a counted ledger entry on the pot after the dry's
        start, or the owner's re-check after it."""
        if recharge:
            return v.get("read_at") or now
        start = rec.get("dry_at")
        if start is None:  # a dry from before the rule: its start is unknown
            start = rec.get("since") or 0
        for k in ("charged_at", "recheck_at"):
            t = v.get(k)
            if t is not None and t > start:
                return t
        return None

    def _due(self, rec, now):
        """(kind, slot) of what is due on the record, or None. A reminder's
        slot is the 24 h mark of the ran-dry message it is sent for: the
        latest one passed, so a brain that was down sends one, not each. No
        low warning within 24 h of a ran-dry or reminder message."""
        if rec["state"] == "dry":
            if rec["dry_sent_at"] is None:
                return "dry", None
            base = rec["last_reminder_at"] or rec["dry_sent_at"]
            if now >= base + DAY:
                k = math.floor((now - rec["dry_sent_at"]) / DAY)
                return "reminder", rec["dry_sent_at"] + k * DAY
            return None
        if rec["state"] == "low" and rec["warned_at"] is None and not self._told_within_a_day(rec, now):
            return "low", None
        return None

    def tick(self, views, now):
        """One pass in the caller's path: fold each view in and note what is
        due, with its words; the worker delivers it (kick()). views is
        {account: view} for every prepaid account now (core._notices_views).
        -> True when anything changed."""
        url = self.url()
        with self._lock:
            self._load_locked()
            changed = False
            for acct in sorted(views):
                v = views[acct]
                rec = self._accounts.setdefault(acct, _new_record())
                changed |= self._observe(acct, rec, v, now)
                due = self._due(rec, now)
                p = rec["pending"]
                if due is None or not url:
                    if p is not None:
                        rec["pending"] = None
                        changed = True
                    continue
                if p is None or p["kind"] != due[0] or p.get("slot") != due[1]:
                    self._seq += 1
                    p = rec["pending"] = {"id": f"n-{int(now)}-{self._seq}", "kind": due[0],
                                          "slot": due[1], "tries": 0, "next_at": None}
                    changed = True
                pv = dict(v, pace=pace(rec["samples"], v.get("balance"), v.get("unit")))
                if pv["pace"] is not None:
                    pv["pace"]["span_days"] = max(1, round(min(
                        rec["samples"][-1][0] - rec["samples"][0][0], WINDOW_S) / DAY))
                words = list(message(due[0], acct, pv, since=rec["since"]))
                if p.get("words") != words:
                    p["words"] = words
                    changed = True
            if changed:
                self._save_locked()
        return changed

    # -- delivery --

    def deliver(self, now):
        """Post every pending notice whose time has come, and record each
        answer. A 2xx where any channel delivered is sent; anything else is
        tried again under the same id after _retry_after. -> the number
        posted. Runs on the worker; a pass already running makes this one a
        no-op."""
        url = self.url()
        if not url or not self._deliver_lock.acquire(blocking=False):
            return 0
        try:
            with self._lock:
                self._load_locked()
                out = [(acct, dict(rec["pending"])) for acct, rec in sorted(self._accounts.items())
                       if rec["pending"] is not None and rec["pending"].get("words")
                       and (rec["pending"].get("next_at") or 0) <= now]
                queued = [dict(q) for q in self._queued["pending"] if (q.get("next_at") or 0) <= now]
            for acct, p in out:
                ok, channels, detail = self._post(url, p["kind"], acct, *p["words"])
                with self._lock:
                    self._delivered_locked(acct, p, ok, channels, detail, now)
                    self._save_locked()
            for q in queued:
                ok, channels, detail = self._post(url, q["kind"], q["account"], *q["words"],
                                                  labels=q["labels"])
                with self._lock:
                    self._queued_delivered_locked(q, ok, channels, detail, now)
                    self._save_locked()
            return len(out) + len(queued)
        finally:
            self._deliver_lock.release()

    def _delivered_locked(self, acct, p, ok, channels, detail, now):
        rec = self._accounts.get(acct)
        if rec is None:
            return
        tries = (p.get("tries") or 0) + 1
        self._record_locked(rec, {"id": p["id"], "at": now, "kind": p["kind"], "ok": ok,
                                  "telegram": channels.get("telegram"), "ha": channels.get("ha"),
                                  "channels": channels, "tries": tries, "detail": detail})
        still = (rec["pending"] or {}).get("id") == p["id"]
        if ok:
            if still:
                rec["pending"] = None
            # A message sent counts for the 24 h rule even when its episode ended in flight.
            if p["kind"] == "dry":
                rec["dry_sent_at"] = now
            elif p["kind"] == "reminder":
                rec["last_reminder_at"] = p["slot"]
            elif still:
                rec["warned_at"] = now
            self.log(f"notices: {acct} {p['kind']} sent ({p['id']}" + (f"; {detail}" if detail else "") + ")")
        elif still:
            rec["pending"].update(tries=tries, next_at=now + _retry_after(tries))
            if tries == 1:
                self.log(f"WARN notices: {acct} {p['kind']} not delivered ({detail}); "
                         f"trying again from {RETRY_FIRST_S:.0f} s, doubling to {RETRY_MAX_S:.0f} s")

    @staticmethod
    def _record_locked(rec, entry):
        """Keep entry in the sent ring, replacing an earlier try of its id."""
        sent = rec["sent"]
        for i, e in enumerate(sent):
            if e["id"] == entry["id"]:
                sent[i] = entry
                break
        else:
            sent.append(entry)
        del sent[:-SENT_MAX]

    def _post(self, url, kind, acct, title, summary, desc, labels=None):
        try:
            status, reply = self.post(url, alert_body(kind, acct, title, summary, desc, labels))
        except Exception as e:  # noqa: BLE001 — no answer at all
            return False, {}, f"{type(e).__name__}: {e}"[:300]
        return outcome(status, reply)

    # -- the queue --

    def queue(self, id, kind, account, title, summary, desc, labels, now=None):
        """Queue one message of the caller's: delivered by the worker's next
        pass (kick() it), with the prepaid notices' retry rule under this id.
        labels are the message's whole label set (alertname, alertroute,
        delivery, account). An id already queued or sent is not queued again.
        With the notices off it is recorded as not sent and never posted.
        Written to the file before it returns. -> True when it was queued."""
        if not (isinstance(id, str) and id) or not all(isinstance(labels.get(k), str) and labels[k]
                                                       for k in QUEUE_LABELS):
            raise ValueError("a queued message needs an id and its labels: " + ", ".join(QUEUE_LABELS))
        now = time.time() if now is None else now
        url = self.url()
        with self._lock:
            self._load_locked()
            q = self._queued
            if any(e["id"] == id for e in q["pending"]) or any(e["id"] == id for e in q["sent"]):
                return False
            if not url:
                self._record_locked(q, {"id": id, "at": now, "kind": kind, "account": account,
                                        "ok": False, "telegram": None, "ha": None, "channels": {},
                                        "tries": 0, "detail": NO_BRIDGE if self.bridged else NOT_LAN})
                self.log(f"notices: {account} {kind} recorded, not sent ({id}; "
                         f"{NO_BRIDGE if self.bridged else NOT_LAN})")
            else:
                q["pending"].append({"id": id, "kind": kind, "account": account,
                                     "words": [title, summary, desc], "labels": dict(labels),
                                     "queued_at": now, "tries": 0, "next_at": None})
            self._save_locked()
        return True

    def _queued_delivered_locked(self, q, ok, channels, detail, now):
        tries = (q.get("tries") or 0) + 1
        self._record_locked(self._queued, {
            "id": q["id"], "at": now, "kind": q["kind"], "account": q["account"], "ok": ok,
            "telegram": channels.get("telegram"), "ha": channels.get("ha"), "channels": channels,
            "tries": tries, "detail": detail})
        pend = self._queued["pending"]
        mine = next((e for e in pend if e["id"] == q["id"]), None)
        if mine is None:
            return
        if ok:
            pend.remove(mine)
            self.log(f"notices: {q['account']} {q['kind']} sent ({q['id']}" + (f"; {detail}" if detail else "") + ")")
        else:
            mine.update(tries=tries, next_at=now + _retry_after(tries))
            if tries == 1:
                self.log(f"WARN notices: {q['account']} {q['kind']} not delivered ({detail}); "
                         f"trying again from {RETRY_FIRST_S:.0f} s, doubling to {RETRY_MAX_S:.0f} s")

    # -- the worker --

    def start(self):
        """Run the delivery worker (main()): a pass at each kick() and every
        WORKER_PERIOD_S, for the retries."""
        if self._worker is not None:
            return
        self._stop.clear()

        def run():
            while not self._stop.is_set():
                self._wake.wait(WORKER_PERIOD_S)
                self._wake.clear()
                if self._stop.is_set():
                    break
                try:
                    self.deliver(time.time())
                except Exception as e:  # noqa: BLE001 — the worker outlives one bad pass
                    self.log(f"WARN notices: delivery pass failed: {type(e).__name__}: {e}")

        self._worker = threading.Thread(target=run, name="notices", daemon=True)
        self._worker.start()

    def kick(self):
        """Wake the worker: something may be due (compute_preferred, after
        the document is marked)."""
        self._wake.set()

    def stop(self):
        """Stop the worker (tests)."""
        self._stop.set()
        self._wake.set()
        w, self._worker = self._worker, None
        if w is not None:
            w.join(timeout=POST_TIMEOUT_S + 1)

    # -- the document --

    def payload(self, views):
        """/preferred `notices`: every prepaid account, ok ones too."""
        url = self.url()
        with self._lock:
            self._load_locked()
            accounts = {}
            for acct in sorted(views):
                v = views[acct]
                rec = self._accounts.get(acct) or _new_record()
                accounts[acct] = {
                    "state": rec["state"], "since": rec["since"], "name": v.get("name"),
                    "serves": v.get("serves"), "billing_url": v.get("billing_url"),
                    "pace": pace(rec["samples"], v.get("balance"), v.get("unit")),
                    "classes": [dict(c) for c in v.get("classes") or []],
                    "sent": [dict(e) for e in rec["sent"]]}
            queued = [dict(e) for e in self._queued["sent"]]
        why = None if url else (NO_BRIDGE if self.bridged else NOT_LAN)
        return {"enabled": bool(url), "why": why, "accounts": accounts, "queued": queued}


# ---- the owner's test message --------------------------------------------------

def send_test(account, preferred_url=None, post=None, tenant=None):
    """Send one "[test]"-marked ran-dry message for `account` through the
    bridge, so the owner sees the real format once. Run in a one-off process
    beside the brain, with its environment (it reads the running brain's
    /t/<tenant>/preferred and records nothing):

        docker exec <the brain's container> python3 -c \\
            "import notices; print(notices.send_test('<account>', tenant='<tenant>'))"

    An edition that declares its bridge in code (set_bridge_default) imports
    that module before notices. -> {ok, telegram, ha, channels, detail}."""
    url = bridge_url()
    if not url:
        return {"ok": False, "telegram": None, "ha": None, "channels": {}, "detail": NO_BRIDGE}
    if not preferred_url and not tenant:
        raise ValueError("a tenant is required without preferred_url")
    src = preferred_url or f"http://127.0.0.1:{os.environ.get('PORT', '9115')}/t/{tenant}/preferred"
    with urllib.request.urlopen(src, timeout=POST_TIMEOUT_S) as resp:
        doc = json.loads(resp.read().decode("utf-8"))
    n = ((doc.get("notices") or {}).get("accounts") or {}).get(account)
    if n is None:
        raise ValueError(f"{account!r} is not a prepaid account in {src}")
    v = {"name": n.get("name"), "serves": n.get("serves"),
         "billing_url": n.get("billing_url"), "classes": n.get("classes")}
    title, summary, desc = message("dry", account, v, test=True)
    try:
        status, reply = (post or _http_post)(url, alert_body("dry", account, title, summary, desc))
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "telegram": None, "ha": None, "channels": {},
                "detail": f"{type(e).__name__}: {e}"}
    ok, channels, detail = outcome(status, reply)
    return {"ok": ok, "telegram": channels.get("telegram"), "ha": channels.get("ha"),
            "channels": channels, "detail": detail}

