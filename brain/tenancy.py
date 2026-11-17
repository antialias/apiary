"""Tenants (shared): who a brain serves. One brain serves every tenant; every
route that reads or writes tenant data is `/t/<tenant>/…`, bound once at the
dispatcher and passed down, never looked up globally (MEMBERS-PLAN.md, 1a).

A Tenant carries every piece of per-tenant state the core and the drivers
keep: its locks, its documents, its poll health, its events publisher, its
notices and, in `stores`, each driver's credentials and watchers for this
tenant alone. The core fills the state fields (claude_usage_exporter._tenant_fresh);
this module knows only the shape, the files and the registry.

The registry is `<creds dir>/tenants.json`, `[{tenant, name, lan, tap}]`: the
one file outside a tenant's dir besides classes.json, lanes.json, the key and
the switch key. It holds no secret and is plain JSON whatever the store's mode.
It holds exactly one `lan` record, the LAN tenant (the household; Apiary's one
tenant), whose notices reach the owner's bridge: a brain refuses to start
without the file or with another count of them, and `lan` is set once, by the
migration or init, never changed or deleted on /tenants.
"""
import json
import os
import re
import shutil
import threading
import time

TENANT_RE = re.compile(r"^ten_[a-z2-7]{26}$")
TAPS = ("content", "metadata")
NAME_MAX = 64

# The files a tenant's dir holds, by the key the core reads them under. The
# credentials among them are the drivers' (each names its own file).
FILES = {
    "state": "switch_state.json",
    "consumers": "consumers.json",
    "money": "money.json",
    "notices": "notices.json",
    "recheck": "rechecks.json",
    "ledger": "ledger.jsonl",
    "sizing": "sizing.json",
    "harness": "harness.json",
}


class RegistryError(ValueError):
    """tenants.json cannot be served as it is: the brain refuses to start."""


class RegistryConflict(Exception):
    """A change the registry refuses (409): a second lan tenant, a change to
    lan, the lan tenant's deletion."""


ONE_LAN_RULE = "the registry must hold exactly one lan tenant (the LAN tenant, whose notices reach the owner's bridge)"


def new_tenant_id():
    import base64
    import secrets
    return "ten_" + base64.b32encode(secrets.token_bytes(16)).decode("ascii").lower().rstrip("=")


def record_problem(rec):
    """Why `rec` is not a registry record, or None."""
    if not isinstance(rec, dict):
        return "a tenant record must be an object"
    tid = rec.get("tenant")
    if not isinstance(tid, str) or not TENANT_RE.match(tid):
        return "tenant must be ten_ followed by 26 base32 characters"
    name = rec.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > NAME_MAX:
        return f"name must be 1-{NAME_MAX} characters"
    if not isinstance(rec.get("lan"), bool):
        return "lan must be true or false"
    if rec.get("tap") not in TAPS:
        return f"tap must be one of {', '.join(TAPS)}"
    return None


class Tenant:
    """One tenant's identity, dir, files and per-tenant locks. The core adds
    the state fields; a driver keeps its own under stores[pid]."""

    def __init__(self, tid, name, lan, tap, dir_, store):
        self.id = tid
        self.name = name
        self.lan = lan
        self.tap = tap
        self.dir = dir_
        self.store = store
        self.stores = {}
        self.created_at = time.time()
        for key, fname in FILES.items():
            setattr(self, key + "_file", os.path.join(dir_, fname))
        # locks, each the tenant's own (the core's lock order holds within one tenant)
        self.lock = threading.Lock()
        self.refresh_lock = threading.RLock()
        self.plock = threading.Lock()
        self.poll_health_lock = threading.Lock()
        self.ledger_lock = threading.Lock()
        self.doc_locks = {"consumers": threading.Lock(), "money": threading.Lock()}
        self.reports_lock = threading.Lock()
        self.served_lock = threading.Lock()
        self.sizing_lock = threading.Lock()
        self.sizing_cv = threading.Condition(self.sizing_lock)
        self.events_cond = threading.Condition()
        self.events_publish_lock = threading.RLock()
        self.reauth_lock = threading.Lock()
        self.events_dirty = True
        self.stopped = False

    def path(self, fname):
        """A file in this tenant's dir (a driver's credentials file)."""
        return os.path.join(self.dir, fname)

    def store_for(self, pid, make):
        """The driver's per-tenant store, made by `make(tenant)` the first time."""
        s = self.stores.get(pid)
        if s is None:
            s = self.stores[pid] = make(self)
        return s

    def record(self):
        return {"tenant": self.id, "name": self.name, "lan": self.lan, "tap": self.tap}

    def __repr__(self):
        return f"Tenant({self.id})"


class Registry:
    """The tenants a brain serves: tenants.json loaded once, every change
    written through, each tenant made by `make(tid, record)` (the core's
    constructor, which fills the state) and retired by `retire(tenant)`."""

    def __init__(self, creds_dir, make, retire=None, log=print):
        self.dir = creds_dir
        self.path = os.path.join(creds_dir, "tenants.json")
        self.make = make
        self.retire = retire
        self.log = log
        self._lock = threading.RLock()
        self._tenants = {}
        self._order = []
        self.seq = 0
        self.cond = threading.Condition()  # /tenants/events

    # -- the file --

    def load(self):
        """Read tenants.json and make every tenant in it. A missing file, an
        unreadable record or any count of lan records but one is a
        RegistryError: the registry is the operator's (the migration's, or
        init's), and a brain serving none or half of it would be wrong."""
        try:
            with open(self.path) as f:
                doc = json.load(f)
        except FileNotFoundError:
            raise RegistryError(f"{self.path} does not exist: the brain serves the tenants it lists and "
                                f"{ONE_LAN_RULE}; init or the deploy's migration writes it") from None
        except ValueError as e:
            raise RegistryError(f"{self.path} is not JSON: {e}") from None
        if not isinstance(doc, list):
            raise RegistryError(f"{self.path} is not a list of tenant records")
        lan = [r.get("tenant") for r in doc if isinstance(r, dict) and r.get("lan") is True]
        if len(lan) != 1:
            raise RegistryError(f"{self.path}: {ONE_LAN_RULE}; it holds {len(lan)}"
                                + (f" ({', '.join(map(str, lan))})" if lan else ""))
        with self._lock:
            for rec in doc:
                why = record_problem(rec)
                if why:
                    raise RegistryError(f"{self.path}: {why}: {rec!r}")
                if rec["tenant"] in self._tenants:
                    raise RegistryError(f"{self.path}: tenant {rec['tenant']} listed twice")
                self._add_locked(rec)
        return len(self._tenants)

    def _save_locked(self):
        doc = [self._tenants[tid].record() for tid in self._order]
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(doc, f, indent=1)
            f.write("\n")
        os.replace(tmp, self.path)

    def _add_locked(self, rec):
        d = os.path.join(self.dir, rec["tenant"])
        os.makedirs(d, mode=0o700, exist_ok=True)
        t = self.make(rec["tenant"], rec["name"], rec["lan"], rec["tap"], d)
        self._tenants[rec["tenant"]] = t
        self._order.append(rec["tenant"])
        return t

    # -- reads --

    def get(self, tid):
        """The tenant, or None: an unknown id, a malformed one and a retired
        one are all None (404 at the dispatcher)."""
        if not isinstance(tid, str) or not TENANT_RE.match(tid):
            return None
        with self._lock:
            return self._tenants.get(tid)

    def all(self):
        with self._lock:
            return [self._tenants[tid] for tid in self._order]

    def lan(self):
        """The LAN tenants (the household): the ones whose notices reach the
        owner's bridge."""
        return [t for t in self.all() if t.lan]

    def payload(self):
        return [t.record() for t in self.all()]

    # -- writes --

    def put(self, rec):
        """Create or update a tenant from a whole record. -> (tenant, created).
        An update changes name and tap in place; the id never changes, and
        neither does lan (RegistryConflict, as is a second lan tenant): the
        LAN tenant is set once, and its notices' bridge is fixed with it."""
        why = record_problem(rec)
        if why:
            raise ValueError(why)
        with self._lock:
            t = self._tenants.get(rec["tenant"])
            if t is None:
                if rec["lan"]:
                    have = self.lan()
                    raise RegistryConflict(f"{ONE_LAN_RULE}; it is {have[0].id}" if have else ONE_LAN_RULE)
                t = self._add_locked(rec)
                created = True
            else:
                if rec["lan"] != t.lan:
                    raise RegistryConflict(f"lan cannot change: {ONE_LAN_RULE}, set once by the migration")
                t.name, t.tap = rec["name"], rec["tap"]
                created = False
            self._save_locked()
        self._bump()
        self.log(f"tenants: {'created' if created else 'updated'} {t.id} ({t.name})")
        return t, created

    def delete(self, tid):
        """Retire a tenant: out of the registry, its polling stopped and its
        state dropped (retire), its dir removed whole. -> the tenant, or None.
        The lan tenant is never deleted (RegistryConflict)."""
        with self._lock:
            t = self._tenants.get(tid)
            if t is None:
                return None
            if t.lan:
                raise RegistryConflict(f"{tid} is the lan tenant: {ONE_LAN_RULE}")
            self._tenants.pop(tid)
            self._order.remove(tid)
            t.stopped = True
            self._save_locked()
        if self.retire is not None:
            self.retire(t)
        shutil.rmtree(t.dir, ignore_errors=False)
        self._bump()
        self.log(f"tenants: deleted {t.id} ({t.name}); {t.dir} removed")
        return t

    def _bump(self):
        with self.cond:
            self.seq += 1
            self.cond.notify_all()
