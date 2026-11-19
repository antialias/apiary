"""The keyring (shared): the named-key authority every static-key provider
declares itself on. A provider's accounts are static API keys, each enrolled
under a name of its own on the tenant's /auth page (a name and the key) and
kept in the tenant's dir as one file per provider, `[{"name", "api_key"}]`.

A driver calls register() once at import with what is its own: its id, its
label, its file's name, its free read and its enrolment's probe, its words for
the enrolment page, and the rest of its registry row. The keyring supplies the
rest of the row: enabled, accounts, api_key and poll (one target per account,
named like it), and the `auth` record (status rows with each key's
fingerprint, never the key; begin; complete, which probes before it persists
and reads the file again under the edit lock; remove, which drops one key and
the file with its last). A keys file the keyring cannot read whole is never
written over: enrolment and remove refuse, and the status carries a row that
says so.

The provider's own free read is two functions: fetch(key) -> raw makes the
call and raises as urllib does (a failure the poll reports under the read's
name), and reading(raw, as_of) -> (reading, err) parses it, a reading (§1.5)
or None and a reason (no secret in it) when the answer is unusable; a bug in
the parse raises into the poller, which logs it with its traceback. probe(key)
-> None when the key is proved, else a reason; it raises as urllib does. None
keeps state: the keys and their cache are the tenant's (tenant.store_for),
never the module's.
"""
import re
import threading
import time
import urllib.error
from typing import NamedTuple

import claude_usage_exporter as core
from providers import register as _register

# A name is a roster key like any other account's: what /api-key, the routes
# and the standings name it by.
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@+-]{0,63}$")


def name_error(t, pid, name, keys):
    """Why `name` cannot be an account of provider `pid` in tenant t, or None.
    `keys` is the provider's enrolled {name: key}: a name it holds already is
    a replacement. A name another provider's target or the account loop holds
    would be two accounts under one roster key; a provider's id is its own
    provider's alone (a single-account provider's account is named like it)."""
    if not NAME_RE.match(name):
        return "name must be 1-64 letters, digits or ._@+- and start with a letter or digit"
    if name in keys:
        return None
    if name in core.PROVIDERS and name != pid:
        return f"{name!r} is another provider's id"
    if name in core._account_loop_names(t):
        return f"{name!r} already names another account"
    taken = core._provider_poll_targets(t).get(name)
    if taken and taken[0] != pid:
        return f"{name!r} already names a {taken[0]} account"
    return None


class _Store:
    """One tenant's keys of one provider (tenant.stores[pid]), mtime-cached
    so an out-of-band edit of the file is adopted at the next read.
    keys: None = not loaded or no file. unreadable: why the file as read is
    not wholly a keyring (no path, no key), else None."""

    def __init__(self, t, fname):
        self.file = t.path(fname)
        self.keys_lock = threading.Lock()
        # Held from read to write by every edit of the file (enrol, remove), so
        # an edit never writes back a copy another edit has since changed.
        self.edit_lock = threading.Lock()
        self.keys = None
        self.keys_mtime = None
        self.unreadable = None


class Keyring(NamedTuple):
    """One provider's declaration: what the keyring needs of it."""
    pid: str
    fname: str
    fetch: object  # callables: a typing.Callable here would be module state
    reading: object
    probe: object
    what: str
    label: str

    def store(self, t):
        return t.store_for(self.pid, lambda tt: _Store(tt, self.fname))

    def load(self, t):
        """{name: key} from the file, in file order; {} without one. A file
        that does not open or is not a list reads as no keys, and an entry
        that is not {name, api_key} (or names an account twice) is skipped;
        either way the store records why (unreadable) and the log says so."""
        st = self.store(t)
        with st.keys_lock:
            mtime = core._store.mtime(st.file)
            if mtime is None:
                st.keys, st.keys_mtime, st.unreadable = None, None, None
                return {}
            if st.keys is None or mtime != st.keys_mtime:
                keys, bad = {}, None
                try:
                    raw = core._store.read_json(st.file)
                except Exception as e:  # noqa: BLE001
                    raw, bad = None, f"it does not open: {type(e).__name__}"
                if bad is None and not isinstance(raw, list):
                    bad = "it is not a list of {name, api_key}"
                for e in raw if isinstance(raw, list) else []:
                    ok = isinstance(e, dict) and isinstance(e.get("name"), str) \
                        and isinstance(e.get("api_key"), str) and e["api_key"].strip()
                    if ok and e["name"] not in keys:
                        keys[e["name"]] = e["api_key"].strip()
                    elif bad is None:
                        bad = "an entry is not {name, api_key}" if not ok else "it names an account twice"
                if bad:
                    core.log(f"WARN {self.pid} keyring cannot be read ({bad}): it is left as it is, "
                             "and no account is enrolled or removed on it until it is fixed")
                st.keys, st.keys_mtime, st.unreadable = keys, mtime, bad
            return dict(st.keys)

    def _refuse_unreadable(self, t, what):
        """Raise when the file as last read is not wholly a keyring: writing it
        back would lose what the keyring could not read."""
        why = self.store(t).unreadable
        if why:
            raise ValueError(f"the {self.pid} keyring cannot be read ({why}), so {what} — nothing was changed")

    def persist(self, t, keys):
        """Write {name: key} atomically, readable by its owner alone. Raises
        ValueError (operator-readable, never where the file is) on I/O failure."""
        st = self.store(t)
        doc = [{"name": n, "api_key": k} for n, k in keys.items()]
        with st.keys_lock:
            try:
                core._store.write_json(st.file, doc, indent=1)
            except Exception as e:  # noqa: BLE001
                raise ValueError(f"the keys could not be stored ({type(e).__name__}) — nothing was changed") from e
            st.keys, st.keys_mtime = dict(keys), core._store.mtime(st.file)

    def names(self, t):
        return list(self.load(t))

    def api_key(self, t, account):
        return self.load(t).get(account, "")

    def enabled(self, t):
        return bool(self.load(t))

    def poll(self, t, prev=None, account=None):  # noqa: ARG002 — uniform pool dispatch (#71)
        """One account's poll result, shaped as every provider's. No key or a
        failed fetch is a failed poll; a bug in the provider's parse raises
        into the poller, which logs it with its traceback."""
        out = {"name": account, "kind": "provider", "provider": self.pid,
               "ok": 0, "ts": time.time(), "good_ts": None, "stale": 0,
               "http": 0, "err": "", "retry_after": None, "reading": None}
        key = self.load(t).get(account)
        if not key:
            out["err"] = f"no {self.pid} key for {account!r} (not configured: enrol one on /auth)"
            return out
        try:
            raw = self.fetch(key)
        except urllib.error.HTTPError as e:
            out["http"] = e.code
            out["err"] = f"{self.what} HTTP {e.code}"
            out["retry_after"] = core._retry_after(e)
            return out
        except Exception as e:  # noqa: BLE001
            out["err"] = f"{self.what} {type(e).__name__}"
            return out
        reading, err = self.reading(raw, out["ts"])
        if reading is None:
            out["err"] = err
            return out
        out["reading"] = reading
        out["ok"] = 1
        return out

    def status(self, t, now):
        """One row per enrolled key: its poll health and fingerprint, never the
        key. A keyring that cannot be read wholly adds a row of its own, named
        "<provider>:keyring" (no account can take the name), that says so in
        words true for whoever reads it (a member can't reach the file; the
        operator has the log's WARN with the reason)."""
        keys = self.load(t)
        rows = core._account_health(t, list(keys), now)["accounts"]
        for r in rows:
            r["source"] = "file"
            r["key_fingerprint"] = core._key_fingerprint(keys.get(r["name"], ""))
        why = self.store(t).unreadable
        if why:
            rows.append({"name": f"{self.pid}:keyring", "poll_ok": 0, "health": "auth_dead",
                         "last_error_kind": "unreadable", "consecutive_failures": 0, "stale": 0,
                         "good_ts": None, "age_s": None, "next_retry_in_s": 0,
                         "err": f"the saved {self.label} accounts can't be read; adding or removing "
                                "one is refused until the hive's operator repairs them",
                         "source": "file", "key_fingerprint": None})
        return rows

    def begin(self, t, account=None):
        """A bare one-shot state: there is no authorization to visit."""
        return None, core._auth_grant_mint(t, self.pid, "reauth" if account else "add", account=account)

    def complete(self, t, grant, fields):
        name = str(fields.get("name") or grant.get("account") or "").strip()
        key = str(fields.get("api_key") or "").strip()
        if not name:
            raise ValueError("name is required")
        if not key:
            raise ValueError("api_key is required")
        keys = self.load(t)
        self._refuse_unreadable(t, f"{name!r} was not saved")
        err = name_error(t, self.pid, name, keys)
        if err:
            raise ValueError(err + " — nothing was saved")
        # Probe BEFORE persist. The error carries the code and the fingerprint,
        # never the key or upstream prose that could echo it.
        fpr = core._key_fingerprint(key)
        try:
            why = self.probe(key)
        except urllib.error.HTTPError as e:
            raise ValueError(f"key validation HTTP {e.code} (fingerprint {fpr}) "
                             "— nothing was saved") from e
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"key validation {type(e).__name__} — nothing was saved") from e
        if why:
            raise ValueError(f"key was accepted but {why} — nothing was saved")
        # Read again under the edit lock: the probe took seconds, and a key
        # removed (or added) meanwhile is not written back from the first read.
        # The tenant's enrol_lock is held around it, shared by every provider's
        # enrolment, so two of them never take one name on two providers.
        st = self.store(t)
        with t.enrol_lock, st.edit_lock:
            keys = self.load(t)
            self._refuse_unreadable(t, f"{name!r} was not saved")
            err = name_error(t, self.pid, name, keys)
            if err:
                raise ValueError(err + " — nothing was saved")
            replaced = name in keys
            keys[name] = key
            self.persist(t, keys)
        core.log(f"{self.pid} key {'replaced' if replaced else 'enrolled'} for {name} (fingerprint {fpr})")
        with t.poll_health_lock:
            t.poll_fails.pop(name, None)
            t.poll_schedule.pop(name, None)  # due at once
        core._poll_wakeup.set()
        return {"account": name, "enrolled": not replaced, "reauthorized": replaced}

    def remove(self, t, account):
        """Drop one key from the file, and the file with its last key. The core
        holds the tenant's enrol_lock around it."""
        st = self.store(t)
        with st.edit_lock:
            keys = self.load(t)
            self._refuse_unreadable(t, f"{account!r} was not removed")
            if keys.pop(account, None) is None:
                raise ValueError(f"{account!r} is not enrolled — nothing was removed")
            if keys:
                self.persist(t, keys)
            else:
                with st.keys_lock:
                    core._remove_credentials(st.file)
                    st.keys, st.keys_mtime, st.unreadable = None, None, None
        core.log(f"{self.pid} key removed for {account}")


def register(pid, label, file, fetch, reading, probe, instructions, note, *, what, **row):
    """Declare provider `pid` on the keyring: `file` is its keys file's name in
    each tenant's dir; fetch, reading and probe its read and its enrolment's
    probe (above); `what` the word its fetch's failures are named by ("balance
    HTTP 401"); instructions the enrolment page's words and note the custody
    note; `row` the rest of its registry row (api_pot, ledger, lanes, name,
    serves, billing, ...). -> its Keyring."""
    kr = Keyring(pid, file, fetch, reading, probe, what, label)
    _register(pid, dict(row, **{
        "poll": kr.poll,
        "enabled": kr.enabled,
        "accounts": kr.names,
        "api_key": kr.api_key,
        "auth": {
            "kind": "static-key",
            "label": label,
            "custody": {"rotation": "none", "note": note},
            "enroll": {"flow": "paste-key", "can_add": True, "instructions": instructions,
                       "fields": [{"name": "name", "label": "Account name", "secret": False},
                                  {"name": "api_key", "label": "API key", "secret": True}]},
            "status": kr.status,
            "begin": kr.begin,
            "complete": kr.complete,
            "remove": kr.remove,
        },
    }))
    return kr
