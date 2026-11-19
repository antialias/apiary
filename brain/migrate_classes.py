#!/usr/bin/env python3
"""migrate_classes.py: the classes document becomes each tenant's own
(CLASSES-PLAN.md §1, cut 2).

Before, one classes document at the creds root, classes.json, served every
tenant, and a class marked `lan_only: true` was hidden from member tenants.
Now every tenant's dir holds its own classes.json (with classes.windows.json
beside it, and its history), plain like before; the LAN tenant's is the root
document moved, and a class it marks `onboarding: true` is offered to new
members, who start with a copy (the brain's _starter).

Whichever brain runs owns the truth, as in migrate_keyring.py: the step runs
at deploy, from the new image, against the live creds dir, once the old brain
is stopped, and a rollback to a brain before this cut reverts it first.

    migrate  The root classes.json is rewritten: `onboarding: true` on every
             class without `lan_only: true`, `onboarding: false` on every
             class with it, and `lan_only` removed. Every write comes first:
             the rewritten document into the LAN tenant's dir, with
             classes.json.history.jsonl (one line appended, actor
             migrate_classes) and classes.json.windows (as
             classes.windows.json) beside it, and the starter derived from it
             (the edition's drivers decide what a member can connect) for
             every other tenant without a classes.json. Each file is a copy
             read back and compared, then renamed in; then all are read back
             again. Only then do the root's three files leave the root: they
             move beside the tenant's copies, named <file>.before-tenants,
             classes.json last, and stay until finish. Prints the revision the
             LAN tenant's document has, which its GET /t/<tenant>/classes then
             answers. Run again with nothing at the root, it changes nothing.
    finish   Once the new brain passed its checks: the kept originals are
             removed, classes.json's last. Refused while the root still holds
             classes.json, or while the LAN tenant holds none.
    revert   The rollback's direction, never losing an edit: while the
             originals are kept and the LAN tenant's document is still the one
             migrate wrote (or the root still holds classes.json: the migrate
             never finished), the originals go back to the root byte for byte
             and the tenant's copies are removed. Otherwise (after finish, or
             edited since), the LAN tenant's document goes back to the root
             with the key rewrite reversed (`lan_only: true` on every class
             not offered), its edits kept, its history (one line appended) and
             window stamps beside it; then the tenant's copies and any stale
             originals are removed, and the report says so. Other tenants'
             documents are left: a brain before this cut never reads them.
             At every step of migrate, finish and revert a classes.json is at
             the root or in the LAN tenant's dir: each removal follows the
             write that replaces it.
    check    Run inside the new brain's container after its gate: GET
             /t/<tenant>/classes answers --revision, its derived class names
             are --names (the old brain's, read before it stopped), and the
             root /classes answers 404.

migrate, finish and revert print a JSON report (tenant ids, file and class
names). Exit 0 when done; 10 when the step refused or failed with nothing
written; 3 when it failed after a file or dir was written (the report's "changed" says which). Any other
code says nothing of what was written, and the deploy treats it as written.
Never run on a live brain's dir: a deploy runs it with the brain stopped (this
service's nas/remote-deploy.sh), and a self-hosted brain upgraded from before
per-tenant classes runs migrate once, stopped (Apiary's README, "Classes").
"""
import argparse
import copy
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

CLASSES = "classes.json"
HISTORY = CLASSES + ".history.jsonl"
ROOT_WINDOWS = CLASSES + ".windows"  # the previous brain's name for the window stamps
TENANT_WINDOWS = "classes.windows.json"  # tenancy.FILES["windows"]
ROOT_FILES = (CLASSES, HISTORY, ROOT_WINDOWS)
ORIGINAL = ".before-tenants"  # a root file kept in the LAN tenant's dir, migrate to finish
TMP_SUFFIX = ".classes.tmp"
REFUSED = 10  # the exit code of a step that changed nothing
ACTOR = "migrate_classes"


class MigrationError(Exception):
    """A refusal or a failure. changed: the files written before it ([] =
    nothing was changed)."""

    def __init__(self, msg, changed=()):
        super().__init__(msg)
        self.changed = list(changed)


def doc_revision(doc):
    """The brain's revision of a document (claude_usage_exporter._doc_revision)."""
    canon = json.dumps(doc, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


def class_rows(doc):
    """The class array of a classes document ({"classes", "lanes"} or a bare
    array), or None for another shape."""
    if isinstance(doc, list):
        return doc
    if isinstance(doc, dict) and isinstance(doc.get("classes"), list):
        return doc["classes"]
    return None


def class_names(doc):
    return [c.get("class") for c in class_rows(doc) or [] if isinstance(c, dict)]


def tenants(creds_dir):
    """(the LAN tenant's id, [every other tenant's id]) from tenants.json."""
    path = os.path.join(creds_dir, "tenants.json")
    try:
        with open(path) as f:
            reg = json.load(f)
    except FileNotFoundError:
        raise MigrationError("no tenants.json: the classes move into the LAN tenant's dir, and there is none") from None
    except ValueError as e:
        raise MigrationError(f"tenants.json is not JSON ({e})") from None
    if not isinstance(reg, list):
        raise MigrationError("tenants.json is not a list of tenant records")
    lan, others = [], []
    for rec in reg:
        tid = rec.get("tenant") if isinstance(rec, dict) else None
        if not isinstance(tid, str) or not tid or "/" in tid or tid.startswith("."):
            raise MigrationError(f"tenants.json holds a record with no usable tenant id: {rec!r}")
        (lan if rec.get("lan") is True else others).append(tid)
    if len(lan) != 1:
        raise MigrationError(f"tenants.json must hold exactly one lan tenant; it holds {len(lan)}")
    return lan[0], others


def _rewrite(doc, step):
    """doc with each class's key rewritten for `step`: migrate turns lan_only
    into onboarding, revert onboarding into lan_only, each in the key's place
    (a class without either gains the new key last)."""
    out = copy.deepcopy(doc)
    rows = class_rows(out)
    for i, c in enumerate(rows):
        if not isinstance(c, dict):
            continue
        if step == "migrate":
            old, new, value = "lan_only", "onboarding", c.get("lan_only") is not True
        else:
            old, new, value = "onboarding", "lan_only", c.get("onboarding") is not True
        keep = step == "migrate" or value  # the previous brain's absent lan_only is "every tenant"
        row, placed = {}, False
        for k, v in c.items():
            if k == old:
                if keep:
                    row[new] = value
                placed = True
            elif k != new:
                row[k] = v
        if not placed and keep:
            row[new] = value
        rows[i] = row
    return out


def _offered(doc, mark):
    """A classes document's revision with its mark read as "offered" (the
    LAN tenant's `onboarding: true`, or the previous brain's absent or false
    `lan_only`), so a document and its rewrite compare equal however each
    spells the mark. None for a document of another shape."""
    out = copy.deepcopy(doc)
    rows = class_rows(out)
    if rows is None:
        return None
    for c in rows:
        if isinstance(c, dict):
            v = c.pop(mark, None)
            c.pop("onboarding" if mark == "lan_only" else "lan_only", None)
            c["offered"] = (v is True) if mark == "onboarding" else (v is not True)
    return doc_revision(out)


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except ValueError as e:
        raise MigrationError(f"{os.path.basename(path)} is not JSON ({e}); nothing was changed") from None


def _read_text(path):
    try:
        with open(path) as f:
            return f.read()
    except FileNotFoundError:
        return None


def _history_line(rev, reason, doc):
    return json.dumps({"rev": rev, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "actor": ACTOR, "reason": reason, "doc": doc}) + "\n"


def _put(path, text):
    """Write text to a copy beside path, read it back and compare, then rename
    it over path. A copy that fails or differs is removed and path stands."""
    tmp = path + TMP_SUFFIX
    try:
        with open(tmp, "w") as f:
            f.write(text)
        with open(tmp) as f:
            if f.read() != text:
                raise MigrationError(f"{os.path.basename(path)}: the copy does not read back as written")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _doc_text(doc):
    """A document as the brain writes it (_doc_write)."""
    return json.dumps(doc, indent=2, allow_nan=False) + "\n"


class _Writer:
    """Carries out writes, moves and removals in order, remembering each, so a
    failure says what was written before it."""

    def __init__(self, creds_dir):
        self.base = creds_dir
        self.done = []

    def _where(self, path):
        return os.path.relpath(path, self.base)

    def mkdir(self, path):
        if not os.path.isdir(path):
            self._do(path, lambda: os.makedirs(path, mode=0o700))

    def put(self, path, text):
        self._do(path, lambda: _put(path, text))

    def move(self, src, dst):
        self._do(src, lambda: os.rename(src, dst), f"{self._where(src)} -> {self._where(dst)}")

    def remove(self, path):
        if os.path.exists(path):
            self._do(path, lambda: os.remove(path))

    def fail(self, msg):
        raise MigrationError(f"{msg}; written before it: {self.done}", changed=self.done)

    def _do(self, path, fn, what=None):
        try:
            fn()
        except (MigrationError, OSError, ValueError) as e:
            msg = str(e) if isinstance(e, MigrationError) else f"{type(e).__name__}: {e}"
            self.fail(f"{self._where(path)}: {msg}")
        self.done.append(what or self._where(path))


def _starter_fn(creds_dir, edition):
    """The brain's starter derivation, under the edition's drivers (which
    decide what a member can connect): (lan_doc) -> (doc, dropped)."""
    os.environ.setdefault("SWITCH_CREDS_DIR", creds_dir)  # the lanes the brain validates against
    import claude_usage_exporter as core
    __import__(edition)

    def starter(lan_doc):
        live = {c["class"] for c in core._normalize_classes_doc(lan_doc, [], lan=True)[0]}
        return core._starter(lan_doc, live)
    return starter


def _paths(creds_dir, lan):
    """(the root's three files, the LAN tenant's copies, the originals kept
    beside the copies until finish), each keyed by the root file's name."""
    ldir = os.path.join(creds_dir, lan)
    root = {n: os.path.join(creds_dir, n) for n in ROOT_FILES}
    mine = {CLASSES: os.path.join(ldir, CLASSES), HISTORY: os.path.join(ldir, HISTORY),
            ROOT_WINDOWS: os.path.join(ldir, TENANT_WINDOWS)}
    orig = {n: os.path.join(ldir, n + ORIGINAL) for n in ROOT_FILES}
    return root, mine, orig


def migrate(creds_dir, edition="brain"):
    """The root document into the LAN tenant's dir, rewritten; a starter for
    every other tenant without a document. Every file is written and read back
    before the root's originals move beside the copies. -> the report."""
    lan, others = tenants(creds_dir)
    root, mine, orig = _paths(creds_dir, lan)
    report = {"tenant": lan, "moved": [], "starters": {}}
    doc = _read_json(root[CLASSES])
    plan = []  # (path, text), in the order written
    if doc is not None:
        if os.path.exists(mine[CLASSES]):
            raise MigrationError(f"both the root and {lan}/ hold {CLASSES}: which is live is unknown; "
                                 "nothing was changed")
        kept = [n for n in ROOT_FILES if os.path.exists(orig[n])]
        if kept:
            raise MigrationError(f"{lan}/ still holds a previous migrate's originals ({kept}): "
                                 "run revert or finish first; nothing was changed")
        if class_rows(doc) is None:
            raise MigrationError('classes.json is neither {"classes": [...], "lanes": {...}} nor an array; '
                                 "nothing was changed")
        new = _rewrite(doc, "migrate")
        rev = doc_revision(new)
        history = _read_text(root[HISTORY]) or ""
        windows = _read_text(root[ROOT_WINDOWS])
        plan.append((mine[CLASSES], _doc_text(new)))
        plan.append((mine[HISTORY], history + _history_line(
            rev, "classes per tenant: the household's document, lan_only rewritten as onboarding", new)))
        if windows is not None:
            plan.append((mine[ROOT_WINDOWS], windows))
    else:
        new = _read_json(mine[CLASSES])
        new = [] if new is None else new  # a tenant without a document: the brain serves []
        rev = doc_revision(new)
    report["revision"] = rev
    report["classes"] = class_names(new)
    need = [t for t in others if not os.path.exists(os.path.join(creds_dir, t, CLASSES))]
    if need:
        try:
            sdoc, dropped = _starter_fn(creds_dir, edition)(new)
        except Exception as e:  # noqa: BLE001 — derived before any write: nothing changed
            raise MigrationError(f"the starter could not be derived ({type(e).__name__}: {e}); "
                                 "nothing was changed") from e
        srev = doc_revision(sdoc)
        for tid in need:
            plan.append((os.path.join(creds_dir, tid, CLASSES), _doc_text(sdoc)))
            plan.append((os.path.join(creds_dir, tid, HISTORY), _history_line(
                srev, "the starter: the operator's onboarding classes", sdoc)))
            report["starters"][tid] = {"revision": srev, "classes": class_names(sdoc), "dropped": dropped}
    w = _Writer(creds_dir)
    for path, text in plan:
        w.mkdir(os.path.dirname(path))
        w.put(path, text)
    for path, text in plan:
        try:
            back = _read_text(path)
        except OSError as e:
            w.fail(f"{os.path.relpath(path, creds_dir)} does not read back ({type(e).__name__}: {e})")
        if back != text:
            w.fail(f"{os.path.relpath(path, creds_dir)} does not read back as written")
    if doc is not None:
        # The originals move beside the copies, kept until finish; classes.json last, so a root
        # that still holds it says the move never finished.
        for n in (ROOT_WINDOWS, HISTORY, CLASSES):
            if os.path.exists(root[n]):
                w.move(root[n], orig[n])
                report["moved"].append(n)
    if not w.done:
        report["unchanged"] = True
    return report


def finish(creds_dir):
    """After the new brain passed its checks: the originals migrate kept are
    removed, classes.json's last. Refused while the root still holds
    classes.json (the migrate did not finish) or the LAN tenant holds none
    (the original would be the only classes.json left). -> the report."""
    lan, _ = tenants(creds_dir)
    root, mine, orig = _paths(creds_dir, lan)
    if os.path.exists(root[CLASSES]):
        raise MigrationError(f"the root still holds {CLASSES}: the migrate did not finish, so its originals "
                             "stay for revert; nothing was changed")
    if not os.path.exists(mine[CLASSES]) and os.path.exists(orig[CLASSES]):
        raise MigrationError(f"{lan}/ holds no {CLASSES}: its original is the only one left, so it stays; "
                             "nothing was changed")
    w = _Writer(creds_dir)
    for n in (ROOT_WINDOWS, HISTORY, CLASSES):
        w.remove(orig[n])
    return {"tenant": lan, "removed": list(w.done)}


def revert(creds_dir):
    """The LAN tenant's document back to the root, never losing an edit and
    never leaving no classes.json: each removal comes after the write that
    replaces it.

    - The root still holds classes.json (the migrate never finished, so no
      brain ran on the copies): any originals moved go back, the copies go.
    - The originals are kept and the tenant's document is still the one
      migrate wrote: they go back byte for byte, the copies go.
    - Otherwise (after finish, or edited since the migrate): the tenant's
      document is rewritten back to the root, its edits kept; then the copies
      and any stale originals go, and the report says which.
    -> the report."""
    lan, _ = tenants(creds_dir)
    root, mine, orig = _paths(creds_dir, lan)
    report = {"tenant": lan, "restored": [], "discarded": []}
    w = _Writer(creds_dir)
    kept = [n for n in ROOT_FILES if os.path.exists(orig[n])]
    doc = _read_json(mine[CLASSES])
    at_root = os.path.exists(root[CLASSES])
    if kept and not at_root and CLASSES not in kept:
        raise MigrationError(f"{lan}/ keeps originals {kept} but neither the root nor they hold {CLASSES}: "
                             "nothing was changed")
    unedited = False
    if CLASSES in kept and not at_root:
        original = _read_json(orig[CLASSES])
        unedited = doc is None or (class_rows(original) is not None
                                   and doc_revision(doc) == doc_revision(_rewrite(original, "migrate")))
    if at_root and doc is not None:
        # The root's document is meant to be the truth here (a migrate that never finished), but a
        # brain may have served and edited the household's copy (a deploy cut off between the
        # migrate's renames, or an upgrade that never migrated): the operator decides.
        root_doc = _read_json(root[CLASSES])
        if _offered(doc, "onboarding") != _offered(root_doc, "lan_only"):
            newer = (f"{lan}/{CLASSES}" if os.path.getmtime(mine[CLASSES]) > os.path.getmtime(root[CLASSES])
                     else CLASSES)
            raise MigrationError(f"the root's {CLASSES} and {lan}/{CLASSES} differ (beyond the key's rewrite), "
                                 f"so a brain may have edited one; {newer} is the newer. Keep the live one "
                                 "and remove the other, then revert again; nothing was changed")
    if at_root or unedited:
        for n in ROOT_FILES:  # classes.json first: in place before the tenant's copy goes
            if n in kept and not os.path.exists(root[n]):
                w.move(orig[n], root[n])
        report["restored"] = list(w.done)
        n = len(w.done)
        for path in mine.values():
            w.remove(path)
        # An original beside a root file of its name is older than it (a revert that rewrote the
        # root, then stopped): the root's stands.
        for name in ROOT_FILES:
            if os.path.exists(orig[name]):
                w.remove(orig[name])
        report["discarded"] = w.done[n:]
        return report
    if doc is None:
        report["unchanged"] = True
        return report
    if class_rows(doc) is None:
        raise MigrationError(f"{lan}/{CLASSES} is neither {{\"classes\": [...], \"lanes\": {{...}}}} nor an array; "
                             "nothing was changed")
    old = _rewrite(doc, "revert")
    history = _read_text(mine[HISTORY]) or ""
    windows = _read_text(mine[ROOT_WINDOWS])
    w.put(root[CLASSES], _doc_text(old))
    w.put(root[HISTORY], history + _history_line(
        doc_revision(old), "classes back at the root for the previous brain (onboarding rewritten as lan_only)", old))
    if windows is not None:
        w.put(root[ROOT_WINDOWS], windows)
    for path in mine.values():
        w.remove(path)
    report["reverted"] = list(w.done)
    report["classes"] = class_names(old)
    if kept:
        # The document was edited since the migrate: the originals are older than it.
        n = len(w.done)
        for name in (ROOT_WINDOWS, HISTORY, CLASSES):
            w.remove(orig[name])
        report["edited_since_migrate"] = True
        report["stale_originals_removed"] = w.done[n:]
    return report


def _get(url, key):
    req = urllib.request.Request(url, headers={"X-Switch-Key": key})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, None


def check(base, tenant_path, revision, names, key):
    """The post-gate check against the new brain at `base`. -> [problem]."""
    problems = []
    code, body = _get(f"{base}{tenant_path}/classes", key)
    if code != 200 or not isinstance(body, dict):
        return [f"GET {tenant_path}/classes answered {code}"]
    if body.get("revision") != revision:
        problems.append(f"GET {tenant_path}/classes revision {body.get('revision')}, the migration wrote {revision}")
    if names is not None and sorted(body.get("derived") or {}) != sorted(names):
        problems.append(f"GET {tenant_path}/classes serves {sorted(body.get('derived') or {})}, "
                        f"the old brain served {sorted(names)}")
    code, _ = _get(f"{base}/classes", key)
    if code != 404:
        problems.append(f"GET /classes answered {code}, not 404")
    return problems


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("step", choices=("migrate", "revert", "finish", "check"))
    ap.add_argument("--creds-dir", help="migrate, revert, finish: the brain's creds dir")
    ap.add_argument("--edition", default="brain",
                    help="migrate: the entry point whose drivers the starter is derived under (default brain)")
    ap.add_argument("--tenant-path", help="check: /t/<the LAN tenant>")
    ap.add_argument("--revision", help="check: the revision migrate printed")
    ap.add_argument("--names", help="check: the old brain's class names, a JSON array")
    a = ap.parse_args(argv)
    if a.step == "check":
        if not a.tenant_path or not a.revision:
            ap.error("check needs --tenant-path and --revision")
        key = open(os.environ["SWITCH_KEY_FILE"]).read().strip()
        base = f"http://127.0.0.1:{os.environ.get('PORT', '9115')}"
        problems = check(base, a.tenant_path, a.revision,
                         None if a.names is None else json.loads(a.names), key)
        for p in problems:
            print(f"    {p}")
        if not problems:
            print(f"    classes: {a.tenant_path}/classes at {a.revision}"
                  + ("" if a.names is None else ", the old brain's classes") + "; /classes 404")
        return 1 if problems else 0
    if not a.creds_dir:
        ap.error(f"{a.step} needs --creds-dir")
    try:
        report = (migrate(a.creds_dir, a.edition) if a.step == "migrate"
                  else revert(a.creds_dir) if a.step == "revert" else finish(a.creds_dir))
    except MigrationError as e:
        print(json.dumps({"step": a.step, "error": str(e), "changed": e.changed}))
        return 3 if e.changed else REFUSED
    except OSError as e:  # a read before any write: every write goes through _Writer
        print(json.dumps({"step": a.step, "error": f"{type(e).__name__}: {e}", "changed": []}))
        return REFUSED
    print(json.dumps(dict(step=a.step, **report)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
