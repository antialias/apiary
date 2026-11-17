"""The sealed store (shared): every read and write of a tenant's documents and
credentials goes through one Store, in one of two modes fixed at start.

  sealed  SWITCH_CREDS_KEY_FILE names a file of 32 bytes (64 hex digits). Every
          write seals with AES-256-GCM; a plain file is refused on read.
  plain   The variable is unset. Files are plain JSON; a sealed file is refused.

There is no fallback between them: a file in the wrong form is an error that
names the file and the mode, never a silent read of the other form. The key
beside the data defends a backup, a snapshot or a mount review, not root on
the host (MEMBERS-PLAN.md, step 1a).

A sealed file is MAGIC, a 12-byte nonce, then the ciphertext with its 16-byte
tag; the file's base name is the associated data, so a sealed file renamed to
another document's name does not open as it. Append-only logs (the ledger and
the documents' .history.jsonl) are not documents and do not come here.
"""
import json
import os
import secrets

MAGIC = b"switch-sealed/1\n"
KEY_BYTES = 32
NONCE_BYTES = 12
KEY_FILE_VAR = "SWITCH_CREDS_KEY_FILE"


class CredStoreError(Exception):
    """A file in the wrong form for the store's mode, or a key file that is not
    a key; the message names the file and the mode."""


def _aesgcm(key):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # the one non-stdlib import
    return AESGCM(key)


def load_key(path):
    """The 32-byte key in `path` (64 hex digits, whitespace around them
    ignored). Raises CredStoreError for a missing or malformed file."""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError as e:
        raise CredStoreError(f"cannot read the sealing key {path}: {e}") from e
    text = raw.strip()
    try:
        key = bytes.fromhex(text.decode("ascii"))
    except (UnicodeDecodeError, ValueError) as e:
        raise CredStoreError(f"the sealing key {path} is not 64 hex digits") from e
    if len(key) != KEY_BYTES:
        raise CredStoreError(f"the sealing key {path} holds {len(key)} bytes, not {KEY_BYTES}")
    return key


def new_key_hex():
    """A fresh key as the key file holds it: 64 hex digits."""
    return secrets.token_hex(KEY_BYTES)


def is_sealed_bytes(data):
    return data.startswith(MAGIC)


class Store:
    """One store, one mode. `sealed` says which."""

    def __init__(self, key=None):
        self.key = key
        self.sealed = key is not None
        self._aead = _aesgcm(key) if key is not None else None

    @classmethod
    def from_env(cls, environ=os.environ):
        path = environ.get(KEY_FILE_VAR, "")
        return cls(load_key(path)) if path else cls()

    @property
    def mode(self):
        return "sealed" if self.sealed else "plain"

    # -- bytes --

    def seal_bytes(self, data, name):
        if not self.sealed:
            raise CredStoreError("no sealing key: this store is plain")
        nonce = secrets.token_bytes(NONCE_BYTES)
        return MAGIC + nonce + self._aead.encrypt(nonce, data, name.encode("utf-8"))

    def open_bytes(self, blob, name):
        if not self.sealed:
            raise CredStoreError("no sealing key: this store is plain")
        body = blob[len(MAGIC):]
        nonce, ct = body[:NONCE_BYTES], body[NONCE_BYTES:]
        try:
            return self._aead.decrypt(nonce, ct, name.encode("utf-8"))
        except Exception as e:  # noqa: BLE001 — InvalidTag or a truncated file
            raise CredStoreError(f"{name}: sealed file does not open ({type(e).__name__})") from e

    def read_bytes(self, path):
        """The file's plain bytes, or None when there is no such file. A file in
        the other mode's form raises CredStoreError."""
        try:
            with open(path, "rb") as f:
                raw = f.read()
        except FileNotFoundError:
            return None
        name = os.path.basename(path)
        if self.sealed:
            if not is_sealed_bytes(raw):
                raise CredStoreError(f"{path} is a plain file, and this store is sealed "
                                     f"({KEY_FILE_VAR} is set): refused")
            return self.open_bytes(raw, name)
        if is_sealed_bytes(raw):
            raise CredStoreError(f"{path} is a sealed file, and this store is plain "
                                 f"({KEY_FILE_VAR} is unset): refused")
        return raw

    def write_bytes(self, path, data, mode=0o600):
        """Write atomically (tmp + replace), owner-readable, sealed when the
        store is. Raises OSError as open() would."""
        body = self.seal_bytes(data, os.path.basename(path)) if self.sealed else data
        tmp = path + ".tmp"
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
            with os.fdopen(fd, "wb") as f:
                f.write(body)
            os.chmod(tmp, mode)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # -- JSON --

    def read_json(self, path):
        """The parsed document, or None when there is no file. A file that is
        not JSON raises ValueError; the wrong form raises CredStoreError."""
        raw = self.read_bytes(path)
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def write_json(self, path, doc, **dump):
        self.write_bytes(path, (json.dumps(doc, **dump) + "\n").encode("utf-8"))

    def mtime(self, path):
        """The file's mtime, or None without one (the drivers' watchers)."""
        try:
            return os.stat(path).st_mtime
        except OSError:
            return None

    def remove(self, path):
        for p in (path, path + ".tmp"):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

    # -- the migration's step --

    def seal_file(self, src, dst):
        """Seal the plain file `src` as `dst` and read `dst` back: it must give
        `src`'s bytes. Idempotent: a `dst` that already opens to those bytes is
        left as it is. A read-back that differs removes `dst` and raises; `src`
        is never touched."""
        if not self.sealed:
            raise CredStoreError("no sealing key: nothing can be sealed")
        with open(src, "rb") as f:
            plain = f.read()
        try:
            have = self.read_bytes(dst)
        except CredStoreError:
            have = None
        if have == plain:
            return False
        self.write_bytes(dst, plain)
        try:
            back = self.read_bytes(dst)
        except CredStoreError as e:
            self.remove(dst)
            raise CredStoreError(f"{dst}: read-back failed ({e})") from e
        if back != plain:
            self.remove(dst)
            raise CredStoreError(f"{dst}: read-back differs from {src}")
        return True
