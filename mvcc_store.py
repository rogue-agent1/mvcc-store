#!/usr/bin/env python3
"""MVCC key-value store with snapshot isolation."""
import threading

class MVCCStore:
    def __init__(self):
        self._data = {}  # key -> [(version, value, deleted)]
        self._version = 0
        self._lock = threading.Lock()

    def _next_version(self):
        with self._lock:
            self._version += 1
            return self._version

    def put(self, key, value):
        ver = self._next_version()
        self._data.setdefault(key, []).append((ver, value, False))
        return ver

    def delete(self, key):
        ver = self._next_version()
        self._data.setdefault(key, []).append((ver, None, True))
        return ver

    def get(self, key, version=None):
        if version is None:
            version = self._version
        history = self._data.get(key, [])
        for ver, val, deleted in reversed(history):
            if ver <= version:
                return None if deleted else val
        return None

    def snapshot(self):
        return self._version

    def history(self, key):
        return [(v, val, d) for v, val, d in self._data.get(key, [])]

if __name__ == "__main__":
    store = MVCCStore()
    store.put("name", "Alice")
    snap1 = store.snapshot()
    store.put("name", "Bob")
    snap2 = store.snapshot()
    print(f"Current: {store.get('name')}")
    print(f"At snap1: {store.get('name', snap1)}")
    print(f"At snap2: {store.get('name', snap2)}")

def test():
    s = MVCCStore()
    s.put("k", "v1")
    snap1 = s.snapshot()
    s.put("k", "v2")
    snap2 = s.snapshot()
    s.put("k", "v3")
    # Current
    assert s.get("k") == "v3"
    # Historical
    assert s.get("k", snap1) == "v1"
    assert s.get("k", snap2) == "v2"
    # Delete
    s.delete("k")
    assert s.get("k") is None
    assert s.get("k", snap2) == "v2"
    # History
    h = s.history("k")
    assert len(h) == 4
    # Missing key
    assert s.get("missing") is None
    print("  mvcc_store: ALL TESTS PASSED")
