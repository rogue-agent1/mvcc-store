#!/usr/bin/env python3
"""Multi-Version Concurrency Control (MVCC) key-value store."""
import sys, time

class Version:
    def __init__(self, value, ts, deleted=False):
        self.value, self.ts, self.deleted = value, ts, deleted

class MVCCStore:
    def __init__(self):
        self.data, self.clock = {}, 0
    def _tick(self): self.clock += 1; return self.clock
    def begin(self): return self._tick()
    def read(self, key, snapshot):
        if key not in self.data: return None
        for v in reversed(self.data[key]):
            if v.ts <= snapshot:
                return None if v.deleted else v.value
        return None
    def write(self, key, value, txn_ts=None):
        ts = txn_ts or self._tick()
        self.data.setdefault(key, []).append(Version(value, ts))
    def delete(self, key, txn_ts=None):
        ts = txn_ts or self._tick()
        self.data.setdefault(key, []).append(Version(None, ts, deleted=True))
    def gc(self, before_ts):
        for key in list(self.data):
            versions = self.data[key]
            keep = [v for v in versions if v.ts >= before_ts]
            old = [v for v in versions if v.ts < before_ts]
            if old: keep.insert(0, old[-1])
            self.data[key] = keep

def main():
    if len(sys.argv) < 2: print("Usage: mvcc_store.py <demo|test>"); return
    cmd = sys.argv[1]
    if cmd == "demo":
        s = MVCCStore()
        snap1 = s.begin(); s.write("x", 10); snap2 = s.begin()
        s.write("x", 20); snap3 = s.begin()
        print(f"@snap1: {s.read('x', snap1)}, @snap2: {s.read('x', snap2)}, @snap3: {s.read('x', snap3)}")
    elif cmd == "test":
        s = MVCCStore()
        snap0 = s.begin(); s.write("k", "v1"); snap1 = s.begin()
        assert s.read("k", snap0) is None  # written after snap0
        assert s.read("k", snap1) == "v1"
        s.write("k", "v2"); snap2 = s.begin()
        assert s.read("k", snap1) == "v1"
        assert s.read("k", snap2) == "v2"
        s.delete("k"); snap3 = s.begin()
        assert s.read("k", snap2) == "v2"
        assert s.read("k", snap3) is None
        s.gc(snap2); assert s.read("k", snap2) == "v2"
        print("All tests passed!")

if __name__ == "__main__": main()
