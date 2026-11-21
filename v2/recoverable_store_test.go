// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
	v1 "github.com/hashicorp/raft-boltdb"
	"go.etcd.io/bbolt"
)

// testStorePath returns a path to a database file that does not exist yet.
func testStorePath(t *testing.T) string {
	t.Helper()
	fh, err := os.CreateTemp("", "bolt-recoverable-*")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	path := fh.Name()
	fh.Close()
	os.Remove(path)
	t.Cleanup(func() { os.Remove(path) })
	return path
}

// withRawDB opens the database outside of BoltStore so a test can build legacy
// layouts or tamper with the committed-state marker directly.
func withRawDB(t *testing.T, path string, fn func(*bbolt.Tx) error) {
	t.Helper()
	db, err := bbolt.Open(path, dbFileMode, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := db.Update(fn); err != nil {
		db.Close()
		t.Fatalf("err: %s", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func rawStateMarker(t *testing.T, path string) txState {
	t.Helper()
	var st txState
	db, err := bbolt.Open(path, dbFileMode, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer db.Close()
	if err := db.View(func(tx *bbolt.Tx) error {
		st, err = readTxState(tx)
		return err
	}); err != nil {
		t.Fatalf("err: %s", err)
	}
	return st
}

func rawLogCount(t *testing.T, path string) int {
	t.Helper()
	db, err := bbolt.Open(path, dbFileMode, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer db.Close()
	count := 0
	if err := db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(dbLogs).ForEach(func(_, _ []byte) error {
			count++
			return nil
		})
	}); err != nil {
		t.Fatalf("err: %s", err)
	}
	return count
}

// buildLegacyDatabase writes the pre-marker on-disk layout: buckets, logs and
// stable metadata but no committed-state marker.
func buildLegacyDatabase(t *testing.T, path string, logs []*raft.Log, kv map[string]string) {
	t.Helper()
	withRawDB(t, path, func(tx *bbolt.Tx) error {
		logBucket, err := tx.CreateBucketIfNotExists(dbLogs)
		if err != nil {
			return err
		}
		confBucket, err := tx.CreateBucketIfNotExists(dbConf)
		if err != nil {
			return err
		}
		for _, l := range logs {
			val, err := encodeMsgPack(l, false)
			if err != nil {
				return err
			}
			if err := logBucket.Put(uint64ToBytes(l.Index), val.Bytes()); err != nil {
				return err
			}
		}
		for k, v := range kv {
			if err := confBucket.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Requirement 2: old-format data is adopted, never reset, and the adoption
// commits the buckets, data and state marker in one transaction.
func TestRecoverableStore_LegacyDataAdopted(t *testing.T) {
	path := testStorePath(t)
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	buildLegacyDatabase(t, path, logs, map[string]string{"stable": "value"})

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("legacy database should be adopted, got err: %s", err)
	}
	defer store.Close()

	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 1 || last != 3 {
		t.Fatalf("adopted range should be [1, 3], got [%d, %d]", first, last)
	}

	got := new(raft.Log)
	if err := store.GetLog(2, got); err != nil {
		t.Fatalf("adopted log should be readable, got err: %s", err)
	}

	val, err := store.Get([]byte("stable"))
	if err != nil {
		t.Fatalf("adopted stable metadata should be readable, got err: %s", err)
	}
	if string(val) != "value" {
		t.Fatalf("bad stable value: %q", val)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// The adopted state must now be fully committed on disk.
	marker := rawStateMarker(t, path)
	if marker.version != storeVersion1 || marker.firstIndex != 1 || marker.lastIndex != 3 {
		t.Fatalf("bad committed marker after adoption: %+v", marker)
	}
	if count := rawLogCount(t, path); count != 3 {
		t.Fatalf("existing logs must not be reset, found %d", count)
	}
}

// Requirement 2: an empty legacy database is adopted as the empty state rather
// than treated as corruption.
func TestRecoverableStore_EmptyLegacyDatabaseAdopted(t *testing.T) {
	path := testStorePath(t)
	buildLegacyDatabase(t, path, nil, nil)

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("empty legacy database should be adopted, got err: %s", err)
	}
	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 0 || last != 0 {
		t.Fatalf("expected empty range [0, 0], got [%d, %d]", first, last)
	}
	store.Close()

	marker := rawStateMarker(t, path)
	if marker.version != storeVersion1 || marker.firstIndex != 0 || marker.lastIndex != 0 {
		t.Fatalf("bad committed marker: %+v", marker)
	}
}

// Requirement 2: a store whose initializing transaction was interrupted after
// creating only the logs bucket is recovered to a complete state, with any
// logs it already held preserved.
func TestRecoverableStore_PartialInitAdopted(t *testing.T) {
	path := testStorePath(t)
	withRawDB(t, path, func(tx *bbolt.Tx) error {
		logBucket, err := tx.CreateBucketIfNotExists(dbLogs)
		if err != nil {
			return err
		}
		val, err := encodeMsgPack(testRaftLog(7, "log7"), false)
		if err != nil {
			return err
		}
		return logBucket.Put(uint64ToBytes(7), val.Bytes())
	})

	// Read-only access must refuse the half-initialized state.
	if _, err := New(Options{Path: path, BoltOptions: &bbolt.Options{ReadOnly: true}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on partial read-only open, got: %v", err)
	}

	// A writable open completes the interrupted transaction instead of
	// resetting the data.
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("partial init should be recoverable, got: %v", err)
	}
	if first, _ := store.FirstIndex(); first != 7 {
		t.Fatalf("bad adopted first index: %d", first)
	}
	if last, _ := store.LastIndex(); last != 7 {
		t.Fatalf("bad adopted last index: %d", last)
	}
	if err := store.GetLog(7, new(raft.Log)); err != nil {
		t.Fatalf("pre-existing log lost during recovery: %v", err)
	}
	store.Close()

	// After recovery the state is fully committed and opens read-only again.
	ro, err := New(Options{Path: path, BoltOptions: &bbolt.Options{ReadOnly: true}})
	if err != nil {
		t.Fatalf("recovered store should open read-only: %v", err)
	}
	ro.Close()
}

// Requirement 3: a state marker without the log bucket is corruption, not a
// reason to recreate an empty bucket and silently drop history.
func TestRecoverableStore_MissingLogsBucketIsCorrupt(t *testing.T) {
	path := testStorePath(t)
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	store.StoreLog(testRaftLog(1, "log1"))
	store.Close()

	withRawDB(t, path, func(tx *bbolt.Tx) error {
		return tx.DeleteBucket(dbLogs)
	})

	if _, err := NewBoltStore(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt with missing log bucket, got: %v", err)
	}
}

// Requirement 3: read-only opens cannot adopt an uninitialized database; the
// missing committed state is an identifiable corruption error.
func TestRecoverableStore_ReadOnlyRejectsUncommittedState(t *testing.T) {
	path := testStorePath(t)
	buildLegacyDatabase(t, path, []*raft.Log{testRaftLog(1, "log1")}, nil)

	_, err := New(Options{
		Path:        path,
		BoltOptions: &bbolt.Options{ReadOnly: true},
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on uncommitted read-only open, got: %v", err)
	}
}

// Requirement 2 + 3: a partially written state marker is corruption, the
// database is not reset and the failure is repeatable.
func TestRecoverableStore_PartialMarkerIsCorrupt(t *testing.T) {
	path := testStorePath(t)
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.StoreLogs([]*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}); err != nil {
		t.Fatalf("err: %s", err)
	}
	store.Close()

	// Simulate a crash that left the marker half written.
	withRawDB(t, path, func(tx *bbolt.Tx) error {
		return tx.Bucket(dbConf).Delete(dbStateLastIndex)
	})

	if _, err := NewBoltStore(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on partial marker, got: %v", err)
	}
	// Repeated initialization must not mutate or repair the damage.
	if _, err := NewBoltStore(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected repeatable ErrCorrupt, got: %v", err)
	}
	if count := rawLogCount(t, path); count != 3 {
		t.Fatalf("corruption handling must not reset existing logs, found %d", count)
	}
}

// Requirement 2 + 3: contradictory log range metadata is corruption and never
// silently overwritten.
func TestRecoverableStore_RangeMismatchIsCorrupt(t *testing.T) {
	path := testStorePath(t)
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.StoreLogs([]*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}); err != nil {
		t.Fatalf("err: %s", err)
	}
	store.Close()

	withRawDB(t, path, func(tx *bbolt.Tx) error {
		return tx.Bucket(dbConf).Put(dbStateLastIndex, uint64ToBytes(99))
	})

	if _, err := NewBoltStore(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on range mismatch, got: %v", err)
	}
	if count := rawLogCount(t, path); count != 2 {
		t.Fatalf("logs must survive corruption detection intact, found %d", count)
	}
}

// Requirement 3: malformed marker encodings are identifiable corruption.
func TestRecoverableStore_MalformedMarkerIsCorrupt(t *testing.T) {
	path := testStorePath(t)
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	store.StoreLog(testRaftLog(1, "log1"))
	store.Close()

	withRawDB(t, path, func(tx *bbolt.Tx) error {
		return tx.Bucket(dbConf).Put(dbStateVersion, []byte{0x00, 0x01})
	})
	if _, err := NewBoltStore(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on malformed marker, got: %v", err)
	}
}

// Requirement 1: the materialized range and the log mutations share one commit
// boundary across all mutating operations.
func TestRecoverableStore_RangeCommittedWithLogs(t *testing.T) {
	path := testStorePath(t)
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer store.Close()

	if err := store.StoreLogs([]*raft.Log{
		testRaftLog(10, "log10"),
		testRaftLog(11, "log11"),
		testRaftLog(12, "log12"),
	}); err != nil {
		t.Fatalf("err: %s", err)
	}
	if first, _ := store.FirstIndex(); first != 10 {
		t.Fatalf("bad first index: %d", first)
	}
	if last, _ := store.LastIndex(); last != 12 {
		t.Fatalf("bad last index: %d", last)
	}

	if err := store.DeleteRange(11, 11); err != nil {
		t.Fatalf("err: %s", err)
	}
	if first, _ := store.FirstIndex(); first != 10 {
		t.Fatalf("bad first index after delete: %d", first)
	}
	if last, _ := store.LastIndex(); last != 12 {
		t.Fatalf("bad last index after delete: %d", last)
	}
	if err := store.GetLog(11, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("expected log 11 to be gone, got: %v", err)
	}

	// Marker and logs must agree on disk at every committed step.
	store.Close()
	marker := rawStateMarker(t, path)
	if marker.version != storeVersion1 || marker.firstIndex != 10 || marker.lastIndex != 12 {
		t.Fatalf("bad marker after delete: %+v", marker)
	}

	store, err = NewBoltStore(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.DeleteRange(10, 12); err != nil {
		t.Fatalf("err: %s", err)
	}
	if first, _ := store.FirstIndex(); first != 0 {
		t.Fatalf("expected empty first index, got %d", first)
	}
	if last, _ := store.LastIndex(); last != 0 {
		t.Fatalf("expected empty last index, got %d", last)
	}
	store.Close()
	marker = rawStateMarker(t, path)
	if marker.firstIndex != 0 || marker.lastIndex != 0 {
		t.Fatalf("expected empty committed marker, got %+v", marker)
	}
}

// Requirement 4: concurrent readers and writers observe only committed states
// and never surface corruption from in-flight transactions.
func TestRecoverableStore_ConcurrentReadWrite(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < 50; i++ {
				idx := base*1000 + i + 1
				if err := store.StoreLog(testRaftLog(idx, "data")); err != nil {
					t.Errorf("unexpected store error: %v", err)
					return
				}
				got := new(raft.Log)
				if err := store.GetLog(idx, got); err != nil {
					t.Errorf("unexpected get error: %v", err)
					return
				}
			}
		}(uint64(g))
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, err := store.FirstIndex(); err != nil {
					t.Errorf("unexpected first index error: %v", err)
					return
				}
				if _, err := store.LastIndex(); err != nil {
					t.Errorf("unexpected last index error: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// All 400 disjoint logs must be committed and consistent with the marker.
	if count := func() int {
		store.Close()
		return rawLogCount(t, store.path)
	}(); count != 400 {
		t.Fatalf("expected 400 committed logs, got %d", count)
	}
}

// Requirement 4: access after Close returns the identifiable closed error and
// Close itself is idempotent.
func TestRecoverableStore_CloseSemantics(t *testing.T) {
	store := testBoltStore(t)
	if err := store.StoreLog(testRaftLog(1, "log1")); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.Close(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("second Close should return ErrStoreClosed, got: %v", err)
	}

	ops := map[string]func() error{
		"FirstIndex":  func() error { _, err := store.FirstIndex(); return err },
		"LastIndex":   func() error { _, err := store.LastIndex(); return err },
		"GetLog":      func() error { return store.GetLog(1, new(raft.Log)) },
		"StoreLog":    func() error { return store.StoreLog(testRaftLog(2, "log2")) },
		"StoreLogs":   func() error { return store.StoreLogs([]*raft.Log{testRaftLog(2, "log2")}) },
		"DeleteRange": func() error { return store.DeleteRange(1, 1) },
		"Set":         func() error { return store.Set([]byte("k"), []byte("v")) },
		"Get":         func() error { _, err := store.Get([]byte("k")); return err },
		"Sync":        func() error { return store.Sync() },
	}
	for name, op := range ops {
		err := op()
		if !errors.Is(err, ErrStoreClosed) {
			t.Errorf("%s: expected ErrStoreClosed, got: %v", name, err)
		}
		if !errors.Is(err, bbolt.ErrDatabaseNotOpen) {
			t.Errorf("%s: closed error should wrap bbolt.ErrDatabaseNotOpen, got: %v", name, err)
		}
	}
}

// Requirement 4: repeated open/close initialization is idempotent and
// preserves the full state across restarts.
func TestRecoverableStore_ReopenIdempotent(t *testing.T) {
	path := testStorePath(t)
	for cycle := 1; cycle <= 3; cycle++ {
		store, err := NewBoltStore(path)
		if err != nil {
			t.Fatalf("cycle %d: %s", cycle, err)
		}
		if err := store.StoreLog(testRaftLog(uint64(cycle), "log")); err != nil {
			t.Fatalf("cycle %d: %s", cycle, err)
		}
		first, err := store.FirstIndex()
		if err != nil {
			t.Fatalf("cycle %d: %s", cycle, err)
		}
		if first != 1 {
			t.Fatalf("cycle %d: first index reset to %d", cycle, first)
		}
		last, err := store.LastIndex()
		if err != nil {
			t.Fatalf("cycle %d: %s", cycle, err)
		}
		if last != uint64(cycle) {
			t.Fatalf("cycle %d: expected last index %d, got %d", cycle, cycle, last)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("cycle %d: %s", cycle, err)
		}
	}

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("final reopen: %s", err)
	}
	defer store.Close()
	if first, _ := store.FirstIndex(); first != 1 {
		t.Fatalf("bad final first index: %d", first)
	}
	if last, _ := store.LastIndex(); last != 3 {
		t.Fatalf("bad final last index: %d", last)
	}
	for idx := uint64(1); idx <= 3; idx++ {
		if err := store.GetLog(idx, new(raft.Log)); err != nil {
			t.Fatalf("log %d lost across restart: %v", idx, err)
		}
	}
}

// Requirement 4: closing while operations are in flight never panics and every
// failed operation is identifiable as a closed-store error.
func TestRecoverableStore_CloseDuringOps(t *testing.T) {
	store := testBoltStore(t)
	store.StoreLog(testRaftLog(1, "seed"))

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < 1000; i++ {
				idx := base*10000 + i + 10
				if err := store.StoreLog(testRaftLog(idx, "x")); err != nil {
					if !errors.Is(err, ErrStoreClosed) {
						t.Errorf("unexpected write error: %v", err)
					}
					return
				}
				if _, err := store.LastIndex(); err != nil && !errors.Is(err, ErrStoreClosed) {
					t.Errorf("unexpected read error: %v", err)
					return
				}
			}
		}(uint64(g))
	}
	store.Close()
	wg.Wait()
}

// Requirement 1: migration commits the state marker together with the copied
// data, so the destination opens cleanly (including read-only) afterwards.
func TestRecoverableStore_MigrationCommitsState(t *testing.T) {
	dir := t.TempDir()
	srcFile := dir + "/source"
	destFile := dir + "/dest"

	src, err := v1.NewBoltStore(srcFile)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := src.StoreLogs([]*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}); err != nil {
		t.Fatalf("err: %s", err)
	}
	src.Close()

	dest, err := MigrateToV2(srcFile, destFile)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	dest.Close()

	marker := rawStateMarker(t, destFile)
	if marker.version != storeVersion1 || marker.firstIndex != 1 || marker.lastIndex != 2 {
		t.Fatalf("migration did not commit state marker: %+v", marker)
	}

	ro, err := New(Options{
		Path:        destFile,
		BoltOptions: &bbolt.Options{ReadOnly: true},
	})
	if err != nil {
		t.Fatalf("migrated store should open read-only: %v", err)
	}
	defer ro.Close()
	if first, _ := ro.FirstIndex(); first != 1 {
		t.Fatalf("bad migrated first index: %d", first)
	}
}
