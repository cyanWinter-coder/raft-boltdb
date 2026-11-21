// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// testSnapshotStore builds a NoSync store retaining retain snapshots in a
// temporary directory.
func testSnapshotStore(t testing.TB, retain int) (*BboltSnapshotStore, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewBboltSnapshotStoreWithOptions(BboltSnapshotOptions{
		BasePath: dir,
		Retain:   retain,
		Logger:   hclog.NewNullLogger(),
		NoSync:   true,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dir
}

// snapshotStoreDir returns the on-disk directory of a store.
func snapshotStoreDir(dir string) string {
	return filepath.Join(dir, snapPath)
}

// writeDiskSnapshot lays out a completed snapshot directly on disk with the
// given metadata, bypassing Create. It lets tests control the on-disk layout
// independently of directory enumeration order.
func writeDiskSnapshot(t testing.TB, baseDir, id string, term, index uint64) string {
	t.Helper()
	dir := filepath.Join(snapshotStoreDir(baseDir), id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("err: %v", err)
	}

	meta := bboltSnapshotMeta{
		SnapshotMeta: raft.SnapshotMeta{
			Version:            raft.SnapshotVersionMax,
			ID:                 id,
			Index:              index,
			Term:               term,
			Configuration:      raft.Configuration{},
			ConfigurationIndex: 0,
		},
	}
	payload := []byte(fmt.Sprintf("state-%s", id))
	meta.Size = int64(len(payload))
	hash := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := hash.Write(payload); err != nil {
		t.Fatalf("err: %v", err)
	}
	meta.CRC = hash.Sum(nil)

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, metaFilePath), metaBytes, 0o600); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFilePath), payload, 0o600); err != nil {
		t.Fatalf("err: %v", err)
	}
	return dir
}

func TestBboltSnapshotStore_Implements(t *testing.T) {
	var store interface{} = &BboltSnapshotStore{}
	if _, ok := store.(raft.SnapshotStore); !ok {
		t.Fatalf("BboltSnapshotStore does not implement raft.SnapshotStore")
	}
	var sink interface{} = &BboltSnapshotSink{}
	if _, ok := sink.(raft.SnapshotSink); !ok {
		t.Fatalf("BboltSnapshotSink does not implement raft.SnapshotSink")
	}
}

func TestNewBboltSnapshotStore_Validation(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewBboltSnapshotStore(dir, 0); err == nil {
		t.Fatalf("expected error when retain is zero")
	}
	if _, err := NewBboltSnapshotStore(dir, -1); err == nil {
		t.Fatalf("expected error when retain is negative")
	}
}

func TestNewBboltSnapshotStore_MissingParentDir(t *testing.T) {
	parent := t.TempDir()
	missing := filepath.Join(parent, "nested", "store")
	store, err := NewBboltSnapshotStore(missing, 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_ = store.Close()
	if info, err := os.Stat(filepath.Join(missing, snapPath)); err != nil || !info.IsDir() {
		t.Fatalf("snapshot directory was not created: %v", err)
	}
}

func TestBboltSnapshotStore_CreateAndList(t *testing.T) {
	store, _ := testSnapshotStore(t, 3)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())

	sink, err := store.Create(raft.SnapshotVersionMax, 10, 3, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if sink.ID() == "" {
		t.Fatalf("sink has no ID")
	}
	// While the sink is open nothing is visible yet.
	if metas, err := store.List(); err != nil || len(metas) != 0 {
		t.Fatalf("in-progress snapshot must not be listed: %v %v", metas, err)
	}

	if _, err := sink.Write([]byte("hello")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Close is idempotent.
	if err := sink.Close(); err != nil {
		t.Fatalf("duplicate Close should be a no-op: %v", err)
	}

	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("expect 1 snapshot, got %d", len(metas))
	}
	if metas[0].Index != 10 || metas[0].Term != 3 {
		t.Fatalf("bad metadata: %#v", metas[0])
	}
}

func TestBboltSnapshotStore_UnsupportedVersion(t *testing.T) {
	store, _ := testSnapshotStore(t, 3)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	_, err := store.Create(99, 1, 1, raft.Configuration{}, 0, trans)
	if err == nil {
		t.Fatalf("expected unsupported version error")
	}
}

func TestBboltSnapshotStore_Cancel(t *testing.T) {
	store, _ := testSnapshotStore(t, 3)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())

	sink, err := store.Create(raft.SnapshotVersionMax, 10, 3, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := sink.Write([]byte("discard me")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Cancel is idempotent.
	if err := sink.Cancel(); err != nil {
		t.Fatalf("duplicate Cancel should be a no-op: %v", err)
	}

	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("cancelled snapshot must not be listed: %v", metas)
	}
	if len(store.inFlight) != 0 {
		t.Fatalf("in-flight registration was not cleaned up: %v", store.inFlight)
	}
}

func TestBboltSnapshotStore_Open(t *testing.T) {
	store, _ := testSnapshotStore(t, 3)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())

	payload := []byte("snapshot payload")
	sink, err := store.Create(raft.SnapshotVersionMax, 10, 3, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := sink.Write(payload); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	meta, reader, err := store.Open(sink.ID())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if meta.ID != sink.ID() || meta.Size != int64(len(payload)) {
		t.Fatalf("bad metadata: %#v", meta)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("bad payload: %q", data)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Reader Close is idempotent.
	if err := reader.Close(); err != nil {
		t.Fatalf("duplicate reader Close should be a no-op: %v", err)
	}

	if _, _, err := store.Open("does-not-exist"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
}

// TestBboltSnapshotStore_MetadataOrdering proves that selection for listing
// and reaping is driven by the metadata inside meta.json rather than the
// order ReadDir returns names. On-disk directory names are shuffled and use a
// naming that sorts opposite to the embedded term/index.
func TestBboltSnapshotStore_MetadataOrdering(t *testing.T) {
	store, dir := testSnapshotStore(t, 2)

	// Create directories out of metadata order, with names whose lexical
	// ordering is the reverse of the snapshot ordering.
	writeDiskSnapshot(t, dir, "zzz-oldest", 1, 100)
	writeDiskSnapshot(t, dir, "mmm-middle", 2, 200)
	writeDiskSnapshot(t, dir, "aaa-newest", 3, 300)

	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("expect 2 retained snapshots, got %d: %v", len(metas), metas)
	}
	if metas[0].ID != "aaa-newest" || metas[1].ID != "mmm-middle" {
		t.Fatalf("metadata ordering ignored: %v %v", metas[0].ID, metas[1].ID)
	}

	if err := store.ReapSnapshots(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshotStoreDir(dir), "zzz-oldest")); !os.IsNotExist(err) {
		t.Fatalf("oldest snapshot by metadata was not reaped: %v", err)
	}
	for _, id := range []string{"aaa-newest", "mmm-middle"} {
		if _, err := os.Stat(filepath.Join(snapshotStoreDir(dir), id)); err != nil {
			t.Fatalf("retained snapshot %s missing: %v", id, err)
		}
	}
}

func TestBboltSnapshotStore_Retention(t *testing.T) {
	store, dir := testSnapshotStore(t, 2)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())

	for i := 10; i < 15; i++ {
		sink, err := store.Create(raft.SnapshotVersionMax, uint64(i), 3, raft.Configuration{}, 0, trans)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if _, err := sink.Write([]byte("payload")); err != nil {
			t.Fatalf("err: %v", err)
		}
		if err := sink.Close(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("expect 2 snapshots, got %d", len(metas))
	}
	if metas[0].Index != 14 || metas[1].Index != 13 {
		t.Fatalf("unexpected retained snapshots: %#v %#v", metas[0], metas[1])
	}

	entries, err := os.ReadDir(snapshotStoreDir(dir))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 directories on disk, got %d", len(entries))
	}
}

// TestBboltSnapshotStore_ReapNeverTouchesInFlight verifies requirement 2: a
// reaping run that starts while a new snapshot is still being written cannot
// remove the in-progress snapshot, and the snapshot is published intact.
func TestBboltSnapshotStore_ReapNeverTouchesInFlight(t *testing.T) {
	store, dir := testSnapshotStore(t, 1)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())

	// Fill the store up to the retain count.
	sink, err := store.Create(raft.SnapshotVersionMax, 1, 1, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Start a new snapshot and run reaping while it is still open.
	incoming, err := store.Create(raft.SnapshotVersionMax, 2, 1, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := incoming.Write([]byte("in progress")); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := store.ReapSnapshots(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// The temporary directory must still be there.
	tmpDir := incoming.(*BboltSnapshotSink).dir
	if _, err := os.Stat(tmpDir); err != nil {
		t.Fatalf("in-progress snapshot was removed by reaping: %v", err)
	}

	if err := incoming.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 1 || metas[0].Index != 2 {
		t.Fatalf("new snapshot not published correctly: %v", metas)
	}

	// No .tmp directories should ever remain after finalization.
	entries, _ := os.ReadDir(snapshotStoreDir(dir))
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), tmpSuffix) {
			t.Fatalf("temporary directory left behind: %s", entry.Name())
		}
	}
}

// TestBboltSnapshotStore_DeleteLatestRejected verifies requirement 4: the
// latest snapshot is always kept readable.
func TestBboltSnapshotStore_DeleteLatestRejected(t *testing.T) {
	store, dir := testSnapshotStore(t, 5)
	writeDiskSnapshot(t, dir, "old", 1, 1)
	writeDiskSnapshot(t, dir, "new", 2, 2)

	if err := store.DeleteSnapshot("new"); !errors.Is(err, ErrSnapshotLatest) {
		t.Fatalf("expected ErrSnapshotLatest, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshotStoreDir(dir), "new")); err != nil {
		t.Fatalf("latest snapshot must remain on disk: %v", err)
	}

	// An older snapshot can be removed, and the latest still opens.
	if err := store.DeleteSnapshot("old"); err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, reader, err := store.Open("new"); err != nil {
		t.Fatalf("latest snapshot must stay readable: %v", err)
	} else {
		_ = reader.Close()
	}
}

// TestBboltSnapshotStore_DeleteInUse verifies requirement 4: a snapshot held
// open by a caller cannot be deleted until the reader is closed.
func TestBboltSnapshotStore_DeleteInUse(t *testing.T) {
	store, dir := testSnapshotStore(t, 5)
	writeDiskSnapshot(t, dir, "old", 1, 1)
	writeDiskSnapshot(t, dir, "new", 2, 2)

	_, reader, err := store.Open("old")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := store.DeleteSnapshot("old"); !errors.Is(err, ErrSnapshotInUse) {
		t.Fatalf("expected ErrSnapshotInUse, got %v", err)
	}
	// Reaping must also skip the pinned snapshot.
	if err := store.ReapSnapshots(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshotStoreDir(dir), "old")); err != nil {
		t.Fatalf("pinned snapshot must remain on disk: %v", err)
	}

	// The data is still fully readable through the open handle.
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(data) != "state-old" {
		t.Fatalf("bad payload: %q", data)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// After release the deletion succeeds.
	if err := store.DeleteSnapshot("old"); err != nil {
		t.Fatalf("err: %v", err)
	}
}

// TestBboltSnapshotStore_DeleteIdempotent verifies requirement 3: retrying the
// same deletion converges, and deleting an ID that is already absent is a
// no-op success rather than an error.
func TestBboltSnapshotStore_DeleteIdempotent(t *testing.T) {
	store, dir := testSnapshotStore(t, 5)
	writeDiskSnapshot(t, dir, "old", 1, 1)
	writeDiskSnapshot(t, dir, "new", 2, 2)

	if err := store.DeleteSnapshot("old"); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Repeating the same delete succeeds.
	if err := store.DeleteSnapshot("old"); err != nil {
		t.Fatalf("retry should be idempotent: %v", err)
	}
	if _, _, err := store.Open("old"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound after delete, got %v", err)
	}
	// An ID that never existed also converges, since nothing named it exists.
	if err := store.DeleteSnapshot("never-existed"); err != nil {
		t.Fatalf("deleting an absent snapshot should converge to success: %v", err)
	}
}

// TestBboltSnapshotStore_DeleteFailureStaysVisible verifies requirement 3:
// when removal fails before the atomic rename boundary, the snapshot directory
// is untouched and stays openable, and the same deletion succeeds once the
// underlying problem is cleared.
func TestBboltSnapshotStore_DeleteFailureStaysVisible(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	if runtime.GOOS == "windows" {
		t.Skip("permission semantics differ on windows")
	}

	// retain=2 with three snapshots makes "oldest" a reaping candidate while
	// the other two remain within the retained, List-visible window.
	store, dir := testSnapshotStore(t, 2)
	oldestDir := writeDiskSnapshot(t, dir, "oldest", 1, 1)
	writeDiskSnapshot(t, dir, "middle", 2, 2)
	writeDiskSnapshot(t, dir, "newest", 3, 3)
	snapDir := snapshotStoreDir(dir)

	// Removing or renaming an entry requires write permission on the parent
	// directory, so a read-only snapshots directory makes every deletion
	// fail at the rename boundary.
	if err := os.Chmod(snapDir, 0o500); err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(snapDir, 0o755) })

	// The explicit delete fails at the rename boundary.
	if err := store.DeleteSnapshot("oldest"); err == nil {
		t.Fatalf("expected deletion to fail under restrictive permissions")
	}
	// The reap run surfaces the same removal failure for the over-retain
	// candidate.
	if err := store.ReapSnapshots(); err == nil {
		t.Fatalf("expected reaping to surface the removal failure")
	}

	// The directory was never moved and the snapshot is still fully readable.
	if _, err := os.Stat(oldestDir); err != nil {
		t.Fatalf("failed deletion must leave the directory in place: %v", err)
	}
	if _, reader, err := store.Open("oldest"); err != nil {
		t.Fatalf("failed deletion must stay openable: %v", err)
	} else {
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if string(data) != "state-oldest" {
			t.Fatalf("bad payload after failed delete: %q", data)
		}
		_ = reader.Close()
	}

	// The retained snapshots remain listed and readable.
	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 2 || metas[0].ID != "newest" || metas[1].ID != "middle" {
		t.Fatalf("retained snapshots changed after failed delete: %v", metas)
	}

	// Restore permissions; retrying the same deletion now converges.
	if err := os.Chmod(snapDir, 0o755); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := store.ReapSnapshots(); err != nil {
		t.Fatalf("retry after recovery failed: %v", err)
	}
	if _, err := os.Stat(oldestDir); !os.IsNotExist(err) {
		t.Fatalf("snapshot should be gone after retry: %v", err)
	}
	// A further retry on the now-absent snapshot is idempotent.
	if err := store.DeleteSnapshot("oldest"); err != nil {
		t.Fatalf("deleting an absent snapshot should converge: %v", err)
	}
}

// TestBboltSnapshotStore_SweepsPending verifies that a directory left behind
// in the pending-removal (.delete) state by an interrupted deletion is never
// visible, and is swept away by the next reap.
func TestBboltSnapshotStore_SweepsPending(t *testing.T) {
	store, dir := testSnapshotStore(t, 5)
	writeDiskSnapshot(t, dir, "good", 2, 2)

	pending := filepath.Join(snapshotStoreDir(dir), "leftover-1.delete")
	if err := os.MkdirAll(pending, 0o755); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pending, stateFilePath), []byte("stale"), 0o600); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Pending directories never enter the visible set.
	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 1 || metas[0].ID != "good" {
		t.Fatalf("pending directory must not be listed: %v", metas)
	}
	if _, _, err := store.Open("leftover-1.delete"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound for pending dir, got %v", err)
	}

	if err := store.ReapSnapshots(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := os.Stat(pending); !os.IsNotExist(err) {
		t.Fatalf("pending directory was not swept: %v", err)
	}
}

// TestBboltSnapshotStore_BadMetaSkipped ensures corrupted metadata does not
// break enumeration of the healthy snapshots.
func TestBboltSnapshotStore_BadMetaSkipped(t *testing.T) {
	store, dir := testSnapshotStore(t, 5)
	writeDiskSnapshot(t, dir, "good", 2, 2)

	badDir := filepath.Join(snapshotStoreDir(dir), "corrupt")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := os.WriteFile(filepath.Join(badDir, metaFilePath), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("err: %v", err)
	}

	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) != 1 || metas[0].ID != "good" {
		t.Fatalf("healthy snapshot should be the only one listed: %v", metas)
	}
}

// TestBboltSnapshotStore_CreateFailureCleansUp verifies that a Create that
// fails while laying down its working directory does not panic and leaves no
// in-flight registration or temporary directory behind.
func TestBboltSnapshotStore_CreateFailureCleansUp(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	if runtime.GOOS == "windows" {
		t.Skip("permission semantics differ on windows")
	}

	store, dir := testSnapshotStore(t, 2)
	snapDir := snapshotStoreDir(dir)
	if err := os.Chmod(snapDir, 0o500); err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(snapDir, 0o755) })

	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	if _, err := store.Create(raft.SnapshotVersionMax, 1, 1, raft.Configuration{}, 0, trans); err == nil {
		t.Fatalf("expected Create to fail when the directory is not writable")
	}

	if len(store.inFlight) != 0 {
		t.Fatalf("failed create left an in-flight registration: %v", store.inFlight)
	}
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), tmpSuffix) {
			t.Fatalf("failed create left a temporary directory: %s", entry.Name())
		}
	}
}

// TestBboltSnapshotStore_ConcurrentLifecycle exercises requirement 2 under
// the race detector: concurrent creation, enumeration, reaping and deletion
// must never touch an in-progress or in-use snapshot.
func TestBboltSnapshotStore_ConcurrentLifecycle(t *testing.T) {
	store, dir := testSnapshotStore(t, 3)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())

	const writers = 4
	const perWriter = 10
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				sink, err := store.Create(raft.SnapshotVersionMax, uint64(i+1), uint64(w+1),
					raft.Configuration{}, 0, trans)
				if err != nil {
					t.Errorf("create: %v", err)
					return
				}
				if _, err := sink.Write([]byte("concurrent payload")); err != nil {
					t.Errorf("write: %v", err)
					_ = sink.Cancel()
					return
				}
				if err := sink.Close(); err != nil {
					t.Errorf("close: %v", err)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			metas, err := store.List()
			if err != nil {
				t.Errorf("list: %v", err)
				return
			}
			// List must always return metadata in descending term/index
			// order and never exceed the retain count.
			if len(metas) > store.retain {
				t.Errorf("list exceeded retain: %d > %d", len(metas), store.retain)
				return
			}
			if !sort.SliceIsSorted(metas, func(i, j int) bool {
				if metas[i].Term != metas[j].Term {
					return metas[i].Term > metas[j].Term
				}
				return metas[i].Index > metas[j].Index
			}) {
				t.Errorf("list not in descending order: %v", metas)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := store.ReapSnapshots(); err != nil {
				t.Errorf("reap: %v", err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			metas, err := store.List()
			if err != nil || len(metas) == 0 {
				continue
			}
			// Only ever try to delete non-latest snapshots.
			for _, meta := range metas[1:] {
				if err := store.DeleteSnapshot(meta.ID); err != nil &&
					!errors.Is(err, ErrSnapshotLatest) &&
					!errors.Is(err, ErrSnapshotInUse) &&
					!errors.Is(err, ErrSnapshotNotFound) {
					t.Errorf("delete: %v", err)
					return
				}
			}
		}
	}()

	wg.Wait()

	// Final invariant: the newest snapshot is always present and readable,
	// no temporary directories remain, and the disk holds at most retain
	// completed snapshots.
	metas, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(metas) == 0 {
		t.Fatalf("at least the latest snapshot must survive")
	}
	_, reader, err := store.Open(metas[0].ID)
	if err != nil {
		t.Fatalf("latest snapshot not readable: %v", err)
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("err: %v", err)
	}
	_ = reader.Close()

	entries, err := os.ReadDir(snapshotStoreDir(dir))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), tmpSuffix) {
			t.Fatalf("temporary directory left behind: %s", entry.Name())
		}
	}
	if len(entries) > store.retain {
		t.Fatalf("disk holds %d snapshots, retain is %d", len(entries), store.retain)
	}
}

// TestBboltSnapshotStore_ConcurrentOpenBlocksDelete pins many readers while a
// deletion keeps trying; the snapshot must remain readable until all readers
// close.
func TestBboltSnapshotStore_ConcurrentOpenBlocksDelete(t *testing.T) {
	store, dir := testSnapshotStore(t, 5)
	writeDiskSnapshot(t, dir, "old", 1, 1)
	writeDiskSnapshot(t, dir, "new", 2, 2)

	const readers = 8
	var wg sync.WaitGroup
	closeReaders := make(chan struct{})

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, reader, err := store.Open("old")
			if err != nil {
				t.Errorf("open: %v", err)
				return
			}
			<-closeReaders
			_ = reader.Close()
		}()
	}

	// Give readers a moment to register.
	attempts := 0
	for store.readerCount("old") < readers {
		runtime.Gosched()
		attempts++
		if attempts > 1000 {
			close(closeReaders)
			wg.Wait()
			t.Fatalf("readers did not register: %v", store.readers)
		}
	}

	if err := store.DeleteSnapshot("old"); !errors.Is(err, ErrSnapshotInUse) {
		t.Errorf("expected ErrSnapshotInUse, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshotStoreDir(dir), "old")); err != nil {
		t.Fatalf("pinned snapshot removed: %v", err)
	}

	close(closeReaders)
	wg.Wait()

	if err := store.DeleteSnapshot("old"); err != nil {
		t.Fatalf("delete after readers closed: %v", err)
	}
}

// readerCount returns the current open-reader count for a snapshot.
func (s *BboltSnapshotStore) readerCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readers[id]
}
