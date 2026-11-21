// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const (
	// snapPath is the directory, under the store base path, that holds
	// individual snapshot directories.
	snapPath = "snapshots"
	// metaFilePath is the name of the metadata file inside a snapshot.
	metaFilePath = "meta.json"
	// stateFilePath is the name of the state file inside a snapshot.
	stateFilePath = "state.bin"
	// tmpSuffix marks snapshot directories that have not been finalized.
	// In-progress snapshots are never enumerated, opened or reaped.
	tmpSuffix = ".tmp"
	// deleteSuffix marks directories that have been detached from the
	// visible set and are awaiting physical removal. A rename to this name
	// is the deletion boundary: if the rename fails, the snapshot is
	// untouched and stays fully visible; only a successful rename hides it.
	// Leftover .delete directories are swept on the next delete or reap.
	deleteSuffix = ".delete"
	// permTestFile is used to verify write access to the store directory.
	permTestFile = "permTest"
)

var (
	// ErrSnapshotNotFound is returned when a snapshot ID is not known to
	// the store.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrSnapshotInUse is returned when a snapshot cannot be deleted
	// because a caller still holds an open reader on it.
	ErrSnapshotInUse = errors.New("snapshot is in use")

	// ErrSnapshotLatest is returned when a caller asks to delete the most
	// recent snapshot. The latest snapshot is always retained and readable.
	ErrSnapshotLatest = errors.New("latest snapshot cannot be deleted")
)

// bboltSnapshotMeta is the on-disk metadata of a snapshot. It embeds raft's
// SnapshotMeta and adds a CRC used to verify the state file when opening it.
type bboltSnapshotMeta struct {
	raft.SnapshotMeta
	CRC []byte
}

// snapshotMetaSlice sorts snapshot metadata oldest-first. All comparisons are
// made against full metadata, never against directory enumeration order or
// file names.
type snapshotMetaSlice []*bboltSnapshotMeta

func (s snapshotMetaSlice) Len() int {
	return len(s)
}

func (s snapshotMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapshotMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// BboltSnapshotStore implements raft.SnapshotStore on the local file system
// with bounded retention.
//
// Coordination between Create, List, Open and the deletion paths is provided
// by a single mutex plus two registries:
//
//   - inFlight tracks sinks that have not been finalized. Their directories
//     carry a .tmp suffix and are never visible to List, Open or reaping, so
//     a cleanup that starts while a snapshot is being written can never
//     remove it.
//   - readers counts open readers per finalized snapshot. A snapshot with a
//     non-zero reader count cannot be deleted, so snapshots handed out to
//     callers stay readable for the lifetime of the reader.
//
// The visible set is always derived from the metadata stored on disk.
// Deletion only hides a snapshot once its directory has been atomically
// renamed to a pending-removal name; if that rename fails, the snapshot is
// untouched and stays visible and openable. Retrying the same deletion is
// idempotent, and pending removals left behind by a crash are swept on the
// next delete or reap.
type BboltSnapshotStore struct {
	// path is the directory that contains snapshot directories.
	path string
	// retain is the minimum number of completed snapshots kept on disk.
	// Must be at least 1.
	retain int

	logger hclog.Logger
	noSync bool

	// mu serializes all snapshot lifecycle changes: create/finalize,
	// enumeration, opening and deletion.
	mu sync.Mutex

	// inFlight holds the temporary directory names of unfinished sinks.
	inFlight map[string]struct{}
	// readers holds the number of open readers per finalized snapshot ID.
	readers map[string]int

	// seq disambiguates snapshots created within the same millisecond so
	// that snapshot IDs never collide.
	seq uint64
}

// BboltSnapshotOptions configures a BboltSnapshotStore.
type BboltSnapshotOptions struct {
	// BasePath is the directory under which the "snapshots" directory is
	// created. It is created together with the snapshot directory if it
	// does not exist yet.
	BasePath string

	// Retain controls how many completed snapshots are kept. Snapshots
	// beyond this count are reaped, oldest first. Must be at least 1.
	Retain int

	// Logger receives store diagnostics. When nil a default logger writing
	// to stderr is used.
	Logger hclog.Logger

	// NoSync skips fsync calls when true. This is unsafe and intended for
	// tests only.
	NoSync bool
}

// NewBboltSnapshotStore creates a snapshot store below basePath, keeping at
// least retain completed snapshots.
func NewBboltSnapshotStore(basePath string, retain int) (*BboltSnapshotStore, error) {
	return NewBboltSnapshotStoreWithOptions(BboltSnapshotOptions{
		BasePath: basePath,
		Retain:   retain,
	})
}

// NewBboltSnapshotStoreWithLogger creates a snapshot store below basePath,
// keeping at least retain completed snapshots and logging through logger.
func NewBboltSnapshotStoreWithLogger(basePath string, retain int, logger hclog.Logger) (*BboltSnapshotStore, error) {
	return NewBboltSnapshotStoreWithOptions(BboltSnapshotOptions{
		BasePath: basePath,
		Retain:   retain,
		Logger:   logger,
	})
}

// NewBboltSnapshotStoreWithOptions creates a snapshot store from options.
func NewBboltSnapshotStoreWithOptions(options BboltSnapshotOptions) (*BboltSnapshotStore, error) {
	if options.Retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}
	if options.Logger == nil {
		options.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "snapshot",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}

	path := filepath.Join(options.BasePath, snapPath)
	if err := os.MkdirAll(path, 0o755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}

	store := &BboltSnapshotStore{
		path:     path,
		retain:   options.Retain,
		logger:   options.Logger,
		noSync:   options.NoSync,
		inFlight: make(map[string]struct{}),
		readers:  make(map[string]int),
	}

	if err := store.testPermissions(); err != nil {
		return nil, fmt.Errorf("permissions test failed: %v", err)
	}
	return store, nil
}

// Close is used to gracefully release the store. In-progress sinks and open
// readers are not affected; callers are expected to close them first.
func (s *BboltSnapshotStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The store holds no persistent handles of its own beyond directory
	// paths; the lock only gives Close a happens-after relationship with
	// in-flight lifecycle operations.
	return nil
}

// testPermissions verifies that the store can create and remove files in its
// directory.
func (s *BboltSnapshotStore) testPermissions() error {
	path := filepath.Join(s.path, permTestFile)
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}

// snapshotName generates an ID for a new snapshot. The ID is treated as
// opaque by callers; the sequence suffix only guarantees uniqueness within
// the store.
func (s *BboltSnapshotStore) snapshotName(term, index uint64) string {
	msec := time.Now().UnixNano() / int64(time.Millisecond)
	seq := atomic.AddUint64(&s.seq, 1)
	return fmt.Sprintf("%d-%d-%d-%d", term, index, msec, seq)
}

// Create begins a new snapshot. The returned sink accumulates state through
// Write; Close publishes the snapshot atomically, while Cancel discards it.
//
// The in-progress snapshot is registered and kept in a .tmp directory for
// the whole duration of the write, so a concurrent List or reaping run cannot
// observe, let alone delete, it before it is finalized.
func (s *BboltSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64,
	configuration raft.Configuration, configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	// We only support version 1 snapshots at this time.
	if version != raft.SnapshotVersionMax {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	name := s.snapshotName(term, index)
	dir := filepath.Join(s.path, name+tmpSuffix)

	s.mu.Lock()
	s.inFlight[dir] = struct{}{}
	s.mu.Unlock()

	// The directory creation does not need the lock; the .tmp suffix and the
	// inFlight registration keep every other code path away from it.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.removeInFlight(dir)
		s.logger.Error("failed to make snapshot directory", "path", dir, "error", err)
		return nil, err
	}

	sink := &BboltSnapshotSink{
		store:     s,
		dir:       dir,
		parentDir: s.path,
		noSync:    s.noSync,
		meta: bboltSnapshotMeta{
			SnapshotMeta: raft.SnapshotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Peers:              encodePeers(configuration, trans),
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
		},
	}

	if err := sink.writeMeta(); err != nil {
		s.logger.Error("failed to write metadata", "error", err)
		s.abortCreate(dir)
		return nil, err
	}

	statePath := filepath.Join(dir, stateFilePath)
	fh, err := os.Create(statePath)
	if err != nil {
		s.logger.Error("failed to create state file", "error", err)
		s.abortCreate(dir)
		return nil, err
	}
	sink.stateFile = fh

	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))
	sink.buffered = bufio.NewWriter(io.MultiWriter(fh, sink.stateHash))

	return sink, nil
}

// removeInFlight drops an in-progress registration.
func (s *BboltSnapshotStore) removeInFlight(dir string) {
	s.mu.Lock()
	delete(s.inFlight, dir)
	s.mu.Unlock()
}

// abortCreate cleans up a sink that failed before its state file was opened.
// It cannot call Cancel/finalize because the buffered writer and state file
// are not initialized yet.
func (s *BboltSnapshotStore) abortCreate(dir string) {
	_ = os.RemoveAll(dir)
	s.removeInFlight(dir)
}

// List returns the available snapshots in descending order (highest index
// first), capped at the configured retain count.
func (s *BboltSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	s.mu.Lock()
	snapshots, err := s.loadSnapshotsLocked()
	s.mu.Unlock()
	if err != nil {
		s.logger.Error("failed to get snapshots", "error", err)
		return nil, err
	}

	metas := make([]*raft.SnapshotMeta, 0, len(snapshots))
	for _, meta := range snapshots {
		metas = append(metas, &meta.SnapshotMeta)
		if len(metas) == s.retain {
			break
		}
	}
	return metas, nil
}

// loadSnapshotsLocked scans the store and returns every readable, finalized
// snapshot sorted newest-first. In-progress .tmp directories, directories
// pending physical removal (.delete) and entries with unreadable or
// unsupported metadata are skipped. The result is fully derived from on-disk
// metadata, so a snapshot whose deletion never crossed the rename boundary
// remains visible until it is retried successfully. The caller must hold
// s.mu.
func (s *BboltSnapshotStore) loadSnapshotsLocked() ([]*bboltSnapshotMeta, error) {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		return nil, err
	}

	var snapshots []*bboltSnapshotMeta
	for _, entry := range entries {
		// Ignore any files.
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Ignore in-progress and pending-removal directories.
		if strings.HasSuffix(name, tmpSuffix) {
			s.logger.Warn("found temporary snapshot", "name", name)
			continue
		}
		if strings.HasSuffix(name, deleteSuffix) {
			continue
		}

		meta, err := s.readMetaLocked(name)
		if err != nil {
			s.logger.Warn("failed to read metadata", "name", name, "error", err)
			continue
		}

		if meta.Version < raft.SnapshotVersionMin || meta.Version > raft.SnapshotVersionMax {
			s.logger.Warn("snapshot version not supported", "name", name, "version", meta.Version)
			continue
		}

		snapshots = append(snapshots, meta)
	}

	// Reverse the oldest-first ordering so the newest snapshot comes first.
	sort.Sort(sort.Reverse(snapshotMetaSlice(snapshots)))
	return snapshots, nil
}

// readMeta reads and decodes the metadata of the snapshot in directory name.
// The caller must hold s.mu.
func (s *BboltSnapshotStore) readMetaLocked(name string) (*bboltSnapshotMeta, error) {
	metaPath := filepath.Join(s.path, name, metaFilePath)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fh.Close() }()

	meta := &bboltSnapshotMeta{}
	if err := json.NewDecoder(bufio.NewReader(fh)).Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// Open returns the metadata and a ReadCloser for the snapshot with the given
// ID. The snapshot is pinned for the lifetime of the returned reader:
// deletion attempts fail with ErrSnapshotInUse until the reader is closed,
// guaranteeing that snapshots retained by callers stay readable.
func (s *BboltSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()

	// A finalized snapshot lives in a directory named exactly after its ID.
	// In-progress (.tmp) and pending-removal (.delete) directories therefore
	// resolve to not-found here and can never be opened prematurely.
	meta, err := s.readMetaLocked(id)
	if err != nil {
		s.mu.Unlock()
		if os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
		}
		s.logger.Error("failed to get metadata to open snapshot", "error", err)
		return nil, nil, err
	}

	statePath := filepath.Join(s.path, id, stateFilePath)
	fh, err := os.Open(statePath)
	if err != nil {
		s.mu.Unlock()
		s.logger.Error("failed to open state file", "error", err)
		return nil, nil, err
	}

	// Register the reader before releasing the lock so that no deletion can
	// run between opening the file and pinning the snapshot.
	s.readers[id]++
	s.mu.Unlock()

	verify := func() error {
		stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))
		if _, err := io.Copy(stateHash, fh); err != nil {
			return err
		}
		if computed := stateHash.Sum(nil); !bytes.Equal(meta.CRC, computed) {
			return fmt.Errorf("CRC mismatch")
		}
		if _, err := fh.Seek(0, 0); err != nil {
			return err
		}
		return nil
	}

	if err := verify(); err != nil {
		_ = fh.Close()
		s.mu.Lock()
		s.releaseReaderLocked(id)
		s.mu.Unlock()
		s.logger.Error("failed to verify snapshot", "id", id, "error", err)
		return nil, nil, err
	}

	reader := &snapshotReadCloser{
		store: s,
		id:    id,
		fh:    fh,
		bh:    bufio.NewReader(fh),
	}
	return &meta.SnapshotMeta, reader, nil
}

// releaseReaderLocked removes one reader registration, dropping the map entry
// when the count reaches zero. The caller must hold s.mu.
func (s *BboltSnapshotStore) releaseReaderLocked(id string) {
	if s.readers[id] <= 1 {
		delete(s.readers, id)
		return
	}
	s.readers[id]--
}

// DeleteSnapshot removes a single snapshot by ID.
//
// Deletion is a two-phase mark-and-sweep:
//  1. The snapshot directory is atomically renamed to a unique .delete name.
//     Only this step removes it from the visible set. If the rename fails,
//     nothing has changed: the snapshot stays fully visible and openable, and
//     the error is returned for a later retry.
//  2. The renamed directory is physically removed. A failure here leaves an
//     invisible .delete directory behind, which is swept on the next delete or
//     reap; it never affects the visible list.
//
// The latest snapshot cannot be deleted (ErrSnapshotLatest), snapshots with
// open readers cannot be deleted (ErrSnapshotInUse) and unknown IDs return
// ErrSnapshotNotFound. Repeating a delete for an ID that is already absent or
// pending removal succeeds, so retrying the same snapshot is idempotent and
// converges.
func (s *BboltSnapshotStore) DeleteSnapshot(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Retry any removals left behind by an interrupted deletion.
	if err := s.sweepPendingLocked(); err != nil {
		return err
	}

	dir := filepath.Join(s.path, id)
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			// Already absent or pending removal: a repeated delete
			// converges to success.
			return nil
		}
		return err
	}

	snapshots, err := s.loadSnapshotsLocked()
	if err != nil {
		return err
	}
	if len(snapshots) > 0 && snapshots[0].ID == id {
		return fmt.Errorf("%w: %s", ErrSnapshotLatest, id)
	}

	if n := s.readers[id]; n > 0 {
		return fmt.Errorf("%w: %s", ErrSnapshotInUse, id)
	}

	return s.detachAndRemoveLocked(dir, id)
}

// detachAndRemoveLocked renames a snapshot directory out of the visible set
// and then removes it. The caller must hold s.mu.
func (s *BboltSnapshotStore) detachAndRemoveLocked(dir, id string) error {
	pending := s.pendingPathLocked(id)
	s.logger.Info("deleting snapshot", "path", dir)
	if err := os.Rename(dir, pending); err != nil {
		// The boundary move failed: the snapshot is untouched and remains
		// visible to List and Open for a retry.
		s.logger.Error("failed to mark snapshot for deletion", "path", dir, "error", err)
		return err
	}

	if err := os.RemoveAll(pending); err != nil {
		s.logger.Error("failed to remove marked snapshot", "path", pending, "error", err)
		return err
	}
	return nil
}

// pendingPathLocked builds a unique directory name for a snapshot awaiting
// physical removal. The caller must hold s.mu.
func (s *BboltSnapshotStore) pendingPathLocked(id string) string {
	seq := atomic.AddUint64(&s.seq, 1)
	name := fmt.Sprintf("%s-%d%s", id, seq, deleteSuffix)
	return filepath.Join(s.path, name)
}

// sweepPendingLocked retries the physical removal of directories left behind
// by an interrupted delete or reap. Failures are returned but never expose or
// resurrect a visible snapshot. The caller must hold s.mu.
func (s *BboltSnapshotStore) sweepPendingLocked() error {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		return err
	}

	var errs []error
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasSuffix(entry.Name(), deleteSuffix) {
			continue
		}
		pending := filepath.Join(s.path, entry.Name())
		if err := os.RemoveAll(pending); err != nil {
			s.logger.Error("failed to sweep pending snapshot", "path", pending, "error", err)
			errs = append(errs, fmt.Errorf("sweep %s: %w", entry.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// ReapSnapshots removes completed snapshots beyond the retain count,
// oldest first. It is invoked automatically when a snapshot is finalized and
// can also be called directly.
//
// The candidate set is re-derived from on-disk metadata every time, so
// snapshots whose earlier deletion never crossed the rename boundary are
// retried. Each removal is an independent mark-and-sweep: a failure leaves
// that snapshot in place and visible, while other candidates are still
// processed.
func (s *BboltSnapshotStore) ReapSnapshots() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reapSnapshotsLocked()
}

// reapSnapshotsLocked prunes snapshots beyond the retain count. The caller
// must hold s.mu.
func (s *BboltSnapshotStore) reapSnapshotsLocked() error {
	// First finish any removals interrupted by an earlier run.
	var errs []error
	if err := s.sweepPendingLocked(); err != nil {
		errs = append(errs, err)
	}

	snapshots, err := s.loadSnapshotsLocked()
	if err != nil {
		errs = append(errs, err)
		return errors.Join(errs...)
	}

	for i := s.retain; i < len(snapshots); i++ {
		meta := snapshots[i]

		// Never remove a snapshot a caller is currently reading.
		if n := s.readers[meta.ID]; n > 0 {
			s.logger.Warn("skipping in-use snapshot during reaping", "id", meta.ID, "readers", n)
			continue
		}

		path := filepath.Join(s.path, meta.ID)
		if err := s.detachAndRemoveLocked(path, meta.ID); err != nil {
			errs = append(errs, fmt.Errorf("reap snapshot %s: %w", meta.ID, err))
		}
	}
	return errors.Join(errs...)
}

// BboltSnapshotSink implements raft.SnapshotSink on a file.
type BboltSnapshotSink struct {
	store     *BboltSnapshotStore
	dir       string
	parentDir string
	meta      bboltSnapshotMeta

	noSync bool

	stateFile *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer

	closed bool
}

// ID returns the ID the snapshot will have once finalized.
func (s *BboltSnapshotSink) ID() string {
	return s.meta.ID
}

// Write appends state data to the snapshot.
func (s *BboltSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

// Close finalizes the snapshot: the state file is flushed and checksummed,
// the final metadata is written, and the temporary directory is atomically
// renamed into place before it becomes visible. Reaping runs while still
// holding the lock, and the freshly published snapshot is included in the
// metadata-based ordering, so it can never reap itself.
func (s *BboltSnapshotSink) Close() error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if err := s.finalize(); err != nil {
		s.store.logger.Error("failed to finalize snapshot", "error", err)
		if delErr := os.RemoveAll(s.dir); delErr != nil {
			s.store.logger.Error("failed to delete temporary snapshot directory", "path", s.dir, "error", delErr)
			delete(s.store.inFlight, s.dir)
			return delErr
		}
		delete(s.store.inFlight, s.dir)
		return err
	}

	if err := s.writeMeta(); err != nil {
		s.store.logger.Error("failed to write metadata", "error", err)
		_ = os.RemoveAll(s.dir)
		delete(s.store.inFlight, s.dir)
		return err
	}

	// Publish atomically: before this rename the directory carries the
	// .tmp suffix and is invisible to enumeration and reaping.
	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := os.Rename(s.dir, newPath); err != nil {
		s.store.logger.Error("failed to move snapshot into place", "error", err)
		_ = os.RemoveAll(s.dir)
		delete(s.store.inFlight, s.dir)
		return err
	}
	delete(s.store.inFlight, s.dir)

	if err := syncParentDir(s.parentDir, s.noSync); err != nil {
		return err
	}

	// Reap while holding the lock; the new snapshot is already on disk and
	// sorts above older ones, so it is never selected for removal.
	return s.store.reapSnapshotsLocked()
}

// Cancel aborts an unfinished snapshot and removes all of its artifacts.
func (s *BboltSnapshotSink) Cancel() error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if err := s.finalize(); err != nil {
		s.store.logger.Error("failed to finalize snapshot", "error", err)
		delete(s.store.inFlight, s.dir)
		return err
	}
	delete(s.store.inFlight, s.dir)
	return os.RemoveAll(s.dir)
}

// finalize flushes and syncs the state file and records its size and CRC.
func (s *BboltSnapshotSink) finalize() error {
	if err := s.buffered.Flush(); err != nil {
		return err
	}
	if !s.noSync {
		if err := s.stateFile.Sync(); err != nil {
			return err
		}
	}

	stat, statErr := s.stateFile.Stat()
	if err := s.stateFile.Close(); err != nil {
		return err
	}
	if statErr != nil {
		return statErr
	}

	s.meta.Size = stat.Size()
	s.meta.CRC = s.stateHash.Sum(nil)
	return nil
}

// writeMeta writes the snapshot metadata as JSON into the snapshot directory.
func (s *BboltSnapshotSink) writeMeta() error {
	metaPath := filepath.Join(s.dir, metaFilePath)
	fh, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }()

	buffered := bufio.NewWriter(fh)
	if err := json.NewEncoder(buffered).Encode(&s.meta); err != nil {
		return err
	}
	if err := buffered.Flush(); err != nil {
		return err
	}
	if !s.noSync {
		if err := fh.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// snapshotReadCloser wraps the state file with buffered reads and releases
// the snapshot's reader pin when closed. Close is idempotent.
type snapshotReadCloser struct {
	store *BboltSnapshotStore
	id    string
	fh    *os.File
	bh    *bufio.Reader

	closed bool
	mu     sync.Mutex
}

func (r *snapshotReadCloser) Read(p []byte) (int, error) {
	return r.bh.Read(p)
}

func (r *snapshotReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true

	err := r.fh.Close()
	r.store.mu.Lock()
	r.store.releaseReaderLocked(r.id)
	r.store.mu.Unlock()
	return err
}

// syncParentDir fsyncs a directory so that a rename within it is durable.
// It is a no-op with NoSync or on Windows.
func syncParentDir(dir string, noSync bool) error {
	if noSync || runtime.GOOS == "windows" {
		return nil
	}
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }()
	return fh.Sync()
}

// encodePeers serializes the voting servers of a configuration into the
// legacy peers format, matching the on-disk layout of raft's
// FileSnapshotStore.
func encodePeers(configuration raft.Configuration, trans raft.Transport) []byte {
	var encPeers [][]byte
	for _, server := range configuration.Servers {
		if server.Suffrage == raft.Voter {
			encPeers = append(encPeers, trans.EncodePeer(server.ID, server.Address))
		}
	}

	buf, err := encodeMsgPack(encPeers, false)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}
	return buf.Bytes()
}
