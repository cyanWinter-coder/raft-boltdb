// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"errors"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	metrics "github.com/hashicorp/go-metrics"
	"github.com/hashicorp/raft"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0600
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
	dbConf = []byte("conf")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BoltStore provides access to BoltDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BoltStore struct {
	// conn is the underlying handle to the db.
	conn *bolt.DB

	// The path to the Bolt database file
	path string

	// stateMu serializes Close against all transactions. Readers hold an
	// RLock for the lifetime of their transaction; writers and Close hold the
	// full lock. Once closed is set, no new transaction can begin, so every
	// operation either completes against a live database or fails with
	// ErrStoreClosed.
	stateMu sync.RWMutex
	closed  bool
}

// Options contains all the configuration used to open the BoltDB
type Options struct {
	// Path is the file path to the BoltDB to use
	Path string

	// BoltOptions contains any specific BoltDB options you might
	// want to specify [e.g. open timeout]
	BoltOptions *bolt.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool
}

// readOnly returns true if the contained bolt options say to open
// the DB in readOnly mode [this can be useful to tools that want
// to examine the log]
func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// NewBoltStore takes a file path and returns a connected Raft backend.
func NewBoltStore(path string) (*BoltStore, error) {
	return New(Options{Path: path})
}

// New uses the supplied options to open the BoltDB and prepare it for use as a raft backend.
//
// Initialization is an idempotent, recoverable transaction: the buckets,
// stored logs, stable metadata and the committed-state marker are either all
// present together or absent together on disk. Opening an existing database
// never resets its data; a database left half-initialized by a crash is
// adopted on the first write or reported via ErrCorrupt on reads.
func New(options Options) (*BoltStore, error) {
	// Try to connect
	handle, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	// Create the new store
	store := &BoltStore{
		conn: handle,
		path: options.Path,
	}

	// If the store was opened read-only, don't try and create buckets or
	// adopt an uninitialized database. Fail fast if the stored state is not
	// fully committed.
	if !options.readOnly() {
		// Set up our buckets
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	} else if err := store.view(func(tx *bolt.Tx) error {
		return verifyCommittedState(tx)
	}); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

// initialize ensures the store's buckets and committed-state marker exist in
// a single transaction. It is safe to run repeatedly: a fresh database gets
// its initial state, an old-format or interrupted database has its existing
// data adopted, and an already initialized database is only validated.
func (b *BoltStore) initialize() error {
	return b.update(func(tx *bolt.Tx) error {
		return b.bootstrapCommittedState(tx)
	})
}

// checkClosed returns ErrStoreClosed once Close has run. The state lock is
// held by the caller for the full lifetime of the transaction it guards.
func (b *BoltStore) checkClosed() error {
	if b.closed {
		return errors.Join(ErrStoreClosed, bolt.ErrDatabaseNotOpen)
	}
	return nil
}

// view runs fn inside a read-only transaction that can only observe committed
// state. The RLock spans the whole transaction, preventing Close from
// closing the database underneath it.
func (b *BoltStore) view(fn func(*bolt.Tx) error) error {
	b.stateMu.RLock()
	defer b.stateMu.RUnlock()
	if err := b.checkClosed(); err != nil {
		return err
	}
	return b.conn.View(fn)
}

// update runs fn inside a read-write transaction. Bootstrap or validation of
// the committed state runs inside the same transaction before fn, so every
// state change (including the initialization) shares one commit boundary.
func (b *BoltStore) update(fn func(*bolt.Tx) error) error {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if err := b.checkClosed(); err != nil {
		return err
	}
	return b.conn.Update(func(tx *bolt.Tx) error {
		if err := b.bootstrapCommittedState(tx); err != nil {
			return err
		}
		return fn(tx)
	})
}

// Close is used to gracefully close the DB connection. It is idempotent:
// repeated calls return ErrStoreClosed without touching the underlying
// database again, and in-flight transactions are allowed to finish first.
func (b *BoltStore) Close() error {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.closed {
		return errors.Join(ErrStoreClosed, bolt.ErrDatabaseNotOpen)
	}
	b.closed = true
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BoltStore) FirstIndex() (uint64, error) {
	var first uint64
	err := b.view(func(tx *bolt.Tx) error {
		if err := verifyCommittedState(tx); err != nil {
			return err
		}
		st, err := readTxState(tx)
		if err != nil {
			return err
		}
		first = st.firstIndex
		return nil
	})
	return first, err
}

// LastIndex returns the last known index from the Raft log.
func (b *BoltStore) LastIndex() (uint64, error) {
	var last uint64
	err := b.view(func(tx *bolt.Tx) error {
		if err := verifyCommittedState(tx); err != nil {
			return err
		}
		st, err := readTxState(tx)
		if err != nil {
			return err
		}
		last = st.lastIndex
		return nil
	})
	return last, err
}

// GetLog is used to retrieve a log from BoltDB at a given index.
func (b *BoltStore) GetLog(idx uint64, raftlog *raft.Log) error {
	return b.view(func(tx *bolt.Tx) error {
		if err := verifyCommittedState(tx); err != nil {
			return err
		}
		val := tx.Bucket(dbLogs).Get(uint64ToBytes(idx))
		if val == nil {
			return raft.ErrLogNotFound
		}
		return decodeMsgPack(val, raftlog)
	})
}

// StoreLog is used to store a single raft log
func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	now := time.Now()

	err := b.update(func(tx *bolt.Tx) error {
		batchSize := 0
		for _, log := range logs {
			key := uint64ToBytes(log.Index)
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}

			logLen := val.Len()
			bucket := tx.Bucket(dbLogs)
			if err := bucket.Put(key, val.Bytes()); err != nil {
				return err
			}
			batchSize += logLen
			metrics.AddSample([]string{"raft", "boltdb", "logSize"}, float32(logLen))
		}

		metrics.AddSample([]string{"raft", "boltdb", "logsPerBatch"}, float32(len(logs)))
		metrics.AddSample([]string{"raft", "boltdb", "logBatchSize"}, float32(batchSize))

		// Materialize the new log range in the same transaction so the range
		// marker and the log entries share a single commit boundary.
		return refreshCommittedState(tx)
	})

	// Both the deferral and the inline function are important for this metrics
	// accuracy. Deferral allows us to calculate the metric after the tx.Commit
	// has finished and thus account for all the processing of the operation.
	// The inlined function ensures that we do not calculate the time.Since(now)
	// at the time of deferral but rather when the go runtime executes the
	// deferred function.
	defer func() {
		metrics.AddSample([]string{"raft", "boltdb", "writeCapacity"}, (float32(1_000_000_000)/float32(time.Since(now).Nanoseconds()))*float32(len(logs)))
		metrics.MeasureSince([]string{"raft", "boltdb", "storeLogs"}, now)
	}()

	return err
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	return b.update(func(tx *bolt.Tx) error {
		curs := tx.Bucket(dbLogs).Cursor()
		for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
			// Handle out-of-range log index
			if bytesToUint64(k) > max {
				break
			}

			// Delete in-range log index
			if err := curs.Delete(); err != nil {
				return err
			}
		}

		// Commit the adjusted range together with the deletions.
		return refreshCommittedState(tx)
	})
}

// Set is used to set a key/value set outside of the raft log
func (b *BoltStore) Set(k, v []byte) error {
	return b.update(func(tx *bolt.Tx) error {
		return tx.Bucket(dbConf).Put(k, v)
	})
}

// Get is used to retrieve a value from the k/v store by key
func (b *BoltStore) Get(k []byte) ([]byte, error) {
	var val []byte
	err := b.view(func(tx *bolt.Tx) error {
		if err := verifyCommittedState(tx); err != nil {
			return err
		}
		v := tx.Bucket(dbConf).Get(k)
		if v == nil {
			return ErrKeyNotFound
		}
		val = append([]byte(nil), v...)
		return nil
	})
	return val, err
}

// SetUint64 is like Set, but handles uint64 values
func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (b *BoltStore) Sync() error {
	b.stateMu.RLock()
	defer b.stateMu.RUnlock()
	if err := b.checkClosed(); err != nil {
		return err
	}
	return b.conn.Sync()
}
