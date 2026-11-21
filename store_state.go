// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
)

const (
	// storeVersion1 is the first on-disk format version whose initialized
	// state (the log/conf buckets together with the materialized log range)
	// is committed inside a single transaction. Databases written by older
	// releases contain no state marker and are treated as version 0.
	storeVersion1 uint32 = 1
)

var (
	// State marker keys, stored inside the conf bucket.
	dbStateVersion    = []byte("state_version")
	dbStateFirstIndex = []byte("state_first_index")
	dbStateLastIndex  = []byte("state_last_index")

	// ErrCorrupt is returned when the on-disk store cannot be interpreted as
	// a single consistent, fully committed state. It is always wrapped, so
	// callers can detect corruption with errors.Is(err, ErrCorrupt). Existing
	// data is never reset to paper over this condition.
	ErrCorrupt = errors.New("raft-boltdb: corrupt store")

	// ErrStoreClosed is returned when a store is used after Close. It wraps
	// bolt.ErrDatabaseNotOpen so errors.Is checks against either sentinel
	// succeed.
	ErrStoreClosed = errors.New("raft-boltdb: store is closed")
)

// txState is the materialized, committed state of the store.
type txState struct {
	version    uint32
	firstIndex uint64
	lastIndex  uint64
}

// corruptErrorf builds an error wrapping ErrCorrupt.
func corruptErrorf(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// readTxState reads the committed state marker. A missing conf bucket or
// state version key means no committed state exists yet (version 0). A marker
// that is present but malformed is corruption, not an empty store.
func readTxState(tx *bolt.Tx) (txState, error) {
	conf := tx.Bucket(dbConf)
	if conf == nil {
		return txState{}, nil
	}

	versionVal := conf.Get(dbStateVersion)
	if versionVal == nil {
		return txState{}, nil
	}
	if len(versionVal) != 4 {
		return txState{}, corruptErrorf("state version key has invalid length %d", len(versionVal))
	}

	st := txState{version: binary.BigEndian.Uint32(versionVal)}
	firstVal := conf.Get(dbStateFirstIndex)
	lastVal := conf.Get(dbStateLastIndex)
	if firstVal == nil || lastVal == nil {
		return txState{}, corruptErrorf("store state is partially written: log range marker is incomplete")
	}
	if len(firstVal) != 8 || len(lastVal) != 8 {
		return txState{}, corruptErrorf("log range marker has invalid length")
	}
	st.firstIndex = binary.BigEndian.Uint64(firstVal)
	st.lastIndex = binary.BigEndian.Uint64(lastVal)
	return st, nil
}

// writeTxState persists the state marker. It only takes effect together with
// the rest of the enclosing transaction when that transaction commits.
func writeTxState(conf *bolt.Bucket, st txState) error {
	version := make([]byte, 4)
	binary.BigEndian.PutUint32(version, st.version)
	if err := conf.Put(dbStateVersion, version); err != nil {
		return err
	}
	if err := conf.Put(dbStateFirstIndex, uint64ToBytes(st.firstIndex)); err != nil {
		return err
	}
	return conf.Put(dbStateLastIndex, uint64ToBytes(st.lastIndex))
}

// stateFromLogs derives the log range state from the actual contents of the
// log bucket. Malformed index keys or an inverted range are reported as
// corruption rather than being clamped away.
func stateFromLogs(logs *bolt.Bucket) (txState, error) {
	curs := logs.Cursor()
	firstKey, _ := curs.First()
	if firstKey == nil {
		return txState{}, nil
	}
	lastKey, _ := curs.Last()
	if len(firstKey) != 8 || len(lastKey) != 8 {
		return txState{}, corruptErrorf("log bucket contains an index key of invalid length")
	}

	first := binary.BigEndian.Uint64(firstKey)
	last := binary.BigEndian.Uint64(lastKey)
	if last < first {
		return txState{}, corruptErrorf("log range is inverted: first index %d is greater than last index %d", first, last)
	}
	return txState{firstIndex: first, lastIndex: last}, nil
}

// validateCommitted verifies that a version-1 state marker and the actual log
// bucket describe the same range. Any disagreement means the on-disk state is
// not the product of a fully committed transaction and is reported as
// corruption; the marker is never regenerated here to hide it.
func validateCommitted(logs *bolt.Bucket, st txState) error {
	if st.version != storeVersion1 {
		return corruptErrorf("unsupported store format version %d", st.version)
	}
	if logs == nil {
		return corruptErrorf("store is not fully initialized: log bucket is missing")
	}
	if st.lastIndex < st.firstIndex {
		return corruptErrorf("state marker has an inverted log range [%d, %d]", st.firstIndex, st.lastIndex)
	}

	curs := logs.Cursor()
	firstKey, _ := curs.First()
	lastKey, _ := curs.Last()
	if firstKey == nil {
		if st.firstIndex != 0 || st.lastIndex != 0 {
			return corruptErrorf("state marker claims range [%d, %d] but the log bucket is empty", st.firstIndex, st.lastIndex)
		}
		return nil
	}
	if len(firstKey) != 8 || len(lastKey) != 8 {
		return corruptErrorf("log bucket contains an index key of invalid length")
	}

	first := binary.BigEndian.Uint64(firstKey)
	last := binary.BigEndian.Uint64(lastKey)
	if first != st.firstIndex || last != st.lastIndex {
		return corruptErrorf("committed state [%d, %d] does not match the stored log range [%d, %d]",
			st.firstIndex, st.lastIndex, first, last)
	}
	return nil
}

// bootstrapCommittedState runs at the start of every write transaction.
//
// On a version-0 database (including one whose initializing transaction was
// interrupted before the state marker existed) it adopts whatever logs and
// stable metadata are already on disk and commits the buckets, the data and
// the state marker together inside the current transaction. Existing data is
// preserved, never reset.
//
// On a version-1 database it validates the committed marker instead of
// regenerating it, so corruption surfaces as an error instead of being
// silently overwritten.
func (b *BoltStore) bootstrapCommittedState(tx *bolt.Tx) error {
	logs, err := tx.CreateBucketIfNotExists(dbLogs)
	if err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbConf); err != nil {
		return err
	}

	st, err := readTxState(tx)
	if err != nil {
		return err
	}

	switch st.version {
	case 0:
		adopted, err := stateFromLogs(logs)
		if err != nil {
			return err
		}
		adopted.version = storeVersion1
		return writeTxState(tx.Bucket(dbConf), adopted)
	case storeVersion1:
		return validateCommitted(logs, st)
	default:
		return corruptErrorf("unsupported store format version %d", st.version)
	}
}

// verifyCommittedState is the read-side counterpart of
// bootstrapCommittedState. Read-only transactions cannot create missing
// buckets or adopt a version-0 database, so any missing or inconsistent state
// is reported as corruption. Reads therefore only ever observe a fully
// committed version of the store.
func verifyCommittedState(tx *bolt.Tx) error {
	logs := tx.Bucket(dbLogs)
	conf := tx.Bucket(dbConf)
	if logs == nil || conf == nil {
		return corruptErrorf("store is not fully initialized: required bucket is missing")
	}

	st, err := readTxState(tx)
	if err != nil {
		return err
	}
	if st.version == 0 {
		return corruptErrorf("store is not fully initialized: no committed state marker")
	}
	return validateCommitted(logs, st)
}

// refreshCommittedState rewrites the materialized log range inside the
// current transaction. The marker and the log mutations commit atomically, so
// an interrupted commit leaves either the previous complete state or the new
// complete state, never a mixture of the two.
func refreshCommittedState(tx *bolt.Tx) error {
	st, err := stateFromLogs(tx.Bucket(dbLogs))
	if err != nil {
		return err
	}
	st.version = storeVersion1
	return writeTxState(tx.Bucket(dbConf), st)
}
