// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

/*
	Log replication
	To implement log replication, you may want to start with

	- handling MsgAppend and MsgAppendResponse on both the sender and receiver sides.
	- Checkout raft.RaftLog in raft/log.go which is a helper struct that helps you manage the raft log,
	  in here you also need to interact with the upper application by the Storage interface defined in raft/storage.go
	to get the persisted data like log entries and snapshot.

	You can run make project2ab to test the implementation and see some hints at the end of this part.
*/

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	/*
		type Storage interface {
			// InitialState returns the saved HardState and ConfState information.
			InitialState() (pb.HardState, pb.ConfState, error)

			// Entries returns a slice of log entries in the range [lo,hi).
			// MaxSize limits the total size of the log entries returned, but
			// Entries returns at least one entry if any.
			Entries(lo, hi uint64) ([]pb.Entry, error)

			// Term returns the term of entry i, which must be in the range
			// [FirstIndex()-1, LastIndex()]. The term of the entry before
			// FirstIndex is retained for matching purposes even though the
			// rest of that entry may not be available.
			Term(i uint64) (uint64, error)

			// LastIndex returns the index of the last entry in the log.
			LastIndex() (uint64, error)

			// FirstIndex returns the index of the first log entry that is
			// possibly available via Entries (older entries have been incorporated
			// into the latest Snapshot; if storage only contains the dummy entry the
			// first log entry is not available).
			FirstIndex() (uint64, error)

			// Snapshot returns the most recent snapshot.
			// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
			// so raft state machine could know that Storage needs some time to prepare
			// snapshot and call Snapshot later.
			Snapshot() (pb.Snapshot, error)
		}
		type HardState struct {
			Term                 uint64   `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
			Vote                 uint64   `protobuf:"varint,2,opt,name=vote,proto3" json:"vote,omitempty"`
			Commit               uint64   `protobuf:"varint,3,opt,name=commit,proto3" json:"commit,omitempty"`
		}
		type Snapshot struct {
			Data                 []byte            `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
			Metadata             *SnapshotMetadata `protobuf:"bytes,2,opt,name=metadata" json:"metadata,omitempty"`
		}
		type SnapshotMetadata struct {
			ConfState            *ConfState `protobuf:"bytes,1,opt,name=conf_state,json=confState" json:"conf_state,omitempty"`
			Index                uint64     `protobuf:"varint,2,opt,name=index,proto3" json:"index,omitempty"`
			Term                 uint64     `protobuf:"varint,3,opt,name=term,proto3" json:"term,omitempty"`
		}
	*/


	// note stabled, committed and applied start counting from 0
	// they don't reflect actual index value in log
	// ref type MemoryStorage struct : ents[i] has raft log position i+snapshot.Metadata.Index

	hardstate, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	snapshot, err := storage.Snapshot()
	if err != nil {
		panic(err.Error())
	}

	entriesStartInd, err := storage.FirstIndex()
	if err != nil {
		panic(err.Error())
	}

	entriesLastInd, err := storage.LastIndex()
	if err != nil {
		panic(err.Error())
	}

	entries, err := storage.Entries(entriesStartInd, entriesLastInd+1)
	if err != nil {
		panic(err.Error())
	}

	return &RaftLog{
		storage: 			storage, 												// storage contains all stable entries since the last snapshot.
																					// refer to section "To restart a node from previous state:" in raft/doc.go
		stabled: 			entriesLastInd - entriesStartInd, 								// persisted to storage >> ?
		committed: 			hardstate.Commit - storage.snapshot.Metadata.Index - 1, 	// committed is the highest known log position
		applied: 			-1, 													// highest log position applied to the rest of the servers
		
		entries:			entries, 											// everything? from snapshot to MemoryStorage.ents? or just ents?
		pendingSnapshot: 	snapshot,												// (Used in 2C)
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	
	// create empty slice
	ret := make([]pb.Entry, 0, len(l.entries))

	// append to slice if data not empty
	for _, entry := range l.entries {
		if entry.data != nil {
			ret = append(ret, entry)
		}
	}
	return ret
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	ret := l.entries[stabled + 1 : ]
	return ret
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).

	ret := l.entries[applied + 1 : committed + 1]
	return ret
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	lastI := len(l.entries)
	return lastI + l.storage.snapshot.Metadata.Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	ind := i - l.storage.snapshot.Metadata.Index
	if ind < len(l.entries) {
		return l.entries[ind].Term, nil
	}
	// what if it's < l.storage.snapshot.Metadata.Index?? how to read from []byte?
	return 0, fmt.Errorf("invalid index")
}
