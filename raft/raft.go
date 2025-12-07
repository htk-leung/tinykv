// // Copyright 2015 The etcd Authors
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

package raft

import (
	"errors"
	"bytes"
	"math"
	// "fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64 // << this must be the vote cast for this term, which means that every time term changes Vote must become 0

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records << in this term this other server voted for this candidate
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
// V
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _, p := range c.peers {
		prs[p] = &Progress{
			Match:	0,
			Next:	1,
		}
	}

	log := newLog(c.Storage)
	log.applied = c.Applied

	hardstate, _, _ := c.Storage.InitialState() // error is always nil

	// Your Code Here (2A).
	return &Raft{
		id:					c.ID,
		Term:				hardstate.Term,
		Vote:				hardstate.Vote,
		RaftLog:			log,
		Prs:				prs,
		State:				StateFollower,
		votes: 				make(map[uint64]bool),
		msgs:				make([]pb.Message, 0),	
		Lead:				0,
		heartbeatTimeout:	c.HeartbeatTick,
		electionTimeout:	c.ElectionTick,
		heartbeatElapsed:	0,
		electionElapsed :	0,
		leadTransferee:		0,
		PendingConfIndex:	0,
	}
}



// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	// If leader advance heartbeatElapsed
	if r.State == StateLeader {
		r.heartbeatElapsed++
		// if times up
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// send heartbeat by sending self
			r.heartbeatElapsed++
			if r.heartbeatElapsed >= r.heartbeatTimeout {
				r.heartbeatElapsed = 0  // Reset after timeout
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgBeat,
					From: r.id, 
					To: r.id, 
				})
			}
			return
		}
		return
	}
	// Advance electionElapsed in any other role
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
        r.electionElapsed = 0
        r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup, 
			To: r.id, 
			From: r.id, 
			Term: r.Term,
		})
		r.electionTimeout += int(float64(r.id) * (math.Pow(2, float64(r.id) - 1)))
		// how to reset?
		// if r.electionTimeout > 500 {
		// 	r.electionTimeout = r.electionTimeout % r.electionTimeoutInit + r.electionTimeoutInit
		// }
		// r.electionTimeout = len(r.Prs) + rand.Intn(len(r.Prs)*2)
    }
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	// Same as above but Entries is always empty
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// becomeFollower transform this peer's state to Follower
// V
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	r.State = StateFollower
	r.Term = term
	r.Lead = lead

	r.Vote = 0
	r.votes = make(map[uint64]bool) // when leader/candidates become followers a new term started >> don't casually call becomeFollower
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
// V
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	// transitions to candidate state
	r.State = StateCandidate
	// follower increments its current term
	r.Term++
	// votes for itself and
	r.Vote = r.id
	r.votes[r.id] = true
	// reset electionTimeout
	r.electionElapsed = 0

	// edge case : self is the only member! must count your own vote and become leader here
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
// V
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// fmt.Printf("In becomeLeader for r.id = %d\n", r.id)

	// update state
	r.State = StateLeader
	r.Vote = 0
	r.votes = make(map[uint64]bool)
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// propose noop entry = new empty entry in log
	lastEntryi := uint64(len(r.RaftLog.entries) - 1)
	lastIndex := r.RaftLog.entries[lastEntryi].Index

	entries := make([]*pb.Entry, 0)
	entries = append(entries, &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     lastIndex + 1,
	})
	r.AppendEntries(entries)

	// broadcast
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).


	switch m.MsgType {

	// MessageType_MsgHup >> start new election
	case pb.MessageType_MsgHup:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			// issued when there is an electionTimeout, server starts campaign
			r.campaign(m)
		case StateCandidate:
			r.campaign(m)
		case StateLeader:
		}
		

	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:
		switch r.State {
		case StateFollower:
		case StateCandidate:
		case StateLeader:
			for p := range r.Prs {
				if p != r.id {
					r.sendHeartbeat(p)
				}
			}
		}

	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			// normally recevied by leader
			// if a follower receives it maybe some server is mistaken
			// save to msgs to be forwarded to leader later
			m.From = r.id
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		case StateCandidate:
		case StateLeader:
			// calls appendEntry to append to entries
			// calls bcastAppend to call sendAppend
			r.handleAppendEntries(m)
		}

	// 'MessageType_MsgAppend' >> AppendEntries RPC
	case pb.MessageType_MsgAppend:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			// message sent with entries to append to log, call function to save entries
			r.handleAppendEntries(m)
		case StateCandidate:
			// received from leader, means someone is elected
			// become follower and append entries
			r.handleAppendEntries(m)
		case StateLeader:
			// should only be sent by leader, ignore
			// unless term is higher? then become follower and respond
			r.handleAppendEntries(m)
		}

	// 'MessageType_MsgAppendResponse' >> AppendEntries RPC
	case pb.MessageType_MsgAppendResponse:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			// MsgAppend is sent from leader to follower, and response from follower to leader
			// doc.go: 	When 'MessageType_MsgAppend' is passed to candidate or follower's Step method, it responds by
			// 			calling 'handleAppendEntries' method, which sends 'MessageType_MsgAppendResponse' to raft mailbox.
			r.handleAppendEntries(m)
		case StateCandidate:
			r.handleAppendEntries(m)
		case StateLeader:
			r.handleAppendEntriesResponse(m)
		}

	// 'MessageType_MsgRequestVote' >> RequestVoteRPC
	case pb.MessageType_MsgRequestVote:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			r.fHandleRequestVote(m)
		case StateCandidate:
			r.clHandleRequestVote(m)
		case StateLeader:
			r.clHandleRequestVote(m)
		}

	// 'MessageType_MsgRequestVoteResponse' >> RequestVoteRPC
	case pb.MessageType_MsgRequestVoteResponse:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			// if a follower somehow gets a response it means the candidate lost the election and has become a follower again
			// vote is obsolete, ignore
		case StateCandidate:
			// only a candidate needs to handle the response
			r.handleRequestVoteResponse(m)
		case StateLeader:
			// if a leader somehow gets a response it means the candidate has won and the response has no use
			// ignore
		}

	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			r.handleSnapshot(m)
		case StateCandidate:
			r.handleSnapshot(m)
		case StateLeader:
		}

	// 'MessageType_MsgHeartbeat' >> AppendEntries RPC
	case pb.MessageType_MsgHeartbeat:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			r.handleHeartbeat(m)
		case StateCandidate:
			r.handleHeartbeat(m)
		case StateLeader:
			r.handleHeartbeat(m)
		}

	// 'MessageType_MsgHeartbeatResponse' >> AppendEntries RPC
	case pb.MessageType_MsgHeartbeatResponse:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
			// normally received by leader, forward to leader
			r.handleHeartbeat(m)
		case StateCandidate:
		case StateLeader:
			r.handleHeartbeatResponse(m)
		}

	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
		case StateCandidate:
		case StateLeader:
		}

	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower:
		case StateCandidate:
		case StateLeader:
		}
	}

	return nil
}

// campaign method used to kickstart an election campaign for a follower
func (r *Raft) campaign(m pb.Message) {
	/* from raft/doc.go
	When a node is a follower or candidate and 'MessageType_MsgHup' is passed to its Step method,
	then the node calls 'campaign' method to campaign itself to become a leader. Once 'campaign'
	method is called, the node becomes candidate and sends 'MessageType_MsgRequestVote' to peers
	in cluster to request votes.
	*/
	/* from Raft paper
	Arguments:
	term			candidate’s term
	candidateId		candidate requesting vote
	lastLogIndex	index of candidate’s last log entry (§5.4)
	lastLogTerm		term of candidate’s last log entry (§5.4)
	*/
	// fmt.Printf("In Campaign(m) for r.id = %d\n", r.id)
	// node becomes candidate
	r.becomeCandidate()
	// sends 'MessageType_MsgRequestVote' to peers
	for p := range r.Prs {
		if p != r.id {			
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      p,
				From:    r.id,
				Term:    r.Term,
				LogTerm: r.RaftLog.entries[r.RaftLog.committed].Term, // term of candidate’s last log entry (§5.4)
				Index:   r.RaftLog.committed,                         // index of candidate’s last log entry (§5.4)
			})
		}
	}
}
func (r *Raft) clHandleRequestVote(m pb.Message) {
	/* from raft/doc.go
	When passed to the leader or candidate's Step method if:
	- message's Term is lower than leader's or candidate's,
	'MessageType_MsgRequestVote' will be rejected
	('MessageType_MsgRequestVoteResponse' is returned with Reject true).
	- receives 'MessageType_MsgRequestVote' with higher term, it will
	revert back to follower.
	*/
	/* from Raft paper
	Results:
	term			currentTerm, for candidate to update itself
	voteGranted		true means candidate received vote
	*/
	// fmt.Printf("In clHandleRequestVote(m) r.id = %d\n", r.id)
	// reject if term is smaller
	if m.Term < r.Term || (r.State != StateCandidate && r.State != StateLeader) {
		return
	}

	// become follower if term is higher, handle as follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.fHandleRequestVote(m)
		return
	}
	
	// if terms are the same msg is received from a fellow competitor
	// 		candidate : it's coming from its competition, but a candidate had voted for itself, so reject vote
	// 		leader : means this server won the election and the sender lost. But leader should have sent heartbeat. ignore
	if r.State == StateCandidate {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
	}
}
func (r *Raft) fHandleRequestVote(m pb.Message) {
	/* from raft/doc.go
	When 'MessageType_MsgRequestVote' is passed to follower, it
	- votes for the sender only when
	- sender's last term is greater than MessageType_MsgRequestVote's term or
	- sender's last term is equal to MessageType_MsgRequestVote's term but sender's last committed
	index is greater than or equal to follower's.
	*/
	/* from raft paper
	voter denies its vote if its own log is more up-to-date than that of the candidate.
	Raft determines which of two logs is more up-to-date
	by comparing the index and term of the last entries in the
	logs. If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer is
	more up-to-date.

	Receiver implementation:
	1. 	Reply false if term < currentTerm (§5.1)
	2. 	If votedFor is null or candidateId, and candidate’s log is at
		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	Also :
	- each server can only vote for 1 candidate per term
	*/
	// fmt.Printf("In fHandleRequestVote(m) r.id = %d\n", r.id)
	// ignore stale message
	if m.Term > r.Term {
		r.Term = m.Term
	}

	// if candidate log is *at least as* up to date as the local log
	var candUpToDate bool

	// if different last term, later term is more up to date
	localLogInd := r.RaftLog.committed
	if m.LogTerm != r.RaftLog.entries[localLogInd].Term {
		candUpToDate = m.LogTerm > r.RaftLog.entries[localLogInd].Term

	} else { // if same last term, larger entry index is more up to date
		candUpToDate = m.Index >= localLogInd
	}

	// if votedFor is null or candidateId then it can still vote
	var votedForValid bool

	// voter must not have voted yet
	// if it has then it should have voted for candidate
	votedForValid = (r.Vote == 0 || r.Vote == m.From)

	// return
	voteForHim := candUpToDate && votedForValid
	if voteForHim {
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  !(voteForHim),
	})
}
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	/*	from raft/doc.go
		Candidate
		- calculates how many votes it has won
		- if it's more than majority (quorum),
			- it becomes leader and
			- calls 'bcastAppend'.
		- if candidate receives majority of votes of denials, it
			- reverts back to follower.
	*/

	// fmt.Printf("In handleRequestVoteResponse(m) r.id = %d, m.From = %d\n", r.id, m.From)

	// save response to map
	r.votes[m.From] = !m.Reject // when becomes leader clear slice
	// fmt.Print("Response saved to votes\n")

	// count to see where we are now
	var votedFor, votedAgainst, quorum int

	quorum = len(r.Prs)/2 + 1
	for _, vote := range r.votes {
		if vote == true {
			votedFor++
		}
	}
	votedAgainst = len(r.votes) - votedFor
	// fmt.Printf("Counted votedFor = %d, votedAgainst = %d\n", votedFor, votedAgainst)

	if votedFor >= quorum {
		r.becomeLeader()
		// fmt.Print("became leader\n")
	} else if votedAgainst >= quorum {
		r.becomeFollower(r.Term, r.Lead) // update again when first heartbeat from leader received
		// fmt.Print("became follower\n")
	}
	// else do nothing
}
func (r *Raft) AppendEntries(entries []*pb.Entry) {
	// fmt.Printf("In AppendEntries for r.id = %d\n", r.id)

	// leader must append to entries
	for _, entry := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		// r.RaftLog.applied++
	}
	// persisting to memory is in the ready() part of raft, not here
	// // save to storage
	// r.RaftLog.storage.Append(entries)
	// // then update applied & stabled
	// r.RaftLog.stabled += len(entries)

}

// broadcast append
func (r *Raft) bcastAppend() {
	/*
		'MessageType_MsgPropose' proposes to append data to its log entries. This is a special
		type to redirect proposals to the leader. Therefore, send method overwrites
		eraftpb.Message's term with its HardState's term to avoid attaching its
		local term to 'MessageType_MsgPropose'. When 'MessageType_MsgPropose' is passed to the leader's 'Step'
		method, the leader first calls the 'appendEntry' method to append entries
		to its log, and then calls 'bcastAppend' method to send those entries to
		its peers. When passed to candidate, 'MessageType_MsgPropose' is dropped. When passed to
		follower, 'MessageType_MsgPropose' is stored in follower's mailbox(msgs) by the send
		method. It is stored with sender's ID and later forwarded to the leader by
		rafthttp package.

		'MessageType_MsgAppend' contains log entries to replicate. A leader calls bcastAppend,
		which calls sendAppend, which sends soon-to-be-replicated logs in 'MessageType_MsgAppend'
		type. When 'MessageType_MsgAppend' is passed to candidate's Step method, candidate reverts
		back to follower, because it indicates that there is a valid leader sending
		'MessageType_MsgAppend' messages. Candidate and follower respond to this message in
		'MessageType_MsgAppendResponse' type.

		A.
		- leader receives proposal to append entries to log
		- leader adds entries
		- leader calls bcastappend to send entries to peers

		'MessageType_MsgRequestVoteResponse' contains responses from voting request. When 'MessageType_MsgRequestVoteResponse' is
		passed to candidate, the candidate calculates how many votes it has won. If
		it's more than majority (quorum), it becomes leader and calls 'bcastAppend'.
		If candidate receives majority of votes of denials, it reverts back to
		follower.

		B.
		- candidate becomes leader
		- leader calls bcastappend to send entry to peers

		'MessageType_MsgSnapshot' requests to install a snapshot message. When a node has just
		become a leader or the leader receives 'MessageType_MsgPropose' message, it calls
		'bcastAppend' method, which then calls 'sendAppend' method to each
		follower. In 'sendAppend', if a leader fails to get term or entries,
		the leader requests snapshot by sending 'MessageType_MsgSnapshot' type message.

		>> always sent from leader to followers to ask followers to append entries
		>> assumes entries already in r.RaftLog.entries
	*/
	// fmt.Printf("In bcastappend for r.id = %d\n", r.id)
	// validity check
	if r.Lead != r.id {
		return
	}
	// send requests
	for p := range r.Prs {
		if p != r.id {
			r.sendAppend(p)
		}
	}
	// fmt.Printf("finished calling sendAppend for each peer\n")
}
// sendAppend is called by leader through bcastappend to send an append RPC with new entries (if any) and
// the current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	/* 	from doc.go
	If you need to send out a message, just push it to raft.Raft.msgs and
	all messages the raft received will be passed to raft.Raft.Step()
	*/
	// only called by leader
	// fmt.Printf("In sendAppend for r.id = %d\n", r.id)

	if to == r.Lead {
		return false
	}

	// fmt.Printf("committed = %d, match = %d\n", r.RaftLog.committed, r.Prs[to].Match)

	firstInd, _ := r.RaftLog.storage.FirstIndex()
	offset :=  firstInd - 1
	prevLogEntry := r.RaftLog.entries[r.Prs[to].Match-offset]

	if uint64(len(r.RaftLog.entries)) + offset > r.Prs[to].Match { // if there are things to send
		
		entriesptrs := make([]*pb.Entry, 0)
		for _, entry := range r.RaftLog.entries[r.Prs[to].Next : ] {
			entriesptrs = append(entriesptrs, &entry)
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgAppend,
			To:      	to,
			From:    	r.id,
			Term:    	r.Term,
			LogTerm: 	prevLogEntry.Term, 			// prevLogTerm - term of prevLogIndex entry
			Index:   	prevLogEntry.Index,			// prevLogIndex - index of log entry immediately preceding new ones
			Entries: 	entriesptrs,
			Commit:  	r.RaftLog.committed,
		})
	} else { // nothing to send
		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgAppend,
			To:      	to,
			From:    	r.id,
			Term:    	r.Term,
			LogTerm: 	prevLogEntry.Term,
			Index:   	prevLogEntry.Index,
			Commit:  	r.RaftLog.committed,
		})
	}

	return true
}

// handleAppendEntries handles AppendEntries RPC request
// called by follower & candidate
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	/*	from Raft paper
		Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
		input m Arguments:
			term			leader’s term
			leaderId		so follower can redirect clients
			prevLogIndex 	index of log entry immediately preceding new ones
			prevLogTerm		term of prevLogIndex entry
			entries[]		log entries to store (empty for heartbeat;
							may send more than one for efficiency)
			leaderCommit	leader’s commitIndex
		Results:
			term		currentTerm, for leader to update itself
			success		true if follower contained entry matching
						prevLogIndex and prevLogTerm
		Receiver implementation:
		1. Reply false if term < currentTerm (§5.1)
		2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		3. If an existing entry conflicts with a new one (same index
		   but different terms), delete the existing entry and all that
		   follow it (§5.3)
		4. Append any new entries not already in the log
		5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	*/
	// assume m.entries have increasing index
	// first entry should have smallest index

	// detect that it's forwarding a response by type
	if m.MsgType == pb.MessageType_MsgAppendResponse {
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
		return
	}

	// assume incoming messages of lower term handled in Step()
	// change candidate to follower if someone else has won the election
	if r.State == StateCandidate { 
		r.becomeFollower(m.Term, m.From)
	} else if m.Term > r.Term && (r.State == StateLeader || r.State == StateFollower) {
		r.becomeFollower(m.Term, m.From)
		// leader receiving AppendEntries RPC in same term is byzantine error
		// panic("Leader in the same term sending another leader AppendEntries RPC")
	}

	var rLastInd uint64
	rLastInd = r.RaftLog.entries[len(r.RaftLog.entries) - 1].Index
	rFirstInd, _ := r.RaftLog.storage.FirstIndex()
	offset := rFirstInd - 1

	// if there are entires in the rpc
	if m.Entries != nil {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		mInd := m.Index - offset
		if mInd > uint64(len(r.RaftLog.entries)) - 1 || r.RaftLog.entries[mInd].Term != m.LogTerm {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      	m.From,
				From:    	r.id,
				Term:    	r.Term,
				Index:		r.RaftLog.entries[len(r.RaftLog.entries)-1].Index,
				Reject:  true,
			})
			return
		}

		// append new entries to log. How to find from where to start:
		// if entry exists && everything matches, check next
		var matches bool
		var rEndInd, mStartInd, i uint64

		for _, mEntry := range m.Entries {
			// reset
			matches = false

			// if entry exists && everything matches
			if r.RaftLog.committed >= mEntry.Index {
				if r.RaftLog.entries[mEntry.Index].EntryType == mEntry.EntryType &&
					r.RaftLog.entries[mEntry.Index].Term == mEntry.Term &&
					r.RaftLog.entries[mEntry.Index].Index == mEntry.Index &&
					bytes.Equal(r.RaftLog.entries[mEntry.Index].Data, mEntry.Data) {
					matches = true // mark match
				}
			}
			// and check until record doesn't match
			// record i to start appending from here
			if matches == false {
				rEndInd = mEntry.Index
				mStartInd = i
				break
			}
			i++
		}
		// entry doesn't exist / doesn't match, remove entries from that entry onwards
		r.RaftLog.entries = r.RaftLog.entries[:rEndInd]

		// append new ones
		for _, mEntry := range m.Entries[mStartInd:] {
			r.RaftLog.entries = append(r.RaftLog.entries, *mEntry)
			rLastInd = mEntry.Index
		}
	}

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, rLastInd)
		// !! trigger apply if there are new entries to apply
	}

	// done
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.committed,
		Reject:  false,
	})
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// if successful
	// update nextindex and matchindex for follower
	// >> progress struct Prs map[uint64]*Progress
	if m.Reject == false {
		// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
		r.Prs[m.From].Next = m.Index + 1
		// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
		r.Prs[m.From].Match = m.Index
		// update committed
		var currMaxMatch uint64 = math.MaxUint64
		for _, progress := range r.Prs {
			currMaxMatch = min(currMaxMatch, progress.Match)
		}
		if r.RaftLog.committed < currMaxMatch {
			r.RaftLog.committed = currMaxMatch
			// !! apply entries! how? when?
		} else if r.RaftLog.committed > currMaxMatch {
			// panic("Leader committed entries before getting enough AppendEntries RPC responses to verify majority verification")
		}
	} else {
		// if rejected decrement nextIndex and retry
		// this means that the reply needs to carry the same entries as the request? Only if rejected? No because leader has raw info.
		// details to be carried out in sendAppend
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
		r.sendAppend(m.From)
	}
}

// handle request to send heartbeat
func (r *Raft) handleBeat(m pb.Message) {
	// if follower, forward to leader
	// if candidate -- nothing could be done because election is under way and there is no leader?
	// if leader -- send heartbeat
	m.To = r.Lead
	r.msgs = append(r.msgs, m)
}

// handleHeartbeat handle Heartbeat RPC request
// function called by both candidate and follower
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	/* 	raft/doc.go
	'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'. When 'MessageType_MsgHeartbeatResponse'
	is passed to the leader's Step method, the leader knows which follower responded.
	*/

	// if message is stale ignore
	if m.Term < r.Term {
		return
	}
	// if we are forwarding response message
	if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	}
	// message should only be heartbeat from here
	if m.MsgType != pb.MessageType_MsgHeartbeat {
		return
	}

	// becomeFollower updates Term and Lead
	// there cannot be >1 leader per term, so only update when m.Term > r.Term
	// but candidate should become follower if heartbeat received >> but still, only when m.Term > r.Term
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	// reset heartbeat timeout
	r.heartbeatElapsed = 0

	// return
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
	})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {


}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
