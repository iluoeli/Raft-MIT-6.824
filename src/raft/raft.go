package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Log entry
type  LogEntry struct {
	Command interface{}
	Term	int
	Index	int
}

// Definition of ServerState
type ServerState int8

const (
	LEADER		ServerState = 0
	FOLLOWER	ServerState = 1
	CANDIDATE	ServerState = 2
)

func (s ServerState) String() string {
	switch s {
		case LEADER:	return "Leader"
		case FOLLOWER:	return "Follower"
		case CANDIDATE:	return "Candidate"
		default:		return "Unknown"
	}
}

// Definition of time interval (ms): election, heartbeat
const (
	ElecTimeoutLB     int64 = 200
	ElecTimeoutUB     int64 = 400
	HeartbeatDuration time.Duration = 100 * time.Millisecond
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm	int
	votedFor	int
	log			[]LogEntry

	// Volatile state on all servers
	commitIndex	int
	lastApplied int

	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int

	// Server state
	serverState	ServerState
	votes		int	// the number of votes server got

	// channel & timer
	applyCh 			chan ApplyMsg
	voteCh				chan struct{}
	electionDuration	time.Duration
	electionTimer		*time.Timer
	heartbeatDuration	time.Duration
	heartbeatTimer		*time.Timer

	commitCh			chan struct{}
	stateChangeCh		chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.serverState == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// Get server state with string type: [me@term, serverState]
func (rf *Raft) toString() string {
	return fmt.Sprintf("%d\t%d-%s", rf.currentTerm, rf.me, rf.serverState.String())
}

// Switch to other state
func (rf *Raft) switchTo(s ServerState) {
	if s != rf.serverState || s != LEADER {
		DPrintf("%s: Switching to %s", rf.toString(), s.String())

		switch s {
		case FOLLOWER:
			rf.votedFor = -1
			rf.heartbeatTimer.Stop()
			rf.resetElectionTimer()
			rf.serverState = s
			//rf.stateChangeCh <- struct{}{}
		case CANDIDATE:
			rf.votedFor = rf.me
			rf.resetElectionTimer()
			rf.serverState = s
			//rf.stateChangeCh <- struct{}{}
		case LEADER:
			if rf.serverState == CANDIDATE {
				rf.serverState = s
				rf.electionTimer.Stop()
				rf.resetHeartbeatTimer()
				nLogs := len(rf.log)
				DPrintf("%s, init nextIndex to %d", rf.toString(), nLogs)
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nLogs
				}
				//rf.stateChangeCh <- struct{}{}
			} else {
				DPrintf("%s: illegal switching from %s to %s",
					rf.toString(), rf.serverState.String(), s.String())
			}
		}
	} else {
		DPrintf("%s: meaningless switch %s", rf.toString(), s.String())
	}
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term		int
	VoteGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Specifically, you should only restart your election timer if :
	// a) you get an AppendEntries RPC from the current leader (i.e.,
	// if the term in the AppendEntries arguments is outdated, you should not reset your timer);
	// b) you are starting an election;
	// or c) you grant a vote to another peer.

	if args.Term < rf.currentTerm {
		// reject vote from weaker
		DPrintf("%s [RV]: Reject vote from %d", rf.toString(), args.CandidateId)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		if args.Term > rf.currentTerm {
			// vote for stronger unconditionally. NO. election restriction
			//DPrintf("%s: Vote for %d unconditionally", rf.toString(), args.CandidateId)
			DPrintf("%s [RV]: Update term (%d -> %d)", rf.toString(), rf.currentTerm, args.Term)

			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.serverState = FOLLOWER
			rf.heartbeatTimer.Stop()
			rf.persist()
			// NOTE: do not reset election timer (case c)
		}

		// 1) If the logs have last entries with different terms, then
		// the log with the later term is more up-to-date. 2) If the logs
		// end with the same term, then whichever log is longer is
		// more up-to-date.
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term

		// restriction on leader election
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (
			args.LastLogTerm > lastLogTerm ||	// case 1)
			args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) { // case 2)

			DPrintf("%s [RV]: Vote for %d", rf.toString(), args.CandidateId)

			reply.Term = args.Term
			reply.VoteGranted = true
			// update self
			rf.switchTo(FOLLOWER)
			rf.votedFor = args.CandidateId
			rf.persist()
			//rf.resetElectionTimer()
			//rf.voteCh <- struct{}{}
		} else {
			DPrintf("%s [RV]: Reject vote from Candidate %d, VoteFor %d, lastLogTerm(%d, %d), lastLogIndex(%d, %d)",
				rf.toString(), args.CandidateId, rf.votedFor, args.LastLogTerm, lastLogTerm, args.LastLogIndex, lastLogIndex)

			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


type AppendEntryArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntryReply struct {
	Term			int
	Success			bool
	NextIndex		int		// optimization to reduce the number of rejected AppendEntries RPCs
}

func (args *AppendEntryArgs) toString() string {
	return fmt.Sprintf("Term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, nLoad=%d, LeaderComit=%d",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
}

func (rf *Raft) sendAppendEntries(server int , args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// follower-end replicates entries
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%s: AE [%s]", rf.toString(), args.toString())

	// Check term
	if args.Term < rf.currentTerm {
		// reject
		DPrintf("%s [AE]: Reject append from %d (stale leader)", rf.toString(), args.LeaderId)

		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = len(rf.log)
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}

	reply.Term = args.Term
	rf.switchTo(FOLLOWER)
	rf.persist()
	//rf.resetElectionTimer()

	// Check log index & term
	lastLogIndex := len(rf.log) - 1
	if args.PrevLogIndex > lastLogIndex {
		// Leader should decrement nextIndex
		DPrintf("%s [AE]: PrevLogIndex > lastLogIndex (%d > %d)", rf.toString(), args.PrevLogIndex, lastLogIndex)

		reply.Success = false
		reply.NextIndex = lastLogIndex + 1
	} else if args.PrevLogIndex <= lastLogIndex && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
		// Apply load
		reply.Success = true

		//newLogIdx := args.PrevLogIndex
		if args.Entries != nil {
			//for _, entry := range args.Entries {
			//	newLogIdx += 1
			//	if newLogIdx != entry.Index {
			//		DPrintf("%s [ERROR]: log index not equal (%d != %d)", newLogIdx, entry.Index)
			//	}
			//	if newLogIdx <= lastLogIndex {
			//		rf.log[newLogIdx] = entry
			//	} else {
			//		break
			//	}
			//}

			rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
			rf.persist()
		}

		reply.NextIndex = rf.log[len(rf.log) - 1].Index + 1

		// Check commit index
		lastLogIndex = len(rf.log) - 1
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > lastLogIndex {
				rf.commitIndex = lastLogIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}

		DPrintf("%s [AE]: Apply log loads (%d)", rf.toString(), len(rf.log))

		if rf.commitIndex > rf.lastApplied {
			rf.commitCh <- struct {}{}
		}

	} else if args.PrevLogIndex <= lastLogIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// Find the last matching index
		DPrintf("%s [AE]: Terms not equal: (%d != %d)", rf.toString(), args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)

		reply.Success = false
		reply.NextIndex = 1

		// Find the latest matching log
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term == args.Term {
				reply.NextIndex = i
				break
			}
		}
	} else { // This should NOT happen
		DPrintf("%s [AE]: ERROR, AE arges is [%s]", rf.toString(), args.toString())

		reply.Success = false
		reply.NextIndex = 1
	}
}

func (rf *Raft) resetElectionTimer()  {
	//if !rf.electionTimer.Stop() {
	//	<-rf.electionTimer.C
	//}
	rf.electionDuration = time.Duration(ElecTimeoutLB + rand.Int63n(ElecTimeoutUB - ElecTimeoutLB)) * time.Millisecond
	rf.electionTimer.Reset(rf.electionDuration)
	DPrintf("%s [RESET]: election timer", rf.toString())
}

func (rf *Raft) resetHeartbeatTimer()  {
	//if !rf.heartbeatTimer.Stop() {
	//	<-rf.heartbeatTimer.C
	//}
	rf.heartbeatTimer.Reset(rf.heartbeatDuration)
	DPrintf("%s [RESET]: heartbeat timer", rf.toString())
}

func (rf *Raft) electionHandler() {
	// vote for self
	//rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	nPeers := len(rf.peers)
	nLogs := len(rf.log)
	var args = RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		nLogs-1,
		rf.log[nLogs-1].Term}

	var votes int32 = 0
	// broadcast request vote msg
	for i := 0; i < nPeers; i ++ {
		if i == rf.me {	// Surely I vote for myself
			atomic.AddInt32(&votes, 1)
		} else {
			go func(voter int) {
				var reply RequestVoteReply
				if rf.serverState == CANDIDATE && rf.sendRequestVote(voter, args, &reply) {
					// Just drop old RPC replies
					rf.mu.Lock()
					if args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if reply.VoteGranted && reply.Term == rf.currentTerm { // Check term whenever receiving message
						// successful voting
						DPrintf("%s [POLL]: Successful voting from %d", rf.toString(), voter)

						atomic.AddInt32(&votes, 1)
						// check your votes already gained
						if int(votes) > nPeers / 2 && rf.serverState == CANDIDATE {
							rf.switchTo(LEADER)
							rf.persist()
							rf.broadcastEntryHandler()	// NOTE: send heartbeat right now once becoming a leader
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("%s [POLL]: My voting is out-of-date, update term (%d -> %d)",
							rf.toString(), rf.currentTerm, reply.Term)

						rf.currentTerm = reply.Term
						rf.switchTo(FOLLOWER)
						rf.persist()
					} else {
						DPrintf("%s [POLL]: %d did NOT vote for me", rf.toString(), voter)
					}
					rf.mu.Unlock()
				} else {
					DPrintf("%s [POLL]: Can NOT connect to %d (Term=%d)", rf.toString(), voter, args.Term)
				}
			}(i)
		}
	}
}

// Leader broadcasts the new entry or send heartbeat
func (rf *Raft) broadcastEntryHandler() {
	for i := range rf.peers {
		if i != rf.me {
			var entries []LogEntry = nil
			prevLogIndex := rf.nextIndex[i] - 1
			lastLogIndex := len(rf.log) - 1
			if lastLogIndex >  prevLogIndex {
				nLoad := lastLogIndex - prevLogIndex
				entries = make([]LogEntry, nLoad)
				copy(entries, rf.log[prevLogIndex+1:])
			}

			DPrintf("%s [HB]: to %d, nLoad=%d, prevLogIndex=%d, lastLogIndex=%d",
				rf.toString(), i, len(entries), prevLogIndex, lastLogIndex)

			var args = AppendEntryArgs{
				rf.currentTerm,
				rf.me,
				prevLogIndex,
				rf.log[prevLogIndex].Term,
				entries,
				rf.commitIndex}

			go func(follower int) {
				var reply 	AppendEntryReply

				if rf.serverState == LEADER && rf.sendAppendEntries(follower, args, &reply) {
					rf.mu.Lock()
					// Just drop old RPC replies
					if args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if reply.Success && rf.serverState == LEADER && reply.Term == rf.currentTerm {
						// Update nextIndex & matchIndex
						if args.Entries != nil {
							rf.nextIndex[follower] = args.Entries[len(args.Entries)-1].Index + 1
							rf.matchIndex[follower] = rf.nextIndex[follower] - 1
							DPrintf("%s [HB]: Update nextIndex[%d] (%d -> %d)",
								rf.toString(), follower, rf.nextIndex[follower]-1, rf.nextIndex[follower])
						}

						// Check commitment
						// A leader is not allowed to update commitIndex to somewhere in a previous term
						for i := len(rf.log)-1; i > rf.lastApplied; i-- {
							nCommitted := 0
							for _, matchIdx := range rf.matchIndex {
								if matchIdx >= i {
									nCommitted += 1
								}
							}
							if nCommitted > len(rf.peers) / 2 && rf.log[i].Term == rf.currentTerm { // Leader do not commit log of previous term
								DPrintf("%s [HB]: Update commitIndex (%d -> %d)", rf.toString(), rf.commitIndex, i)
								rf.commitIndex = rf.log[i].Index
								rf.commitCh <- struct{}{}
								break
							}
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("%s [HB]: Stale leader, update term (%d -> %d)",
							rf.toString(), rf.currentTerm, reply.Term)

						rf.currentTerm = reply.Term
						rf.switchTo(FOLLOWER)
						rf.persist()
					} else if args.Term == rf.currentTerm {
						// Failed because of inconsistent prevLog
						DPrintf("%s [HB]: Decrement nextIndex[%d] (%d -> %d)",
							rf.toString(), follower, rf.nextIndex[follower], reply.NextIndex)
						rf.nextIndex[follower] = reply.NextIndex
					}
					rf.mu.Unlock()
				} else if rf.serverState == LEADER {
					DPrintf("%s [HB]: Can NOT connect to %d (Term=%d)", rf.toString(), follower, args.Term)
				}
			}(i)
		}
	}
}

func (rf *Raft) committer() {
	DPrintf("%s: Init committer goroutine", rf.toString())

	for {
		select {
			case <-rf.commitCh:
				rf.mu.Lock()
				DPrintf("%s [CMT]: Committing index from %d to %d",
					rf.toString(), rf.lastApplied+1, rf.commitIndex)

				entries := rf.log[rf.lastApplied+1:rf.commitIndex+1]
				rf.mu.Unlock()

				go func(entries []LogEntry) {
					for _, entry := range entries {
						applyMsg := ApplyMsg{
							Index:       entry.Index,
							Command:     entry.Command,
							UseSnapshot: false,
							Snapshot:    nil,
						}
						rf.mu.Lock()
						rf.applyCh <- applyMsg
						if rf.lastApplied < entry.Index {
							rf.lastApplied = entry.Index
						}
						DPrintf("%s [CMT]: Committed command %d at %d", rf.toString(), entry.Command, entry.Index)
						rf.mu.Unlock()
					}
				}(entries)

		}
	}
}

// The main loop of raft server
func (rf *Raft) eventLoop()  {
	DPrintf("%s: Enter eventLoop", rf.toString())
	for {
		// TODO: atomic serverState
		// FIXME: busy waiting
		switch rf.serverState {
			case FOLLOWER:
				select {
					case <-rf.stateChangeCh:
						DPrintf("%s [EventLoop]: state change", rf.toString())
					case <-rf.voteCh:
						rf.resetElectionTimer()
					case <-rf.electionTimer.C:
						// switch to candidate
						DPrintf("%s [EventLoop]: Start voting because of election timeout", rf.toString())

						rf.mu.Lock()
						rf.currentTerm += 1
						rf.switchTo(CANDIDATE)
						rf.persist()
						rf.electionHandler()
						rf.mu.Unlock()
					default:
				}
			case CANDIDATE:
				select {
					case <-rf.stateChangeCh:
						DPrintf("%s [EventLoop]: state change", rf.toString())
					case <-rf.electionTimer.C: // election timeout
						// resetElectionTimer & restart election
						DPrintf("%s [EventLoop]: Restart voting because of election timeout", rf.toString())

						rf.mu.Lock()
						rf.currentTerm += 1
						rf.persist()
						rf.resetElectionTimer()
						rf.electionHandler()
						rf.mu.Unlock()
					default:
				}
			case LEADER:
				select {
					case <-rf.stateChangeCh:
						DPrintf("%s [EventLoop]: state change", rf.toString())
					case <-rf.heartbeatTimer.C:
						// send heartbeat periodically
						DPrintf("%s [EventLoop]: Broadcast heartbeat", rf.toString())

						rf.mu.Lock()
						rf.resetHeartbeatTimer()
						rf.broadcastEntryHandler()
						rf.mu.Unlock()
					default:
				}
			}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.serverState == LEADER

	if isLeader {
		index = len(rf.log)
		logEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("%s: new command %d in %d", rf.toString(), command, index)

		rf.resetHeartbeatTimer()
		rf.broadcastEntryHandler()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.votedFor = -1
	rf.serverState = FOLLOWER
	rf.log = make([]LogEntry, 1)	// start from index 1 with dummy entry
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// chanel & timer
	rf.applyCh = applyCh
	rf.commitCh = make(chan struct{}, 10)	// in case of dead-lock
	rf.stateChangeCh = make(chan struct{})

	rf.electionDuration = time.Duration(ElecTimeoutLB + rand.Int63n(ElecTimeoutUB - ElecTimeoutLB)) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionDuration)
	rf.heartbeatDuration = HeartbeatDuration
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatDuration)
	rf.heartbeatTimer.Stop()

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	DPrintf("%s: Created", rf.toString())

	go rf.eventLoop()
	go rf.committer()

	return rf
}
