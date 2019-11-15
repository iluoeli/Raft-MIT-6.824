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
	ElecTimeoutLB     int64 = 150
	ElecTimeoutUB     int64 = 300
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
	// TODO: lab1
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// Get server state with string type: [me@term, serverState]
func (rf *Raft) sState() string {
	return fmt.Sprintf("%d\t%d-%s", rf.currentTerm, rf.me, rf.serverState.String())
}

// Switch to other state
func (rf *Raft) switchTo(s ServerState) {
	DPrintf("%s: Switching to %s", rf.sState(), s.String())

	if s != rf.serverState {
		switch s {
		case FOLLOWER:
			rf.votedFor = -1
			rf.heartbeatTimer.Stop()
			rf.resetElectionTimer()
		case CANDIDATE:
			rf.votedFor = -1
			rf.resetElectionTimer()
		case LEADER:
			// do something
			rf.electionTimer.Stop()
			rf.resetHeartbeatTimer()
			if rf.serverState != CANDIDATE {
				DPrintf("%s: illegal switching from %s to %s",
					rf.sState(), rf.serverState.String(), s.String())
			}
		}
		rf.serverState = s
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
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// reject vote from weaker
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		DPrintf("%s: Reject vote from %d", rf.sState(), args.CandidateId)
		return
	} else if args.Term > rf.currentTerm {
		// vote for stronger unconditionally
		DPrintf("%s: Vote for %d unconditionally", rf.sState(), args.CandidateId)

		reply.Term = args.Term
		reply.VoteGranted = true
		// and update self
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.switchTo(FOLLOWER)
		rf.resetElectionTimer()
		//rf.voteCh <- struct{}{}
	} else { // when terms are equal
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= len(rf.log) {
			DPrintf("%s: Vote for %d", rf.sState(), args.CandidateId)

			reply.Term = args.Term
			reply.VoteGranted = true
			// update self
			rf.votedFor = args.CandidateId
			rf.switchTo(FOLLOWER)
			//rf.voteCh <- struct{}{}
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
}

func (rf *Raft) sendAppendEntries(server int , args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// follower-end replicates entries
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// reject
		reply.Term = rf.currentTerm
		reply.Success = false

		DPrintf("%s: Reject append from %d (stale leader)", rf.sState(), args.LeaderId)
		return
	} else if args.Term >= rf.currentTerm {
		// apply
		DPrintf("%s: Append entries from %d", rf.sState(), args.LeaderId)

		reply.Term = args.Term
		reply.Success = true
		rf.currentTerm = args.Term
		rf.switchTo(FOLLOWER)
		rf.resetElectionTimer()
	}
}

func (rf *Raft) resetElectionTimer()  {
	//if !rf.electionTimer.Stop() {
	//	<-rf.electionTimer.C
	//}
	rf.electionDuration = time.Duration(ElecTimeoutLB + rand.Int63n(ElecTimeoutUB - ElecTimeoutLB)) * time.Millisecond
	rf.electionTimer.Reset(rf.electionDuration)
}

func (rf *Raft) resetHeartbeatTimer()  {
	//if !rf.heartbeatTimer.Stop() {
	//	<-rf.heartbeatTimer.C
	//}
	rf.heartbeatTimer.Reset(rf.heartbeatDuration)
}

func (rf *Raft) electionHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// vote for self
	// FIXME: change term here is dangerous
	//rf.currentTerm += 1
	rf.votedFor = rf.me
	nPeers := len(rf.peers)
	nLogs := len(rf.log)
	var args = RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		nLogs,
		rf.log[nLogs-1].Term}

	var votes int32 = 0
	// broadcast request vote msg
	for i := 0; i < nPeers; i ++ {
		if i == rf.me {	// Surely I vote for myself
			atomic.AddInt32(&votes, 1)
		} else {
			go func(voter int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(voter, args, &reply) {
					if reply.VoteGranted {
						// successful voting
						//DPrintf("%s: Successful voting from %d", rf.sState(), voter)

						atomic.AddInt32(&votes, 1)
						// check your votes already gained
						if int(votes) > nPeers / 2 {
							rf.mu.Lock()
							rf.switchTo(LEADER)
							rf.mu.Unlock()
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("%s: My voting is out-of-date, the latest term: %d", rf.sState(), reply.Term)

						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.switchTo(FOLLOWER)
						rf.mu.Unlock()
					} else {
						DPrintf("%s: My voting is rejected by %d", rf.sState(), voter)
					}
				}
			}(i)
		}
	}
}

// Leader broadcasts the new entry or send heartbeat
func (rf *Raft) broadcastEntryHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	for i := range rf.peers {
		if i != rf.me {
			go func(follower int) {
				var args = AppendEntryArgs{
					rf.currentTerm,
					rf.me,
					len(rf.log),
					rf.log[len(rf.log)-1].Term,
					nil,
					rf.commitIndex}
				var reply 	AppendEntryReply

				if rf.sendAppendEntries(follower, args, &reply) {
					if reply.Success {
					} else if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.switchTo(FOLLOWER)
						rf.mu.Unlock()

						//DPrintf("%s, Appending entries on %d succeed", rf.sState(), follower)
					}
				} else {
					DPrintf("%s, can't connect to %d", rf.sState(), follower)
				}
			}(i)
		}
	}
}

// The main loop of raft server
func (rf *Raft) eventLoop()  {
	DPrintf("%s: Enter eventLoop", rf.sState())
	for {
		// TODO: atomic serverState
		switch rf.serverState {
			case FOLLOWER:
				select {
					case <-rf.voteCh:
						rf.resetElectionTimer()
					case <-rf.electionTimer.C:
						// switch to candidate
						DPrintf("%s: Start voting because of election timeout", rf.sState())

						rf.mu.Lock()
						rf.currentTerm += 1
						rf.switchTo(CANDIDATE)
						rf.mu.Unlock()
						go rf.electionHandler()
					default:
						//DPrintf("%s", rf.sState())
				}
			case CANDIDATE:
				select {
					case <-rf.electionTimer.C: // election timeout
						// resetElectionTimer & restart election
						DPrintf("%s: Restart voting because of election timeout", rf.sState())

						rf.mu.Lock()
						rf.currentTerm += 1
						rf.mu.Unlock()
						rf.resetElectionTimer()
						go rf.electionHandler()
					default:
						// check votes
						//DPrintf("%s", rf.sState())
				}
			case LEADER:
				// send heartbeat periodically
				select {
					case <-rf.heartbeatTimer.C:
						DPrintf("%s: broadcast heartbeat", rf.sState())
						rf.resetHeartbeatTimer()
						go rf.broadcastEntryHandler()
					default:
						//DPrintf("%s", rf.sState())
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
	term := -1
	isLeader := rf.serverState == LEADER


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
	rf.log = make([]LogEntry, 1)	// start from index 0 with dummy entry
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// chanel & timer
	rf.applyCh = applyCh

	// fixed election timeout
	rf.electionDuration = time.Duration(ElecTimeoutLB + rand.Int63n(ElecTimeoutUB - ElecTimeoutLB)) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionDuration)
	rf.heartbeatDuration = HeartbeatDuration
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatDuration)
	rf.heartbeatTimer.Stop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("%s: Created", rf.sState())

	go rf.eventLoop()

	return rf
}
