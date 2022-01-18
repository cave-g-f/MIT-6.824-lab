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
	"math"
	"math/rand"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// log entry struct
type LogEntry struct {
	Command interface{}
	Term    int
}

// state enum
type stateType int

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state        stateType
	currentTerm  int
	votedFor     int
	log          []LogEntry
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	electionTime uint32
	leaderID     int
	applyCond    sync.Cond
	lastNewEntry int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader
	currentTerm := rf.currentTerm
	return currentTerm, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term := args.Term
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm
	candidateID := args.CandidateID

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.leaderID = -1
	currLastLogTerm := rf.log[len(rf.log)-1].Term

	// not vote
	if term < rf.currentTerm || currLastLogTerm > lastLogTerm || (currLastLogTerm == lastLogTerm && len(rf.log)-1 > lastLogIndex) || (term == rf.currentTerm && rf.votedFor != -1) {
		reply.VoteGranted = false
		DPrintf("server: %d, disagree %d, currTerm: %d, peerTerm: %d\n", rf.me, candidateID, rf.currentTerm, term)
		// update term
		if term > rf.currentTerm {
			rf.currentTerm = term
		}
		reply.Term = rf.currentTerm
		return
	}

	// vote
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = candidateID
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	DPrintf("server: %d, vote for %d, time out: %d\n", rf.me, rf.votedFor, rf.electionTime)
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesPeerRPC(server int, isHeartBreak bool) {

	rf.mu.Lock()

	if rf.state != Leader {
		DPrintf("server: %d, not leader now!\n", rf.me)
		rf.mu.Unlock()
		return
	}

	// no need to send append entries
	if !isHeartBreak && len(rf.log)-1 < rf.nextIndex[server] {
		rf.mu.Unlock()
		return
	}

	// pack append entries rpc args
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.Entries = nil
	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[rf.nextIndex[server]-1].Term
	currLogLen := len(rf.log)

	// pack the log entries
	if !isHeartBreak {
		args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
		copy(args.Entries, rf.log[rf.nextIndex[server]:])
	}

	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		DPrintf("server: %d, send append entries to %d error, is heart break: %v\n", rf.me, server, isHeartBreak)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// append success, update matchIndex
	if reply.Success {
		if isHeartBreak {
			rf.matchIndex[server] = args.PrevLogIndex
		} else {
			rf.matchIndex[server] = currLogLen - 1
			rf.nextIndex[server] = currLogLen
			DPrintf("server: %d, server %d match index %d next index %d\n", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
		}
		return
	}

	// failed because of term
	if reply.Term > args.Term {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		DPrintf("server: %d, turn back to follower\n", rf.me)
		return
	}

	// failed because of consistency, resend
	rf.nextIndex[server] = args.PrevLogIndex - 1
	DPrintf("server: %d, server %d prevlogindex error\n", rf.me, server)
	go rf.sendAppendEntriesPeerRPC(server, false)
}

func (rf *Raft) sendAppendEntriesAllRPC(isHeartBreak bool) {

	rf.mu.Lock()

	// judge if the log can be committed
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Ints(matchIndexCopy)
	needCommitIndex := matchIndexCopy[(len(rf.matchIndex)+1)>>1]
	if rf.commitIndex < needCommitIndex && rf.log[needCommitIndex].Term == rf.currentTerm {
		rf.commitIndex = needCommitIndex
	}

	// start apply
	if rf.commitIndex > rf.lastApplied {
		rf.applyCond.Signal()
	}

	rf.mu.Unlock()

	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		if isHeartBreak {
			// send heart break
			go rf.sendAppendEntriesPeerRPC(index, true)
		} else {
			// send append entries
			go rf.sendAppendEntriesPeerRPC(index, false)
		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	term := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	entries := args.Entries
	leaderCommit := args.LeaderCommit
	leaderID := args.LeaderID

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTime = rand.Uint32()%10*100 + 500

	if entries == nil {
		DPrintf("server: %d, heart beat from %d, term: %d, currTerm: %d, prevIndex: %d, prevTerm: %d\n", rf.me, leaderID, term, rf.currentTerm, prevLogIndex, prevLogTerm)
	} else {
		DPrintf("server: %d, append entries from %d, prevIndex: %d, prevTerm: %d\n", rf.me, leaderID, prevLogIndex, prevLogTerm)
	}

	// term error
	if term < rf.currentTerm {
		DPrintf("server: %d, append term error\n", rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// log index error
	if len(rf.log)-1 < prevLogIndex || rf.log[prevLogIndex].Term != prevLogTerm {
		DPrintf("server: %d, append consistency error, prevLogIndex: %d, curr last index: %d\n", rf.me, prevLogIndex, len(rf.log)-1)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// new leader in new term, reset the lastNewEntry
	if leaderID != rf.leaderID || term != rf.currentTerm {
		rf.lastNewEntry = 0
	}

	// append entries conditions:
	// 1. not heart break
	// 2. not duplicate append
	if entries != nil && prevLogIndex+len(entries) > rf.lastNewEntry {
		DPrintf("server: %d, append entries success, start: %d, len: %d!\n", rf.me, prevLogIndex+1, len(entries))
		rf.log = append(rf.log[:prevLogIndex+1], entries[:]...)
		rf.lastNewEntry = len(rf.log) - 1
	}

	if leaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(leaderCommit), float64(rf.lastNewEntry)))
		DPrintf("server: %d start apply, leaderCommit: %d, lastNewEntry: %d\n", rf.me, leaderCommit, rf.lastNewEntry)
		rf.applyCond.Signal()
	}

	rf.state = Follower
	rf.currentTerm = term
	rf.leaderID = leaderID
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	logIndex := len(rf.log)
	isLeader := rf.state == Leader

	if !isLeader {
		return -1, term, false
	}

	rf.log = append(rf.log, LogEntry{command, term})

	DPrintf("leader %d, add entry index %d term %d command %v\n", rf.me, logIndex, rf.currentTerm, command)

	// check the latest leader
	go rf.sendAppendEntriesAllRPC(true)

	return logIndex, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendRequestVoteRPC() {
	rf.mu.Lock()
	DPrintf("server: %d, start election\n", rf.me)

	var votedCount uint32
	votedCount = 1
	requestArgs := &RequestVoteArgs{}

	rf.state = Candidate
	rf.currentTerm += 1
	rf.electionTime = rand.Uint32()%10*100 + 500
	rf.votedFor = rf.me
	rf.leaderID = -1
	requestArgs.Term = rf.currentTerm
	requestArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
	requestArgs.LastLogIndex = len(rf.log) - 1
	requestArgs.CandidateID = rf.me
	rf.mu.Unlock()

	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			replyArgs := &RequestVoteReply{}
			DPrintf("server: %d, request vote for %d\n", rf.me, server)
			ok := rf.sendRequestVote(server, requestArgs, replyArgs)
			if !ok {
				DPrintf("server: %d, receive vote from %d error\n", rf.me, server)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// vote failed
			if !replyArgs.VoteGranted {

				// failed because of term
				if rf.currentTerm < replyArgs.Term {
					rf.currentTerm = replyArgs.Term
					rf.state = Follower
					rf.votedFor = -1
				}

				return
			}

			// vote success
			atomic.AddUint32(&votedCount, 1)
			DPrintf("server: %d, vote count: %d, state: %d, term: %d\n", rf.me, votedCount, rf.state, rf.currentTerm)
			// become leader?
			if int(atomic.LoadUint32(&votedCount)) >= (len(rf.peers)/2 + 1) {
				if rf.state == Candidate {
					rf.state = Leader

					// initial
					for i, _ := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
					}
					for i, _ := range rf.matchIndex {
						rf.matchIndex[i] = 0
					}

					// send heart beat
					go rf.sendAppendEntriesAllRPC(true)
				}
			}
			DPrintf("server: %d, state: %d\n", rf.me, rf.state)

		}(index)
	}

}

func (rf *Raft) applyEntryToClient(applyCh chan ApplyMsg) {

	for {
		rf.applyCond.L.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		for ; rf.lastApplied < rf.commitIndex; rf.lastApplied++ {
			DPrintf("server: %d, apply entry index %d term %d command %v\n", rf.me, rf.lastApplied+1, rf.log[rf.lastApplied+1].Term, rf.log[rf.lastApplied+1].Command)
			applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.lastApplied+1].Command,
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}

		rf.applyCond.L.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			// send heart beat
			go rf.sendAppendEntriesAllRPC(true)
			// send append entries
			go rf.sendAppendEntriesAllRPC(false)
		} else {
			// periodically check if the election time elapsed
			if rf.electionTime <= 100 {
				rf.electionTime = 0
				go rf.sendRequestVoteRPC()
			} else {
				rf.electionTime -= 100
			}

		}
		rf.mu.Unlock()
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.electionTime = rand.Uint32()%10*100 + 500
	rf.state = Follower
	rf.leaderID = -1
	rf.applyCond.L = &rf.mu
	rf.lastNewEntry = 0

	DPrintf("server: %d, created\n", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start ticker check apply entries
	go rf.applyEntryToClient(applyCh)

	return rf
}
