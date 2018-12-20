# COS 418 Assignments 3 and 4 - Raft Leader Election and Consensus Redo Fall 2018

Decided to redo this assignment now that I'm a TA for the course, to refresh my memory when helping other students during office hours.

## Key Ideas

### Raft protocol as a state machine

- Reference the state machine diagram from the Raft extended paper (Figure 4). Each Raft server is essentially a state machine with three states: Follower, Candidate, and Leader. The protocol manages what to do in each state and what triggers transitions between them.
- Each state boils down to *doing some action* (e.g. as leader, send a round of heartbeats), *then waiting for a signal* that indicates what state to go to next (or stay in the same state and repeat, e.g. after a follower receives a heartbeat, reset its timer and wait for the next heartbeat)
  - Ex:
    ```text
              timeout
    Follower --------> Candidate
     ^    |
      \  /
       --
      Reset
    ```
    Follower is passive (no action), and then waits for one of 2 signals: timeout (in which case, transition to Candidate) or "reset" which is triggered by receiving AppendEntries (in which case, loop back and repeat).
- Each state can be implemented as a **handler function**, which does the state's normal action, and then waits to be triggered by a signal to transition to a different state
- Signals can be implemented as **channels**, and waiting for one of multiple signals can be implemented with **select statements**
  - Ex: Part of the protocol is to convert to Follower whenever receiving an RPC (request or reply) with a term > your term. So for instance, upon receiving RequestVote, we may have:
    - If RV term < my term, reject message and reply with my term
    - If RV term > my term, signal "convert to Follower" (i.e. over a channel)
    - ...

    One of the state handlers will be running concurrently, and it will receive the signal and transition to the Follower state.

    In this case, some extra care must be taken to ensure that we complete the conversion to Follower before proceeding in the RequestVote handler. We accomplish this by requiring the state handler to "ack" to the RequestVote handler when it has finished processing the signal, and upon receiving the ack, RequestVote can proceed.

### Leadership and consensus

We break down the Leader state here a bit more, since it is a bit more intricate than what's described above.

Generally, when reading the protocol in the paper, one can see two approaches for implementing log replication: an *eager* approach, and a *lazy* approach. Eager would immediately try to get consensus for a command upon receiving it from the client - sending out AppendEntries, waiting for a majority of replies, committing, and applying the command. On the other hand, the lazy approach would aggregate multiple commands from clients in its log, and only periodically check whether there are new entries to be replicated, committed, and applied.

We take the lazy approach here, as it turns out to be more minimal and simpler to implement correctly. So the Leader has the following routines:

- Start Command (Figure 2, Leader Rules, bullet 2): Upon receiving a command from the client, append it to my log
- Send Periodic AppendEntries (Figure 2, Leader Rules, bullet 3): Periodically check if new entries have been added but not replicated (my last log index >= nextIndex for each Follower), and if so, send AppendEntries with the new log entries. Otherwise send AppendEntries with no log entries. (This serves the purpose of "heartbeats".)
- Update Commit Index (Figure 2, Leader Rules, bullet 4): Periodically check if there are uncommitted entries (for some log index N > commit index, a majority of the matchIndex's are >= N and that entry's term is the same as my current term). If so, update the commit index to N.

And all servers have the following routine running at all times (regardless of their state) in the background:

- Apply Log Entries (Figure 2, All Server Rules, bullet 1): Periodically check if there are entries committed but not yet applied (commit index > the last applied index). If so, apply all entries between the last applied index and commit index, and update the last applied index.

So in this design, the log always has the property: last applied index <= commit index <= last log index, and each index is independently advanced.

When the Leader steps down (i.e. via an external "convert to Follower" signal), it stops its Leader-specific routines, but Apply Log Entries continues to run.

## State Handlers

- Follower
  - Take *new term* as arg
  - Do:
    - Set state to Follower
    - If new term > my term:
      - Update my term to new term
      - Reset who I voted for
    - Ack to caller if relevant
    - Start timer for election
  - Wait for signals:
    - Election Timeout: Transition to Candidate
    - Convert To Follower: Repeat from beginning, passing the new term

- Candidate
  - Do:
    - Set state to Candidate, inc term, vote for self
    - Send RequestVote to all other servers
      - In bg, receive replies and tally up votes. When you get majority, Send Win signal, or if you get a reply with term > my term, send Convert To Follower signal (new term = reply term)
    - Start timer for election timeout
  - Wait for signals:
    - Win: Transition to Leader
    - Convert To Follower: Transition to Follower, passing the new term
    - Election Timeout: Repeat from beginning

- Leader
  - Take *newly elected* (boolean indicator) as arg
  - Do:
    - Set state to Leader
    - If newly elected:
      - Initialize nextIndex to 1 + my last log index, for each other server
      - Initialize matchIndex to 0, for each other server
    - Send AppendEntries to each other server [1], containing entries from its nextIndex to my last log index, if there are any, or no entries otherwise
      - In bg, receive replies. If you get a reply with term > my term, send a Convert To Follower signal (new term = reply term). Otherwise, if you get a failure, set nextIndex to the next index in the reply. Otherwise, if you get success, set its nextIndex to 1 + the last log index sent in this AE, and set its matchIndex to the last log index sent in this AE.
    - Check to update the commit index - if for some log index N > commit index with log[N].term = my term, a majority of the matchIndex's are >= N, update the commit index to N.
    - Start timer for periodic timeout
  - Wait for signals:
    - Convert To Follower: Transition to Follower, passing the new term
    - Periodic Timeout: Repeat from beginning

[1] Note that since replies are received asynchronously, you must ensure externally that different rounds of AEs do not overlap. A simple way to do this is just to time out the RPCs with a shorter timeout than the periodic timeout.

## RPC Handlers

- AppendEntries
  - If AE term < my term, reject message and reply with my term
  - Send Convert To Follower signal (new term = AE term) and wait for ack
  - If I don't have log[prevLogIndex], reject and reply with my term and next index = 1 + my last log index
  - If my log[prevLogIndex].term doesn't match prevLogTerm, reject and reply with my term and next index = the first log index with term log[prevLogIndex].term
  - If one of my log entries conflicts with a new log entry (same index but different terms), delete it and all entries after it
  - Append all new entries *not* already in the log
  - If the leader's commit index > my commit index, set mine to min(leader's, index of my last log entry)

- RequestVote
  - If RV term < my term, reject message and reply with my term
  - If RV term > my term, send a Convert To Follower signal (new term = RV term) and wait for ack
  - If I voted for no one or for this candidate already, and my log is not more up to date than the candidate's:
    - Grant vote
    - Send a Convert To Follower signal and wait for ack

## Start Command

This routine is run by a leader whenever the client sends a command via the `Start()` function.

- Start Command
  - Append a log entry for this command

## Apply Log Entries

This background routine is started when the server starts, and runs indefinitely (regardless of state changes, etc.).

- Apply Log Entries
  - Repeat indefinitely
    - If commit index > last applied index:
      - Increment last applied index
      - Send log entry at last applied index to the applyCh
    - Wait for a fixed interval

## Persisting

According to the paper, we must "persist" the following state to recover from a crash: currentTerm, votedFor, and the log. So we persist everywhere we change one of these values, immediately after.

## Logging

- Where to log: in every function, every path of execution, at notable points
- What to log: a meaningful message indicating what has happened or is about to happen in this path, variable values relevant to that path, and relevant tags (see below)
- Tags:
  - One per function, to indicate what function we're in
  - Categories:
    - `consensus`: Anywhere pertaining to the consensus protocol (appending entries to the log, sending new entries out to followers, updating commit index, etc.)
    - `election`: Anywhere pertaining to the election process
    - `follower`, `candidate`, `leader`: Anywhere pertaining to that state (e.g. in the state handler, in branches for that state elsewhere)
    - `inactivity`: Anywhere the protocol is "inactive" (just sending/receiving empty heartbeats)
    - `lock`: Anywhere a lock is obtained or returned
    - `signal`: Where signals are sent/received
