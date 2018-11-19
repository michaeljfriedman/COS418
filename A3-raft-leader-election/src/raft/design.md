# COS 418 Assignment 3 - Raft Leader Election Redo Fall 2018

Decided to redo this assignment now that I'm a TA for the course, to refresh my memory when helping other students during office hours.

## Approach

- **Key idea**: Reference the state machine diagram from the Raft extended paper (Figure 4). Each Raft server is essentially a state machine with three states: Follower, Candidate, and Leader. The protocol manages what to do in each state and what triggers transitions between them.
- Each state boils down to *doing some action* (e.g. as leader, send a round of heartbeats), *then waiting for a signal* that indicates what state to go to next (or stay in the same state and repeat, e.g. after a follower receives a heartbeat, reset its timer and wait for the next heartbeat)
  - Ex:
    ```text
              timeout
    Follower --------> Candidate
    ```
    Follower is passive (no action), and then waits for one of 2 signals: timeout (in which case, transition to Candidate) or "reset" which is triggered by receiving AppendEntries (in which case, loop back and repeat).
- Each state can be implemented as a **handler function**, which does the state's normal action, and then waits to be triggered by a signal to transition to a different state
- Signals can be implemented as **channels**, and waiting for one of multiple can be **select statements**
  - Receiving a message will have to trigger a state handler externally, so we'll need a channel for this to communicate between the RPC handler and state handler
    - Ex: Upon receiving AppendEntries:
      - If Follower, signal "reset"
      - If Candidate, signal "new leader was chosen"
      - If Leader, signal "step down"
    Then some state handler will be running concurrently, which will receive the signal, and transition to the next state

## State Handlers

- Follower (Ref: Section 5.2, par. 1)
  - Take *new term* as optional arg
  - Do action:
    - Set state for Follower
    - If new term:
      - Update my term to new term
      - Reset who I voted for
    - Start timer for election
  - Wait for signals: [2]
    - Election timeout: Transition to Candidate
    - Reset: Reset election timer and repeat from beginning, possibly with a new term

- Candidate (Ref: Fig 2, Rules for Servers)
  - Do action:
    - Set state to Candidate, inc term, vote for self
    - Send RequestVote to all other servers
      - In bg, receive replies and tally up votes. When you get majority, Send Win signal, or if you get a reply with term > my term, send Step Down signal with current-term=my term, new-term=reply term [2] [3]
    - Start timer for election timeout
  - Wait for signals: [2]
    - Win: Transition to Leader
    - Step Down: Transition to Follower, possibly passing a new term
    - Election Timeout: Repeat from beginning

- Leader (Ref: Section 5.2, par. 3)
  - Do action:
    - Set state to Leader
    - Send a round of heartbeats
      - In bg, receive replies. If you get a reply with term > my term, send Step Down signal with current-term=my term, new-term=reply term [2]
    - Start timer for heartbeat timeout
  - Wait for signals: [2]
    - Step Down: Transition to Follower, possibly passing a new term (Ref: Fig 2, Rules for Servers, All Server)
    - Heartbeat Timeout: Repeat from beginning

## External triggers - RPC Handlers

- AppendEntries (Ref: Fig 2, AppendEntries RPC and Rules for Servers, All Servers) [1]
  - If AE term < my term, reject message and reply with my term
  - If Follower:
    - If AE term > my term, signal Reset with current-term=my term, new-term=AE term
    - Otherwise, signal Reset with current-term=my term, new-term=none
  - If Candidate:
    - If AE term > my term, signal Step Down with current-term=my term, new-term=AE term
    - Otherwise, signal Step Down with current-term=my term, new-term=none
  - If Leader:
    - If AE term > my term:
      - Signal Step Down (with current-term=my term, new-term=AE term)

- RequestVote (Ref: Fig 2, RequestVote RPC and Rules for Servers, All Servers) [1]
  - If RV term < my term, reject message and reply with my term
  - If I voted for no one or for this candidate already, grant vote
  - If RV term > my term:
    - If Candidate: Signal Step Down (with current-term=my term, new-term=RV term)
    - If Leader: Signal Step Down (with current-term=my term, new-term=RV term)

## Concurreny issues to watch out for

[1] Since AE/RV handler can depend on my term at any time, make sure to lock around usage of the term everywhere

[2] Must make sure signal channels for a state are "emptied" before the next time you return to a state, otherwise you could get signals from last time you were in this state – e.g. In Candidate, if you get a RV reply with term > my term, you'll send signal Step Down. But processing the reply is concurrent with processing votes/determining if you won, so if this happens after you already won, that signal will sit on the channel even after you left the Candidate state.

  - **Solution**: Send your term in the signal, and only process the signal if its term matches your term (for that "instance" of the state).
    - e.g. In Candidate, save your term in a local var – this will be the term for that instance of Candidate. When it's time to send a signal, the term it applies to is sent. Then when it's received, the Candidate handler checks that the term matches its local term before processing, otherwise it'll ignore it and keep waiting for another signal.

[3] If you get a delayed RV reply with term > my term after you've already won the election, how to send Step Down signal to your leader channel?

  - Step Down channel will be shared (global) between Candidate and Leader. (Because of [2], this shouldn’t violate correctness.)

## Logging

- Tags:
  - `lock`: Anywhere a lock is obtained or returned
  - `follower`, `candidate`, `leader`: Anywhere pertaining to that state (e.g. in the state handler, in branches for that state elsewhere)
  - `heartbeat`: Where heartbeats are sent by the leader
  - `newState`: Where a server enters a new state
  - `signal`: Where signals are sent/received

- In general, in every function, log every path of execution, at notable points

- State Handlers
  - Upon changing to each state (beg of state handler)
  - Upon sending or receiving a signal
  - Candidate
    - Before sending each RV
    - When a RV response is received
  - Leader
    - Before sending each heartbeat

- RPC Handlers
  - Upon sending a signal
  - Upon returning, indicating which execution path was taken
    - e.g. In RequestVote, log if you take the "vote granted" path, and also if a signal is sent (separately)
