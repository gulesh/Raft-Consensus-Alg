# Technical Report

## Message Queue

### Implementation
- Architecture and design decisions
  - Each Node stores topics and related messages in a dict with an associated Queue per topic
  - Each node stores information about peers loaded from `config.json` at startup
  - Each node writes to a log file (based on index) every commit
- Data structures used
  - Dictionary to store Topics
  - Queue per topic to store messages (provides natural FIFO ordering)
  - List to store addresses of all nodes
  - Dict to store index and port for peers
- Communication protocols
  - REST for communication between client and server via Flask

### Possible Shortcomings
- Under very high message throughput, a single in-memory queue per topic could become a bottleneck as all operations are serialized through Flask's request handling
- Could add more input validation for topic names and messages
- Error messages could be more descriptive for the client

---

## Election

### Implementation

##### Raft
- Each node starts as a Follower with a randomized election timer (0.3–0.6 seconds) on a dedicated thread
- If no heartbeat is received within the timeout, the node starts a new election:
  - Increments current term
  - Changes role to CANDIDATE
  - Votes for itself
  - Sends RequestVote RPC to all peers
- A vote is granted only if the candidate's term is at least as large as the receiver's, the receiver has not already voted this term, and the candidate's log is at least as up-to-date as the receiver's (checked via `last_log_index` and `last_log_term`)
- If the node receives votes from a majority it declares itself Leader and starts sending heartbeats every 20ms on a separate HeartbeatThread
- Heartbeats are sent at a faster interval (20ms) than the election timeout (300–600ms) so followers never time out under a live leader
- In total there are two background threads per node: ElectionTimer and HeartbeatThread (leader only)
- Message passing protocol
  - REST POST requests for both RequestVote and AppendEntries between nodes
- Conflict resolution
  - If a node sees a higher term in any response, it immediately steps down to Follower and updates its term
  - Split votes are resolved by randomized timeouts — the first node to time out wins the next round
- State management
  - `current_term`, `voted_for`, and `Node_Type` are updated per election event
  - `voted_for` is reset to None whenever a higher term is observed
  - State is persisted to disk on every term update or vote

### Possible Shortcomings
- Since election state is accessed from both the ElectionTimer thread and Flask request threads without comprehensive locking, there is a small theoretical window for a race condition under extreme concurrent load
- Timeout values are hardcoded and could be made configurable
- Could add more logging to make debugging easier

---

## Fault Tolerance

### Implementation

Each follower runs an election timer (0.3–0.6s randomized). If no heartbeat arrives in time, it assumes the leader failed and starts a new election. The system tolerates up to N/2-1 failures — with 5 nodes it survives 2 simultaneous failures and still maintains a majority of 3.

On every term update, vote, or commit, nodes persist state to `logs/log_N.json` containing `current_term`, `voted_for`, `log`, and `commit_index`. On restart, `load_from_file_entries` restores this state and replays all committed log entries in order to rebuild `self.data`, so a restarted node recovers its full queue state without any client intervention.

When a restarted or lagging follower rejoins, the leader tracks `next_index` per follower and sends all missing log entries via AppendEntries until the follower catches up to the current `commit_index`.

### Possible Shortcomings
- Recovery time scales linearly with log length since the entire log is replayed on restart — a snapshot mechanism would bound this cost for long-running clusters
- Could add more detailed status reporting on node recovery progress
- Log files are never cleaned up and will grow indefinitely over time

---

## Log Replication

### Implementation

All client writes (create topic, put message, get message) go through `replicate_command`. The leader appends the command as a log entry containing the term, index, and command, then sends it to all followers in parallel using threads. Once a majority ACKs the entry, `commit_index` advances and `apply_committed_entries` executes the command against `self.data`.

Each AppendEntries message includes `prev_log_index` and `prev_log_term` so followers can verify log consistency. Followers that are behind or have conflicting entries reject the message and the leader retries with earlier entries using per-follower `next_index` tracking. Followers apply committed entries when `leader_commit > commit_index` and persist their state after each commit.

Non-leader nodes reject all client write requests and return the current `leader_address` so clients can redirect their request to the correct node.

### Possible Shortcomings
- `replicate_command` blocks the Flask request thread while waiting for follower ACKs, meaning write latency is directly tied to the slowest responding majority node — an asynchronous pipeline would improve throughput
- Could add more detailed error responses for the client
- Heartbeat interval is hardcoded and could be made configurable

---

## Conclusion

The RRMQ implementation successfully demonstrates all three components: a working single-node message queue, Raft leader election with fault recovery, and log replication with consistency guarantees. All tests pass including edge cases such as follower crash and catch-up, leader change redirects, and split vote scenarios. The design prioritizes correctness and simplicity, with clear opportunities to extend the system with optimizations in future work.

---

## References

Ongaro and Ousterhout, *"In Search of an Understandable Consensus Algorithm"*, USENIX ATC 2014.