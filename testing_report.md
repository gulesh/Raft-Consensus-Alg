# Raft Message Queue – Testing Report

## Overview

This report describes how we tested the three main components of the Raft Message Queue system — Message Queue API, Leader Election, and Replication — and discusses limitations of our testing approach.

---

## 1. Message Queue

### 1.1 How we tested

We tested the queue semantics and REST routes at the single‑node level, independently of Raft, using unit tests that exercise both happy paths and edge cases.

**Topic routes**

- Add topic to empty store: verify new topic appears in the in‑memory dict and the API returns `success: True`.
- Get topic when there are no topics: ensure the API returns `success: False`/404 and does not crash.
- Add multiple topics: create several topics and check that all keys exist in the dict after the operations.
- Retrieve topics again: confirm idempotence and that previously created topics are still present.
- Add duplicate topic: attempt to create an existing topic and assert `success: False` and no duplicate entry.

**Message routes**

- Get message when store is empty: with zero topics, `GET` on any topic returns `success: False` and an appropriate error.
- Get message from empty topic: topic exists but queue is empty; verify graceful failure and no data corruption.
- Add message to non‑existent topic: ensure request fails with `success: False` and does not silently create a topic.
- Add message to existing topic: check the message is appended to the topic queue and the response includes the message id/payload.
- Get message from non‑empty queue: verify FIFO behavior for basic enqueue/dequeue within a single node.

**Status routes**

- Node status: call the status endpoint and verify it returns health, current role, and term (or minimal heartbeat) without blocking.

### 1.2 Limitations

- No performance stress testing (latency, throughput, large queue sizes) at the single‑node API level.
- Limited validation of malformed JSON, very large payloads, or adversarial inputs.
- No long‑running tests for memory leaks or state drift over hours of operation.

---

## 2. Election

### 2.1 How we tested

We tested leader election both directly (with dedicated election tests) and indirectly (through replication/end‑to‑end tests that force leader changes).

**Basic leader election**

- `test_correct_status_message`, `test_leader_in_single_node_swarm`: for a 1‑node cluster, we verify that `/status` returns a JSON with `role` and `term`, and that the single node is always in the `LEADER` role, even after a restart.
- `test_is_leader_elected`: in a 5‑node swarm, we use `swarm.get_leader_loop` to confirm that a leader is eventually elected within a bounded time.
- `test_is_leader_elected_unique` and `test_no_two_leaders_same_term_over_time`: we repeatedly collect statuses and bucket leaders by term, then assert that at most one leader exists per term.

**Leader stability and uniqueness**

- `test_leader_eventually_stable`: we poll the leader several times and assert that the same node remains leader across multiple checks, showing that leadership does not flap unnecessarily.
- `test_follower_does_not_become_leader_with_active_leader`: while a stable leader is active, we ensure no follower transitions to leader, so there is exactly one leader in the cluster at all times.

**Leader crash and re‑election**

- `test_leader_crash_and_re_election`: we wait for a leader, record its term, then fully clean (stop) that node and confirm that a new node becomes leader with a strictly higher term.
- `test_is_newleader_elected`: we stop the current leader with `clean`, then verify that another node is eventually elected as the new leader.

**Timeouts, candidates, and quorum behavior**

- `test_candidate_timeout_and_new_election`: we pause a majority of followers so the leader cannot make progress, wait for election timeouts, then resume them; we then check that a new leader is elected with a higher term, exercising candidate timeouts and re‑election logic.
- `test_flapping_candidate_does_not_steal_leadership`: we choose a follower, pause it so it may become a “flapping candidate,” and confirm that this node does not cause multiple leaders to appear; after resuming, we still see at most one leader and non‑decreasing term.
- `test_no_leader_without_quorum`: we pause a majority of nodes and verify that the remaining nodes do not elect a leader (or at most one), demonstrating that leadership is only possible with a quorum.

**Failure modes: pause, terminate, kill**

- `test_single_node_persists_leader_across_restarts`: for a single node, we restart it and ensure it remains leader, showing persistence of leadership role across restarts.
- `test_new_leader_after_pause` (marked xfail): we pause the leader (simulating a SIGSTOP), then look for a new leader with a higher term; this documents a known liveness edge case under OS‑level pauses.
- `test_new_leader_after_terminate` and `test_new_leader_after_kill`: we terminate/kill the current leader process and then confirm that another node becomes leader with a strictly higher term, covering more realistic crash scenarios.

In addition, our replication and end‑to‑end tests implicitly exercise election by crashing leaders during writes and ensuring a new leader takes over and continues serving requests correctly.

### 2.2 Limitations

- We do not test explicit split‑brain or asymmetric network partitions; failures are modeled as pauses, termination, or clean crashes, not arbitrary message loss or one‑way links.
- Election timeouts and randomized backoff are validated only functionally (a leader eventually appears); we do not measure or assert distribution of election times or sensitivity to parameter choices.
- We test clusters of size 1 and 5; we do not explore behavior at larger scales or under very frequent membership changes.
- The xfailed `test_new_leader_after_pause` captures a known liveness issue under `SIGSTOP`‑style pauses, which remains as future work rather than being fully fixed and tested.

--- 
## 3. Replication

Tests are implemented in `replication_test.py`, `replication_edge_case_test.py`, `general_edge_case_tests.py`, and `submission_test.py`.

### 3.1 How we tested

We focused on safety (no data loss, no duplication, correct FIFO semantics) and basic liveness under node crashes.

**Redirects and write routing (`replication_test.py`)**

- Followers reject writes and return a redirect.
- Clients follow redirects to the current leader and successfully perform writes.
- Followers update redirects correctly after a leader change.

**Replication semantics (`replication_test.py`)**

- Topics replicated: create a topic on the leader, crash the leader, and verify the new leader still has the topic.
- Messages replicated: enqueue a message, crash the leader, and confirm the message is readable from the new leader.
- Commit index agreement: after a sequence of writes, all nodes reach the same `commit_index`.
- Follower crash and catch‑up: crash a follower, perform writes, then bring the follower back and assert it replays missing log entries in order.
- Single‑consumption: once a message is consumed, subsequent reads do not return it again.
- FIFO order: enqueue multiple messages and verify they are consumed in insertion order, both in normal operation and after leader crash and re‑election.

**Replication edge cases (`replication_edge_case_test.py`)**

- Non‑existent topics: put/get on missing topics return `success: False` without modifying replicated state.
- Duplicate topics: second creation attempt fails and does not replicate a duplicate.
- Empty queues: once all messages are consumed, further gets fail instead of returning stale data.
- Multiple topics: messages in different topics remain independent; all topics survive a leader crash.
- Larger workloads: enqueue 20 messages and ensure they are all committed and retrieved in order.
- Crash sequences: simulate one and two consecutive leader crashes and verify that committed data is never lost.

**General edge cases (`general_edge_case_tests.py`)**

- Write then immediate leader crash: writes replicated to a majority must appear on the new leader even if the original leader crashes immediately afterward.
- Two followers crash: cluster remains writable with 3/5 nodes.
- Rapid‑fire requests: send 50 rapid messages and ensure they are stored and read back in order.
- Concurrent read/write: overlapping reads and writes do not corrupt state or break invariants.

**End‑to‑end integration (`submission_test.py`)**

- End‑to‑end basic flow: elect leader, create topics, enqueue messages, crash leader, and verify the new leader serves all messages in order.
- End‑to‑end redirect and replication: start from a follower, follow redirects to the leader, perform writes, then verify that all nodes eventually hold the committed data.

### 3.2 Limitations

- No measurements of throughput, tail latency, or log compaction under sustained load.
- Crash model assumes clean node stops/starts and correct durable storage; no disk corruption or partial writes.
- No explicit modeling of packet loss, reordering, or long partitions.
- Workloads are small (tens of messages, single client) compared to real‑world deployments.

---

## Conclusion

Our tests give good confidence in basic queue semantics, correct leader election, and Raft‑based replication under 
single‑node and simple multi‑node failure scenarios, but they do not fully cover performance, complex network partitions,
or adversarial workloads. Future work includes adding fault‑injection for network partitions, long‑running stress tests, 
and fuzzing of Raft RPCs to further validate correctness and robustness.

---

## References

- Raft paper and official documentation.
- Course handouts and lab descriptions for Raft testing patterns.
- General software testing report guidelines.
