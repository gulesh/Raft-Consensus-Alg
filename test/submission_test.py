# test_integration.py
from test_utils import Swarm, LEADER, FOLLOWER
import pytest
import time
import requests
import os
import glob

PROGRAM_FILE_PATH = "src/node.py"
ELECTION_TIMEOUT = 2.0
NUM_NODES_ARRAY = [5]
NUMBER_OF_LOOP_FOR_SEARCHING_LEADER = 3
LOG_DIR = "logs"
TOPIC = "/topic"
MESSAGE = "/message"

def clean_logs():
    if not os.path.isdir(LOG_DIR):
        return
    for path in glob.glob(os.path.join(LOG_DIR, "log_*.json")):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

@pytest.fixture
def swarm(num_nodes):
    clean_logs()
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()

@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_end_to_end_basic_flow(swarm: Swarm, num_nodes: int):
    """
    End-to-end: elect leader, create topic, write+read messages,
    crash leader, elect new leader, verify data preserved.
    """
    topic = "e2e_topic"
    msgs  = ["m1", "m2", "m3"]

    # 1) initial leader
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None
    status1 = leader1.get_status().json()
    assert status1["role"] == LEADER

    # 2) create topic + write messages
    assert leader1.create_topic(topic).json()["success"] is True
    for m in msgs:
        assert leader1.put_message(topic, m).json()["success"] is True

    # 3) allow replication
    time.sleep(ELECTION_TIMEOUT * 2)

    # 4) crash leader
    leader1.commit_clean(ELECTION_TIMEOUT)

    # 5) new leader
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None
    assert leader2 is not leader1
    status2 = leader2.get_status().json()
    assert status2["role"] == LEADER
    assert status2["term"] > status1["term"]

    # 6) read back all messages via new leader
    seen = []
    while True:
        resp = leader2.get_message(topic)
        assert resp.ok
        data = resp.json()
        if not data["success"]:
            break
        seen.append(data["message"])

    assert seen == msgs

@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_end_to_end_redirect_and_replication(swarm: Swarm, num_nodes: int):
    """
    End-to-end: client hits follower, follows redirect to leader,
    writes topic+messages, verifies replication across nodes.
    """
    topic = "e2e_redirect_topic"
    msgs  = ["r1", "r2"]

    # 1) find leader and one follower
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    statuses = swarm.get_status()
    follower_idx = next(i for i, st in statuses.items() if st["role"] == FOLLOWER)
    follower = swarm[follower_idx]

    # 2) client mistakenly writes to follower
    resp = follower.create_topic(topic)
    data = resp.json()
    assert data["success"] is False
    leader_addr = data["leader_address"]
    assert leader_addr is not None

    # 3) client follows redirect and writes topic + messages
    resp2 = requests.put(leader_addr + TOPIC, json={"topic": topic}, timeout=1)
    assert resp2.ok and resp2.json()["success"] is True
    for m in msgs:
        respm = requests.put(
            leader_addr + MESSAGE,
            json={"topic": topic, "message": m},
            timeout=1,
        )
        assert respm.ok and respm.json()["success"] is True

    # 4) wait for replication
    time.sleep(ELECTION_TIMEOUT * 2)

    # 5) check commit_index on all nodes
    statuses2 = swarm.get_status()
    last_index = len(msgs)  # topic create at index 0, then messages
    for i, st in statuses2.items():
        assert st["commit_index"] >= last_index
