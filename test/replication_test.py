from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests
import os
import glob

MESSAGE = "/message"
TOPIC = "/topic"
STATUS = "/status"

NUM_NODES_ARRAY = [5]
PROGRAM_FILE_PATH = "src/node.py"
TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"
TEST_MESSAGES = ["Test - 1", "Test - 2", "Test - 3"]

ELECTION_TIMEOUT = 2.0
NUMBER_OF_LOOP_FOR_SEARCHING_LEADER = 3

LOG_DIR = "logs"


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


def wait_for_commit(seconds=1):
    time.sleep(seconds)


"""
Redirects
"""

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_follower_rejects_request(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None

    statuses = swarm.get_status()
    follower_idx = None
    for i, st in statuses.items():
        if st["role"] == FOLLOWER:
            follower_idx = i
            break
    assert follower_idx is not None

    follower_node = swarm[follower_idx]
    resp = follower_node.create_topic("sports")
    data = resp.json()
    assert data["success"] is False
    assert "leader_address" in data


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_client_can_follow_redirect(swarm: Swarm, num_nodes: int):
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    statuses = swarm.get_status()
    follower_idx = next(i for i, st in statuses.items() if st["role"] == FOLLOWER)
    follower = swarm[follower_idx]

    topic = "redirect_topic"

    resp = follower.create_topic(topic)
    data = resp.json()
    assert data["success"] is False
    leader_addr = data["leader_address"]

    resp2 = requests.put(leader_addr + TOPIC, json={"topic": topic}, timeout=1)
    data2 = resp2.json()
    assert data2["success"] is True

    resp3 = leader.get_topics()
    data3 = resp3.json()
    assert data3["success"] is True
    assert topic in data3["topics"]


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_follower_redirects_updates_after_leader_change(swarm: Swarm, num_nodes: int):
    old_leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert old_leader is not None

    statuses = swarm.get_status()
    follower_idx = next(i for i, st in statuses.items() if st["role"] == FOLLOWER)
    follower = swarm[follower_idx]
    assert follower is not old_leader

    topic = "leader_change_topic"

    resp1 = follower.create_topic(topic)
    data1 = resp1.json()
    assert data1["success"] is False
    assert data1["leader_address"] == old_leader.address

    old_leader.clean(ELECTION_TIMEOUT)

    new_leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert new_leader is not None

    if follower is new_leader:
        resp2 = follower.create_topic(topic)
        data2 = resp2.json()
        assert data2["success"] is True
    else:
        resp2 = follower.create_topic(topic)
        data2 = resp2.json()
        assert data2["success"] is False
        assert data2["leader_address"] == new_leader.address


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_follower_cannot_put_message(swarm: Swarm, num_nodes: int):
    """A follower should reject PUT message requests."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic(TEST_TOPIC)

    statuses = swarm.get_status()
    follower_idx = next(i for i, st in statuses.items() if st["role"] == FOLLOWER)
    follower = swarm[follower_idx]

    resp = follower.put_message(TEST_TOPIC, TEST_MESSAGE)
    assert resp.json()["success"] is False


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_follower_cannot_create_topic(swarm: Swarm, num_nodes: int):
    """A follower should reject PUT topic requests."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    statuses = swarm.get_status()
    follower_idx = next(i for i, st in statuses.items() if st["role"] == FOLLOWER)
    follower = swarm[follower_idx]

    resp = follower.create_topic("should_fail")
    assert resp.json()["success"] is False


"""
Replication
"""

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_topic_shared(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert(leader2.get_topics().json() ==
           {"success": True, "topics": [TEST_TOPIC]})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_message_shared(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
            == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert(leader2.get_message(TEST_TOPIC).json()
           == {"success": True, "message": TEST_MESSAGE})


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_happy_path_replication(swarm: Swarm, num_nodes: int):
    """Write to the leader, check that all nodes see the same commit_index."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    resp = leader.create_topic(TEST_TOPIC)
    assert resp.ok
    assert resp.json()["success"] is True

    for msg in TEST_MESSAGES:
        resp = leader.put_message(TEST_TOPIC, msg)
        assert resp.ok
        assert resp.json()["success"] is True

    time.sleep(ELECTION_TIMEOUT * 2)

    last_index = len(TEST_MESSAGES) - 1
    statuses = swarm.get_status()
    for i, st in statuses.items():
        assert st["commit_index"] >= last_index


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_follower_crash_and_catch_up(swarm: Swarm, num_nodes: int):
    """A follower that is down should catch up once it comes back."""
    before_msgs = ["a1", "a2"]
    after_msgs = ["b1", "b2"]
    topic = "crash_catchup_topic"

    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    follower = next(node for node in swarm.nodes if node is not leader)

    assert leader.create_topic(topic).json()["success"] is True
    for msg in before_msgs:
        assert leader.put_message(topic, msg).json()["success"] is True

    time.sleep(ELECTION_TIMEOUT * 2)

    follower.clean()
    time.sleep(0.5)

    for msg in after_msgs:
        assert leader.put_message(topic, msg).json()["success"] is True

    time.sleep(ELECTION_TIMEOUT * 2)

    follower.start()
    time.sleep(ELECTION_TIMEOUT * 2)

    expected = before_msgs + after_msgs
    seen = []
    while True:
        resp = leader.get_message(topic)
        assert resp.ok
        data = resp.json()
        if not data["success"]:
            break
        seen.append(data["message"])

    assert seen == expected
    commited_indices = 8
    statuses = swarm.get_status()
    for i, st in statuses.items():
        assert st["commit_index"] >= commited_indices


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_message_consumed_only_once(swarm: Swarm, num_nodes: int):
    """A message should only be consumed once across the cluster."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic(TEST_TOPIC)
    leader.put_message(TEST_TOPIC, TEST_MESSAGE)
    time.sleep(ELECTION_TIMEOUT)

    resp1 = leader.get_message(TEST_TOPIC)
    assert resp1.json() == {"success": True, "message": TEST_MESSAGE}

    resp2 = leader.get_message(TEST_TOPIC)
    assert resp2.json()["success"] is False


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_fifo_order_preserved(swarm: Swarm, num_nodes: int):
    """Messages should be consumed in the same order they were added."""
    messages = ["first", "second", "third", "fourth", "fifth"]

    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic(TEST_TOPIC)
    for msg in messages:
        leader.put_message(TEST_TOPIC, msg)

    time.sleep(ELECTION_TIMEOUT)

    for expected in messages:
        resp = leader.get_message(TEST_TOPIC)
        assert resp.json()["success"] is True
        assert resp.json()["message"] == expected


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_fifo_order_preserved_after_leader_crash(swarm: Swarm, num_nodes: int):
    """Message order should be preserved even after the leader crashes."""
    messages = ["first", "second", "third"]

    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None

    leader1.create_topic(TEST_TOPIC)
    for msg in messages:
        leader1.put_message(TEST_TOPIC, msg)

    time.sleep(ELECTION_TIMEOUT)

    leader1.commit_clean(ELECTION_TIMEOUT)

    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None

    for expected in messages:
        resp = leader2.get_message(TEST_TOPIC)
        assert resp.json()["success"] is True
        assert resp.json()["message"] == expected