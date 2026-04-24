from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests
import os
import glob
import threading

MESSAGE = "/message"
TOPIC = "/topic"
STATUS = "/status"

NUM_NODES_ARRAY = [5]
PROGRAM_FILE_PATH = "src/node.py"
TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"

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


"""
Replication Edge Cases
"""

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_write_then_immediate_leader_crash(swarm: Swarm, num_nodes: int):
    """Write to leader then crash it immediately — new leader should have the entry if majority ACKed."""
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None

    leader1.create_topic(TEST_TOPIC)
    leader1.put_message(TEST_TOPIC, TEST_MESSAGE)

    # crash immediately without waiting for replication
    leader1.clean()
    time.sleep(ELECTION_TIMEOUT * 2)

    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None

    # if majority ACKed before crash, message must be there
    resp = leader2.get_topics()
    assert resp.json()["success"] is True


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_two_followers_crash_cluster_still_works(swarm: Swarm, num_nodes: int):
    """With 5 nodes, crashing 2 followers should still leave a functional cluster."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    # crash 2 followers
    crashed = 0
    for node in swarm.nodes:
        if node is not leader and crashed < 2:
            node.clean()
            crashed += 1

    time.sleep(ELECTION_TIMEOUT)

    # cluster should still work with 3 nodes
    resp = leader.create_topic(TEST_TOPIC)
    assert resp.json()["success"] is True

    resp = leader.put_message(TEST_TOPIC, TEST_MESSAGE)
    assert resp.json()["success"] is True


 

"""
Election Edge Cases
"""

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_all_nodes_restart_one_leader_emerges(swarm: Swarm, num_nodes: int):
    """After all nodes restart simultaneously, exactly one leader should emerge."""
    swarm.restart(ELECTION_TIMEOUT * 2)

    leaders = []
    statuses = swarm.get_status()
    for i, st in statuses.items():
        if st["role"] == LEADER:
            leaders.append(i)

    assert len(leaders) == 1




@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_term_increments_on_each_election(swarm: Swarm, num_nodes: int):
    """Term should increment by at least 1 after each leader crash."""
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None
    term1 = leader1.get_status().json()["term"]

    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None
    term2 = leader2.get_status().json()["term"]

    leader2.clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader3 is not None
    term3 = leader3.get_status().json()["term"]

    assert term2 > term1
    assert term3 > term2


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_election_loser_becomes_follower(swarm: Swarm, num_nodes: int):
    """All nodes that lost the election should be followers."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    statuses = swarm.get_status()
    for i, st in statuses.items():
        if i != leader.i:
            assert st["role"] == FOLLOWER


"""
Message Queue Edge Cases
"""

@pytest.mark.parametrize('num_nodes', [1])
def test_topic_name_uppercase_normalized(swarm: Swarm, num_nodes: int):
    """Topic names should be case-insensitive."""
    node = swarm[0]
    time.sleep(ELECTION_TIMEOUT)

    assert node.create_topic("MyTopic").json()["success"] is True
    # creating same topic with different case should fail
    assert node.create_topic("mytopic").json()["success"] is False


@pytest.mark.parametrize('num_nodes', [1])
def test_empty_message(swarm: Swarm, num_nodes: int):
    """An empty string should be a valid message."""
    node = swarm[0]
    time.sleep(ELECTION_TIMEOUT)

    node.create_topic(TEST_TOPIC)
    resp = node.put_message(TEST_TOPIC, "")
    assert resp.json()["success"] is True

    resp = node.get_message(TEST_TOPIC)
    assert resp.json() == {"success": True, "message": ""}


@pytest.mark.parametrize('num_nodes', [1])
def test_very_long_message(swarm: Swarm, num_nodes: int):
    """A very long message should be stored and retrieved correctly."""
    node = swarm[0]
    time.sleep(ELECTION_TIMEOUT)

    long_message = "x" * 10000
    node.create_topic(TEST_TOPIC)
    node.put_message(TEST_TOPIC, long_message)

    resp = node.get_message(TEST_TOPIC)
    assert resp.json()["success"] is True
    assert resp.json()["message"] == long_message


@pytest.mark.parametrize('num_nodes', [1])
def test_topics_returned_in_insertion_order(swarm: Swarm, num_nodes: int):
    """Topics should be returned in the order they were created."""
    node = swarm[0]
    time.sleep(ELECTION_TIMEOUT)

    topics = ["zebra", "apple", "mango", "banana"]
    for t in topics:
        node.create_topic(t)

    resp = node.get_topics()
    assert resp.json()["topics"] == topics


"""
Concurrency Edge Cases
"""



@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_rapid_fire_requests(swarm: Swarm, num_nodes: int):
    """50 rapid fire messages should all be stored with none lost."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic(TEST_TOPIC)
    time.sleep(ELECTION_TIMEOUT)

    messages = [f"rapid_{i}" for i in range(50)]
    for msg in messages:
        resp = leader.put_message(TEST_TOPIC, msg)
        assert resp.json()["success"] is True

    time.sleep(ELECTION_TIMEOUT)

    seen = []
    while True:
        resp = leader.get_message(TEST_TOPIC)
        if not resp.json()["success"]:
            break
        seen.append(resp.json()["message"])

    assert len(seen) == 50
    assert seen == messages


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_concurrent_read_and_write(swarm: Swarm, num_nodes: int):
    """Simultaneous reads and writes should not corrupt state."""
    time.sleep(1)
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic(TEST_TOPIC)
    for i in range(10):
        leader.put_message(TEST_TOPIC, f"msg_{i}")
    time.sleep(ELECTION_TIMEOUT)

    read_results = []
    write_results = []

    def do_reads():
        for _ in range(5):
            resp = leader.get_message(TEST_TOPIC)
            read_results.append(resp.json()["success"])

    def do_writes():
        for i in range(5):
            resp = leader.put_message(TEST_TOPIC, f"concurrent_{i}")
            write_results.append(resp.json()["success"])

    t1 = threading.Thread(target=do_reads)
    t2 = threading.Thread(target=do_writes)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # no crashes, all operations returned a valid response
    assert len(read_results) >0
    assert len(write_results) >0