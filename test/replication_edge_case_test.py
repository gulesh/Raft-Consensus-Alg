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


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_put_message_to_nonexistent_topic(swarm: Swarm, num_nodes: int):
    """Putting a message to a topic that doesn't exist should return False."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    resp = leader.put_message("nonexistent_topic", TEST_MESSAGE)
    assert resp.json()["success"] is False


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_get_message_from_nonexistent_topic(swarm: Swarm, num_nodes: int):
    """Getting a message from a topic that doesn't exist should return False."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    resp = leader.get_message("nonexistent_topic")
    assert resp.json()["success"] is False


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_duplicate_topic_not_created(swarm: Swarm, num_nodes: int):
    """Creating the same topic twice should fail on the second attempt."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    assert leader.create_topic(TEST_TOPIC).json()["success"] is True
    assert leader.create_topic(TEST_TOPIC).json()["success"] is False

    resp = leader.get_topics()
    assert resp.json()["topics"].count(TEST_TOPIC) == 1


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_duplicate_topic_not_replicated(swarm: Swarm, num_nodes: int):
    """After leader crash, duplicate topic should still not exist on new leader."""
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None

    leader1.create_topic(TEST_TOPIC)
    leader1.create_topic(TEST_TOPIC)  
    time.sleep(ELECTION_TIMEOUT)

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None

    resp = leader2.get_topics()
    assert resp.json()["topics"].count(TEST_TOPIC) == 1


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_empty_queue_after_all_messages_consumed(swarm: Swarm, num_nodes: int):
    """After consuming all messages queue should be empty."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic(TEST_TOPIC)
    leader.put_message(TEST_TOPIC, "msg1")
    leader.put_message(TEST_TOPIC, "msg2")
    time.sleep(ELECTION_TIMEOUT)

    leader.get_message(TEST_TOPIC)
    leader.get_message(TEST_TOPIC)

    resp = leader.get_message(TEST_TOPIC)
    assert resp.json()["success"] is False


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_multiple_topics_independent(swarm: Swarm, num_nodes: int):
    """Messages in different topics should not interfere with each other."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic("topic_a")
    leader.create_topic("topic_b")
    leader.put_message("topic_a", "msg_a")
    leader.put_message("topic_b", "msg_b")
    time.sleep(ELECTION_TIMEOUT)

    resp_a = leader.get_message("topic_a")
    resp_b = leader.get_message("topic_b")

    assert resp_a.json() == {"success": True, "message": "msg_a"}
    assert resp_b.json() == {"success": True, "message": "msg_b"}


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_multiple_topics_replicated(swarm: Swarm, num_nodes: int):
    """Multiple topics should all be replicated to the new leader after crash."""
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None

    leader1.create_topic("topic_a")
    leader1.create_topic("topic_b")
    leader1.create_topic("topic_c")
    time.sleep(ELECTION_TIMEOUT)

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None

    topics = leader2.get_topics().json()["topics"]
    assert "topic_a" in topics
    assert "topic_b" in topics
    assert "topic_c" in topics


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_large_number_of_messages_replicated(swarm: Swarm, num_nodes: int):
    """A large batch of messages should all be replicated correctly."""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    leader.create_topic(TEST_TOPIC)
    messages = [f"msg_{i}" for i in range(20)]
    for msg in messages:
        leader.put_message(TEST_TOPIC, msg)

    time.sleep(ELECTION_TIMEOUT * 2)

    seen = []
    while True:
        resp = leader.get_message(TEST_TOPIC)
        if not resp.json()["success"]:
            break
        seen.append(resp.json()["message"])

    assert seen == messages


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_messages_not_lost_during_reelection(swarm: Swarm, num_nodes: int):
    """Messages committed before leader crash should not be lost."""
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None

    leader1.create_topic(TEST_TOPIC)
    leader1.put_message(TEST_TOPIC, "before_crash")
    time.sleep(ELECTION_TIMEOUT)  # wait for commit

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None

    resp = leader2.get_message(TEST_TOPIC)
    assert resp.json() == {"success": True, "message": "before_crash"}


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_two_leader_crashes_data_survives(swarm: Swarm, num_nodes: int):
    """Data should survive two consecutive leader crashes."""
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader1 is not None

    leader1.create_topic(TEST_TOPIC)
    leader1.put_message(TEST_TOPIC, TEST_MESSAGE)
    time.sleep(ELECTION_TIMEOUT)

    # first crash
    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader2 is not None
    assert leader2 is not leader1

    # second crash
    leader2.commit_clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader3 is not None
    assert leader3 is not leader2

    resp = leader3.get_message(TEST_TOPIC)
    assert resp.json() == {"success": True, "message": TEST_MESSAGE}
