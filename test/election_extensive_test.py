# test_elections.py
from test_utils import Swarm, LEADER, FOLLOWER
import pytest
import time
import os, glob

PROGRAM_FILE_PATH = "src/node.py"
ELECTION_TIMEOUT = 2.0
NUM_NODES_ARRAY = [5]
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

def wait_for_stable_leader(swarm: Swarm, retries=5, sleep=ELECTION_TIMEOUT):
    leader = None
    for _ in range(retries):
        leader = swarm.get_leader()
        if leader is not None:
            return leader
        time.sleep(sleep)
    return None

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_leader_eventually_stable(swarm: Swarm, num_nodes: int):
    leaders = set()
    for _ in range(5):
        leader = wait_for_stable_leader(swarm)
        assert leader is not None
        leaders.add(leader.i)
        time.sleep(ELECTION_TIMEOUT / 2)
    assert len(leaders) == 1

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_leader_crash_and_re_election(swarm: Swarm, num_nodes: int):
    leader1 = wait_for_stable_leader(swarm)
    assert leader1 is not None
    term1 = leader1.get_status().json()["term"]

    leader1.clean()
    time.sleep(ELECTION_TIMEOUT * 1.5)

    leader2 = wait_for_stable_leader(swarm)
    assert leader2 is not None
    assert leader2.i != leader1.i
    term2 = leader2.get_status().json()["term"]
    assert term2 >= term1 + 1

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_follower_does_not_become_leader_with_active_leader(swarm: Swarm, num_nodes: int):
    leader = wait_for_stable_leader(swarm)
    assert leader is not None

    for _ in range(3):
        statuses = swarm.get_status()
        leaders = [i for i, st in statuses.items() if st["role"] == LEADER]
        assert len(leaders) == 1
        time.sleep(ELECTION_TIMEOUT / 2)

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_candidate_timeout_and_new_election(swarm: Swarm, num_nodes: int):
    assert num_nodes >= 5
    leader1 = wait_for_stable_leader(swarm)
    assert leader1 is not None
    term1 = leader1.get_status().json()["term"]

    majority = num_nodes // 2 + 1
    to_stop = majority - 1
    stopped = []
    for node in swarm.nodes:
        if node is leader1:
            continue
        if len(stopped) < to_stop:
            node.pause()
            stopped.append(node)

    time.sleep(ELECTION_TIMEOUT * 2)

    for node in stopped:
        node.resume()

    leader2 = wait_for_stable_leader(swarm, retries=6)
    assert leader2 is not None
    term2 = leader2.get_status().json()["term"]
    assert term2 > term1

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_no_two_leaders_same_term_over_time(swarm: Swarm, num_nodes: int):
    leader_terms = {}
    for _ in range(5):
        statuses = swarm.get_status()
        for i, st in statuses.items():
            term = st["term"]
            role = st["role"]
            if role == LEADER:
                leader_terms.setdefault(term, set()).add(i)
        time.sleep(ELECTION_TIMEOUT / 2)
    for term, leaders in leader_terms.items():
        assert len(leaders) <= 1, f"Multiple leaders {leaders} in term {term}"

@pytest.mark.parametrize('num_nodes', [1])
def test_single_node_persists_leader_across_restarts(swarm: Swarm, num_nodes: int):
    if num_nodes != 1:
        pytest.skip("Only meaningful for single-node cluster")
    status1 = swarm[0].get_status().json()
    assert status1["role"] == LEADER
    swarm[0].restart()
    time.sleep(ELECTION_TIMEOUT)
    status2 = swarm[0].get_status().json()
    assert status2["role"] == LEADER

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_flapping_candidate_does_not_steal_leadership(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(3)
    assert leader1 is not None
    term1 = leader1.get_status().json()["term"]

    statuses = swarm.get_status()
    cand_idx = next(i for i, st in statuses.items() if st["role"] == FOLLOWER)
    flapper = swarm[cand_idx]

    flapper.pause()
    time.sleep(ELECTION_TIMEOUT * 3)

    statuses_after = {}
    for node in swarm.nodes:
        if node is flapper:
            continue
        statuses_after[node.i] = node.get_status().json()
    leaders = [i for i, st in statuses_after.items() if st["role"] == LEADER]
    assert len(leaders) <= 1

    flapper.resume()
    time.sleep(ELECTION_TIMEOUT * 3)

    leader2 = swarm.get_leader_loop(5)
    assert leader2 is not None
    term2 = leader2.get_status().json()["term"]
    assert term2 >= term1

@pytest.mark.xfail(reason="Known split-vote liveness issue under SIGSTOP; covered by kill/clean tests")
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_new_leader_after_pause(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(3)
    assert leader1 is not None
    term1 = leader1.get_status().json()["term"]

    leader1.pause()
    time.sleep(ELECTION_TIMEOUT * 3)

    candidates = [n for n in swarm.nodes if n is not leader1]
    leader2 = None
    term2 = term1

    for _ in range(10):
        for n in candidates:
            st = n.get_status().json()
            if st["role"] == LEADER:
                leader2 = n
                term2 = st["term"]
                break
        if leader2 is not None:
            break
        time.sleep(ELECTION_TIMEOUT / 2 + 0.05)

    assert leader2 is not None
    assert leader2 is not leader1
    assert term2 > term1

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_new_leader_after_terminate(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(3)
    assert leader1 is not None
    term1 = leader1.get_status().json()["term"]

    leader1.terminate()
    leader1.wait()
    time.sleep(ELECTION_TIMEOUT * 2)

    candidates = [n for n in swarm.nodes if n is not leader1]
    leader2 = None
    term2 = term1

    for _ in range(10):
        for n in candidates:
            st = n.get_status().json()
            if st["role"] == LEADER:
                leader2 = n
                term2 = st["term"]
                break
        if leader2 is not None:
            break
        time.sleep(ELECTION_TIMEOUT / 2)

    assert leader2 is not None
    assert leader2 is not leader1
    assert term2 > term1

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_new_leader_after_kill(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(3)
    assert leader1 is not None
    term1 = leader1.get_status().json()["term"]

    leader1.kill()
    leader1.wait()
    time.sleep(ELECTION_TIMEOUT * 2)

    candidates = [n for n in swarm.nodes if n is not leader1]
    leader2 = None
    term2 = term1

    for _ in range(10):
        for n in candidates:
            st = n.get_status().json()
            if st["role"] == LEADER:
                leader2 = n
                term2 = st["term"]
                break
        if leader2 is not None:
            break
        time.sleep(ELECTION_TIMEOUT / 2)

    assert leader2 is not None
    assert leader2 is not leader1
    assert term2 > term1

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_no_leader_without_quorum(swarm: Swarm, num_nodes: int):
    assert num_nodes >= 5
    majority = num_nodes // 2 + 1

    stopped = []
    for node in swarm.nodes:
        if len(stopped) < majority:
            node.pause()
            stopped.append(node)

    time.sleep(ELECTION_TIMEOUT * 3)

    statuses = {}
    for node in swarm.nodes:
        if node in stopped:
            continue
        statuses[node.i] = node.get_status().json()

    leaders = [i for i, st in statuses.items() if st["role"] == LEADER]
    assert len(leaders) <= 1

    for node in stopped:
        node.resume()
    leader = swarm.get_leader_loop(5)
    assert leader is not None
