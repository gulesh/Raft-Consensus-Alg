
import argparse
import json
import random
import threading
import time
import os

from flask import Flask, request, jsonify
from queue import Queue
import requests

#queue
LOG_FILE_PATH = "logs/"
FOLLOWER = "Follower"
LEADER = "Leader"
CANDIDATE = "Candidate"

MESSAGE = "/message"
TOPIC = "/topic"
STATUS = "/status"
ELECTION_ROUTE = "/request_vote"
APPEND_ENTRIES = "/append_entries"
HEART_BEAT_INTERVAL = 0.02
CONFIG_PATH = "config.json"

class Node:
    def __init__(self, config_file_path: str, index:int):
        self.config_path = config_file_path
        self.i = index
        self.config = self.load_config() #list of all the addresses
        self.addresses = self.config["addresses"]
        self.peers = {
            i: addr
            for i, addr in enumerate(self.addresses)
            if i != self.i
        } #stores an index based dict of all the other ports
        self.peer_queues = self.create_peer_queues()
        self.my_address = self.addresses[self.i]
        self.app = Flask(__name__)
        self.setup_routes()
        self.election_timeout = random.uniform(0.15, 0.3)
        self.current_term = 0
        self.last_heartbeat = time.time()
        self.voted_for = None
        self.Node_Type = FOLLOWER
        self.data = {} 
        self.log_file = LOG_FILE_PATH + f"log_{self.i}.json"
        self.log = []
        self.lock = threading.Lock()
        self.current_leader_id= None

        self.commit_index = -1    #highest entry majority agreed on
        self.last_applied = -1    # highest entry applied to self.data
        self.next_index = {}      #  what to send each follower next
        self.match_index = {}     # what each follower has confirmed

    def create_peer_queues(self):
        return {
            peer_id: Queue()
            for peer_id in self.peers.keys()
        }

    def load_config(self):
        with open(self.config_path) as f:
            return json.load(f)

    def setup_routes(self):

        @self.app.route("/")
        def home():
            return {
                "index": self.i,
                "success": True
            }

        @self.app.route(TOPIC, methods=["PUT", "GET"])
        def add_get_topic():
            if request.method == "GET":
                topics = self.get_topics()
                return {"success": True, "topics": topics}
            elif request.method == "PUT":
                if request.is_json:
                    data = request.get_json()
                    print(f"data: {data}")
                    topic = data["topic"].lower()
                    res = self.replicate_command({"cmd": "create_topic", "topic": topic})
                    return jsonify(res)
                return {"success": False}
            return {"success": False}

        @self.app.route(MESSAGE, methods=["PUT"])
        def add_message():
            if request.is_json:
                data = request.get_json()
                print(f"data: {data}")
                topic = data["topic"]
                msg = data["message"]
                res = self.replicate_command({"cmd": "put_message", "topic": topic, "message": msg})
                return jsonify(res)
            return {"success": False}

        @self.app.route(f"{MESSAGE}/<topic>", methods=["GET"])
        def retrieve_message(topic):
            res = self.replicate_command({"cmd": "get_message", "topic": topic})
            return jsonify(res)

        @self.app.route(STATUS, methods=["GET"])
        def return_status():
            return self.get_status()

        @self.app.route(ELECTION_ROUTE, methods=["POST"])
        def request_vote():
            data = request.get_json()
            response = self.handle_vote_request(data)
            return jsonify(response)

        @self.app.route(APPEND_ENTRIES, methods=["POST"])
        def append_entries():
            data = request.get_json()
            response = self.handle_append_entries(data)
            return jsonify(response)
        
    def handle_append_entries(self, data):
        """
        Called by leader via POST /append_entries.
        Heartbeat if entries=[], replication if entries has data.
        1. Reject if leader is stale
        2. Reset election timer, accept leader
        3. Check our log matches leader's at prev_log_index, reject if not
        4. Store new entries in our log
        5. If leader committed new entries, apply them to self.data
        """

        term           = data["term"]
        current_leader = data["leader_id"]
        prev_log_index = data["prev_log_index"]
        prev_log_term  = data["prev_log_term"]
        entries        = data.get("entries", [])
        leader_commit  = data["leader_commit"]

        # reject stale leader
        if term < self.current_term:
            return {"success": False, "term": self.current_term}

        # valid leader
        self.reset_election_timer()
        self.current_term = term
        self.Node_Type = FOLLOWER

        #update leader id
        self.current_leader_id = current_leader

        # do we have the entry the leader expects us to have?
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                return {"success": False, "term": self.current_term}  # behind
            if self.log[prev_log_index]["term"] != prev_log_term:
                self.log = self.log[:prev_log_index]  # conflict, truncate
                return {"success": False, "term": self.current_term}

        # store the new entries
        for entry in entries:
            idx = entry["index"]
            if idx < len(self.log):
                if self.log[idx]["term"] != entry["term"]:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        # leader says its safe to commit up to here
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self.apply_committed_entries()
            self.persist_state()

        return {"success": True, "term": self.current_term}

    def reject_provide_leader_address(self):
         # need to format this so that we reject and give the address of the leader
         leader = self.current_leader_id
         if leader is None:
             # leader is unknown
             return {"success": False, "leader_address": None}

         leader_addr = self.addresses[leader]
         address = f"http://{leader_addr['ip']}:{leader_addr['port']}"
         return {"success": False, "leader_address": address}

    def replicate_command(self, command):
        """
        Called when a client write request arrives (create_topic, put_message, get_message).
        Leader appends to its own log, sends to followers, waits for majority ACK,
        then commits to self.data. Returns False if not the leader or no quorum.
        """
        if self.Node_Type != LEADER:
            return self.reject_provide_leader_address()

        # append to leader's own log first
        entry = {
            "index": len(self.log),
            "term": self.current_term,
            "command": command,
        }
        self.log.append(entry)
        self.persist_state()

        # send to all followers in parallel, count ACKs
        ack_count = 1  # leader counts itself
        ack_lock = threading.Lock()

        def send_to_peer(peer_id, addr):
            nonlocal ack_count
            ni = self.next_index.get(peer_id, 0)
            prev_log_index = ni - 1
            prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0
            payload = {
                "term": self.current_term,
                "leader_id": self.i,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": self.log[ni:],
                "leader_commit": self.commit_index
            }
            try:
                url = f"http://{addr['ip']}:{addr['port']}{APPEND_ENTRIES}"
                resp = requests.post(url, json=payload, timeout=1)
                if resp.ok and resp.json()["success"]:
                    with ack_lock:
                        ack_count += 1
            except:
                pass

        threads = [threading.Thread(target=send_to_peer, args=(pid, addr))
                for pid, addr in self.peers.items()]
        for t in threads: t.start()
        for t in threads: t.join(timeout=1)

        # if majority ACKed, commit
        if ack_count >= (len(self.addresses) // 2 + 1):
            self.commit_index = entry["index"]
            res = self.apply_committed_entries()
            self.persist_state()
            return res

        return {"success": False}  # not enough nodes alive
        
    
    def apply_committed_entries(self):
        """
        Applies all log entries that were committed but not yet applied to self.data.
        Runs every time commit_index advances.
        """
        result = None
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            command = self.log[self.last_applied]["command"]
            result = self.apply_command(command)
        return result
    
    def apply_command(self, command):
        """
        Executes a single command against self.data.
        This is the only place self.data ever changes.
        """
        if not command:
            return {"success": "not command"}

        cmd   = command.get("cmd")
        topic = command.get("topic", "").lower()

        if cmd == "create_topic":
            return self.create_topic(topic)
        elif cmd == "put_message":
            return self.put_message(command.get("message", ""), topic)
        elif cmd == "get_message":
            return self.get_message(topic)
        return {"success": False}

    """
    State APIs method for Client
    """
    def create_topic(self, topic: str):
        topic = topic.lower()
        if topic not in self.data:
            self.data[topic] = Queue()
            return {"success": True}
        return {"success": False}

    def put_message(self, message: str, topic: str):
        if topic not in self.data:
            return {"success": False}
        self.data[topic].put(message)
        return {"success": True}

    def get_message(self, topic: str):
        if topic in self.data and not self.data[topic].empty():
            return {"success": True, "message": self.data[topic].get_nowait()}
        return {"success": False}

    def get_topics(self):
        return list(self.data.keys())

    def get_address(self, port: int):
        return "http://" + self.my_address["ip"] + ":" + str(port)

    def get_status(self):
        return {
            "role": self.Node_Type,
            "term": self.current_term,
            "commit_index": self.commit_index,
            "log_len": len(self.log)
        }

    def set_role(self, new_role):
        if self.Node_Type != new_role:
            print(f"[node {self.i}] {self.Node_Type} -> {new_role} term={self.current_term}", flush=True)
        self.Node_Type = new_role

    """
    Election Functions
    """
    def start_election(self):
        self.current_term += 1
        self.Node_Type = CANDIDATE
        self.voted_for = self.i
        self.persist_state()
        self.request_vote()

    def reset_election_timer(self):
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(0.15, 0.3)

    def election_timer_thread(self):
        print(f"[{threading.current_thread().name}] Starting election")
        while True:
            time.sleep(0.01 + random.uniform(0, 0.01))  # extra jitter

            if self.Node_Type == LEADER:
                continue

            elapsed = time.time() - self.last_heartbeat
            if elapsed > self.election_timeout:
                self.start_election()

    def handle_vote_request(self, data):
        candidate_term = data["term"]
        candidate_id = data["candidate_id"]

        if candidate_term < self.current_term:
            return {"success": False, "term": self.current_term}

        #newer term update self term
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.voted_for = None
            self.Node_Type = FOLLOWER

        my_last_index = len(self.log) - 1
        my_last_term = self.log[my_last_index]["term"] if self.log else 0

        candidate_log_index = data["last_log_index"]
        candidate_log_term = data["last_log_term"]

        up_to_date = (
                candidate_log_term > my_last_term or
                (candidate_log_term == my_last_term and
                 candidate_log_index >= my_last_index)
        )
        vote_granted = False
        if up_to_date and (self.voted_for is None or self.voted_for == candidate_id):
            self.voted_for = candidate_id
            self.persist_state()
            self.reset_election_timer()
            vote_granted = True
        return {"success": vote_granted, "term": self.current_term}

    def request_vote(self):
        pay_load = {
            "term": self.current_term,
            "candidate_id": self.i,
            "last_log_index": len(self.log) - 1,
            "last_log_term": self.log[-1]["term"] if self.log else 0
        }
        total_vote_count = 1 #voted for self
        for peer_id, addr in self.peers.items():
            try:
                url = f"http://{addr['ip']}:{addr['port']}{ELECTION_ROUTE}"
                resp = requests.post(url, json=pay_load, timeout=1)
                if not resp.ok:
                    continue
                data = resp.json()

                # if responder has higher term, step down immediately
                their_term = data.get("term", 0)
                if their_term > self.current_term:
                    self.current_term = their_term
                    self.Node_Type = FOLLOWER
                    self.voted_for = None
                    self.persist_state()
                    return

                if data.get("success"):
                    total_vote_count += 1
            except requests.RequestException:
                continue

        if total_vote_count > len(self.addresses) // 2:
            self.become_leader()

    def become_leader(self):
        if self.Node_Type == LEADER:  # already leader, don't start again
            return
        self.Node_Type = LEADER
        print(f"Node {self.i} became leader for term {self.current_term}")

        # initialize follower tracking
        for peer_id in self.peers:
            self.next_index[peer_id]  = len(self.log)  # start sending from here
            self.match_index[peer_id] = -1             # nothing confirmed yet

        threading.Thread(
            target=self.send_heartbeat,
            daemon=True,
            name="HeartbeatThread"
        ).start()

    def send_heartbeat(self):
        while self.Node_Type == LEADER:
            for peer_id, addr in self.peers.items():
                try:
                    ni = self.next_index.get(peer_id, len(self.log))
                    prev_log_index = ni - 1
                    prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0
                    payload = {
                        "term": self.current_term,
                        "leader_id": self.i,
                        "prev_log_index": prev_log_index,
                        "prev_log_term": prev_log_term,
                        "entries": [],          # empty = heartbeat
                        "leader_commit": self.commit_index
                    }
                    url = f"http://{addr['ip']}:{addr['port']}{APPEND_ENTRIES}"
                    requests.post(url, json=payload, timeout=0.5)
                except:
                    pass
            time.sleep(HEART_BEAT_INTERVAL)

    """
    Persistence/ Fault Tolerance Functions
    """
    def persist_state(self):
        os.makedirs("logs", exist_ok=True)
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "logs": self.log,
            "commit_index": self.commit_index,
        }

        with open(self.log_file, "w") as f:
            json.dump(state, f, indent=2)

    def append_entry(self, command):
        entry = {
            "index": len(self.log),
            "term": self.current_term,
            "leader_id": self.i,
            "last_log_index": len(self.log) - 1,
            "last_log_term": self.log[-1]["term"] if self.log else 0,
            "command": command
        }
        self.log.append(entry)
        self.persist_state()
        self.broad_cast(entry, APPEND_ENTRIES)

    def broad_cast(self, message, end_point):
        for peer_id, addr in self.peers.items():
            try:
                url = f"http://{addr['ip']}:{addr['port']}{end_point}"
                requests.post(url, json=message, timeout=1)
            except:
                pass

    """
    OnServer start functions
    """
    def start(self):
        ip = self.my_address["ip"]
        port = self.my_address["port"]
        self.load_from_file_entries()
        election_thread = threading.Thread(
            target=self.election_timer_thread,
            daemon=True,
            name = "ElectionTimer"
        )
        election_thread.start()
        print(f"Starting node {self.i} on port {port}")
        self.app.run(port=port)

    def load_from_file_entries(self):
        if not os.path.exists(self.log_file):
            self.current_term = 0
            self.voted_for = None
            self.log = []
            self.commit_index = -1
            self.last_applied = -1
            return

        with open(self.log_file, "r") as f:
            log_dict = json.load(f)
            self.current_term = log_dict.get("current_term", 0)
            self.voted_for = log_dict.get("voted_for", None)
            self.log = log_dict.get("logs", [])

            self.commit_index = log_dict.get("commit_index", -1)
            self.last_applied = -1
            for i in range(self.commit_index + 1):
                entry = self.log[i]
                self.apply_command(entry["command"])
                self.last_applied = i




def main():
    parsar = argparse.ArgumentParser(description="Start Node service")
    parsar.add_argument("config_path", type=str, help="Path to config file")
    parsar.add_argument("index", type=str, help="Node Index")

    args = parsar.parse_args()
    config_path = args.config_path
    index = int(args.index)

    print("Config:", config_path)
    print("Index", index)

    print("Start the flask server")
    node = Node(config_path, index)

    node.start()


if __name__ == '__main__':
    main()