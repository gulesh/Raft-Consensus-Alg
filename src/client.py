import requests
import sys

def call_service_topic(port, command, topic):
    base_url = f"http://localhost:{port}/topic"
    try:
        if command == "put":
            response = requests.put(base_url, json={"topic": topic})
        else:  # "get"
            response = requests.get(base_url)
    except requests.exceptions.RequestException as e:
        print(f"request error (topic): {e}")
        return None
    return response


def call_service_message(port, command, topic, msg=None):
    base_url = f"http://localhost:{port}/message"
    try:
        if command == "put":
            response = requests.put(base_url, json={"topic": topic, "message": msg})
        else:  # "get"
            response = requests.get(base_url + f"/{topic}")
    except requests.exceptions.RequestException as e:
        print(f"request error (message): {e}")
        return None
    return response


def client():
    if len(sys.argv) < 4:
        print("Usage: python client.py <port> <put|get> <topic> [message]")
        sys.exit(1)

    port = int(sys.argv[1])
    command = sys.argv[2]      # "put" or "get"
    topic = sys.argv[3]
    msg = sys.argv[4] if len(sys.argv) > 4 else ""

    print("port", port, "command", command, "topic", topic)

    # For any command, make sure topic exists
    res = call_service_topic(port, "put", topic)
    if res is None:
        return
    print("Res TOPIC:", res.json())

    if command == "get":
        # Just read messages
        res = call_service_message(port, "get", topic)
        if res is not None:
            print("GET MSG:", res.json())
    else:
        # Put then get
        res = call_service_message(port, "put", topic, msg)
        if res is not None:
            print("PUT MSG:", res.json())

        res = call_service_message(port, "get", topic)
        if res is not None:
            print("GET MSG:", res.json())


if __name__ == '__main__':
    client()
