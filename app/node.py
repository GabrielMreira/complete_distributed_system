import os
import socket
import time
import threading
import psutil
import json
import random

from cryptography.fernet import Fernet

NODE_ID = int(os.getenv('NODE_ID', '0'))
PORT = int(os.getenv('PORT', '5000'))
ALL_NODES_STR = os.getenv('ALL_NODES', '')
ALL_NODES_MAP = {int(addr.split(':')[0].split('-')[1]): (addr.split(':')[0], int(addr.split(':')[1])) for addr in
                 ALL_NODES_STR.split(',')}

HEARTBEAT_INTERVAL = 5
LEADER_TIMEOUT = HEARTBEAT_INTERVAL * 4
MAX_FAILED_HEARTBEATS = 3
SNAPSHOT_INTERVAL = 15

MULTICAST_GROUP = '224.1.1.1'
MULTICAST_PORT = 8888
SECRET_KEY = Fernet.generate_key()
cipher_suite = Fernet(SECRET_KEY)

print(f"CLIENT SECRET KEY: {SECRET_KEY.decode()}", flush=True)

state_lock = threading.Lock()
leader_id = None
active_nodes = set(ALL_NODES_MAP.keys())
failed_heartbeats = {node_id: 0 for node_id in ALL_NODES_MAP}
election_in_progress = False
last_heartbeat_received = time.time()
lamport_clock = 0
global_snapshot_data = {}
snapshot_responses_received = set()


def encrypt_message(data):
    json_data = json.dumps(data, sort_keys=True).encode('utf-8')
    return cipher_suite.encrypt(json_data)


def tick():
    global lamport_clock
    with state_lock:
        lamport_clock += 1
        return lamport_clock


def update_clock(received_time):
    global lamport_clock
    with state_lock:
        lamport_clock = max(lamport_clock, received_time) + 1
        return lamport_clock


def send_tcp_message(target_node_id, message_type, payload=""):
    clock_value = tick()
    message = f"{message_type}:{clock_value}:{payload}"
    host, port = ALL_NODES_MAP[target_node_id]
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1.0)
            s.connect((host, port))
            s.sendall(message.encode())
            return True
    except:
        return False


def start_election():
    global election_in_progress, leader_id
    with state_lock:
        if election_in_progress:
            return
        print(f"[NODE-{NODE_ID}] INITIALIZING ELECTION (Clock: {tick()})", flush=True)
        election_in_progress = True
        leader_id = None

    higher_nodes = [node_id for node_id in active_nodes if node_id > NODE_ID]
    if not higher_nodes:
        announce_leader()
        return

    for node_id in higher_nodes:
        send_tcp_message(node_id, "ELECTION", str(NODE_ID))

    time.sleep(2)
    with state_lock:
        if election_in_progress:
            announce_leader()


def announce_leader():
    global leader_id, election_in_progress
    with state_lock:
        leader_id = NODE_ID
        election_in_progress = False
        print(f"[NODE-{NODE_ID}] NEW LEADER (Clock: {tick()})", flush=True)

    for node_id in active_nodes:
        if node_id != NODE_ID:
            send_tcp_message(node_id, "COORDINATOR", str(NODE_ID))


def send_heartbeats():
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with state_lock:
            if leader_id != NODE_ID:
                continue

        nodes_to_check = list(active_nodes)
        for node_id in nodes_to_check:
            if node_id == NODE_ID: continue

            if not send_tcp_message(node_id, "HEARTBEAT_PING", str(NODE_ID)):
                with state_lock:
                    failed_heartbeats[node_id] += 1
                    if failed_heartbeats[node_id] >= MAX_FAILED_HEARTBEATS:
                        if node_id in active_nodes:
                            print(f"[LIDER-{NODE_ID}] NODE-{node_id} INATIVE, REMOVING.", flush=True)
                            active_nodes.remove(node_id)
            else:
                with state_lock:
                    failed_heartbeats[node_id] = 0


def monitor_leader():
    while True:
        with state_lock:
            is_leader = (leader_id == NODE_ID)
            has_leader = (leader_id is not None)

        if is_leader or not has_leader:
            time.sleep(LEADER_TIMEOUT)
            continue

        time_since_last_heartbeat = time.time() - last_heartbeat_received
        if time_since_last_heartbeat > LEADER_TIMEOUT:
            start_election()

        time.sleep(1)


def handle_tcp_client(conn, addr):
    global leader_id, election_in_progress, last_heartbeat_received, global_snapshot_data, snapshot_responses_received
    try:
        data = conn.recv(1024).decode()
        if not data: return

        message_type, clock_str, payload = data.split(":", 2)
        received_clock = int(clock_str)
        update_clock(received_clock)

        if message_type == "HEARTBEAT_PING":
            with state_lock:
                last_heartbeat_received = time.time()
        elif message_type == "ELECTION":
            send_tcp_message(int(payload), "OK", str(NODE_ID))
            threading.Thread(target=start_election, daemon=True).start()
        elif message_type == "OK":
            with state_lock:
                election_in_progress = False
        elif message_type == "COORDINATOR":
            with state_lock:
                leader_id = int(payload)
                election_in_progress = False
                last_heartbeat_received = time.time()
            print(f"[NODE-{NODE_ID}] NEW LEADER: NODE-{leader_id}", flush=True)

        elif message_type == "SNAPSHOT_REQUEST":
            my_status = get_system_status()
            my_status['lamport_clock'] = tick()
            send_tcp_message(leader_id, "SNAPSHOT_RESPONSE", json.dumps({'node_id': NODE_ID, 'status': my_status}))

        elif message_type == "SNAPSHOT_RESPONSE":
            with state_lock:
                if leader_id == NODE_ID:
                    response = json.loads(payload)
                    node_id = response['node_id']
                    global_snapshot_data[node_id] = response['status']
                    snapshot_responses_received.add(node_id)

    except Exception:
        pass
    finally:
        conn.close()


def start_tcp_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', PORT))
    server_socket.listen(5)
    print(f"NODE-{NODE_ID} LISTENING ON PORT {PORT}", flush=True)
    while True:
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_tcp_client, args=(conn, addr), daemon=True)
        client_thread.start()


def get_system_status():
    return {'cpu_usage_percent': psutil.cpu_percent(interval=None),
            'memory_usage_percent': psutil.virtual_memory().percent,
            'uptime_seconds': int(time.time() - psutil.boot_time())}


def leader_task():
    global global_snapshot_data, snapshot_responses_received

    multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)  # Time-To-Live

    while True:
        time.sleep(SNAPSHOT_INTERVAL)
        current_leader_id, current_active_nodes = None, []
        with state_lock:
            current_leader_id = leader_id
            current_active_nodes = list(active_nodes)

        if current_leader_id != NODE_ID:
            continue

        with state_lock:
            snapshot_responses_received = set()
            my_status = get_system_status()
            my_status['lamport_clock'] = lamport_clock
            global_snapshot_data = {NODE_ID: my_status}

        for node_id in current_active_nodes:
            if node_id != NODE_ID:
                send_tcp_message(node_id, "SNAPSHOT_REQUEST")

        time.sleep(5)

        with state_lock:
            snapshot_to_send = {
                "leader_id": leader_id,
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "global_state": global_snapshot_data
            }
            encrypted_data = encrypt_message(snapshot_to_send)

            multicast_socket.sendto(encrypted_data, (MULTICAST_GROUP, MULTICAST_PORT))
            print(f"📡 [LEAD-{NODE_ID}] GLOBAL SNAPSHOT SENDED.", flush=True)


if __name__ == "__main__":
    threading.Thread(target=start_tcp_server, daemon=True).start()
    threading.Thread(target=send_heartbeats, daemon=True).start()
    threading.Thread(target=monitor_leader, daemon=True).start()
    threading.Thread(target=leader_task, daemon=True).start()

    time.sleep(5 + random.uniform(0, 2))
    start_election()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nNODE-{NODE_ID} OFF.", flush=True)