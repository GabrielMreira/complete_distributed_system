import os
import socket
import threading
import time
import json
import struct
import psutil
from collections import OrderedDict

import grpc
from concurrent import futures
import Pyro5.api
from generated import resources_pb2, resources_pb2_grpc

GROUP_ID = os.getenv('GROUP_ID', 'A')
NODE_ID = int(os.getenv('NODE_ID', '0'))
ALL_NODES_STR = os.getenv('ALL_NODES', '')
MY_NODE_NAME = f"g-{GROUP_ID.lower()}-node-{NODE_ID}"

ALL_NODES = OrderedDict()
MY_TCP_PORT = 0
for node_str in ALL_NODES_STR.split(','):
    name, address = node_str.split(':')
    _, group, _, node_num = name.split('-')
    node_info = {"name": name, "group": group.upper(), "id": int(node_num), "host": name, "port": int(address)}
    ALL_NODES[name] = node_info
    if group.upper() == GROUP_ID and int(node_num) == NODE_ID:
        MY_TCP_PORT = int(address)

MY_GROUP_NODES = OrderedDict((name, info) for name, info in ALL_NODES.items() if info['group'] == GROUP_ID)

MULTICAST_GROUP_A_ADDR = '224.1.1.1'
MULTICAST_GROUP_B_ADDR = '224.2.2.2'
MULTICAST_PORT = 10000
MULTICAST_LISTEN_ADDR = MULTICAST_GROUP_A_ADDR if GROUP_ID == 'A' else MULTICAST_GROUP_B_ADDR
MULTICAST_SEND_ADDR = MULTICAST_GROUP_B_ADDR if GROUP_ID == 'A' else MULTICAST_GROUP_A_ADDR

state_lock = threading.Lock()
leader_name = None
election_in_progress = False
super_coordinator_name = None
group_snapshot = {}

def get_system_status():
    return {'cpu': psutil.cpu_percent(), 'mem': psutil.virtual_memory().percent}


def send_tcp_message(target_name, message):
    try:
        target_info = ALL_NODES[target_name]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1.0);
            s.connect((target_info['host'], target_info['port']));
            s.sendall(message.encode())
            return True
    except:
        return False


def send_multicast_message(message):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        sock.sendto(message.encode(), (MULTICAST_SEND_ADDR, MULTICAST_PORT))

if GROUP_ID == 'A':

    class ResourceMonitorServicer_A(resources_pb2_grpc.ResourceMonitorServicer):
        def GetStatus(self, request, context):
            status = get_system_status()
            return resources_pb2.StatusResponse(cpu_usage_percent=status['cpu'], memory_usage_percent=status['mem'])


    def start_specific_services():
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        resources_pb2_grpc.add_ResourceMonitorServicer_to_server(ResourceMonitorServicer_A(), server)
        grpc_port = MY_TCP_PORT + 1000
        server.add_insecure_port(f'[::]:{grpc_port}')
        server.start()
        print(f"{MY_NODE_NAME} - SERVER FRPC (GROUP A) LISTENING IN {grpc_port}", flush=True)
        time.sleep(5)
        start_election()


    def start_election():
        global election_in_progress, leader_name
        with state_lock:
            if election_in_progress: return
            print(f"{MY_NODE_NAME} - STARTING BULLY ELECTION", flush=True)
            election_in_progress = True
        higher_nodes = [name for name, info in MY_GROUP_NODES.items() if info['id'] > NODE_ID]
        if not higher_nodes: announce_leader(); return
        for node_name in higher_nodes: send_tcp_message(node_name, "BULLY_ELECTION")
        time.sleep(2)
        with state_lock:
            if election_in_progress: announce_leader()


    def announce_leader():
        global leader_name, election_in_progress
        with state_lock:
            leader_name = MY_NODE_NAME
            election_in_progress = False
            print(f"{MY_NODE_NAME} - ELECTED LEADER OF GROUP A (BULLY)", flush=True)
        for node_name in MY_GROUP_NODES:
            if node_name != MY_NODE_NAME: send_tcp_message(node_name, f"BULLY_COORDINATOR:{MY_NODE_NAME}")


    def handle_group_message(message, conn=None):
        global election_in_progress, leader_name
        if message == "BULLY_ELECTION":
            send_tcp_message(MY_NODE_NAME, "BULLY_OK");
            start_election()
        elif message == "BULLY_OK":
            with state_lock:
                election_in_progress = False
        elif message.startswith("BULLY_COORDINATOR"):
            _, new_leader = message.split(':')
            with state_lock:
                leader_name = new_leader; election_in_progress = False
            print(f"{MY_NODE_NAME} - NEW LEADER OF GROUP A {leader_name}", flush=True)

elif GROUP_ID == 'B':
    @Pyro5.api.expose
    class ResourceMonitor_B:
        def get_status(self): return get_system_status()

    def start_specific_services():
        if NODE_ID == 1:
            threading.Thread(target=Pyro5.api.start_ns, kwargs={'host': '0.0.0.0'}, daemon=True).start()
            print(f"{MY_NODE_NAME} - NAME SERVER PYRO5 STARTED", flush=True)
            time.sleep(2)
        ns = None
        while not ns:
            try:
                ns = Pyro5.api.locate_ns()
                print(f"{MY_NODE_NAME} - NAME SERVER LOCATED", flush=True)
            except Pyro5.errors.NamingError:
                print(f"{MY_NODE_NAME} - WAITING NAME SERVER BE ON", flush=True)
                time.sleep(1)
        daemon = Pyro5.api.Daemon(host='0.0.0.0', port=MY_TCP_PORT + 2000)
        uri = daemon.register(ResourceMonitor_B, objectId=MY_NODE_NAME)
        ns.register(MY_NODE_NAME, uri)
        print(f"{MY_NODE_NAME} - SERVER PYRO5 (GROUP B) REGISTRED", flush=True)
        threading.Thread(target=daemon.requestLoop, daemon=True).start()
        time.sleep(2)
        start_election()


    def get_successor():
        node_names = list(MY_GROUP_NODES.keys())
        my_index = node_names.index(MY_NODE_NAME)
        return node_names[(my_index + 1) % len(node_names)]


    def start_election():
        global election_in_progress
        with state_lock:
            if election_in_progress: return
            print(f"{MY_NODE_NAME} - STARTING RING ELECTION", flush=True)
            election_in_progress = True
        send_tcp_message(get_successor(), f"RING_ELECTION:{NODE_ID}:{NODE_ID}")


    def handle_group_message(message, conn=None):
        global leader_name, election_in_progress
        if message.startswith("RING_ELECTION"):
            parts = message.split(':')
            winner_id = int(parts[1])
            participants = parts[2].split(',')
            if str(NODE_ID) not in participants:
                new_winner_id = max(winner_id, NODE_ID)
                participants.append(str(NODE_ID))
                send_tcp_message(get_successor(), f"RING_ELECTION:{new_winner_id}:{','.join(participants)}")
            else:
                winner_name = f"g-b-node-{winner_id}"
                with state_lock:
                    leader_name = winner_name; election_in_progress = False
                print(f"COORDINATOR: {MY_NODE_NAME} - RING ELECTION FINISHED, LEADER OF B: {leader_name}", flush=True)
                if MY_NODE_NAME == winner_name: send_tcp_message(get_successor(), f"RING_COORDINATOR:{leader_name}")
        elif message.startswith("RING_COORDINATOR"):
            _, new_leader = message.split(':')
            if leader_name != new_leader:
                with state_lock: leader_name = new_leader
                send_tcp_message(get_successor(), message)


def coordinator_logic():
    global super_coordinator_name, group_snapshot
    last_super_coord_announcement = 0

    while True:
        time.sleep(10)
        with state_lock:
            am_i_leader = (leader_name == MY_NODE_NAME)
            current_super_coordinator = super_coordinator_name

        if am_i_leader and (time.time() - last_super_coord_announcement > 20):
            print(f"{MY_NODE_NAME} - I AM THE SUPER COORDINATOR", flush=True)
            send_multicast_message(f"LEADER_ANNOUNCE:{MY_NODE_NAME}")
            last_super_coord_announcement = time.time()


        if current_super_coordinator == MY_NODE_NAME:
            print(f"{MY_NODE_NAME} - SUPER COORDINATOR, STARTING GLOBAL SNAPSHOT", flush=True)

            group_snapshot = {}
            for node_name in MY_GROUP_NODES:
                if node_name != MY_NODE_NAME:
                    send_tcp_message(node_name, "SNAPSHOT_REQUEST")

            send_multicast_message(f"GLOBAL_SNAPSHOT_REQUEST:{MY_NODE_NAME}")

            my_status = get_system_status()
            group_snapshot[MY_NODE_NAME] = my_status

            time.sleep(5)
            print(f"GLOBAL SNAPSHOT PICKED BY {MY_NODE_NAME}", flush=True)
            print(json.dumps(group_snapshot, indent=2), flush=True)


def start_tcp_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', MY_TCP_PORT))
    server_socket.listen(10)
    print(f"{MY_NODE_NAME} - TCP SERVER LISTENING IN {MY_TCP_PORT}", flush=True)
    while True:
        conn, _ = server_socket.accept()
        try:
            data = conn.recv(1024).decode()
            if not data: continue

            if data == "SNAPSHOT_REQUEST":
                status = get_system_status()
                response_message = f"SNAPSHOT_RESPONSE:{json.dumps({'name': MY_NODE_NAME, 'status': status})}"
                if leader_name: send_tcp_message(leader_name, response_message)
            elif data.startswith("SNAPSHOT_RESPONSE"):
                _, payload = data.split(':', 1)
                status_data = json.loads(payload)
                with state_lock:
                    group_snapshot[status_data['name']] = status_data['status']
            else:
                handle_group_message(data)
        except Exception:
            pass
        finally:
            conn.close()


def listen_multicast():
    global super_coordinator_name, group_snapshot
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', MULTICAST_PORT))
    group = socket.inet_aton(MULTICAST_LISTEN_ADDR)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    print(f"{MY_NODE_NAME} - LISTENING MULTICAST IN {MULTICAST_LISTEN_ADDR}", flush=True)
    while True:
        data, _ = sock.recvfrom(1024)
        message = data.decode()

        if message.startswith("LEADER_ANNOUNCE"):
            _, candidate_leader = message.split(':')
            with state_lock:
                am_i_leader = (leader_name == MY_NODE_NAME)

                if am_i_leader:
                    new_super_coordinator = max(MY_NODE_NAME, candidate_leader)
                    if super_coordinator_name != new_super_coordinator:
                        super_coordinator_name = new_super_coordinator
                        print(f"🤝 [{MY_NODE_NAME}] SUPER COORDINATOR DEFINED: {super_coordinator_name}", flush=True)

                        if MY_NODE_NAME == super_coordinator_name:
                            send_multicast_message(f"SUPER_COORDINATOR_IS:{super_coordinator_name}")

        elif message.startswith("SUPER_COORDINATOR_IS"):
            _, new_super_coordinator = message.split(':')
            with state_lock:
                super_coordinator_name = new_super_coordinator

        elif message.startswith("GLOBAL_SNAPSHOT_REQUEST"):
            with state_lock:
                am_i_leader = (leader_name == MY_NODE_NAME)
            if am_i_leader:
                print(f"{MY_NODE_NAME} - SNAPSHOT ORDER RECEIVED", flush=True)
                group_snapshot = {}
                my_status = get_system_status()
                group_snapshot[MY_NODE_NAME] = my_status

                for node_name in MY_GROUP_NODES:
                    if node_name != MY_NODE_NAME: send_tcp_message(node_name, "SNAPSHOT_REQUEST")

                def send_group_result():
                    time.sleep(3)
                    send_multicast_message(f"GROUP_SNAPSHOT_RESULT:{json.dumps(group_snapshot)}")
                threading.Thread(target=send_group_result, daemon=True).start()
        elif message.startswith("GROUP_SNAPSHOT_RESULT"):
            _, payload = message.split(':', 1)
            other_group_data = json.loads(payload)
            with state_lock:
                group_snapshot.update(other_group_data)


# ==============================================================================
# === EXECUÇÃO PRINCIPAL =======================================================
# ==============================================================================
if __name__ == "__main__":
    print(f"STARTING NODE {MY_NODE_NAME} OF GROUP {GROUP_ID}", flush=True)

    threading.Thread(target=start_tcp_server, daemon=True).start()
    threading.Thread(target=listen_multicast, daemon=True).start()
    threading.Thread(target=coordinator_logic, daemon=True).start()

    start_specific_services()

    while True: time.sleep(60)