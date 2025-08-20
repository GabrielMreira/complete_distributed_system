import os
import socket
import threading
import time
import json
import struct

GROUP_ID = os.getenv('GROUP_ID', 'A')
NODE_ID = int(os.getenv('NODE_ID', '0'))
ALL_NODES_STR = os.getenv('ALL_NODES', '')

ALL_NODES = {}
TCP_PORT = 0

for node in ALL_NODES_STR.split(','):
    name, address = node.split(':')
    _, group, _, node_num = name.split('-')

    node_info = {
        "name": name,
        "group": group,
        "id": int(node_num),
        "host": name,
        "port": int(address)
    }
    ALL_NODES[name] = node_info

    if group.upper() == GROUP_ID and int(node_num) == NODE_ID:
        TCP_PORT = int(address)

NODE_NAME = f"g-{GROUP_ID.lower()}-node-{NODE_ID}"
MY_GROUP_NODES = {name: info for name, info in ALL_NODES.items() if info['group'] == GROUP_ID}

MULTICAST_GROUP_A_ADDR = '224.1.1.1'
MULTICAST_GROUP_B_ADDR = '224.2.2.2'
MULTICAST_PORT = 10000

MULTICAST_LISTEN_ADDR = MULTICAST_GROUP_A_ADDR if GROUP_ID == 'A' else MULTICAST_GROUP_B_ADDR
MULTICAST_SEND_ADDR = MULTICAST_GROUP_B_ADDR if GROUP_ID == 'A' else MULTICAST_GROUP_A_ADDR

def start_tcp_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', TCP_PORT))
    server_socket.listen(5)

    print(f"{NODE_NAME} - LISTENING ON PORT: {TCP_PORT}", flush=True)

    while True:
        conn, addr = server_socket.accept()
        data = conn.recv(1024).decode()
        print(f"{NODE_NAME} - MESSAGE RECEIVED FROM {addr}: {data}", flush=True)
        conn.close()

def listen_multicast():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', MULTICAST_PORT))

    group = socket.inet_aton(MULTICAST_LISTEN_ADDR)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"{NODE_NAME} - LISTENING MULTICAST ON {MULTICAST_LISTEN_ADDR}: {MULTICAST_PORT}", flush=True)

    while True:
        data, addr = sock.recvfrom(1024)
        message = data.decode()
        print(f"{NODE_NAME} - MULTICAST MESSAGE RECEIVED FROM {addr}: {message}", flush=True)

def send_multicast_message(message : str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    full_message = f"HELLO FROM {NODE_NAME}: {message}"
    sock.sendto(full_message.encode(), (MULTICAST_SEND_ADDR, MULTICAST_PORT))
    print(f"{NODE_NAME} - SENDING MULTICAST MESSAGE TO {MULTICAST_SEND_ADDR}", flush=True)

def run_tests():
    time.sleep(10)

    if NODE_ID == 1:
        print(f"{NODE_NAME} - STARTING MULTICAST TEST", flush=True)
        send_multicast_message("TESTING INTER-GROUP COMMUNICATION")
        print(f"{NODE_NAME} - END TEST", flush=True)


    time.sleep(5)
    print(f"{NODE_NAME} - STARTING TCP TEST INTRA-GROUP", flush=True)
    for name, info in MY_GROUP_NODES.items():
        if name != NODE_NAME:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((info['host'], info['port']))
                    s.sendall(f"HELLO FROM {NODE_NAME}".encode())
            except Exception as e:
                print(f"[{NODE_NAME}] FAIL SEND TCP TO {name}: {e}", flush=True)
    print(f"{NODE_NAME} - END TCP TEST", flush=True)


if __name__ == "__main__":
    print(f"STARTING NODE: {NODE_NAME}", flush=True)
    print(f"MEMBERS OF GROUP ({GROUP_ID}): {[name for name in MY_GROUP_NODES.keys()]}", flush=True)

    threading.Thread(target=start_tcp_server, daemon=True).start()
    threading.Thread(target=listen_multicast, daemon=True).start()

    threading.Thread(target=run_tests, daemon=True).start()

    while True:
        time.sleep(60)



