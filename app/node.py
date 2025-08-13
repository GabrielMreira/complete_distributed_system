import os
import socket
import time
import threading

import grpc
from concurrent import futures
import psutil

from generated import resources_pb2_grpc
from generated import resources_pb2

NODE_ID = int(os.getenv('NODE_ID', '0'))
PORT = int(os.getenv('PORT', '5000'))
GRPC_PORT = int(os.getenv('GRPC_PORT', '6000'))
ALL_NODES_STR = os.getenv('ALL_NODES', '')
ALL_NODES = {int(addr.split(':')[0].split('-')[1]): (addr.split(':')[0], int(addr.split(':')[1])) for addr in ALL_NODES_STR.split(',')}
ALL_GRPC_NODES_STR = os.getenv('ALL_GRPC_NODES', '')
ALL_GRPC_NODES = {int(addr.split(':')[0].split('-')[1]): addr for addr in ALL_GRPC_NODES_STR.split(',')}

print(f"NODE {NODE_ID} STARTED")
print(f"ID: {NODE_ID} - PORT: {PORT}")
print(f"ALL NODES: {ALL_NODES}")


def get_system_status():
    return {
        'cpu_percent_usage': psutil.cpu_percent(interval=1),
        'memory_percent_usage': psutil.virtual_memory().percent,
        'uptime_seconds': time.time() - psutil.boot_time()
    }

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', PORT))
    server_socket.listen(5)

    print(f"NODE: {NODE_ID} LISTENING ON PORT {PORT}")

    while True:
        conn, addr = server_socket.accept()
        data = conn.recv(1024)
        print(f"{NODE_ID} - MESSAGE RECEIVED FROM {addr}: {data.decode()}")
        conn.close()

class ResourceMonitorService(resources_pb2_grpc.ResourceMonitorServicer):
    def GetStatus(self, request, context):
        print(f'NODE {NODE_ID} - GET STATUS GRPC REQUEST RECEIVED')
        status = get_system_status()
        return resources_pb2.StatusResponse(
            cpu_percent_usage = status['cpu_percent_usage'],
            memory_percent_usage = status['memory_percent_usage'],
            uptime_seconds = status['uptime_seconds']
        )

def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    resources_pb2_grpc.add_ResourceMonitorServicer_to_server(ResourceMonitorService(), server)
    server.add_insecure_port(f'[::]:{GRPC_PORT}')
    server.start()
    print(f'NODE {NODE_ID} LISTENING GRPC ON PORT {GRPC_PORT}')
    server.wait_for_termination()


def get_node_status_grpc(target_node_id):
    target_address = ALL_GRPC_NODES.get(target_node_id)
    if not target_address:
        print(f'INVALID TARGET: {target_node_id}')
        return

    try:
        with grpc.insecure_channel(target_address) as channel:
            stub = resources_pb2_grpc.ResourceMonitorStub(channel)
            request = resources_pb2.StatusRequest()
            response = stub.GetStatus(request, timeout=2)
            return response
    except grpc.RpcError as e:
        print(f'NODE {NODE_ID} ERRO CALLING {target_node_id}: {str(e)}')
        return None


def send_message(target_node_id, message):
    target_host, target_port = ALL_NODES[target_node_id]

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((target_host, target_port))
        client_socket.sendall(message.encode())
        print(f'NODE {NODE_ID} SENDING MESSAGE TO {target_node_id}: {message}')
    except ConnectionRefusedError:
        print(f'CONNECTING NODE {NODE_ID} FAILED')
    except Exception as e:
        print(f'ERROR SENDING MESSAGE TO {NODE_ID}: {str(e)}')
    finally:
        client_socket.close()

if __name__ == "__main__":
    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    grpc_server_thread = threading.Thread(target=start_grpc_server, daemon=True)
    grpc_server_thread.start()

    time.sleep(10)

    if NODE_ID == 1:
        print("\nTESTING COLLECTING RECOURSES")
        for node_id in range(1, 4):
            if node_id != NODE_ID:
                print(f"REQUESTING STATUS FROM-{node_id}...")
                status = get_node_status_grpc(node_id)
                if status:
                    print(f" STATUS RECEIVED FROM -{node_id}: "
                          f"CPU: {status.cpu_usage_percent:.2f}%, "
                          f"Memory: {status.memory_usage_percent:.2f}%, "
                          f"Uptime: {status.uptime_seconds:.0f}s")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nNODE-{NODE_ID} DOWN.")