import os
import socket
import time
import threading

import psutil

NODE_ID = int(os.getenv('NODE_ID', '0'))
PORT = int(os.getenv('PORT', '5000'))
ALL_NODES_STR = os.getenv('ALL_NODES', '')
ALL_NODES = {int(addr.split(':')[0].split('-')[1]): (addr.split(':')[0], int(addr.split(':')[1])) for addr in ALL_NODES_STR.split(',')}

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
    server_threading = threading.Thread(target=start_server(), daemon=True)
    server_threading.start()

    time.sleep(5)

    if NODE_ID == 1:
        print('Testing')
        send_message(2, 'Test from 1')

    while True:
        status = get_system_status()
        time.sleep(10)