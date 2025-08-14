import socket
import struct
import json
from cryptography.fernet import Fernet
import os

SECRET_KEY_STR = ""

MULTICAST_GROUP = '224.1.1.1'
MULTICAST_PORT = 8888
SERVER_ADDRESS = ('', MULTICAST_PORT)

try:
    SECRET_KEY = SECRET_KEY_STR.encode()
    cipher_suite = Fernet(SECRET_KEY)
except (ValueError, TypeError):
    print("ERROR: SECRET KEY INVALID.")
    print("PLEASE COPY THE SECRET KEY AND PAST ON SECRET_KEY_STR.")
    exit(1)


def decrypt_message(encrypted_data):
    try:
        decrypted_data = cipher_suite.decrypt(encrypted_data)
        return json.loads(decrypted_data.decode('utf-8'))
    except Exception as e:
        return None


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    sock.bind(SERVER_ADDRESS)

    group = socket.inet_aton(MULTICAST_GROUP)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"LISTENING MULTCAST ON {MULTICAST_GROUP}:{MULTICAST_PORT}")
    print("WAITING REPORT FROM LEADER")

    while True:
        data, address = sock.recvfrom(4096)  # Buffer de 4KB

        decrypted_report = decrypt_message(data)

        if decrypted_report:
            os.system('cls' if os.name == 'nt' else 'clear')  # Limpa o terminal
            print(f"REPORT FROM DS")
            print(f"RECEIVED AT: {decrypted_report.get('timestamp')}")
            print(f"LEADER: NODE-{decrypted_report.get('leader_id')}\n")

            states = decrypted_report.get('global_state', {})
            for node_id, status in sorted(states.items(), key=lambda item: int(item[0])):
                print(f"[NODE-{node_id}]")
                print(f"CPU: {status.get('cpu_usage_percent', 'N/A'):.1f}% | "
                      f"MEMORY: {status.get('memory_usage_percent', 'N/A'):.1f}% | "
                      f"Uptime: {status.get('uptime_seconds', 'N/A')}s | "
                      f"Clock: {status.get('lamport_clock', 'N/A')}")


if __name__ == '__main__':
    main()