import socket
import sys
import time
from config_reader import read_peer_info, read_common_config


def peer_process(my_peer_id):
    # Call the parsign fucntions to get the values
    peers = read_peer_info()
    common = read_common_config()

    # Match the value passed as a parameter to the config value
    my_peer = next(p for p in peers if p.peer_id == my_peer_id)
    print(f"\n[Peer {my_peer_id}] Starting on {my_peer.host}:{my_peer.port}")
    print(f"[Peer {my_peer_id}] Has file: {my_peer.file_exist}")

    # Create a socket and bind it to the port form the config file
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((my_peer.host, my_peer.port))
    server_socket.listen(5)
    print(f"[Peer {my_peer_id}] Listening on port {my_peer.port}...")

    # If the peerid is higher than others connect to them
    connections = []
    for p in peers:
        if p.peer_id < my_peer_id:
            while True:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((p.host, p.port))
                    print(f"[Peer {my_peer_id}] Connected to Peer {p.peer_id}")
                    connections.append((p.peer_id, s))
                    break
                except ConnectionRefusedError:
                    # Keep trying until that peer's server is up
                    time.sleep(0.3)

    # If the peerid is lower than other start the connection
    for p in peers:
        if p.peer_id > my_peer_id:
            conn, addr = server_socket.accept()
            print(f"[Peer {my_peer_id}] Accepted connection from {addr}")
            connections.append((p.peer_id, conn))


if __name__ == "__main__":
    # Get the peer id from the terminal and log an error if it is invalid
    if len(sys.argv) != 2:
        print("Usage: python3 peerProcess.py <peerID>")
        sys.exit(1)

    try:
        peer_id = int(sys.argv[1])
    except ValueError:
        print("Peer ID must be an integer.")
        sys.exit(1)

    peer_process(peer_id)

