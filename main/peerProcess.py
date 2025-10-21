import socket
import sys
import time
import threading
import os
from datetime import datetime
from config_reader import read_peer_info, read_common_config


# Handles writing all peer-related events to a log file and printing to console
class Logger:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.log_file = f"log_peer_{peer_id}.log"
        self.lock = threading.Lock()
        # Initialize the log file (overwrite any old log)
        with open(self.log_file, 'w') as f:
            f.write(f"=== Log start for Peer {peer_id} ===\n")

    def log(self, message):
        """Thread-safe logging with timestamps"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] Peer {self.peer_id}: {message}"
        with self.lock:
            print(log_entry)
            with open(self.log_file, 'a') as f:
                f.write(log_entry + "\n")

    def connection_made(self, peer_id):
        self.log(f"Established connection with Peer {peer_id}.")

    def connection_accepted(self, addr):
        self.log(f"Accepted connection from {addr}.")

    def file_status(self, has_file):
        status = "has complete file" if has_file else "has no file initially"
        self.log(f"Initialization: {status}.")

    def shutdown(self):
        self.log("Shutting down.")


# Tracks which pieces of the file this peer currently has
class Bitfield:
    def __init__(self, num_pieces):
        self.num_pieces = num_pieces
        self.pieces = [False] * num_pieces
        self.lock = threading.Lock()

    def set_piece(self, index):
        """Mark a piece as downloaded"""
        with self.lock:
            if 0 <= index < self.num_pieces:
                self.pieces[index] = True

    def has_piece(self, index):
        """Return True if the peer has this piece"""
        with self.lock:
            return 0 <= index < self.num_pieces and self.pieces[index]

    def num_completed(self):
        """Count how many pieces we have"""
        with self.lock:
            return sum(1 for p in self.pieces if p)

    def is_complete(self):
        """Return True if the peer has all pieces"""
        with self.lock:
            return all(self.pieces)

    def to_bytes(self):
        """Serialize the bitfield into bytes"""
        with self.lock:
            return bytes([1 if p else 0 for p in self.pieces])

    def from_bytes(self, data):
        """Load bitfield from received bytes"""
        with self.lock:
            for i, b in enumerate(data):
                if i < self.num_pieces:
                    self.pieces[i] = (b == 1)


def peer_process(my_peer_id):
    # Call the parsing functions to get peer and common config data
    peers = read_peer_info()
    common = read_common_config()

    # Match the current peer ID to the config
    my_peer = next(p for p in peers if p.peer_id == my_peer_id)

    # Initialize logger
    logger = Logger(my_peer_id)
    logger.log(f"Starting on {my_peer.host}:{my_peer.port}")
    logger.file_status(my_peer.file_exist)

    # Create a directory for this peer's files if it doesn't exist
    peer_dir = f"peer_{my_peer_id}"
    os.makedirs(peer_dir, exist_ok=True)
    logger.log(f"Created or verified directory '{peer_dir}'.")

    # Initialize bitfield
    num_pieces = (common['FileSize'] + common['PieceSize'] - 1) // common['PieceSize']
    bitfield = Bitfield(num_pieces)
    if my_peer.file_exist:
        # If the peer starts with the full file, mark all pieces
        for i in range(num_pieces):
            bitfield.set_piece(i)
    logger.log(f"Bitfield initialized with {num_pieces} pieces.")

    # Create a socket and bind it to the port from the config file
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((my_peer.host, my_peer.port))
    server_socket.listen(5)
    logger.log(f"Listening on port {my_peer.port}...")

    # List to hold active socket connections
    connections = []

    # Connect to peers with smaller peer IDs
    for p in peers:
        if p.peer_id < my_peer_id:
            while True:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((p.host, p.port))
                    logger.connection_made(p.peer_id)
                    connections.append((p.peer_id, s))
                    break
                except ConnectionRefusedError:
                    # Keep retrying until peer’s listener is up
                    time.sleep(0.3)

    # Accept incoming connections from peers with higher IDs
    for p in peers:
        if p.peer_id > my_peer_id:
            conn, addr = server_socket.accept()
            logger.connection_accepted(addr)
            connections.append((p.peer_id, conn))

    # Still need to implement:
    # - Handshake exchange
    # - Message parsing and sending (bitfield, interested, have, etc.)
    # - Piece requests and transfers
    # - Choking/unchoking logic

    # Keep peer process alive (simulating a running node)
    try:
        while True:
            time.sleep(2)
            # Example: Log progress periodically
            if bitfield.is_complete():
                logger.log("Download complete.")
                break
    except KeyboardInterrupt:
        logger.shutdown()
    finally:
        server_socket.close()
        for _, s in connections:
            s.close()
        logger.log("Connections closed. Peer exiting.")


if __name__ == "__main__":
    # Get the peer ID from the terminal and log an error if it is invalid
    if len(sys.argv) != 2:
        print("Usage: python3 peerProcess.py <peerID>")
        sys.exit(1)

    try:
        peer_id = int(sys.argv[1])
    except ValueError:
        print("Peer ID must be an integer.")
        sys.exit(1)

    peer_process(peer_id)
