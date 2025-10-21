import socket
import sys
import time
import threading
import os
import struct
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
        log_entry = f"[{timestamp}] Peer {self.peer_id} {message}"
        with self.lock:
            print(log_entry)
            with open(self.log_file, 'a') as f:
                f.write(log_entry + "\n")

    def connection_made(self, peer_id):
        self.log(f"makes a connection to Peer {peer_id}.")

    def connection_accepted(self, peer_id):
        self.log(f"is connected from Peer {peer_id}.")
    
    def choked_by(self, peer_id):
        self.log(f"is choked by {peer_id}")
    
    def unchoked_by(self, peer_id):
        self.log(f"is unchoked by {peer_id}")
    
    def received_interested(self, peer_id):
        self.log(f"received the 'interested' message from {peer_id}")
    
    def received_not_interested(self, peer_id):
        self.log(f"received the 'not interested' message from {peer_id}")
    
    def received_have(self, peer_id, piece_index):
        self.log(f"received the 'have' message from {peer_id} for the piece {piece_index}")
    
    def downloaded_piece(self, piece_index, from_peer_id, total_pieces):
        self.log(f"has downloaded the piece {piece_index} from {from_peer_id}. Now the number of pieces it has is {total_pieces}")
    
    def download_complete(self):
        self.log(f"has downloaded the complete file.")

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


# Message types as defined in the protocol
class MessageType:
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7


# Handshake message class
class HandshakeMessage:
    HEADER = b'P2PFILESHARINGPROJ'
    HEADER_LENGTH = 18
    ZERO_BITS_LENGTH = 10
    PEER_ID_LENGTH = 4
    TOTAL_LENGTH = 32
    
    def __init__(self, peer_id):
        self.peer_id = peer_id
    
    def to_bytes(self):
        """Serialize handshake message to bytes"""
        header = self.HEADER
        zero_bits = b'\x00' * self.ZERO_BITS_LENGTH
        peer_id_bytes = struct.pack('>I', self.peer_id)  # Big-endian 4-byte integer
        return header + zero_bits + peer_id_bytes
    
    @classmethod
    def from_bytes(cls, data):
        """Parse handshake message from bytes"""
        if len(data) != cls.TOTAL_LENGTH:
            raise ValueError(f"Invalid handshake length: {len(data)}")
        
        header = data[:cls.HEADER_LENGTH]
        if header != cls.HEADER:
            raise ValueError(f"Invalid handshake header: {header}")
        
        zero_bits = data[cls.HEADER_LENGTH:cls.HEADER_LENGTH + cls.ZERO_BITS_LENGTH]
        if zero_bits != b'\x00' * cls.ZERO_BITS_LENGTH:
            raise ValueError("Invalid zero bits in handshake")
        
        peer_id_bytes = data[cls.HEADER_LENGTH + cls.ZERO_BITS_LENGTH:]
        peer_id = struct.unpack('>I', peer_id_bytes)[0]
        
        return cls(peer_id)


# Base message class for all protocol messages
class Message:
    def __init__(self, message_type, payload=b''):
        self.message_type = message_type
        self.payload = payload
    
    def to_bytes(self):
        """Serialize message to bytes with length prefix"""
        message_length = 1 + len(self.payload)  # Payload length (bytes) + 1 byte for message type
        length_bytes = struct.pack('>I', message_length)  # 4-byte message length field
        type_bytes = struct.pack('B', self.message_type)  # 1-byte message type field
        return length_bytes + type_bytes + self.payload
    
    @classmethod
    def from_bytes(cls, data):
        """Parse message from bytes"""
        if len(data) < 5:  # Should always have at least 5 bytes
            raise ValueError("Message too short")
        
        message_length = struct.unpack('>I', data[:4])[0]
        message_type = struct.unpack('B', data[4:5])[0]
        payload = data[5:5+message_length-1] if message_length > 1 else b''
        
        return cls(message_type, payload)


# Message classes for each message type 
class ChokeMessage(Message):
    def __init__(self):
        super().__init__(MessageType.CHOKE)


class UnchokeMessage(Message):
    def __init__(self):
        super().__init__(MessageType.UNCHOKE)


class InterestedMessage(Message):
    def __init__(self):
        super().__init__(MessageType.INTERESTED)


class NotInterestedMessage(Message):
    def __init__(self):
        super().__init__(MessageType.NOT_INTERESTED)


class HaveMessage(Message):
    def __init__(self, piece_index):
        payload = struct.pack('>I', piece_index)
        super().__init__(MessageType.HAVE, payload)
    
    @classmethod
    def from_message(cls, message):
        if message.message_type != MessageType.HAVE:
            raise ValueError("Not a HAVE message")
        piece_index = struct.unpack('>I', message.payload)[0]
        return cls(piece_index)


class BitfieldMessage(Message):
    def __init__(self, bitfield_data):
        super().__init__(MessageType.BITFIELD, bitfield_data)
    
    @classmethod
    def from_message(cls, message):
        if message.message_type != MessageType.BITFIELD:
            raise ValueError("Not a BITFIELD message")
        return cls(message.payload)


class RequestMessage(Message):
    def __init__(self, piece_index):
        payload = struct.pack('>I', piece_index)
        super().__init__(MessageType.REQUEST, payload)
    
    @classmethod
    def from_message(cls, message):
        if message.message_type != MessageType.REQUEST:
            raise ValueError("Not a REQUEST message")
        piece_index = struct.unpack('>I', message.payload)[0]
        return cls(piece_index)


class PieceMessage(Message):
    def __init__(self, piece_index, piece_data):
        payload = struct.pack('>I', piece_index) + piece_data
        super().__init__(MessageType.PIECE, payload)
    
    @classmethod
    def from_message(cls, message):
        if message.message_type != MessageType.PIECE:
            raise ValueError("Not a PIECE message")
        piece_index = struct.unpack('>I', message.payload[:4])[0]
        piece_data = message.payload[4:]
        return cls(piece_index, piece_data)


# Message handler for parsing incoming messages
class MessageHandler:
    def __init__(self, logger):
        self.logger = logger
    
    def send_handshake(self, socket, peer_id):
        """Send handshake message"""
        handshake = HandshakeMessage(peer_id)
        socket.send(handshake.to_bytes())
    
    def receive_handshake(self, socket):
        """Receive and parse handshake message"""
        data = socket.recv(HandshakeMessage.TOTAL_LENGTH)
        if len(data) != HandshakeMessage.TOTAL_LENGTH:
            raise ValueError(f"Invalid handshake length in receive_handshake: {len(data)}")
        
        handshake = HandshakeMessage.from_bytes(data)
        return handshake
    
    def send_message(self, socket, message):
        """Send a protocol message"""
        data = message.to_bytes()
        socket.send(data)
    
    def receive_message(self, socket):
        """Receive and parse a protocol message"""
        # First, read the 4-byte length field
        length_data = socket.recv(4)
        if len(length_data) != 4:
            raise ValueError("Could not read message length in receive_message")
        
        message_length = struct.unpack('>I', length_data)[0]
        
        # Then read the rest of the message
        remaining_data = socket.recv(message_length)
        if len(remaining_data) != message_length:
            raise ValueError(f"Could not read complete message in receive_message: expected {message_length}, got {len(remaining_data)}")
        
        # Parse the complete message
        full_data = length_data + remaining_data
        message = Message.from_bytes(full_data)
        return message
    
    # Send message functions for each message type
    def send_bitfield(self, socket, bitfield):
        """Send bitfield message"""
        bitfield_data = bitfield.to_bytes()
        message = BitfieldMessage(bitfield_data)
        self.send_message(socket, message)
    
    def send_have(self, socket, piece_index):
        """Send have message"""
        message = HaveMessage(piece_index)
        self.send_message(socket, message)
    
    def send_interested(self, socket):
        """Send interested message"""
        message = InterestedMessage()
        self.send_message(socket, message)
    
    def send_not_interested(self, socket):
        """Send not interested message"""
        message = NotInterestedMessage()
        self.send_message(socket, message)
    
    def send_request(self, socket, piece_index):
        """Send request message"""
        message = RequestMessage(piece_index)
        self.send_message(socket, message)
    
    def send_piece(self, socket, piece_index, piece_data):
        """Send piece message"""
        message = PieceMessage(piece_index, piece_data)
        self.send_message(socket, message)
    
    def send_choke(self, socket):
        """Send choke message"""
        message = ChokeMessage()
        self.send_message(socket, message)
    
    def send_unchoke(self, socket):
        """Send unchoke message"""
        message = UnchokeMessage()
        self.send_message(socket, message)


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

    # Initialize message handler
    message_handler = MessageHandler(logger)
    
    # List to hold active socket connections
    connections = []
    connection_info = {}  # Store connection info for each peer

    # Connect to peers with smaller peer IDs
    for p in peers:
        if p.peer_id < my_peer_id:
            while True:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((p.host, p.port))
                    logger.connection_made(p.peer_id)
                    
                    # Perform handshake
                    message_handler.send_handshake(s, my_peer_id)
                    received_handshake = message_handler.receive_handshake(s)
                    
                    connections.append((p.peer_id, s))
                    connection_info[p.peer_id] = {
                        'socket': s,
                        'choked': True,
                        'interested': False,
                        'bitfield': None
                    }
                    break
                except ConnectionRefusedError:
                    # Keep retrying until peer’s listener is up
                    time.sleep(0.3)

    # Accept incoming connections from peers with higher IDs
    for p in peers:
        if p.peer_id > my_peer_id:
            conn, addr = server_socket.accept()
            logger.connection_accepted(p.peer_id)
            
            # Perform handshake
            received_handshake = message_handler.receive_handshake(conn)
            message_handler.send_handshake(conn, my_peer_id)
            
            connections.append((p.peer_id, conn))
            connection_info[p.peer_id] = {
                'socket': conn,
                'choked': True,
                'interested': False,
                'bitfield': None
            }

    # Exchange bitfields with all connected peers
    for peer_id, sock in connections:
        try:
            # Send our bitfield if we have any pieces
            if bitfield.num_completed() > 0:
                message_handler.send_bitfield(sock, bitfield)
            
            # Receive bitfield from peer
            try:
                message = message_handler.receive_message(sock)
                if message.message_type == MessageType.BITFIELD:
                    bitfield_msg = BitfieldMessage.from_message(message)
                    peer_bitfield = Bitfield(num_pieces)
                    peer_bitfield.from_bytes(bitfield_msg.payload)
                    connection_info[peer_id]['bitfield'] = peer_bitfield
                    
                    # Check if peer has interesting pieces
                    has_interesting = False
                    for i in range(num_pieces):
                        if peer_bitfield.has_piece(i) and not bitfield.has_piece(i):
                            has_interesting = True
                            break
                    
                    if has_interesting:
                        message_handler.send_interested(sock)
                        connection_info[peer_id]['interested'] = True
                    else:
                        message_handler.send_not_interested(sock)
                else:
                    raise ValueError(f"Expected bitfield from peer {peer_id}, got message type {message.message_type}")
            except Exception as e:
                raise ValueError(f"Error receiving bitfield from peer {peer_id}: {e}")
        except Exception as e:
            raise ValueError(f"Error exchanging bitfield with peer {peer_id}: {e}")

    # Main message handling loop
    def handle_messages():
        """Handle incoming messages from all peers"""
        # TODO: Make logging messages begin with "[TIME]:"
        while True:
            for peer_id, sock in connections:
                try:
                    sock.settimeout(0.1)
                    message = message_handler.receive_message(sock)
                    
                    if message.message_type == MessageType.CHOKE:
                        connection_info[peer_id]['choked'] = True
                        logger.choked_by(peer_id)
                    
                    elif message.message_type == MessageType.UNCHOKE:
                        connection_info[peer_id]['choked'] = False
                        logger.unchoked_by(peer_id)
                    
                    elif message.message_type == MessageType.INTERESTED:
                        connection_info[peer_id]['interested'] = True
                        logger.received_interested(peer_id)
                    
                    elif message.message_type == MessageType.NOT_INTERESTED:
                        connection_info[peer_id]['interested'] = False
                        logger.received_not_interested(peer_id)
                    
                    elif message.message_type == MessageType.HAVE:
                        have_msg = HaveMessage.from_message(message)
                        piece_index = have_msg.payload
                        logger.received_have(peer_id, piece_index)
                        
                        # Update peer's bitfield
                        if connection_info[peer_id]['bitfield']:
                            connection_info[peer_id]['bitfield'].set_piece(piece_index)
                    
                    elif message.message_type == MessageType.REQUEST:
                        request_msg = RequestMessage.from_message(message)
                        piece_index = request_msg.payload
                        
                        # Send piece if we have it and peer is not choked
                        if bitfield.has_piece(piece_index) and not connection_info[peer_id]['choked']:
                            # TODO: Send piece to peer
                            pass
                    
                    elif message.message_type == MessageType.PIECE:
                        piece_msg = PieceMessage.from_message(message)
                        piece_index = piece_msg.payload[:4]
                        piece_data = piece_msg.payload[4:]
                        
                        # TODO: Save piece to file 

                        # Update bitfield
                        bitfield.set_piece(piece_index)
                        logger.downloaded_piece(piece_index, peer_id, bitfield.num_completed())
                        
                except socket.timeout:
                    # No message available, continue to next peer
                    continue
                except Exception as e:
                    break
            
            # Check if download is complete
            if bitfield.is_complete():
                logger.download_complete()
                break
            
            time.sleep(0.1)  # Small delay to prevent busy waiting

    # Start message handling in a separate thread
    message_thread = threading.Thread(target=handle_messages, daemon=True)
    message_thread.start()

    # Keep peer process alive (simulating a running node)
    try:
        while True:
            time.sleep(2)
            # Example: Log progress periodically
            if bitfield.is_complete():
                logger.download_complete()
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
