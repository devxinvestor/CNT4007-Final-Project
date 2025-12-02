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

    def has_interesting(self, other):
        """Return True if other has pieces we don't"""
        with self.lock:
            for i in range(self.num_pieces):
                if other.has_piece(i) and not self.pieces[i]:
                    return True
            return False

    def missing_pieces(self):
        """Return list of indices we do not have"""
        with self.lock:
            return [i for i in range(self.num_pieces) if not self.pieces[i]]


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
        # return the message (caller can unpack payload)
        return message


class BitfieldMessage(Message):
    def __init__(self, bitfield_data):
        super().__init__(MessageType.BITFIELD, bitfield_data)
    
    @classmethod
    def from_message(cls, message):
        if message.message_type != MessageType.BITFIELD:
            raise ValueError("Not a BITFIELD message")
        return message


class RequestMessage(Message):
    def __init__(self, piece_index):
        payload = struct.pack('>I', piece_index)
        super().__init__(MessageType.REQUEST, payload)
    
    @classmethod
    def from_message(cls, message):
        if message.message_type != MessageType.REQUEST:
            raise ValueError("Not a REQUEST message")
        return message


class PieceMessage(Message):
    def __init__(self, piece_index, piece_data):
        payload = struct.pack('>I', piece_index) + piece_data
        super().__init__(MessageType.PIECE, payload)
    
    @classmethod
    def from_message(cls, message):
        if message.message_type != MessageType.PIECE:
            raise ValueError("Not a PIECE message")
        return message


# Message handler for parsing incoming messages
class MessageHandler:
    def __init__(self, logger):
        self.logger = logger
    
    def send_handshake(self, sock, peer_id):
        """Send handshake message"""
        handshake = HandshakeMessage(peer_id)
        sock.sendall(handshake.to_bytes())
    
    def receive_handshake(self, sock):
        """Receive and parse handshake message"""
        data = self.recv_exact(sock, HandshakeMessage.TOTAL_LENGTH)
        if len(data) != HandshakeMessage.TOTAL_LENGTH:
            raise ValueError(f"Invalid handshake length in receive_handshake: {len(data)}")
        
        handshake = HandshakeMessage.from_bytes(data)
        return handshake
    
    def send_message(self, sock, message):
        """Send a protocol message"""
        data = message.to_bytes()
        sock.sendall(data)
    
    def receive_message(self, sock):
        """Receive and parse a protocol message. Returns None on EOF."""
        try:
            length_data = self.recv_exact(sock, 4)
        except ConnectionError:
            return None
        if len(length_data) != 4:
            return None
        
        message_length = struct.unpack('>I', length_data)[0]
        try:
            remaining = self.recv_exact(sock, message_length)
        except ConnectionError:
            return None
        if len(remaining) != message_length:
            return None
        
        full = length_data + remaining
        msg = Message.from_bytes(full)
        return msg
    
    def recv_exact(self, sock, n):
        """Receive exactly n bytes from socket or raise ConnectionError"""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Socket connection broken")
            data += chunk
        return data
    
    # Send message functions for each message type
    def send_bitfield(self, sock, bitfield):
        """Send bitfield message"""
        bitfield_data = bitfield.to_bytes()
        message = BitfieldMessage(bitfield_data)
        self.send_message(sock, message)
    
    def send_have(self, sock, piece_index):
        """Send have message"""
        message = HaveMessage(piece_index)
        self.send_message(sock, message)
    
    def send_interested(self, sock):
        """Send interested message"""
        message = InterestedMessage()
        self.send_message(sock, message)
    
    def send_not_interested(self, sock):
        """Send not interested message"""
        message = NotInterestedMessage()
        self.send_message(sock, message)
    
    def send_request(self, sock, piece_index):
        """Send request message"""
        message = RequestMessage(piece_index)
        self.send_message(sock, message)
    
    def send_piece(self, sock, piece_index, piece_data):
        """Send piece message"""
        message = PieceMessage(piece_index, piece_data)
        self.send_message(sock, message)
    
    def send_choke(self, sock):
        """Send choke message"""
        message = ChokeMessage()
        self.send_message(sock, message)
    
    def send_unchoke(self, sock):
        """Send unchoke message"""
        message = UnchokeMessage()
        self.send_message(sock, message)


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
                        'choked': False,           # simplified model: not using choking logic yet
                        'interested': False,
                        'bitfield': None,
                        'requested': None          # track one outstanding request per peer
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
                'choked': False,           # simplified model
                'interested': False,
                'bitfield': None,
                'requested': None
            }

    # Helper functions for file I/O and broadcasting
    def _ensure_placeholder():
        """Create placeholder file in peer_dir if missing"""
        file_path = os.path.join(peer_dir, common['FileName'])
        if not os.path.exists(file_path):
            try:
                total_size = common['FileSize']
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * total_size)
            except Exception as e:
                logger.log(f"Error creating placeholder file: {e}")

    def _save_piece(piece_index, piece_data):
        """Save received piece into the file (creates placeholder file if necessary)"""
        try:
            file_path = os.path.join(peer_dir, common['FileName'])
            piece_size = common['PieceSize']
            total_size = common['FileSize']
            # Create placeholder file if missing
            if not os.path.exists(file_path):
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * total_size)
            # Write piece at offset
            with open(file_path, 'r+b') as f:
                offset = piece_index * piece_size
                f.seek(offset)
                f.write(piece_data)
        except Exception as e:
            logger.log(f"Error saving piece {piece_index}: {e}")

    def _read_piece(piece_index):
        """Read a piece from file (if exists)"""
        try:
            file_path = os.path.join(peer_dir, common['FileName'])
            piece_size = common['PieceSize']
            with open(file_path, 'rb') as f:
                f.seek(piece_index * piece_size)
                return f.read(piece_size)
        except Exception as e:
            logger.log(f"Error reading piece {piece_index}: {e}")
            return None

    def _broadcast_have(piece_index):
        """Broadcast a HAVE message to all connected peers"""
        for pid, sock in connections:
            try:
                message_handler.send_have(sock, piece_index)
            except Exception as e:
                logger.log(f"Error broadcasting have to peer {pid}: {e}")

    # After connecting, exchange bitfields with all connected peers
    for peer_id, sock in connections:
        try:
            # Send our bitfield if we have any pieces
            if bitfield.num_completed() > 0:
                message_handler.send_bitfield(sock, bitfield)
            
            # Receive bitfield from peer
            try:
                message = message_handler.receive_message(sock)
                if message is None:
                    raise ValueError("No message received when expecting bitfield")
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
                    # If peer did not send bitfield (allowed if they have no pieces), handle gracefully
                    # We'll not force an error; continue with empty bitfield
                    connection_info[peer_id]['bitfield'] = Bitfield(num_pieces)
            except Exception as e:
                # If bitfield exchange fails, keep going but log
                logger.log(f"Error receiving bitfield from peer {peer_id}: {e}")
                connection_info[peer_id]['bitfield'] = Bitfield(num_pieces)
        except Exception as e:
            logger.log(f"Error exchanging bitfield with peer {peer_id}: {e}")

    # Main message handling loop
    def handle_messages():
        """Handle incoming messages from all peers"""
        while True:
            for peer_id, sock in connections:
                try:
                    sock.settimeout(0.1)
                    message = message_handler.receive_message(sock)
                    if message is None:
                        continue
                    
                    if message.message_type == MessageType.CHOKE:
                        connection_info[peer_id]['choked'] = True
                        logger.choked_by(peer_id)
                    
                    elif message.message_type == MessageType.UNCHOKE:
                        connection_info[peer_id]['choked'] = False
                        logger.unchoked_by(peer_id)
                        # When unchoked, request a piece if available
                        # choose a piece randomly that peer has and we don't
                        peer_bf = connection_info[peer_id]['bitfield']
                        if peer_bf:
                            candidates = []
                            for i in range(num_pieces):
                                if peer_bf.has_piece(i) and not bitfield.has_piece(i):
                                    candidates.append(i)
                            if candidates and connection_info[peer_id]['requested'] is None:
                                choice = candidates[int(time.time() * 1000) % len(candidates)]
                                try:
                                    message_handler.send_request(sock, choice)
                                    connection_info[peer_id]['requested'] = choice
                                except Exception as e:
                                    logger.log(f"Error sending request to peer {peer_id}: {e}")
                    
                    elif message.message_type == MessageType.INTERESTED:
                        connection_info[peer_id]['interested'] = True
                        logger.received_interested(peer_id)
                    
                    elif message.message_type == MessageType.NOT_INTERESTED:
                        connection_info[peer_id]['interested'] = False
                        logger.received_not_interested(peer_id)
                    
                    elif message.message_type == MessageType.HAVE:
                        # unpack piece index from payload
                        piece_index = struct.unpack('>I', message.payload)[0]
                        logger.received_have(peer_id, piece_index)
                        
                        # Update peer's bitfield
                        if connection_info[peer_id]['bitfield'] is None:
                            connection_info[peer_id]['bitfield'] = Bitfield(num_pieces)
                        connection_info[peer_id]['bitfield'].set_piece(piece_index)
                        
                        # If we don't have it, signal interest
                        if not bitfield.has_piece(piece_index):
                            try:
                                message_handler.send_interested(sock)
                                connection_info[peer_id]['interested'] = True
                            except Exception as e:
                                logger.log(f"Error sending interested to peer {peer_id}: {e}")
                    
                    elif message.message_type == MessageType.REQUEST:
                        # Peer requests a piece from us
                        piece_index = struct.unpack('>I', message.payload)[0]
                        # In simplified model we always upload if we have the piece
                        if bitfield.has_piece(piece_index):
                            data = None
                            _ensure_placeholder()
                            data = _read_piece(piece_index)
                            if data is not None:
                                try:
                                    message_handler.send_piece(sock, piece_index, data)
                                except Exception as e:
                                    logger.log(f"Error sending piece to peer {peer_id}: {e}")
                    
                    elif message.message_type == MessageType.PIECE:
                        # receive piece payload and save
                        piece_index = struct.unpack('>I', message.payload[:4])[0]
                        piece_data = message.payload[4:]
                        
                        # Save the piece
                        _save_piece(piece_index, piece_data)
                        
                        # Update bitfield
                        bitfield.set_piece(piece_index)
                        logger.downloaded_piece(piece_index, peer_id, bitfield.num_completed())
                        
                        # Clear requested marker for this peer if it matches
                        if connection_info[peer_id]['requested'] == piece_index:
                            connection_info[peer_id]['requested'] = None
                        
                        # Broadcast HAVE to all peers
                        _broadcast_have(piece_index)
                        
                except socket.timeout:
                    # No message available, continue to next peer
                    continue
                except Exception:
                    # If a peer disconnects or any other error happens, ignore and continue
                    continue
            
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
            # If we become complete, break and allow graceful shutdown
            if bitfield.is_complete():
                logger.download_complete()
                break
                
    except KeyboardInterrupt:
        logger.shutdown()
    finally:
        server_socket.close()
        for _, s in connections:
            try:
                s.close()
            except:
                pass
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
