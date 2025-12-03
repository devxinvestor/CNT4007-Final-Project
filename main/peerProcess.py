import socket
import sys
import time
import threading
import os
import struct
import random
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
        self.log(f"is choked by {peer_id}.")
    
    def unchoked_by(self, peer_id):
        self.log(f"is unchoked by {peer_id}.")
    
    def received_interested(self, peer_id):
        self.log(f"received the 'interested' message from {peer_id}.")
    
    def received_not_interested(self, peer_id):
        self.log(f"received the 'not interested' message from {peer_id}.")
    
    def received_have(self, peer_id, piece_index):
        self.log(f"received the 'have' message from {peer_id} for the piece {piece_index}.")
    
    def downloaded_piece(self, piece_index, from_peer_id, total_pieces):
        self.log(f"has downloaded the piece {piece_index} from {from_peer_id}. Now the number of pieces it has is {total_pieces}.")
    
    def download_complete(self):
        self.log(f"has downloaded the complete file.")
    
    def preferred_neighbors(self, neighbor_list):
        """Log preferred neighbors change"""
        neighbor_str = ','.join(map(str, neighbor_list))
        self.log(f"has the preferred neighbors {neighbor_str}.")
    
    def optimistic_unchoked(self, peer_id):
        """Log optimistically unchoked neighbor change"""
        self.log(f"has the optimistically unchoked neighbor {peer_id}.")



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
            # Calculate number of bytes needed (round up)
            num_bytes = (self.num_pieces + 7) // 8
            result = bytearray(num_bytes)
            for i in range(self.num_pieces):
                if self.pieces[i]:
                    byte_idx = i // 8
                    bit_idx = 7 - (i % 8)  # High bit to low bit (0-7)
                    result[byte_idx] |= (1 << bit_idx)
            return bytes(result)

    def from_bytes(self, data):
        """Load bitfield from received bytes"""
        with self.lock:
            for i in range(self.num_pieces):
                byte_idx = i // 8
                if byte_idx < len(data):
                    bit_idx = 7 - (i % 8)  # High bit to low bit (0-7)
                    self.pieces[i] = bool(data[byte_idx] & (1 << bit_idx))

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

    # Create a directory for this peer's files if it doesn't exist
    peer_dir = f"peer_{my_peer_id}"
    os.makedirs(peer_dir, exist_ok=True)

    # Initialize bitfield
    num_pieces = (common['FileSize'] + common['PieceSize'] - 1) // common['PieceSize']
    bitfield = Bitfield(num_pieces)
    started_with_file = my_peer.file_exist
    if started_with_file:
        # If the peer starts with the full file, mark all pieces
        for i in range(num_pieces):
            bitfield.set_piece(i)

    # Create a socket and bind it to the port from the config file
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Use empty string to bind to all interfaces (I was getting a bunch of connection refused errors otherwise)
    bind_host = '' if my_peer.host == 'localhost' else my_peer.host
    server_socket.bind((bind_host, my_peer.port))
    server_socket.listen(5)
    server_socket.settimeout(1.0)  # Short timeout to allow checking for connections

    # Initialize message handler
    message_handler = MessageHandler(logger)

    # List to hold active socket connections
    connections = []
    connection_info = {}  # Store connection info for each peer
    connections_lock = threading.Lock() 

    # Accept incoming connections from peers with higher IDs
    # Accept connections as they come in, verify peer ID via handshake
    expected_peers = [p.peer_id for p in peers if p.peer_id > my_peer_id]
    accepted_peers = set()
    accepted_peers_lock = threading.Lock()
    
    def accept_connections():
        """Accept incoming connections in a separate thread"""
        if not expected_peers:
            return
        
        # Continue accepting connections until socket is closed
        while True:
            try:
                # Check if socket is still valid
                if server_socket.fileno() == -1:
                    break
                conn, addr = server_socket.accept()
                
                # Perform handshake (incoming connection sends handshake first)
                received_handshake = message_handler.receive_handshake(conn)
                peer_id_from_handshake = received_handshake.peer_id
                
                # Verify the peer ID is one we expect
                if peer_id_from_handshake not in expected_peers:
                    conn.close()
                    continue
                
                with accepted_peers_lock:
                    if peer_id_from_handshake in accepted_peers:
                        conn.close()
                        continue
                    accepted_peers.add(peer_id_from_handshake)
                
                message_handler.send_handshake(conn, my_peer_id)
                logger.connection_accepted(peer_id_from_handshake)
                
                with connections_lock:
                    connections.append((peer_id_from_handshake, conn))
                    connection_info[peer_id_from_handshake] = {
                        'socket': conn,
                        'choked': True,            # Start choked, will be unchoked by algorithm
                        'interested': False,
                        'bitfield': None,
                        'requested': None,
                        'bytes_downloaded': 0,     # Bytes downloaded in current interval
                        'download_start_time': time.time(),  # Start time of current interval
                        'is_preferred': False,      # Whether this peer is a preferred neighbor
                        'is_optimistic': False      # Whether this peer is optimistically unchoked
                    }
                
                # Exchange bitfields with the newly connected peer
                try:
                    # Send our bitfield if we have any pieces
                    if bitfield.num_completed() > 0:
                        message_handler.send_bitfield(conn, bitfield)
                except Exception:
                    pass
                
            except socket.timeout:
                # Timeout is expected, continue waiting for connections
                continue
            except (OSError, socket.error):
                # Socket was closed or invalid, exit accept thread
                if server_socket.fileno() == -1:
                    break
                try:
                    if 'conn' in locals():
                        conn.close()
                except:
                    pass
                continue
            except Exception:
                try:
                    if 'conn' in locals():
                        conn.close()
                except:
                    pass
                continue
    
    # Start accepting connections in a separate thread
    if expected_peers:
        accept_thread = threading.Thread(target=accept_connections, daemon=True)
        accept_thread.start()
        time.sleep(0.2) 

    # Connect to peers with smaller peer IDs
    for p in peers:
        if p.peer_id < my_peer_id:
            while True:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(5.0)  # 5 second timeout for connection attempts
                    s.connect((p.host, p.port))
                    s.settimeout(None)  # Remove timeout after connection
                    logger.connection_made(p.peer_id)
                    
                    # Perform handshake (outgoing connection sends handshake first)
                    message_handler.send_handshake(s, my_peer_id)
                    received_handshake = message_handler.receive_handshake(s)
                    
                    # Verify the peer ID matches
                    if received_handshake.peer_id != p.peer_id:
                        s.close()
                        time.sleep(0.3)
                        continue
                    
                    with connections_lock:
                        connections.append((p.peer_id, s))
                        connection_info[p.peer_id] = {
                            'socket': s,
                            'choked': True,            # Start choked, will be unchoked by algorithm
                            'interested': False,
                            'bitfield': None,
                            'requested': None,         # track one outstanding request per peer
                            'bytes_downloaded': 0,     # Bytes downloaded in current interval
                            'download_start_time': time.time(),  # Start time of current interval
                            'is_preferred': False,      # Whether this peer is a preferred neighbor
                            'is_optimistic': False      # Whether this peer is optimistically unchoked
                        }
                    break
                except (ConnectionRefusedError, socket.timeout, OSError):
                    # Keep retrying until peer's listener is up
                    try:
                        s.close()
                    except:
                        pass
                    time.sleep(0.5)
                except Exception:
                    try:
                        s.close()
                    except:
                        pass
                    time.sleep(0.5)
    
    # Give a short time for initial connections to be established
    # But don't block - start transferring with whatever peers are connected
    time.sleep(1.0)  # Brief pause to allow some connections to establish

    def calculate_download_rate(peer_id):
        """Calculate download rate for a peer in bytes per second"""
        info = connection_info[peer_id]
        elapsed = time.time() - info['download_start_time']
        if elapsed > 0:
            return info['bytes_downloaded'] / elapsed
        return 0.0
    
    def select_preferred_neighbors():
        """Select preferred neighbors based on download rates"""
        # Get all interested peers
        interested_peers = [pid for pid, info in connection_info.items() 
                          if info['interested']]
        
        if not interested_peers:
            return []
        
        # If we have complete file, select randomly
        if bitfield.is_complete():
            k = common['NumberOfPreferredNeighbors']
            return random.sample(interested_peers, min(k, len(interested_peers)))
        
        # Otherwise, select based on download rates
        peer_rates = []
        # Calculate rates
        for pid in interested_peers:
            rate = calculate_download_rate(pid)
            peer_rates.append((pid, rate))
        
        # Sort by download rate
        peer_rates.sort(key=lambda x: x[1], reverse=True)
        
        # Select top k, breaking ties randomly
        k = common['NumberOfPreferredNeighbors']
        if len(peer_rates) <= k:
            return [pid for pid, _ in peer_rates]
        
        # Find peers with same rate as k-th peer
        kth_rate = peer_rates[k-1][1]
        same_rate_peers = [pid for pid, rate in peer_rates if rate == kth_rate]
        
        # Select randomly to break ties
        if len(same_rate_peers) > 1:
            selected = [pid for pid, rate in peer_rates if rate > kth_rate]
            needed = k - len(selected)
            selected.extend(random.sample(same_rate_peers, needed))
            return selected
        
        return [pid for pid, _ in peer_rates[:k]]
    
    def select_optimistic_unchoked():
        """Select an optimistically unchoked neighbor randomly from choked but interested peers"""
        choked_interested = [pid for pid, info in connection_info.items()
                           if info['choked'] and info['interested']]
        
        if not choked_interested:
            return None
        
        return random.choice(choked_interested)
    
    def update_choking():
        """Update choking/unchoking based on preferred neighbors and optimistic unchoking"""
        # Select preferred neighbors
        preferred = select_preferred_neighbors()
        
        # Select optimistic unchoked neighbor
        optimistic = select_optimistic_unchoked()
        
        # Update connection info
        for pid, info in connection_info.items():
            was_preferred = info['is_preferred']
            was_optimistic = info['is_optimistic']
            was_choked = info['choked']
            
            info['is_preferred'] = (pid in preferred)
            info['is_optimistic'] = (pid == optimistic)
            
            # Determine if should be unchoked
            should_unchoke = info['is_preferred'] or info['is_optimistic']
            
            # Send choke/unchoke messages if state changed
            if should_unchoke and info['choked']:
                try:
                    message_handler.send_unchoke(info['socket'])
                    info['choked'] = False
                except Exception:
                    pass
            elif not should_unchoke and not info['choked']:
                try:
                    message_handler.send_choke(info['socket'])
                    info['choked'] = True
                    info['requested'] = None  
                except Exception:
                    pass
        
        if preferred:
            logger.preferred_neighbors(preferred)
        if optimistic is not None:
            logger.optimistic_unchoked(optimistic)
        
        # Reset download tracking for next interval
        for pid, info in connection_info.items():
            info['bytes_downloaded'] = 0
            info['download_start_time'] = time.time()
    
    # Helper functions for file I/O and broadcasting
    def _ensure_placeholder():
        """Create placeholder file in peer_dir if missing"""
        file_path = os.path.join(peer_dir, common['FileName'])
        if not os.path.exists(file_path):
            try:
                total_size = common['FileSize']
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * total_size)
            except Exception:
                pass

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
        except Exception:
            pass

    def _read_piece(piece_index):
        """Read a piece from file (if exists)"""
        try:
            file_path = os.path.join(peer_dir, common['FileName'])
            piece_size = common['PieceSize']
            with open(file_path, 'rb') as f:
                f.seek(piece_index * piece_size)
                return f.read(piece_size)
        except Exception:
            return None

    def _broadcast_have(piece_index):
        """Broadcast a HAVE message to all connected peers"""
        for pid, sock in connections:
            try:
                message_handler.send_have(sock, piece_index)
            except Exception:
                pass

    # After connecting, exchange bitfields with all connected peers
    with connections_lock:
        connections_copy = list(connections)  # Make a copy to avoid connection change issues
    for peer_id, sock in connections_copy:
        try:
            # Send our bitfield if we have any pieces
            if bitfield.num_completed() > 0:
                message_handler.send_bitfield(sock, bitfield)
            
            # Receive bitfield from peer
            try:
                sock.settimeout(2.0)  # 2 second timeout for bitfield exchange
                message = message_handler.receive_message(sock)
                sock.settimeout(0.1)  # Reset to normal timeout for message loop
                
                if message is None:
                    # Peer didn't send bitfield, thus they have no pieces, thus we're not interested
                    connection_info[peer_id]['bitfield'] = Bitfield(num_pieces)
                    message_handler.send_not_interested(sock)
                elif message.message_type == MessageType.BITFIELD:
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
                    connection_info[peer_id]['bitfield'] = Bitfield(num_pieces)
            except socket.timeout:
                # Peer didn't send bitfield within timeout (they have no pieces)
                sock.settimeout(0.1)  # Reset timeout
                connection_info[peer_id]['bitfield'] = Bitfield(num_pieces)
                # Since peer has no pieces, we're not interested
                try:
                    message_handler.send_not_interested(sock)
                except:
                    pass
            except Exception:
                sock.settimeout(0.1)  # Reset timeout
                connection_info[peer_id]['bitfield'] = Bitfield(num_pieces)
        except Exception:
            pass

    # Main message handling loop
    def handle_messages():
        """Handle incoming messages from all peers"""
        while True:
            with connections_lock:
                connections_copy = list(connections)  # Make a copy to avoid connection change issues
            for peer_id, sock in connections_copy:
                try:
                    sock.settimeout(0.1)
                    message = message_handler.receive_message(sock)
                    if message is None:
                        continue
                    
                    if message.message_type == MessageType.CHOKE:
                        # Peer is choking us, we can't request pieces from them
                        logger.choked_by(peer_id)
                        connection_info[peer_id]['requested'] = None
                    
                    elif message.message_type == MessageType.UNCHOKE:
                        # Peer is unchoking us, we can request pieces from them
                        logger.unchoked_by(peer_id)
                        peer_bf = connection_info[peer_id]['bitfield']
                        if peer_bf is None:
                            peer_bf = Bitfield(num_pieces)
                            connection_info[peer_id]['bitfield'] = peer_bf
                        
                        candidates = []
                        for i in range(num_pieces):
                            if peer_bf.has_piece(i) and not bitfield.has_piece(i):
                                candidates.append(i)
                        if candidates and connection_info[peer_id]['requested'] is None:
                            choice = random.choice(candidates)
                            try:
                                message_handler.send_request(sock, choice)
                                connection_info[peer_id]['requested'] = choice
                            except Exception:
                                pass
                    
                    elif message.message_type == MessageType.INTERESTED:
                        connection_info[peer_id]['interested'] = True
                        logger.received_interested(peer_id)
                    
                    elif message.message_type == MessageType.NOT_INTERESTED:
                        connection_info[peer_id]['interested'] = False
                        logger.received_not_interested(peer_id)
                    
                    elif message.message_type == MessageType.BITFIELD:
                        # Handle bitfield message for peers that connect late
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
                            try:
                                message_handler.send_interested(sock)
                                connection_info[peer_id]['interested'] = True
                            except Exception:
                                pass
                        else:
                            try:
                                message_handler.send_not_interested(sock)
                            except Exception:
                                pass
                    
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
                            except Exception:
                                pass
                    
                    elif message.message_type == MessageType.REQUEST:
                        # Peer requests a piece from us
                        piece_index = struct.unpack('>I', message.payload)[0]
                        # Only send piece if we have it AND peer is not choked
                        if bitfield.has_piece(piece_index) and not connection_info[peer_id]['choked']:
                            data = None
                            _ensure_placeholder()
                            data = _read_piece(piece_index)
                            if data is not None:
                                try:
                                    message_handler.send_piece(sock, piece_index, data)
                                except Exception:
                                    pass
                    
                    elif message.message_type == MessageType.PIECE:
                        # receive piece payload and save
                        piece_index = struct.unpack('>I', message.payload[:4])[0]
                        piece_data = message.payload[4:]
                        
                        connection_info[peer_id]['bytes_downloaded'] += len(piece_data)
                        
                        _save_piece(piece_index, piece_data)
                        
                        # Update bitfield
                        bitfield.set_piece(piece_index)
                        logger.downloaded_piece(piece_index, peer_id, bitfield.num_completed())
                        
                        # Clear requested marker for this peer if it matches
                        if connection_info[peer_id]['requested'] == piece_index:
                            connection_info[peer_id]['requested'] = None
                        
                        # Check if we just completed the file
                        if bitfield.is_complete() and not started_with_file:
                            logger.download_complete()
                        
                        # Broadcast HAVE to all peers
                        _broadcast_have(piece_index)
                        
                        # If still unchoked, request another piece from this peer
                        if not connection_info[peer_id]['choked']:
                            peer_bf = connection_info[peer_id]['bitfield']
                            if peer_bf:
                                candidates = []
                                for i in range(num_pieces):
                                    if peer_bf.has_piece(i) and not bitfield.has_piece(i):
                                        candidates.append(i)
                                if candidates and connection_info[peer_id]['requested'] is None:
                                    choice = random.choice(candidates)
                                    try:
                                        message_handler.send_request(connection_info[peer_id]['socket'], choice)
                                        connection_info[peer_id]['requested'] = choice
                                    except Exception:
                                        pass
                        
                except socket.timeout:
                    # No message available, continue to next peer
                    continue
                except Exception:
                    # If a peer disconnects or any other error happens, ignore and continue
                    continue
            
            # Check if download is complete and all peers have completed
            time.sleep(0.1)  

    message_thread = threading.Thread(target=handle_messages, daemon=True)
    message_thread.start()
    
    # Periodic unchoking timer
    def unchoking_timer():
        """Periodically update preferred neighbors"""
        while True:
            time.sleep(common['UnchokingInterval'])
            try:
                update_choking()
            except Exception:
                pass
    
    # Periodic optimistic unchoking timer
    def optimistic_unchoking_timer():
        """Periodically update optimistic unchoked neighbor"""
        while True:
            time.sleep(common['OptimisticUnchokingInterval'])
            try:
                # Only update optimistic unchoked, not preferred neighbors
                optimistic = select_optimistic_unchoked()
                if optimistic is not None:
                    # Update optimistic unchoked neighbor
                    old_optimistic = None
                    for pid, info in connection_info.items():
                        if info['is_optimistic']:
                            old_optimistic = pid
                            info['is_optimistic'] = False
                            if not info['is_preferred']:
                                try:
                                    message_handler.send_choke(info['socket'])
                                    info['choked'] = True
                                    info['requested'] = None
                                except:
                                    pass
                    
                    # Set new optimistic unchoked
                    if optimistic != old_optimistic:
                        connection_info[optimistic]['is_optimistic'] = True
                        if connection_info[optimistic]['choked']:
                            try:
                                message_handler.send_unchoke(connection_info[optimistic]['socket'])
                                connection_info[optimistic]['choked'] = False
                                logger.optimistic_unchoked(optimistic)
                            except Exception:
                                pass
            except Exception:
                pass
    
    # Start periodic timers
    unchoking_thread = threading.Thread(target=unchoking_timer, daemon=True)
    unchoking_thread.start()
    
    optimistic_thread = threading.Thread(target=optimistic_unchoking_timer, daemon=True)
    optimistic_thread.start()
    
    # Initial unchoking update
    time.sleep(0.5)  # Give connections time to establish
    update_choking()

    try:
        while True:
            time.sleep(2)
            # Check if we and all peers have completed
            if bitfield.is_complete():
                # Check if all peers from the config have completed
                all_peers_complete = True
                all_peer_ids = {p.peer_id for p in peers}
                
                # First, check if we have connections to all peers
                connected_peer_ids = set(connection_info.keys())
                if connected_peer_ids != all_peer_ids - {my_peer_id}:
                    # Not all peers are connected yet
                    all_peers_complete = False
                else:
                    # All peers are connected, check if they all have the file
                    for pid in all_peer_ids:
                        if pid == my_peer_id:
                            continue
                        if pid not in connection_info:
                            all_peers_complete = False
                            break
                        peer_bf = connection_info[pid]['bitfield']
                        if peer_bf is None or not peer_bf.is_complete():
                            all_peers_complete = False
                            break
                
                if all_peers_complete:
                    if not started_with_file:
                        logger.download_complete()
                    break
                
    except KeyboardInterrupt:
        pass
    finally:
        server_socket.close()
        for _, s in connections:
            try:
                s.close()
            except:
                pass


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
