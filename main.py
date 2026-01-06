import asyncio
import logging
import socket
import sys
import netifaces
from typing import Optional
import os
import uuid


from services.discovery import DiscoveryService
from services.messaging_service import MessagingService
from services.ring_service import RingService
from services.leader_election import LeaderElectionService
from Data.message import Message


def setup_node_logging(node_id: str, level=logging.INFO) -> str:
    """
    Configure logging for a specific node.
    Creates a log file per node and minimizes console output.
    """
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Create log file for this node
    log_file = os.path.join(log_dir, f"{node_id}.log")

    # Configure file handler for detailed logs
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))

    # Configure console handler for minimal output (WARNING and above only)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s - %(message)s'
    ))

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Remove any existing handlers
    root_logger.handlers.clear()

    # Add our handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return log_file


class Node:
    """
    Represents a single node in the distributed emergency alert system.
    Each node operates as both a client and server in the P2P network.
    """

    def __init__(self, node_id: str, host: str, port: int, generate_uuid: bool = True):
        """
        Initialize a node.

        Args:
            node_id: Human-readable identifier for this node (e.g., "Node-5000")
            host: IP address to bind to
            port: Port number to listen on
            generate_uuid: If True, generate a UUID for election; if False, use node_id
        """
        # Generate UUID for election purposes
        if generate_uuid:
            self.uuid = str(uuid.uuid4())
            self.name = node_id  # Human-readable name
            self.id = self.uuid  # Use UUID as the actual ID for all operations
        else:
            self.uuid = node_id
            self.name = node_id
            self.id = node_id

        # Setup logging with human-readable name
        self.log_file = setup_node_logging(self.name)

        self.host = host
        self.port = port
        self.addr = (host, port)            # Tuple for socket binding
        self.vector_clock = {self.id: 0}    # Initialize vector clock with UUID
        self.is_leader = False              # Flag to indicate if this node is the leader
        self.leader_id: Optional[str] = None
        self.logger = logging.getLogger(f"Node-{self.name}")

        # Network communication
        self.server_socket: Optional[socket.socket] = None
        self.running = False

        # Peers: store connected nodes {node_id: (host, port)}
        self.peers = {}

        # Discovery service (now uses multicast only)
        self.discovery_service = DiscoveryService(self)

        # Messaging service
        self.messaging = MessagingService(self)

        # Ring service
        self.ring_service = RingService(self)

        # Leader election service (HS algorithm)
        self.leader_election = LeaderElectionService(self)

        self.logger.info(
            f"Node initialized: {self.name} (UUID: {self.uuid}) at {self.host}:{self.port}")
        self.logger.info(f"Logging to: {self.log_file}")

        # Print to console so user knows where to find logs
        print(f"\n{'='*60}")
        print(f"Node {self.name} initialized")
        print(f"UUID: {self.uuid}")
        print(f"Listening on: {self.host}:{self.port}")
        print(f"Logs written to: {self.log_file}")
        print(f"{'='*60}\n")
        print(f"Listening on: {self.host}:{self.port}")
        print(f"Logs written to: {self.log_file}")
        print(f"{'='*60}\n")

    async def start(self):
        """Start the node: bind socket and begin accepting connections."""
        try:
            # Create TCP server socket
            self.server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try: 
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except AttributeError:
                pass  # Not all systems support SO_REUSEPORT   
             
            self.server_socket.bind(self.addr)
            self.server_socket.listen(5)
            self.server_socket.setblocking(False)

            self.running = True
            self.logger.info(
                f"Node started, listening on {self.host}:{self.port}")

            # Run server tasks concurrently
            await asyncio.gather(
                self.discovery_service.start(),
                self.messaging.start(),
                self.ring_service.start(),
                self.leader_election.start(),
                self._accept_connections(),
                self._handle_input(),
            )

        except Exception as e:
            self.logger.error(f"Error starting node: {e}")
            await self.shutdown()

    async def _accept_connections(self):
        """Accept incoming connections from other nodes."""
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                # Non-blocking accept wrapped in executor
                client_socket, client_addr = await loop.run_in_executor(
                    None, lambda: self.server_socket.accept()
                )
                self.logger.info(f"New connection from {client_addr}")
                asyncio.create_task(self._handle_client(
                    client_socket, client_addr))
            except BlockingIOError:
                await asyncio.sleep(0.1)
            except Exception as e:
                if self.running:
                    self.logger.error(f"Error accepting connection: {e}")
                await asyncio.sleep(0.1)

    async def _handle_client(self, client_socket: socket.socket, client_addr: tuple):
        """
        Handle communication with a connected client.

        Args:
            client_socket: Socket of the connected client
            client_addr: Address of the connected client
        """
        try:
            loop = asyncio.get_event_loop()
            client_socket.setblocking(False)

            while self.running:
                try:
                    # Receive data from client
                    data = await loop.run_in_executor(
                        None, lambda: client_socket.recv(4096)
                    )

                    if not data:
                        self.logger.info(f"Connection closed by {client_addr}")
                        break

                    message = data.decode('utf-8', errors='ignore')
                    self.logger.info(f"Received from {client_addr}: {message}")

                    # Process message (placeholder for now)
                    response = f"ACK: {message}"
                    await loop.run_in_executor(
                        None, lambda: client_socket.send(
                            response.encode('utf-8'))
                    )

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(
                        f"Error handling client {client_addr}: {e}")
                    break

        finally:
            client_socket.close()

    async def _handle_input(self):
        """Handle user input from stdin."""
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                # Read input in a non-blocking way
                user_input = await loop.run_in_executor(None, input, f"[{self.name}]> ")
                parts = user_input.strip().split(maxsplit=1)
                cmd = parts[0].lower() if parts else ""

                if user_input.lower() == "exit":
                    await self.shutdown()
                    break
                elif user_input.lower() == "peers":
                    self.logger.info(f"Known peers: {self.peers}")
                elif cmd == "multicast" and len(parts) > 1:
                    payload = {"type": "CHAT", "text": parts[1]}
                    await self.messaging.multicast_message(payload)
                    self.logger.info("Multicast sent")

                elif cmd == "leader" and len(parts) > 1:
                    payload = {"cmd": "LEADER", "data": parts[1]}
                    await self.messaging.send_message_to_leader(payload)
                    self.logger.info("Sent to leader")
                elif cmd == "message" and len(parts) >= 2:
                    print(parts)
                    peer_id = parts[1].split()[0]
                    msg_args = parts[1].strip().split()[1:]
                    # first word after "message"    # ALL remaining words, with spaces preserved
                    msg_args = " ".join(msg_args)
                    payload = {"type": "CHAT", "text": msg_args}

                    await self.messaging.send_to(peer_id, payload)
                    self.logger.info(f"Message sent to {peer_id}: {msg_args}")

                elif cmd == "setleader" and len(parts) > 1:
                    self.leader_id = parts[1]
                    self.is_leader = (parts[1] == self.id)
                    self.logger.info(f"Leader set to {self.leader_id}")
                elif cmd == "ring":
                    # Display ring topology information
                    ring_info = self.ring_service.get_ring_info()
                    print("\n=== Ring Topology ===")
                    print(f"Node: {self.name}")
                    # Show first 8 chars of UUID
                    print(f"UUID: {self.uuid[:8]}...")
                    print(
                        f"Predecessor: {ring_info['predecessor'][:8] if ring_info['predecessor'] else 'None'}...")
                    print(
                        f"Successor: {ring_info['successor'][:8] if ring_info['successor'] else 'None'}...")
                    print(f"Ring Established: {ring_info['ring_established']}")
                    print(f"In Ring: {ring_info['in_ring']}")
                    print("====================\n")
                elif cmd == "uuid":
                    # Display full UUID information
                    print(f"\n=== Node Identity ===")
                    print(f"Name: {self.name}")
                    print(f"Full UUID: {self.uuid}")
                    print(f"Host:Port: {self.host}:{self.port}")
                    print("=====================\n")
                elif cmd == "elect":
                    # Start leader election
                    print("Starting leader election using HS algorithm...")
                    await self.leader_election.start_election()
                elif cmd == "leader":
                    # Show leader information
                    leader_info = self.leader_election.get_leader_info()
                    print(f"\n=== Leader Information ===")
                    print(f"Status: {self.leader_election.get_status()}")
                    if leader_info['leader_id']:
                        print(f"Leader UUID: {leader_info['leader_id'][:8]}...")
                        print(f"I am leader: {leader_info['is_leader']}")
                    if leader_info['election_in_progress']:
                        print(f"Election in progress: Phase {leader_info['current_phase']}")
                    print("==========================\n")
                elif user_input.startswith("connect"):
                    # Format: connect <host> <port> <peer_id>
                    parts = user_input.split()
                    if len(parts) == 4:
                        await self.connect_to_peer(parts[1], int(parts[2]), parts[3])
                    else:
                        self.logger.warning(
                            "Usage: connect <host> <port> <peer_id>")
                else:
                    print("Commands:")
                    print("  peers          - Show discovered peers")
                    print("  ring           - Show ring topology (short UUIDs)")
                    print("  uuid           - Show full UUID and node info")
                    print("  elect          - Start leader election (HS algorithm)")
                    print("  leader         - Show current leader information")
                    print("  multicast <msg>- Broadcast message to all peers")
                    print("  message <id> <msg> - Send message to specific peer")
                    print("  exit           - Shutdown node")

            except EOFError:
                await self.shutdown()
                break
            except Exception as e:
                self.logger.error(f"Error in input handler: {e}")

    async def connect_to_peer(self, host: str, port: int, peer_id: str):
        """
        Establish a connection to another node.

        Args:
            host: Host address of the peer
            port: Port of the peer
            peer_id: Identifier of the peer
        """
        try:
            loop = asyncio.get_event_loop()
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Use blocking mode with timeout for connection
            peer_socket.setblocking(True)
            peer_socket.settimeout(5)  # 5 second timeout

            # Connect to peer
            await loop.run_in_executor(None, peer_socket.connect, (host, port))

            self.peers[peer_id] = (host, port)
            self.logger.info(f"Connected to peer {peer_id} at {host}:{port}")

            # Send initial handshake
            handshake = f"JOIN:{self.id}"
            await loop.run_in_executor(None, peer_socket.send, handshake.encode('utf-8'))

            peer_socket.close()

        except Exception as e:
            self.logger.error(
                f"Error connecting to peer {peer_id} at {host}:{port}: {e}")

    async def shutdown(self):
        """Gracefully shutdown the node."""
        self.running = False

        # Shutdown discovery service
        await self.discovery_service.shutdown()
        await self.messaging.shutdown()
        if self.server_socket:
            self.server_socket.close()
        self.logger.info("Node shutdown complete")


def main():
    """Entry point for running a single node."""
    if len(sys.argv) < 3:
        print("Usage: python main.py <node_id> <port>")
        print("Example: python main.py node1 5000")
        sys.exit(1)

    node_id = sys.argv[1]
    port = int(sys.argv[2])
    iface = netifaces.gateways()['default'][netifaces.AF_INET][1]
    host = netifaces.ifaddresses(iface)[netifaces.AF_INET][0]['addr']

    node = Node(node_id, host, port)

    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print("\nShutdown requested")


if __name__ == "__main__":
    main()
