import asyncio
import logging
import socket
import sys
import netifaces
from typing import Optional


from services.discovery import DiscoveryService
from services.messaging_service import MessagingService
from Data.message import Message


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Node:
    """
    Represents a single node in the distributed emergency alert system.
    Each node operates as both a client and server in the P2P network.
    """

    def __init__(self, node_id: str, host: str, port: int):
        """
        Initialize a node.

        Args:
            node_id: Unique identifier for this node
            host: IP address to bind to
            port: Port number to listen on
        """
        self.id = node_id
        self.host = host
        self.port = port
        self.addr = (host, port)            # Tuple for socket binding
        self.vector_clock = {self.id: 0}    # Initialize vector clock
        self.is_leader = False              # Flag to indicate if this node is the leader
        self.leader_id: Optional[str] = None
        self.logger = logging.getLogger(f"Node-{node_id}")

        # Network communication
        self.server_socket: Optional[socket.socket] = None
        self.running = False

        # Peers: store connected nodes {node_id: (host, port)}
        self.peers = {}

        # Discovery service (now uses multicast only)
        self.discovery_service = DiscoveryService(self)

        #Messaging service
        self.messaging = MessagingService(self)
        # ------------------------------------------------------------------
        # NEW: Simple receive handler (no causal ordering)
        # ------------------------------------------------------------------
        # async def on_message(msg: Message):
        #     print(f"\n[RECEIVED] {msg.sender_id} #{msg.seq}: {msg.payload}")

        # self.messaging.on_message_received = on_message

        self.logger.info(f"Node initialized: {self.id} at {self.host}:{self.port}")

        self.logger.info(
            f"Node initialized: {self.id} at {self.host}:{self.port}")

    async def start(self):
        """Start the node: bind socket and begin accepting connections."""
        try:
            # Create TCP server socket
            self.server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
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
                user_input = await loop.run_in_executor(None, input, f"[{self.id}]> ")
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
                    msg_args=" ".join(msg_args)                 # first word after "message"    # ALL remaining words, with spaces preserved
                    payload = {"type": "CHAT", "text": msg_args}
                    
                    await self.messaging.send_to(peer_id, payload)
                    self.logger.info(f"Message sent to {peer_id}: {msg_args}")

                elif cmd == "setleader" and len(parts) > 1:
                    self.leader_id = parts[1]
                    self.is_leader = (parts[1] == self.id)
                    self.logger.info(f"Leader set to {self.leader_id}")
                elif user_input.startswith("connect"):
                    # Format: connect <host> <port> <peer_id>
                    parts = user_input.split()
                    if len(parts) == 4:
                        await self.connect_to_peer(parts[1], int(parts[2]), parts[3])
                    else:
                        self.logger.warning(
                            "Usage: connect <host> <port> <peer_id>")
                else:
                    self.logger.info("Commands: peers | multicast <msg> | leader <msg> | setleader <id> | exit | connect | transmission")

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
    # host = "192.168.2.120"  # Use your local network broadcast or host IP as needed

    node = Node(node_id, host, port)

    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print("\nShutdown requested")


if __name__ == "__main__":
    main()
