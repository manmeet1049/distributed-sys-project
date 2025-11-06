"""
DiscoveryService: Auto-discovery of nodes in the P2P network via broadcast.

Uses UDP broadcast (255.255.255.255) to announce node presence and discover
other nodes without requiring a centralized registry.
"""

import asyncio
import json
import logging
import socket
from typing import Callable, Optional


class DiscoveryService:
    """
    Handles automatic discovery of nodes in the network via UDP broadcast.

    When a node starts, it broadcasts a HELLO message to 255.255.255.255.
    Other nodes receive this message and learn about new peers.
    Each node also listens for HELLO messages from other nodes.
    """

    # Broadcast configuration
    BROADCAST_ADDR = "255.255.255.255"
    BROADCAST_PORT = 9999
    BROADCAST_INTERVAL = 5  # seconds
    PEER_TIMEOUT = 15  # seconds

    def __init__(self, node):
        """
        Initialize the discovery service.

        Args:
            node: Reference to parent Node instance
        """
        self.node = node
        self.logger = logging.getLogger(f"Discovery-{node.id}")

        # Sockets
        self.broadcast_socket: Optional[socket.socket] = None
        self.listen_socket: Optional[socket.socket] = None

        # State
        self.running = False
        self.discovered_peers = {}  # {node_id: {'host': str, 'port': int, 'last_seen': float}}

        # Callbacks
        self.on_peer_discovered: Optional[Callable] = None
        self.on_peer_lost: Optional[Callable] = None

    async def start(self):
        """Start the discovery service: broadcast HELLO and listen for responses."""
        try:
            self._setup_sockets()
            self.running = True
            self.logger.info("Discovery service started")

            # Broadcast immediately to announce presence to existing nodes
            await self._send_hello_broadcast()

            # Run broadcast and listen tasks concurrently
            await asyncio.gather(
                self._broadcast_hello(),
                self._listen_for_peers(),
                self._cleanup_dead_peers(),
            )

        except Exception as e:
            self.logger.error(f"Error in discovery service: {e}")
            await self.shutdown()

    def _setup_sockets(self):
        """Set up UDP sockets for broadcast and listening."""
        # Broadcast socket
        self.broadcast_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Listen socket
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listen_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_socket.bind(("0.0.0.0", self.BROADCAST_PORT))
        self.listen_socket.setblocking(False)

        self.logger.info(
            f"Sockets configured - Broadcast: {self.BROADCAST_ADDR}:{self.BROADCAST_PORT}, "
            f"Listen: 0.0.0.0:{self.BROADCAST_PORT}"
        )

    async def _send_hello_broadcast(self):
        """Send an immediate HELLO broadcast (called at startup)."""
        try:
            loop = asyncio.get_event_loop()
            message = {
                "type": "HELLO",
                "node_id": self.node.id,
                "host": self.node.host,
                "port": self.node.port,
            }
            payload = json.dumps(message).encode('utf-8')

            await loop.run_in_executor(
                None,
                self.broadcast_socket.sendto,
                payload,
                (self.BROADCAST_ADDR, self.BROADCAST_PORT),
            )

            self.logger.info(
                f"Initial HELLO broadcast sent from {self.node.id}")
        except Exception as e:
            self.logger.error(f"Error sending initial HELLO: {e}")

    async def _broadcast_hello(self):
        """Periodically broadcast HELLO message to the network."""
        loop = asyncio.get_event_loop()

        while self.running:
            try:
                # Create HELLO message
                message = {
                    "type": "HELLO",
                    "node_id": self.node.id,
                    "host": self.node.host,
                    "port": self.node.port,
                }
                payload = json.dumps(message).encode('utf-8')

                # Broadcast to network
                await loop.run_in_executor(
                    None,
                    self.broadcast_socket.sendto,
                    payload,
                    (self.BROADCAST_ADDR, self.BROADCAST_PORT),
                )

                self.logger.debug(f"Broadcast HELLO from {self.node.id}")

                # Wait before next broadcast
                await asyncio.sleep(self.BROADCAST_INTERVAL)

            except Exception as e:
                self.logger.error(f"Error broadcasting HELLO: {e}")
                await asyncio.sleep(1)

    async def _listen_for_peers(self):
        """Listen for HELLO messages from other nodes."""
        loop = asyncio.get_event_loop()

        while self.running:
            try:
                # Use select with timeout for efficient non-blocking listen
                try:
                    data, addr = self.listen_socket.recvfrom(4096)

                    try:
                        message = json.loads(data.decode('utf-8'))

                        # Only process HELLO messages from other nodes
                        if message.get("type") == "HELLO" and message.get("node_id") != self.node.id:
                            await self._handle_peer_discovery(message, addr)
                    except json.JSONDecodeError:
                        self.logger.warning(
                            f"Invalid JSON message from {addr}")

                except BlockingIOError:
                    # No data available, small sleep to prevent busy-waiting
                    await asyncio.sleep(0.01)

            except Exception as e:
                self.logger.error(f"Error listening for peers: {e}")
                await asyncio.sleep(0.5)

    async def _handle_peer_discovery(self, message: dict, addr: tuple):
        """
        Handle discovery of a new peer.

        Args:
            message: HELLO message from peer
            addr: Address tuple (host, port) of sender
        """
        peer_id = message.get("node_id")
        peer_host = message.get("host")
        peer_port = message.get("port")

        if not all([peer_id, peer_host, peer_port]):
            self.logger.warning(
                f"Invalid HELLO message from {addr}: {message}")
            return

        import time

        # Check if this is a new peer
        is_new = peer_id not in self.discovered_peers

        # Update or add peer
        self.discovered_peers[peer_id] = {
            "host": peer_host,
            "port": peer_port,
            "last_seen": time.time(),
        }

        if is_new:
            self.logger.info(
                f"Discovered new peer: {peer_id} at {peer_host}:{peer_port}")

            # Add to node's peers
            self.node.peers[peer_id] = (peer_host, peer_port)

            # Trigger callback if set
            if self.on_peer_discovered:
                await self.on_peer_discovered(peer_id, peer_host, peer_port)
        else:
            self.logger.debug(f"Updated peer {peer_id}")

    async def _cleanup_dead_peers(self):
        """Periodically remove peers that haven't sent HELLO in a while."""
        import time

        while self.running:
            try:
                current_time = time.time()
                dead_peers = []

                for peer_id, peer_info in self.discovered_peers.items():
                    if current_time - peer_info["last_seen"] > self.PEER_TIMEOUT:
                        dead_peers.append(peer_id)

                for peer_id in dead_peers:
                    del self.discovered_peers[peer_id]
                    if peer_id in self.node.peers:
                        del self.node.peers[peer_id]

                    self.logger.info(f"Removed dead peer: {peer_id}")

                    # Trigger callback if set
                    if self.on_peer_lost:
                        await self.on_peer_lost(peer_id)

                await asyncio.sleep(5)  # Check every 5 seconds

            except Exception as e:
                self.logger.error(f"Error in cleanup: {e}")
                await asyncio.sleep(1)

    async def shutdown(self):
        """Gracefully shutdown the discovery service."""
        self.running = False

        if self.broadcast_socket:
            self.broadcast_socket.close()
        if self.listen_socket:
            self.listen_socket.close()

        self.logger.info("Discovery service shutdown complete")
