"""
DiscoveryService: Auto-discovery of nodes in the P2P network via broadcast.

Uses UDP broadcast (255.255.255.255) to announce node presence and discover
other nodes without requiring a centralized registry.
"""

import asyncio
import json
import logging
import socket
import netifaces
from typing import Callable, Optional


class DiscoveryService:
    """
    Handles automatic discovery of nodes in the network via UDP broadcast.

    When a node starts, it broadcasts a HELLO message to 255.255.255.255.
    Other nodes receive this message and learn about new peers.
    Each node also listens for HELLO messages from other nodes.
    """

    # Multicast configuration
    MCAST_GRP = '224.0.0.251'
    MCAST_PORT = 50000
    MCAST_PORT_STR = '50000'
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
        self.mcast_socket: Optional[socket.socket] = None

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

            # Run multicast and listen tasks concurrently
            await asyncio.gather(
                self._broadcast_hello(),
                self._listen_for_peers(),
                self._cleanup_dead_peers(),
            )

        except Exception as e:
            self.logger.error(f"Error in discovery service: {e}")
            await self.shutdown()

    def _setup_sockets(self):

        def _get_local_ip(self) -> str:
            """Get the local IP address on the default route (robust across OSes)"""
            try:
                # Connect to a remote server to force OS to pick the default route interface
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.settimeout(2)
                s.connect(("8.8.8.8", 1))  # Doesn't send data
                local_ip = s.getsockname()[0]
                s.close()
                return local_ip
            except Exception as e:
                self.logger.warning(f"Failed to auto-detect IP: {e}, falling back...")
            
            # Fallback: scan interfaces
            for iface in netifaces.interfaces():
                addrs = netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                if addrs:
                    for addr in addrs:
                        ip = addr['addr']
                        if ip.startswith('127.') or ip.startswith('169.254.'):
                            continue
                        return ip
            return "127.0.0.1"
        """Set up UDP socket for multicast send and receive."""
        # ----- FIX FOR MACOS: Join multicast using actual interface -----
        # Detect default interface (e.g. en0)
        iface = netifaces.gateways()['default'][netifaces.AF_INET][1]
        ip_addr = netifaces.ifaddresses(iface)[netifaces.AF_INET][0]['addr']
        import struct
        self.mcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.mcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.mcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass  # Not all systems support SO_REUSEPORT
        self.mcast_socket.bind(('', self.MCAST_PORT))
        # self.logger.info(f"bound to {ip_addr}:{self.MCAST_PORT}")
        
        # Join multicast group
        intf = _get_local_ip(self)
        # mreq = struct.pack("4s4s", socket.inet_aton(self.MCAST_GRP)+socket.inet_aton(intf))
        self.mcast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(self.MCAST_GRP)+socket.inet_aton(intf))
        self.mcast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        self.mcast_socket.setsockopt(
            socket.SOL_IP,
            socket.IP_MULTICAST_IF,
            socket.inet_aton(intf)
        )
        
        self.mcast_socket.setblocking(False)
        self.logger.info(
            f"Multicast socket configured - Group: {self.MCAST_GRP}:{self.MCAST_PORT}"
        )

    async def _send_hello_broadcast(self):
        """Send an immediate HELLO multicast (called at startup)."""
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
                self.mcast_socket.sendto,
                payload,
                (self.MCAST_GRP, self.MCAST_PORT)
            )

            self.logger.info(
                f"Initial HELLO multicast sent from {self.node.id}")
        except Exception as e:
            self.logger.error(f"Error sending initial HELLO: {e}")

    async def _broadcast_hello(self):
        """Periodically multicast HELLO message to the network."""
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

                # Multicast to network
                await loop.run_in_executor(
                    None,
                    self.mcast_socket.sendto,
                    payload,
                    (self.MCAST_GRP, self.MCAST_PORT),
                )

                self.logger.debug(f"Multicast HELLO from {self.node.id}")

                # Wait before next multicast
                await asyncio.sleep(self.BROADCAST_INTERVAL)

            except Exception as e:
                self.logger.error(f"Error multicasting HELLO: {e}")
                await asyncio.sleep(1)

    async def _listen_for_peers(self):
        """Listen for HELLO messages from other nodes via multicast."""
        import struct
        loop = asyncio.get_event_loop()

        while self.running:
            try:
                try:
                    data, addr = self.mcast_socket.recvfrom(4096)

                    if len(data) >= 4:
                        try:
                            length = struct.unpack("!I", data[:4])[0]
                            # If it looks exactly like our framed message → ignore it here
                            if 0 < length <= len(data) - 4:
                                # This is a chat/multicast message → skip discovery processing
                                continue
                        except struct.error:
                            pass  # not a valid length prefix → probably a real HELLO

                    try:
                        message = json.loads(data.decode('utf-8'))

                        # Only process HELLO messages from other nodes
                        if message.get("type") == "HELLO" and message.get("node_id") != self.node.id:
                            await self._handle_peer_discovery(message, addr)
                    except json.JSONDecodeError:
                        self.logger.warning(
                            f"Invalid JSON message from {addr}")
                            

                except BlockingIOError:
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
            if peer_id not in self.node.vector_clock:
                self.node.vector_clock[peer_id] = 0

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

        if self.mcast_socket:
            self.mcast_socket.close()

        self.logger.info("Discovery service shutdown complete")