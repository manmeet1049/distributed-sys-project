import asyncio
import json
import socket
import struct
import netifaces
import uuid
from collections import defaultdict
from typing import Dict, Optional, Tuple, Callable, Any
from Data.message import Message
from services.causal_buffer import CausalBuffer
from services.vector_causal_buffer import VectorCausalBuffer

# ----------------------------------------------------------------------
# Helper: get the local IP of the interface that has the default gateway
# ----------------------------------------------------------------------
def _get_local_ip() -> str:
    """Return the IP of the interface used for the default route (en0 on macOS)."""
    gw = netifaces.gateways()['default'][netifaces.AF_INET][1]   # interface name
    return netifaces.ifaddresses(gw)[netifaces.AF_INET][0]['addr']

# ----------------------------------------------------------------------
# MessagingService – UDP only
# ----------------------------------------------------------------------
class MessagingService:
    """
    UDP-based messaging on top of DiscoveryService.
    * One outbound socket per peer (address is stored, not a real TCP socket).
    * One inbound socket bound to (node.host, node.port) that receives from everybody.
    * All messages are JSON with a 4-byte big-endian length prefix.
    """

    def __init__(self, node):
        self.node = node                     # reference to the Node object
        self.local_ip = _get_local_ip()      # e.g. 192.168.1.42
        self.inbound_sock: Optional[socket.socket] = None
        self.peers: Dict[str, Tuple[str, int]] = {}   # peer_id -> (host, port)

        # ------------------------------------------------------------------
        # Hook the discovery callbacks
        # ------------------------------------------------------------------
        self.node.discovery_service.on_peer_discovered = self._peer_discovered
        self.node.discovery_service.on_peer_lost       = self._peer_lost

        # Causal buffer
        self.causal_buffer = VectorCausalBuffer(node)

        # Final handler (your app logic)
        async def final_handler(msg: Message):
            text = msg.payload.get("text", msg.payload)
            print(f"\n[CAUSAL DELIVERY] [{msg.sender_id}] {text}")

        # Hook: receive → buffer → final
        self.on_message_received = lambda m: self.causal_buffer.deliver(m, final_handler)

        # ---------- NEW: per-sender sequence counters ----------
        self._out_seq: defaultdict[str, int] = defaultdict(int)   # sender_id → next seq number


    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        """Create the single UDP receive socket and start the receive loop."""
        self.inbound_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.inbound_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.inbound_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass

        # ---- macOS requires bind to the concrete interface IP ----
        bind_addr = self.local_ip if self.node.host == "0.0.0.0" else self.node.host
        self.inbound_sock.bind((bind_addr, self.node.port))
        self.inbound_sock.setblocking(False)

        self.node.logger.info(
            f"MessagingService listening on UDP {bind_addr}:{self.node.port}"
        )
        asyncio.create_task(self._receive_loop())
        # asyncio.create_task(self._receive_multicast_loop())

    async def shutdown(self):
        if self.inbound_sock:
            self.inbound_sock.close()
        self.node.logger.info("MessagingService shut down")

    # ------------------------------------------------------------------
    # Discovery callbacks
    # ------------------------------------------------------------------
    def _peer_discovered(self, peer_id: str, host: str, port: int):
        """Called by DiscoveryService when a new peer appears."""
        if peer_id == self.node.id:
            return
        self.peers[peer_id] = (host, port)
        self.node.logger.info(f"MessagingService knows peer {peer_id} -> {host}:{port}")

    def _peer_lost(self, peer_id: str):
        """Called when a peer times-out."""
        self.peers.pop(peer_id, None)
        self.node.logger.info(f"MessagingService removed dead peer {peer_id}")

    # ------------------------------------------------------------------
    # Low-level send / receive
    # ------------------------------------------------------------------
    async def _send_raw(self, addr: Tuple[str, int], payload: bytes):
        """Send a fully-framed payload to a single address."""
        loop = asyncio.get_event_loop()
        print (f"Sending to {addr}: {payload}")
        await loop.sock_sendto(self.inbound_sock, payload, addr)

    def _frame(self, message: dict) -> bytes:
        """JSON + 4-byte length prefix."""
        data = json.dumps(message).encode("utf-8")
        return struct.pack("!I", len(data)) + data

    async def _receive_loop(self):
        """One task that receives from *all* peers."""
        loop = asyncio.get_event_loop()
        while True:
            try:
                data, addr = await loop.sock_recvfrom(self.inbound_sock, 65536)
                print (f"Received from {addr}: {data}")
                if len(data) < 4:
                    continue
                length = struct.unpack("!I", data[:4])[0]
                if len(data) != 4 + length:
                    continue                     # malformed – drop
                msg = json.loads(data[4:].decode("utf-8"))
                print(f"Decoded message: {msg}")
                # RECONSTRUCT Message object
                msg = Message.from_dict(msg)
                print(f"test line:",msg)

                # Identify sender (reverse lookup in our peer table)
                sender_id = None
                for pid, (h, p) in self.peers.items():
                    if (h, p) == addr:
                        sender_id = pid
                        break
                print(f"Dispatching message from {sender_id}: {msg.to_dict()}")
                print(f"addr: {addr}")
                await self._handle_incoming(sender_id, addr, msg)
            except Exception as e:
                self.node.logger.debug(f"Receive loop error: {e}")
                await asyncio.sleep(0.01)
    
    

    async def _handle_incoming(self, sender_id: Optional[str], addr: Tuple[str, int], msg: Message):
        """Dispatch incoming message."""
        if msg.type == "NACK":
            await self.if_negative_ack_received(sender_id, msg.payload.get("original", ""))
            return
        display_sender = sender_id if sender_id is not None else msg.sender_id
        # This line will now ALWAYS print
        print(f"Handling incoming message from {display_sender}: {msg.payload.get('text', msg.payload)}")

        # Deliver through causal buffer (this already works)
        if self.on_message_received:
            await self.on_message_received(msg)
        else:
            # fallback (only if you still have the old on_message_received = None case)
            self.node.logger.info(f"← {display_sender} : {msg.payload.get('text', msg.payload)}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def send_to(self, peer_id: str, payload: dict):
        """Send a message to a specific peer."""
        print(self.peers);
        if peer_id not in self.peers:
            self.node.logger.warning(f"Cannot send to unknown peer {peer_id}")
            return
        msg = self._build_message(payload)
        framed = self._frame(msg.to_dict())
        await self._send_raw(self.peers[peer_id], framed)

    async def send_message_to_leader(self, payload: dict):
        """Convenient wrapper for leader communication."""
        if self.node.is_leader:
            self.node.logger.warning("I am the leader – not sending to myself.")
            return
        if not self.node.leader_id:
            self.node.logger.warning("No leader known yet.")
            return
        await self.send_to(self.node.leader_id, payload)

    async def multicast_message(self, payload: dict):
        """Send ONE multicast packet to the entire network (224.0.0.251)"""
        if not hasattr(self.node, 'discovery_service') or not self.node.discovery_service.mcast_socket:
            self.node.logger.warning("Discovery service not ready — cannot multicast")
            return

        msg = self._build_message(payload)
        framed = self._frame(msg.to_dict())

        try:
            # Reuse the same multicast socket that DiscoveryService created
            mcast_sock = self.node.discovery_service.mcast_socket
            loop = asyncio.get_event_loop()
            await loop.sock_sendto(
                mcast_sock,
                framed,
                (self.node.discovery_service.MCAST_GRP, self.node.discovery_service.MCAST_PORT)
            )
            self.node.logger.info(f"Multicast sent to {self.node.discovery_service.MCAST_GRP}:{self.node.discovery_service.MCAST_PORT}")
        except Exception as e:
            self.node.logger.error(f"Failed to send multicast: {e}")

    # ------------------------------------------------------------------
    # Helper: build a Message with a fresh seq counter
    # ------------------------------------------------------------------
    def _build_message(self, payload: dict, type: str = "APP") -> Message:
        self.node.vector_clock[self.node.id] = self.node.vector_clock.get(self.node.id, 0) + 1
        return Message(
            msg_id    = str(uuid.uuid4()),
            sender_id = self.node.id,
            vector_clock = self.node.vector_clock.copy(),
            payload   = payload,
            type      = type,
        )

    # ------------------------------------------------------------------
    # NACK hook (you can expand with retransmission logic)
    # ------------------------------------------------------------------
    async def if_negative_ack_received(self, peer_id: str, original_message: str):
        self.node.logger.warning(f"NACK from {peer_id}: {original_message}")
        # Example: resend the original payload
        # await self.send_to(peer_id, json.loads(original_message))
