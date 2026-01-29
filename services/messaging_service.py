import asyncio
import json
import socket
import struct
import netifaces
import uuid
import logging
from collections import defaultdict, OrderedDict
from typing import Dict, Optional, Tuple
from Data.message import Message
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

        # Create dedicated logger for this service
        self.logger = logging.getLogger(f"MessagingService-{node.name}")

        # ------------------------------------------------------------------
        # Hook the discovery callbacks
        # ------------------------------------------------------------------
        self.node.discovery_service.on_peer_discovered = self._peer_discovered
        self.node.discovery_service.on_peer_lost       = self._peer_lost

        # Causal buffer
        self.causal_buffer = VectorCausalBuffer(node, on_causal_gap = self.send_nack)  ## callback on causal gap detected
        self.sent_history: OrderedDict[Tuple[str, int], Message] = OrderedDict()  ## (sender_id, seq) -> Message // store sent messages for potential retransmission
        self.max_history = 100  ## max number of messages to keep in history for retransmission

        # Final handler (your app logic)
        async def final_handler(msg: Message):
            text = msg.payload.get("text", msg.payload)
            self.logger.info(f"[CAUSAL DELIVERY] [{msg.sender_id[:8]}...] {text}")

        # Hook: receive → buffer → final
        self.on_message_received = lambda m: self.causal_buffer.deliver(m, final_handler)

        # ---------- NEW: per-sender sequence counters ----------
        self._out_seq: defaultdict[str, int] = defaultdict(int)   # sender_id → next seq number

        self.history = {}

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

        self.logger.info(
            f"MessagingService listening on UDP {bind_addr}:{self.node.port}"
        )
        asyncio.create_task(self._receive_loop())

    async def shutdown(self):
        if self.inbound_sock:
            self.inbound_sock.close()
        self.logger.info("MessagingService shut down")

    # ------------------------------------------------------------------
    # Discovery callbacks
    # ------------------------------------------------------------------
    def _peer_discovered(self, peer_id: str, host: str, port: int):
        """Called by DiscoveryService when a new peer appears."""
        if peer_id == self.node.id:
            return
        self.peers[peer_id] = (host, port)
        self.logger.info(f"MessagingService knows peer {peer_id} -> {host}:{port}")

    def _peer_lost(self, peer_id: str):
        """Called when a peer times-out."""
        self.peers.pop(peer_id, None)
        self.logger.info(f"MessagingService removed dead peer {peer_id}")

    # ------------------------------------------------------------------
    # Low-level send / receive
    # ------------------------------------------------------------------
    async def _send_raw(self, addr: Tuple[str, int], payload: bytes):
        """Send a fully-framed payload to a single address."""
        loop = asyncio.get_event_loop()
        self.logger.debug(f"Sending to {addr}: {payload[:100]}...")  # Truncate for readability
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
                self.logger.debug(f"Received from {addr}: {data[:100]}...")
                if len(data) < 4:
                    continue
                length = struct.unpack("!I", data[:4])[0]
                if len(data) != 4 + length:
                    continue                     # malformed – drop
                msg = json.loads(data[4:].decode("utf-8"))
                self.logger.debug(f"Decoded message: {msg}")
                # RECONSTRUCT Message object
                msg = Message.from_dict(msg)

                # Identify sender (reverse lookup in our peer table)
                sender_id = None
                for pid, (h, p) in self.peers.items():
                    if (h, p) == addr:
                        sender_id = pid
                        break
                self.logger.debug(f"Dispatching message from {sender_id}: type={msg.type}")
                await self._handle_incoming(sender_id, addr, msg)
            except Exception as e:
                self.logger.debug(f"Receive loop error: {e}")
                await asyncio.sleep(0.01)
    
    

    async def _handle_incoming(self, sender_id: Optional[str], addr: Tuple[str, int], msg: Message):
        """Dispatch incoming message."""
        
        # Handle ring topology messages
        if hasattr(self.node, 'ring_service') and self.node.ring_service:
            if msg.type == "JOIN_REQUEST":
                await self.node.ring_service.handle_join_request(sender_id or msg.sender_id, msg)
                return
            elif msg.type == "JOIN_RESPONSE":
                await self.node.ring_service.handle_join_response(msg)
                return
            elif msg.type == "UPDATE_PREDECESSOR":
                await self.node.ring_service.handle_update_predecessor(msg)
                return
            elif msg.type == "UPDATE_SUCCESSOR":
                await self.node.ring_service.handle_update_successor(msg)
                return
            elif msg.type == "GET_PREDECESSOR":
                await self.node.ring_service.handle_get_predecessor(sender_id or msg.sender_id)
                return
            elif msg.type == "PREDECESSOR_RESPONSE":
                await self.node.ring_service.handle_predecessor_response(msg)
                return
            elif msg.type == "NOTIFY":
                await self.node.ring_service.handle_notify(msg)
                return
        
        # Handle leader election messages
        if hasattr(self.node, 'leader_election') and self.node.leader_election:
            if msg.type == "ELECTION":
                await self.node.leader_election.handle_election_message(msg)
                return
            elif msg.type == "ELECTION_REPLY":
                await self.node.leader_election.handle_election_reply(msg)
                return
            elif msg.type == "LEADER_ANNOUNCEMENT":
                await self.node.leader_election.handle_leader_announcement(msg)
                return
            elif msg.type == "LEADER_HEARTBEAT":
                await self.node.leader_election.handle_leader_heartbeat(msg)
                return
        
        # Handle NACK messages
        if msg.type == "NACK":
            await self.if_negative_ack_received(sender_id, msg.payload.get("original", ""))
            return
        # Handle ALERT messages
        if msg.payload.get("type") == "ALERT": 
            print("IMPORTANT!!!")
            print(f"ALERT MESSAGE FROM {msg.sender_id}: {msg.payload.get('text')}")
            return
        
        display_sender = sender_id if sender_id is not None else msg.sender_id
        self.logger.debug(f"Handling incoming message from {display_sender}: {msg.payload.get('text', msg.payload)}")
        if msg.payload.get("type") != "ALERT": 
            print(f"MESSAGE FROM {msg.sender_id}: {msg.payload.get('text')}")

        # Deliver through causal buffer
        if self.on_message_received:
            await self.on_message_received(msg)
        else:
            # fallback
            self.logger.info(f"← {display_sender} : {msg.payload.get('text', msg.payload)}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def send_to(self, peer_id: str, payload: dict):
        """Send a message to a specific peer."""
        if peer_id not in self.peers:
            self.logger.warning(f"Cannot send to unknown peer {peer_id}")
            return
        msg = self._build_message(payload)
        framed = self._frame(msg.to_dict())
        await self._send_raw(self.peers[peer_id], framed)
        
        # STORE IN HISTORY
        key = (self.node.id, self.node.vector_clock[self.node.id])
        self.sent_history[key] = msg
        self._prune_history()
        
    async def send_message_direct(self, peer_id: str, message_dict: dict):
        """
        Send a pre-built message dictionary directly without wrapping.
        Used by RingService and other control protocols.
        """
        if peer_id not in self.peers:
            self.logger.warning(f"Cannot send to unknown peer {peer_id}")
            return
        framed = self._frame(message_dict)
        await self._send_raw(self.peers[peer_id], framed)

    async def send_message_to_leader(self, payload: dict):
        """Convenient wrapper for leader communication."""
        if self.node.is_leader:
            self.logger.warning("I am the leader - not sending to myself.")
            return
        if not self.node.leader_id:
            self.logger.warning("No leader known yet.")
            return
        await self.send_to(self.node.leader_id, payload)

    async def multicast_message(self, payload: dict):
        """Send ONE multicast packet to the entire network (224.0.0.251)"""
        if not hasattr(self.node, 'discovery_service') or not self.node.discovery_service.mcast_socket:
            self.logger.warning("Discovery service not ready — cannot multicast")
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
            
             # STORE IN HISTORY
            key = (self.node.id, self.node.vector_clock[self.node.id])
            self.sent_history[key] = msg
            self._prune_history()
            self.logger.info(f"Multicast sent to {self.node.discovery_service.MCAST_GRP}:{self.node.discovery_service.MCAST_PORT}")
        except Exception as e:
            self.logger.error(f"Failed to send multicast: {e}")

    # ------------------------------------------------------------------
    # Helper: prunest sent history to limit size
    # ------------------------------------------------------------------ 
    def _prune_history(self):
        while len(self.sent_history) > self.max_history:
            self.sent_history.popitem(last=False)  # remove oldest
            
    # ------------------------------------------------------------------
    # Helper: build a Message with a fresh seq counter
    # ------------------------------------------------------------------
    def _build_message(self, payload: dict, type: str = "APP") -> Message:
        self.node.vector_clock[self.node.id] = self.node.vector_clock.get(self.node.id, 0) + 1
        message = Message(
            msg_id    = str(uuid.uuid4()),
            sender_id = self.node.id,
            vector_clock = self.node.vector_clock.copy(),
            payload   = payload,
            type      = type
        )
        sent_history = (self.node.id, message.vector_clock[self.node.id])
        self.history[sent_history] = message
        return message

    # ------------------------------------------------------------------
    # NACK Send hook
    # ------------------------------------------------------------------ 
    async def send_nack(self, peer_id:str, missing_info: dict):
        """Send a NACK message to a specific peer."""
        if peer_id not in self.peers:
            self.node.logger.warning(f"Cannot send NACK to unknown peer {peer_id}")
            return
        nack_payload = {
            "type": "NACK",
            "my_vector_clock": self.node.vector_clock,
            "reason": f"Missing message with seq {missing_info.get('expected_seq')}",
            "original": json.dumps(missing_info)
        }
        msg = self._build_message(nack_payload, type="NACK")
        framed = self._frame(msg.to_dict())
        await self._send_raw(self.peers[peer_id], framed)
    # ------------------------------------------------------------------
    # NACK Recv hook
    # ------------------------------------------------------------------
    async def if_negative_ack_received(self, peer_id: str, nack_payload: dict):
        self.node.logger.warning(f"NACK from {peer_id}: {nack_payload}")

        # Example: resend the original payload
        # await self.send_to(peer_id, json.loads(original_message))

        #variables to store the vector clokck info from nack payload
        their_vector_clock = nack_payload.get("my_vector_clock", {})
        if( not their_vector_clock):
            self.node.logger.warning(f"NACK from {peer_id} missing vector clock info.")
            return
        my_id = self.node.id

        # Resend missing messages based on their vector clock
        their_known_sequence = their_vector_clock.get(my_id, 0)
        my_local_sequence = self.node.vector_clock.get(my_id, 0)

        # Only resend if they are behind
        if(their_known_sequence >= my_local_sequence):
            self.node.logger.info(f"No messages to resend to {peer_id}.")
            return
        messages_to_resend = []
        for seq in range(their_known_sequence + 1, my_local_sequence + 1):
            key = (my_id, seq)
            if key in self.sent_history:
                messages_to_resend.append(self.sent_history[key])
            else:
                self.node.logger.warning(f"Message with seq {seq} not found in history for resend.")
            
        #send the messages in order, unicast to the peer who sent the nack
        for msg in messages_to_resend:
            #use the send_to function to resend the message
            await self.send_to(peer_id, msg.payload)
            # Small delay to avoid packet burst if many messages
            await asyncio.sleep(0.001)