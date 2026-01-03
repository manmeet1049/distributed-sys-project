"""
Leader Election Service using Hirschberg-Sinclair (HS) Algorithm

The HS algorithm is a bidirectional leader election algorithm for ring topologies.
It elects the node with the highest ID as the leader.

Algorithm Overview:
- Each node sends election messages in both clockwise and counter-clockwise directions
- Messages travel a maximum distance of 2^phase hops
- If a node's message returns successfully, it proceeds to the next phase
- The node with the highest ID eventually wins and announces itself as leader

Message Types:
- ELECTION: Sent to probe both directions (contains: initiator_id, hop_count, direction, phase)
- ELECTION_REPLY: Reply back to initiator when message reaches its hop limit
- LEADER_ANNOUNCEMENT: Broadcast by the winner to inform all nodes
"""

import asyncio
import logging
import uuid
from typing import Optional, Dict, Set
from enum import Enum
from Data.message import Message


class Direction(Enum):
    """Direction of election message travel"""
    CLOCKWISE = "CLOCKWISE"
    COUNTER_CLOCKWISE = "COUNTER_CLOCKWISE"


class LeaderElectionService:
    """
    Implements the Hirschberg-Sinclair (HS) leader election algorithm.
    
    The HS algorithm runs in phases:
    - Phase k: messages travel up to 2^k hops in each direction
    - A node proceeds to phase k+1 only if both its messages return successfully
    - The algorithm terminates when a node's messages complete a full ring traversal
    """

    def __init__(self, node):
        """
        Initialize the leader election service.
        
        Args:
            node: Reference to the parent Node instance
        """
        self.node = node
        self.messaging = None  # Will be set after MessagingService is created
        self.ring_service = None  # Will be set after RingService is created
        
        # Create dedicated logger
        self.logger = logging.getLogger(f"LeaderElection-{node.name}")
        
        # Election state
        self.current_phase = 0
        self.max_phase = 20  # Prevent infinite phases (2^20 = 1M hops max)
        self.is_participating = False
        self.election_in_progress = False
        self.awaiting_replies: Dict[int, Set[Direction]] = {}  # {phase: {directions}}
        
        # Leader information
        self.leader_id: Optional[str] = None
        self.is_leader = False
        
        # Message tracking to prevent loops
        self.processed_messages: Set[str] = set()  # Set of message IDs we've seen
        self.message_ttl = 300  # seconds to keep message IDs in memory
        
        # Tasks
        self._election_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        
        # Leader heartbeat configuration
        self.heartbeat_interval = 3  # seconds - how often leader sends heartbeat
        self.heartbeat_timeout = 10  # seconds - how long to wait before declaring leader dead
        self.last_heartbeat_time: Optional[float] = None
        
        self.logger.info(f"LeaderElectionService initialized for {node.name} (UUID: {node.uuid})")

    async def start(self):
        """Start the leader election service."""
        self.messaging = self.node.messaging
        self.ring_service = self.node.ring_service
        
        # Start cleanup task for processed messages
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        # Start heartbeat task (both sending and monitoring)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        self.logger.info("LeaderElectionService started")

    async def shutdown(self):
        """Stop the leader election service."""
        if self._election_task:
            self._election_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self.logger.info("LeaderElectionService shut down")

    # ------------------------------------------------------------------
    # Election Initiation
    # ------------------------------------------------------------------
    
    async def _validate_ring_neighbors(self):
        """
        Validate that successor and predecessor are alive.
        Trigger ring service to fix them if they're dead.
        """
        successor_alive = (self.ring_service.successor_id and 
                          self.ring_service.successor_id in self.messaging.peers)
        predecessor_alive = (self.ring_service.predecessor_id and 
                            self.ring_service.predecessor_id in self.messaging.peers)
        
        if not successor_alive and self.ring_service.successor_id:
            self.logger.warning(
                f"Successor {self.ring_service.successor_id[:8]}... is dead, fixing..."
            )
            await self.ring_service._fix_successor()
        
        if not predecessor_alive and self.ring_service.predecessor_id:
            self.logger.warning(
                f"Predecessor {self.ring_service.predecessor_id[:8]}... is dead, fixing..."
            )
            await self.ring_service._fix_predecessor()
        
        # Also check if either is None (not just dead)
        if not self.ring_service.successor_id:
            self.logger.warning("Successor is None, fixing...")
            await self.ring_service._fix_successor()
        
        if not self.ring_service.predecessor_id:
            self.logger.warning("Predecessor is None, fixing...")
            await self.ring_service._fix_predecessor()
        
        # Give a moment for notifications to propagate
        if not successor_alive or not predecessor_alive:
            await asyncio.sleep(0.5)
    
    async def start_election(self):
        """
        Initiate a new leader election.
        
        Can be called:
        - Manually by user command
        - Automatically when ring is established
        - After detecting leader failure
        """
        # Special case: If we're not in a ring, check if we should become leader
        if not self.ring_service.is_in_ring():
            # If ring service cleared the ring (set ring_established = False),
            # it means it couldn't find any valid neighbors.
            # We should declare ourselves leader since we're effectively alone.
            self.logger.info("Not in a ring after topology change - declaring self as leader")
            self.leader_id = self.node.uuid
            self.is_leader = True
            self.node.leader_id = self.node.uuid
            self.node.is_leader = True
            self.election_in_progress = False
            self.is_participating = False
            self.logger.info(f"🎉 I am the leader! (UUID: {self.node.uuid[:8]}...)")
            return
        
        # Check if ring is stable before starting election
        if not self.ring_service.is_ring_stable():
            self.logger.warning("Cannot start election: ring is not stable (topology still changing)")
            return
        
        # Check if election already in progress
        if self.election_in_progress:
            self.logger.info("Election already in progress")
            return
        
        # Validate ring neighbors are alive, fix if needed
        await self._validate_ring_neighbors()
        
        # Re-check if we're still in a ring after validation
        # (validation might have discovered we're actually alone)
        if not self.ring_service.is_in_ring():
            self.logger.info("Not in a ring after validation - declaring self as leader")
            self.leader_id = self.node.uuid
            self.is_leader = True
            self.node.leader_id = self.node.uuid
            self.node.is_leader = True
            self.election_in_progress = False
            self.is_participating = False
            self.logger.info(f"🎉 I am the leader! (UUID: {self.node.uuid[:8]}...)")
            return
        
        self.logger.info(f"Starting HS election (Node UUID: {self.node.uuid})")
        self.election_in_progress = True
        self.is_participating = True
        self.current_phase = 0
        self.awaiting_replies = {}
        
        # Start election from phase 0
        await self._run_phase(0)

    async def _run_phase(self, phase: int):
        """
        Execute a single phase of the HS algorithm.
        
        In phase k:
        1. Send ELECTION messages in both directions with hop_count = 2^k
        2. Wait for both replies
        3. If both replies received, proceed to phase k+1
        
        Args:
            phase: Current phase number (0-indexed)
        """
        if phase > self.max_phase:
            self.logger.warning(f"Reached max phase {self.max_phase}, stopping election")
            self.election_in_progress = False
            return
        
        max_hops = 2 ** phase
        self.current_phase = phase
        self.awaiting_replies[phase] = {Direction.CLOCKWISE, Direction.COUNTER_CLOCKWISE}
        
        self.logger.info(f"Phase {phase}: Sending election messages with max_hops={max_hops}")
        
        # Send election messages in both directions
        await self._send_election_message(Direction.CLOCKWISE, max_hops, phase)
        await self._send_election_message(Direction.COUNTER_CLOCKWISE, max_hops, phase)
        
        # Note: We don't wait here - replies are handled asynchronously
        # When both replies are received, _handle_election_reply will trigger next phase

    async def _send_election_message(self, direction: Direction, max_hops: int, phase: int):
        """
        Send an ELECTION message in the specified direction.
        
        Args:
            direction: CLOCKWISE or COUNTER_CLOCKWISE
            max_hops: Maximum number of hops this message can travel
            phase: Current election phase
        """
        # Determine target node based on direction
        if direction == Direction.CLOCKWISE:
            target_id = self.ring_service.successor_id
        else:
            target_id = self.ring_service.predecessor_id
        
        if not target_id:
            self.logger.warning(f"Cannot send election message {direction.value}: no neighbor")
            # Treat as failed reply so election doesn't hang
            await self._handle_failed_direction(phase, direction)
            return
        
        # Check if target is alive (in messaging peers)
        if target_id not in self.messaging.peers:
            self.logger.warning(
                f"Cannot send election message {direction.value}: target {target_id[:8]}... not in peers list"
            )
            # Treat as failed reply so election doesn't hang
            await self._handle_failed_direction(phase, direction)
            return
        
        self.logger.info(
            f"Preparing ELECTION message: direction={direction.value}, "
            f"target={target_id[:8]}..., max_hops={max_hops}, phase={phase}"
        )
        
        # Create election message
        msg = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "initiator_id": self.node.uuid,  # Use UUID for comparison
                "initiator_name": self.node.name,  # For logging
                "current_id": self.node.uuid,  # Current node being compared
                "hop_count": 0,
                "max_hops": max_hops,
                "direction": direction.value,
                "phase": phase,
                "path": [self.node.uuid[:8]]  # Track path for debugging
            },
            type="ELECTION"
        )
        
        try:
            await self.messaging.send_message_direct(target_id, msg.to_dict())
            self.logger.info(
                f"Sent ELECTION message: phase={phase}, direction={direction.value}, "
                f"max_hops={max_hops} to {target_id[:8]}..."
            )
        except Exception as e:
            self.logger.error(f"Failed to send election message to {target_id}: {e}")

    # ------------------------------------------------------------------
    # Message Handlers
    # ------------------------------------------------------------------
    
    async def handle_election_message(self, msg: Message):
        """
        Handle incoming ELECTION message.
        
        The HS algorithm logic:
        1. If initiator_id > my_id: Forward the message
        2. If initiator_id < my_id: Send rejection (don't forward)
        3. If initiator_id == my_id: Message returned to sender (send reply)
        
        Args:
            msg: Election message
        """
        # Check if we've already processed this specific message (prevent loops)
        if msg.msg_id in self.processed_messages:
            return
        
        self.processed_messages.add(msg.msg_id)
        
        payload = msg.payload
        initiator_id = payload["initiator_id"]
        initiator_name = payload.get("initiator_name", initiator_id[:8])
        current_id = payload["current_id"]
        hop_count = payload["hop_count"]
        max_hops = payload["max_hops"]
        direction = Direction(payload["direction"])
        phase = payload["phase"]
        path = payload.get("path", [])
        
        self.logger.info(
            f"Received ELECTION: initiator={initiator_name} ({initiator_id[:8]}...), "
            f"phase={phase}, hops={hop_count}/{max_hops}, direction={direction.value}"
        )
        
        # Check if message has returned to initiator
        if initiator_id == self.node.uuid:
            self.logger.info(
                f"Election message returned to initiator (phase {phase}, {direction.value})"
            )
            # Don't send reply to ourselves, handle it directly
            await self._handle_own_message_return(phase, direction)
            return
        
        # Compare IDs: larger ID continues, smaller ID stops
        if initiator_id < self.node.uuid:
            # Initiator has smaller ID - reject and don't forward
            self.logger.info(
                f"Rejecting election from {initiator_name}: "
                f"their ID {initiator_id[:8]}... < my ID {self.node.uuid[:8]}..."
            )
            # Send rejection back in opposite direction
            await self._send_rejection(direction, initiator_id, phase)
            
            # HS algorithm: If we reject someone, we should start our own election
            # (if we're not already participating)
            if not self.election_in_progress and not self.is_participating:
                self.logger.info(
                    f"Starting own election after rejecting {initiator_name}"
                )
                asyncio.create_task(self.start_election())
            
            return
        
        # If we reach here: initiator_id >= my_id
        # If we were participating and initiator_id > my_id, we stop participating
        if self.is_participating and initiator_id > self.node.uuid:
            self.logger.info(
                f"Stopping participation: {initiator_name} ({initiator_id[:8]}...) > "
                f"my ID ({self.node.uuid[:8]}...)"
            )
            self.is_participating = False
            self.election_in_progress = False
        
        # Check if message has reached its hop limit
        if hop_count >= max_hops:
            self.logger.info(
                f"Election message reached hop limit ({max_hops}), sending reply in reverse"
            )
            # Send reply back in the OPPOSITE direction
            await self._send_election_reply(direction, initiator_id, phase)
            return
        
        # Forward the message to the next node in the direction
        await self._forward_election_message(msg, direction, path)

    async def _forward_election_message(self, msg: Message, direction: Direction, path: list):
        """
        Forward an election message to the next node in the ring.
        
        Args:
            msg: Original election message
            direction: Direction to forward (CLOCKWISE or COUNTER_CLOCKWISE)
            path: Path taken so far (for debugging)
        """
        # Determine next hop
        if direction == Direction.CLOCKWISE:
            next_id = self.ring_service.successor_id
        else:
            next_id = self.ring_service.predecessor_id
        
        if not next_id:
            self.logger.warning(f"Cannot forward election message: no {direction.value} neighbor")
            return
        
        # Update message payload
        payload = msg.payload.copy()
        payload["hop_count"] += 1
        payload["current_id"] = self.node.uuid
        payload["path"] = path + [self.node.uuid[:8]]
        
        # Create new message with updated payload
        forwarded_msg = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload=payload,
            type="ELECTION"
        )
        
        try:
            await self.messaging.send_message_direct(next_id, forwarded_msg.to_dict())
            self.logger.info(
                f"Forwarded ELECTION to {next_id[:8]}... "
                f"(hop {payload['hop_count']}/{payload['max_hops']})"
            )
        except Exception as e:
            self.logger.error(f"Failed to forward election message: {e}")

    async def _send_election_reply(self, original_direction: Direction,
                                   initiator_id: str, phase: int):
        """
        Send an ELECTION_REPLY back toward the initiator.
        The reply travels in the OPPOSITE direction of the original message.
        
        Args:
            original_direction: Direction the election message came from
            initiator_id: Original initiator of the election message
            phase: Election phase
        """
        # Determine reverse direction
        if original_direction == Direction.CLOCKWISE:
            reply_direction = Direction.COUNTER_CLOCKWISE
            target_id = self.ring_service.predecessor_id
        else:
            reply_direction = Direction.CLOCKWISE
            target_id = self.ring_service.successor_id
        
        if not target_id:
            self.logger.warning(f"Cannot send reply: no neighbor in reverse direction")
            return
        
        reply = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "initiator_id": initiator_id,
                "phase": phase,
                "direction": original_direction.value,  # Keep original direction for tracking
                "success": True
            },
            type="ELECTION_REPLY"
        )
        
        try:
            await self.messaging.send_message_direct(target_id, reply.to_dict())
            self.logger.info(
                f"Sent ELECTION_REPLY to {target_id[:8]}... (traveling {reply_direction.value}) for phase {phase}"
            )
        except Exception as e:
            self.logger.error(f"Failed to send election reply: {e}")

    async def _send_rejection(self, original_direction: Direction,
                             initiator_id: str, phase: int):
        """
        Send rejection to indicate initiator should not continue.
        Rejection travels in the OPPOSITE direction of the original message.
        
        Args:
            original_direction: Direction the election message came from
            initiator_id: Original initiator being rejected
            phase: Election phase
        """
        # Determine reverse direction
        if original_direction == Direction.CLOCKWISE:
            target_id = self.ring_service.predecessor_id
        else:
            target_id = self.ring_service.successor_id
        
        if not target_id:
            self.logger.warning(f"Cannot send rejection: no neighbor")
            return
        
        rejection = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "initiator_id": initiator_id,
                "phase": phase,
                "direction": original_direction.value,
                "success": False,
                "reason": f"ID {self.node.uuid[:8]}... > initiator ID"
            },
            type="ELECTION_REPLY"
        )
        
        try:
            await self.messaging.send_message_direct(target_id, rejection.to_dict())
            self.logger.info(f"Sent rejection to {target_id[:8]}...")
        except Exception as e:
            self.logger.error(f"Failed to send rejection: {e}")

    async def handle_election_reply(self, msg: Message):
        """
        Handle ELECTION_REPLY message.
        
        When both directions reply successfully, proceed to next phase.
        If either direction fails, stop participating.
        
        Args:
            msg: Election reply message
        """
        # Check if we've already processed this specific message (prevent loops)
        if msg.msg_id in self.processed_messages:
            return
        
        self.processed_messages.add(msg.msg_id)
        
        payload = msg.payload
        initiator_id = payload["initiator_id"]
        phase = payload["phase"]
        direction = Direction(payload["direction"])
        success = payload.get("success", True)
        
        # If not for us, forward it back toward the initiator
        if initiator_id != self.node.uuid:
            self.logger.info(f"Forwarding reply for {initiator_id[:8]}... back toward initiator")
            # Forward in the opposite direction from where the election came
            if direction == Direction.CLOCKWISE:
                # Election went clockwise, reply goes counter-clockwise
                forward_to = self.ring_service.predecessor_id
            else:
                # Election went counter-clockwise, reply goes clockwise
                forward_to = self.ring_service.successor_id
            
            if forward_to:
                try:
                    await self.messaging.send_message_direct(forward_to, msg.to_dict())
                    self.logger.info(f"Forwarded ELECTION_REPLY to {forward_to[:8]}...")
                except Exception as e:
                    self.logger.error(f"Failed to forward reply: {e}")
            return
        
        # Ignore if we're not participating
        if not self.is_participating:
            self.logger.info("Ignoring reply - not participating in election")
            return
        
        self.logger.info(
            f"Received ELECTION_REPLY: phase={phase}, direction={direction.value}, "
            f"success={success}"
        )
        
        # If rejection, stop participating
        if not success:
            self.logger.info(f"Received rejection, stopping participation")
            self.is_participating = False
            self.election_in_progress = False
            return
        
        # Mark this direction as replied
        if phase in self.awaiting_replies:
            if direction in self.awaiting_replies[phase]:
                self.awaiting_replies[phase].remove(direction)
                
                # Check if both directions have replied
                if len(self.awaiting_replies[phase]) == 0:
                    self.logger.info(f"Phase {phase} complete, both directions replied")
                    # Check if we've completed a full ring traversal
                    max_hops = 2 ** phase
                    ring_size = len(self.messaging.peers) + 1  # +1 for self
                    
                    if max_hops >= ring_size:
                        # We've won! Announce leadership
                        await self._announce_leadership()
                    else:
                        # Proceed to next phase
                        await self._run_phase(phase + 1)

    async def _handle_own_message_return(self, phase: int, direction: Direction):
        """
        Handle when our own election message returns to us.
        This means we've completed a ring traversal.
        
        Args:
            phase: Election phase
            direction: Direction the message traveled
        """
        if not self.is_participating:
            return
            
        # Mark this direction as complete
        if phase in self.awaiting_replies:
            if direction in self.awaiting_replies[phase]:
                self.awaiting_replies[phase].remove(direction)
                
                # Check if both directions have returned
                if len(self.awaiting_replies[phase]) == 0:
                    self.logger.info(
                        f"Phase {phase} complete - messages returned from both directions"
                    )
                    # We've completed a full ring traversal - we're the leader!
                    await self._announce_leadership()

    async def _handle_failed_direction(self, phase: int, direction: Direction):
        """
        Handle a failed election message send (e.g., target is dead).
        Mark that direction as complete so election doesn't hang.
        
        Args:
            phase: Election phase
            direction: Direction that failed
        """
        self.logger.warning(
            f"Phase {phase}: Direction {direction.value} failed, marking as complete"
        )
        
        # Mark this direction as complete to prevent hanging
        if phase in self.awaiting_replies:
            if direction in self.awaiting_replies[phase]:
                self.awaiting_replies[phase].remove(direction)
                
                # If both directions are done (either success or failure), check if we should advance
                if len(self.awaiting_replies[phase]) == 0:
                    # If at least one direction succeeded, we might be leader
                    # For now, if we can't send in a direction, stop participating
                    self.logger.info(
                        f"Phase {phase} complete with failures - stopping participation"
                    )
                    self.is_participating = False
                    self.election_in_progress = False

    async def _announce_leadership(self):
        """
        Announce that this node is the leader.
        Broadcast LEADER_ANNOUNCEMENT to all nodes in the ring.
        """
        self.logger.info(f"🎉 I am the leader! (UUID: {self.node.uuid})")
        self.is_leader = True
        self.leader_id = self.node.uuid
        self.node.is_leader = True
        self.node.leader_id = self.node.uuid
        self.is_participating = False
        self.election_in_progress = False
        
        # Broadcast leadership announcement
        announcement = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "leader_id": self.node.uuid,
                "leader_name": self.node.name
            },
            type="LEADER_ANNOUNCEMENT"
        )
        
        # Send to all known peers
        for peer_id in self.messaging.peers.keys():
            try:
                await self.messaging.send_message_direct(peer_id, announcement.to_dict())
            except Exception as e:
                self.logger.error(f"Failed to send leader announcement to {peer_id}: {e}")

    async def handle_leader_announcement(self, msg: Message):
        """
        Handle LEADER_ANNOUNCEMENT message.
        
        Args:
            msg: Leader announcement message
        """
        payload = msg.payload
        leader_id = payload["leader_id"]
        leader_name = payload.get("leader_name", leader_id[:8])
        
        self.logger.info(f"Leader elected: {leader_name} (UUID: {leader_id[:8]}...)")
        
        # Update leader information
        self.leader_id = leader_id
        self.node.leader_id = leader_id
        self.is_leader = False
        self.node.is_leader = False
        
        # Stop participating if we were
        self.is_participating = False
        self.election_in_progress = False

    # ------------------------------------------------------------------
    # Utility Methods
    # ------------------------------------------------------------------
    
    async def _cleanup_loop(self):
        """Periodic cleanup of old message IDs."""
        while True:
            await asyncio.sleep(60)  # Clean up every minute
            # In a production system, we'd track timestamps and remove old entries
            # For now, just clear if it gets too large
            if len(self.processed_messages) > 10000:
                self.processed_messages.clear()
                self.logger.debug("Cleared processed messages cache")

    # ------------------------------------------------------------------
    # Leader Heartbeat & Failure Detection
    # ------------------------------------------------------------------
    
    async def _heartbeat_loop(self):
        """
        Dual-purpose heartbeat loop:
        1. If I'm the leader: Send periodic heartbeats to all nodes
        2. If I'm a follower: Monitor leader heartbeats and trigger re-election if leader fails
        """
        await asyncio.sleep(5)  # Wait for initial setup
        
        while True:
            try:
                if self.is_leader:
                    # I'm the leader - send heartbeats
                    await self._send_heartbeat()
                    await asyncio.sleep(self.heartbeat_interval)
                else:
                    # I'm a follower - check if leader is alive
                    await self._check_leader_alive()

                    # If we currently have no leader and no election in progress, proactively try to elect
                    if (not self.leader_id) and (not self.election_in_progress):
                        # Validate neighbors and try starting election with a tiny jitter to avoid thundering herd
                        try:
                            await self._validate_ring_neighbors()
                        except Exception as e:
                            self.logger.debug(f"Neighbor validation error: {e}")

                        # Only attempt if we're in a ring and we have at least one neighbor
                        if self.ring_service.is_in_ring() and \
                           (self.ring_service.successor_id or self.ring_service.predecessor_id):
                            import random
                            await asyncio.sleep(0.2 + random.random() * 0.6)
                            if not self.election_in_progress and not self.leader_id:
                                self.logger.info("No leader present - auto-starting election")
                                asyncio.create_task(self.start_election())

                    await asyncio.sleep(2)  # Check more frequently than heartbeat interval
            except Exception as e:
                self.logger.error(f"Heartbeat loop error: {e}")
                await asyncio.sleep(1)
    
    async def _send_heartbeat(self):
        """Send LEADER_HEARTBEAT to all nodes in the ring."""
        if not self.is_leader:
            return
        
        heartbeat = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "leader_id": self.node.uuid,
                "leader_name": self.node.name,
                "timestamp": asyncio.get_event_loop().time()
            },
            type="LEADER_HEARTBEAT"
        )
        
        # Send to all known peers
        for peer_id in self.messaging.peers.keys():
            try:
                await self.messaging.send_message_direct(peer_id, heartbeat.to_dict())
            except Exception as e:
                self.logger.debug(f"Failed to send heartbeat to {peer_id[:8]}...: {e}")
        
        self.logger.debug(f"Sent heartbeat to {len(self.messaging.peers)} peers")
    
    async def handle_leader_heartbeat(self, msg: Message):
        """
        Handle LEADER_HEARTBEAT message from the leader.
        
        Args:
            msg: Heartbeat message from leader
        """
        payload = msg.payload
        leader_id = payload["leader_id"]
        leader_name = payload.get("leader_name", leader_id[:8])
        
        # Update last heartbeat time
        import time
        self.last_heartbeat_time = time.time()
        
        # If we didn't know about this leader, update our state
        if self.leader_id != leader_id:
            self.logger.info(f"Learned about leader via heartbeat: {leader_name} ({leader_id[:8]}...)")
            self.leader_id = leader_id
            self.node.leader_id = leader_id
            self.is_leader = False
            self.node.is_leader = False
        
        self.logger.debug(f"Received heartbeat from leader {leader_name}")
    
    async def _check_leader_alive(self):
        """Check if the leader is still sending heartbeats."""
        if not self.leader_id:
            # No leader known yet
            return
        
        if self.is_leader:
            # I'm the leader, nothing to check
            return
        
        if self.election_in_progress:
            # Election already in progress
            return
        
        # Check if we've received a heartbeat recently
        if self.last_heartbeat_time is None:
            # Haven't received any heartbeat yet, wait a bit longer
            return
        
        import time
        time_since_heartbeat = time.time() - self.last_heartbeat_time
        
        if time_since_heartbeat > self.heartbeat_timeout:
            self.logger.warning(
                f"Leader {self.leader_id[:8]}... appears to be dead "
                f"(no heartbeat for {time_since_heartbeat:.1f}s)"
            )
            # Mark topology as changed BEFORE triggering failure handler
            # This ensures the ring is seen as unstable even if discovery hasn't detected the loss yet
            self.ring_service._mark_topology_change(f"heartbeat_timeout_leader_{self.leader_id[:8]}")
            
            # Manually trigger ring repair if the dead leader is in our ring neighbors
            # Don't wait for discovery's slow timeout (30s) - fix it now!
            if self.leader_id == self.ring_service.successor_id:
                self.logger.info("Dead leader is our successor, triggering repair...")
                await self.ring_service._fix_successor()
            if self.leader_id == self.ring_service.predecessor_id:
                self.logger.info("Dead leader is our predecessor, triggering repair...")
                await self.ring_service._fix_predecessor()
            
            await self._handle_leader_failure()
    
    async def _handle_leader_failure(self):
        """Handle detected leader failure by triggering re-election."""
        self.logger.info("Leader failure detected! Triggering re-election...")
        
        # Clear old leader information
        old_leader = self.leader_id
        self.leader_id = None
        self.node.leader_id = None
        self.is_leader = False
        self.node.is_leader = False
        self.last_heartbeat_time = None
        
        # Wait for ring to stabilize (discovery needs time to detect peer loss and fix topology)
        self.logger.info("Waiting for ring to stabilize after leader failure...")
        is_stable = await self.ring_service.wait_for_stable_ring(timeout=10.0)
        
        if not is_stable:
            self.logger.warning("Ring did not stabilize in time, but proceeding with election check")
        
        # Re-check if a leader was already elected while we were waiting
        if self.leader_id is not None:
            self.logger.info(f"A new leader {self.leader_id[:8]}... was already elected during wait")
            return

        # Check if we're still in a ring
        if not self.ring_service.is_in_ring():
            # Not in a ring - we're likely alone now
            # Declare ourselves as leader instead of giving up
            self.logger.info("Not in a ring after leader failure - declaring self as leader")
            self.leader_id = self.node.uuid
            self.is_leader = True
            self.node.leader_id = self.node.uuid
            self.node.is_leader = True
            self.election_in_progress = False
            self.is_participating = False
            self.logger.info(f"🎉 I am the leader! (UUID: {self.node.uuid[:8]}...)")
            return
        
        # Start new election
        self.logger.info(f"Starting new election after leader {old_leader[:8] if old_leader else 'None'}... failure")
        await self.start_election()

    # ------------------------------------------------------------------
    # Utility Methods
    # ------------------------------------------------------------------

    def get_leader_info(self) -> dict:
        """
        Get current leader information.
        
        Returns:
            dict: Leader information including ID and whether this node is leader
        """
        return {
            "leader_id": self.leader_id,
            "leader_id_short": self.leader_id[:8] if self.leader_id else None,
            "is_leader": self.is_leader,
            "election_in_progress": self.election_in_progress,
            "current_phase": self.current_phase if self.election_in_progress else None
        }

    def get_status(self) -> str:
        """Get a human-readable status string."""
        if self.is_leader:
            return f"LEADER (UUID: {self.node.uuid[:8]}...)"
        elif self.leader_id:
            return f"Follower (Leader: {self.leader_id[:8]}...)"
        elif self.election_in_progress:
            return f"Election in progress (Phase {self.current_phase})"
        else:
            return "No leader elected"
