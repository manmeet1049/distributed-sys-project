import asyncio
import time
import logging
from typing import Optional, Tuple
from Data.message import Message
import uuid


class RingService:
    """
    Manages a logical ring topology using explicit successor/predecessor pointers.
    Implements join protocol, stabilization, and failure detection.
    """

    def __init__(self, node):
        self.node = node
        self.messaging = None  # Will be set after MessagingService is created

        # Create dedicated logger for this service
        self.logger = logging.getLogger(f"RingService-{node.name}")

        # Ring state
        self.successor_id: Optional[str] = None
        self.predecessor_id: Optional[str] = None
        self.ring_established: bool = False

        # Stability tracking for elections
        self._last_topology_change = time.time()
        self._min_stable_duration = 3.0  # Ring must be stable for 3s before election

        # Stabilization state
        self.stabilization_interval = 5  # seconds
        self.ping_timeout = 2  # seconds
        self.max_ping_failures = 3
        self.successor_failures = 0

        # Tasks
        self._stabilization_task: Optional[asyncio.Task] = None
        self._liveness_task: Optional[asyncio.Task] = None

        self.logger.info(f"Initialized for node {self.node.id}")

    async def start(self):
        """Start the ring service and background tasks."""
        self.messaging = self.node.messaging

        # Hook into discovery callbacks
        self.node.discovery_service.on_peer_discovered = self._on_peer_discovered
        self.node.discovery_service.on_peer_lost = self._on_peer_lost

        # Start background tasks
        self._stabilization_task = asyncio.create_task(
            self._stabilization_loop())
        self._liveness_task = asyncio.create_task(self._liveness_check_loop())

        self.logger.info("RingService started")

    async def shutdown(self):
        """Stop background tasks."""
        if self._stabilization_task:
            self._stabilization_task.cancel()
        if self._liveness_task:
            self._liveness_task.cancel()
        self.logger.info("RingService shut down")

    # ------------------------------------------------------------------
    # Discovery callbacks
    # ------------------------------------------------------------------
    async def _on_peer_discovered(self, peer_id: str, host: str, port: int):
        """Called when DiscoveryService finds a new peer."""
        # First call the messaging service's peer discovered handler
        if hasattr(self.messaging, '_peer_discovered'):
            self.messaging._peer_discovered(peer_id, host, port)

        # If we're not in a ring yet and not currently trying to join, try to join through this peer
        # Only attempt one join at a time
        if not self.ring_established and peer_id != self.node.uuid and not hasattr(self, '_joining'):
            self._joining = True
            await self._attempt_join(peer_id)

    async def _on_peer_lost(self, peer_id: str):
        """Called when a peer times out."""
        # CRITICAL: Mark topology change IMMEDIATELY when ANY peer is lost
        # This must happen BEFORE triggering leader failure handler
        self._mark_topology_change(f"peer_lost_{peer_id[:8]}")
        
        # First call the messaging service's peer lost handler
        if hasattr(self.messaging, '_peer_lost'):
            self.messaging._peer_lost(peer_id)

        # Check if the lost peer was the leader
        if hasattr(self.node, 'leader_election') and self.node.leader_election:
            if peer_id == self.node.leader_id:
                self.logger.warning(f"Leader {peer_id[:8]}... has left the ring!")
                # Notify leader election service (it will wait for ring stability)
                asyncio.create_task(self.node.leader_election._handle_leader_failure())

        # Handle ring topology changes
        if peer_id == self.successor_id:
            self.logger.warning(
                f"Successor {peer_id} lost, finding new successor")
            await self._fix_successor()

        if peer_id == self.predecessor_id:
            self.logger.warning(
                f"Predecessor {peer_id} lost, finding new predecessor")
            await self._fix_predecessor()

    # ------------------------------------------------------------------
    # Ring join protocol
    # ------------------------------------------------------------------
    async def _attempt_join(self, bootstrap_peer_id: str):
        """Attempt to join the ring through a bootstrap peer."""
        if self.ring_established:
            if hasattr(self, '_joining'):
                delattr(self, '_joining')
            return

        self.logger.info(f"Attempting to join ring via {bootstrap_peer_id}")

        # Send JOIN_REQUEST to bootstrap peer
        join_msg = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "requester_id": self.node.id,
                "requester_host": self.node.host,
                "requester_port": self.node.port
            },
            type="JOIN_REQUEST"
        )

        try:
            await self.messaging.send_message_direct(bootstrap_peer_id, join_msg.to_dict())
        except Exception as e:
            self.logger.error(
                f"Failed to send JOIN_REQUEST to {bootstrap_peer_id}: {e}")
            # Clear joining flag on failure so we can try with another peer
            if hasattr(self, '_joining'):
                delattr(self, '_joining')

    async def handle_join_request(self, requester_id: str, msg: Message):
        """Handle a JOIN_REQUEST from a new node wanting to join the ring."""
        self.logger.info(f"Handling JOIN_REQUEST from {requester_id}")

        # Wait for peer to be in messaging peers table (up to 5 seconds)
        for _ in range(50):  # 50 attempts * 0.1s = 5 seconds max
            if requester_id in self.messaging.peers:
                break
            await asyncio.sleep(0.1)
        else:
            self.logger.error(
                f"Timeout waiting for {requester_id} in peers table")
            return

        # Check if requester is already in the ring (prevent duplicate insertion)
        if requester_id == self.successor_id or requester_id == self.predecessor_id:
            self.logger.warning(
                f"Ignoring JOIN_REQUEST from {requester_id} - already in ring"
            )
            return

        # If I'm alone (no ring yet), create a ring of two
        if self.successor_id is None:
            self.successor_id = requester_id
            self.predecessor_id = requester_id
            self.ring_established = True

            # Tell requester: you point to me in both directions
            # Also share current leader information (if any)
            response = Message(
                msg_id=str(uuid.uuid4()),
                sender_id=self.node.id,
                payload={
                    "your_successor": self.node.id,
                    "your_predecessor": self.node.id,
                    "current_leader": self.node.leader_id,
                    "leader_exists": self.node.leader_id is not None
                },
                type="JOIN_RESPONSE"
            )

            await self.messaging.send_message_direct(requester_id, response.to_dict())
            self.logger.info(
                f"Created ring of two: {self.node.id} <-> {requester_id}")
            return

        # Otherwise, insert requester between me and my current successor
        old_successor = self.successor_id
        self.successor_id = requester_id

        # Tell requester its position and share current leader information
        response = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "your_successor": old_successor,
                "your_predecessor": self.node.id,
                "current_leader": self.node.leader_id,
                "leader_exists": self.node.leader_id is not None
            },
            type="JOIN_RESPONSE"
        )

        await self.messaging.send_message_direct(requester_id, response.to_dict())

        # Notify old successor about the new predecessor
        notify = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "new_predecessor": requester_id
            },
            type="UPDATE_PREDECESSOR"
        )

        await self.messaging.send_message_direct(old_successor, notify.to_dict())

        self.logger.info(
            f"Inserted {requester_id} between {self.node.id} and {old_successor}"
        )

    async def handle_join_response(self, msg: Message):
        """Handle JOIN_RESPONSE telling us our position in the ring."""
        payload = msg.payload
        self.successor_id = payload["your_successor"]
        self.predecessor_id = payload["your_predecessor"]
        self.ring_established = True
        
        # Clear joining flag
        if hasattr(self, '_joining'):
            delattr(self, '_joining')

        # Accept leader information from bootstrap node (Active Propagation)
        current_leader = payload.get("current_leader")
        leader_exists = payload.get("leader_exists", False)
        
        if leader_exists and current_leader:
            self.logger.info(
                f"Learned existing leader from bootstrap: {current_leader[:8]}..."
            )
            # Update node's leader information
            self.node.leader_id = current_leader
            self.node.is_leader = (current_leader == self.node.uuid)
            
            # Update leader election service if available
            if hasattr(self.node, 'leader_election') and self.node.leader_election:
                self.node.leader_election.leader_id = current_leader
                self.node.leader_election.is_leader = (current_leader == self.node.uuid)
                # Don't start an election since we know the leader
                self.node.leader_election.election_in_progress = False
                self.node.leader_election.is_participating = False
        else:
            self.logger.info(
                "No existing leader reported - may need to start election"
            )

        self.logger.info(
            f"Joined ring: predecessor={self.predecessor_id}, successor={self.successor_id}"
        )

    async def handle_update_predecessor(self, msg: Message):
        """Handle notification that our predecessor has changed."""
        new_pred = msg.payload["new_predecessor"]
        self.predecessor_id = new_pred
        self.logger.info(f"Updated predecessor to {new_pred}")

    async def handle_update_successor(self, msg: Message):
        """Handle notification that our successor has changed."""
        new_succ = msg.payload["new_successor"]
        self.successor_id = new_succ
        self.logger.info(f"Updated successor to {new_succ}")

    # ------------------------------------------------------------------
    # Stabilization protocol
    # ------------------------------------------------------------------
    async def _stabilization_loop(self):
        """Periodic stabilization to maintain ring correctness."""
        await asyncio.sleep(5)  # Wait for initial discovery

        while True:
            try:
                await self._stabilize()
                await asyncio.sleep(self.stabilization_interval)
            except Exception as e:
                self.logger.debug(f"Stabilization error: {e}")
                await asyncio.sleep(1)

    async def _stabilize(self):
        """
        Stabilization protocol:
        1. Ask successor for its predecessor
        2. If that node should be between me and my successor, update my successor
        3. Notify my successor that I exist
        """
        if not self.successor_id or self.successor_id == self.node.id:
            return

        # Ask successor: "Who is YOUR predecessor?"
        query = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={},
            type="GET_PREDECESSOR"
        )

        try:
            await self.messaging.send_message_direct(self.successor_id, query.to_dict())
        except Exception as e:
            self.logger.debug(
                f"Failed to query successor during stabilization: {e}")

    async def handle_get_predecessor(self, sender_id: str):
        """Respond to GET_PREDECESSOR query."""
        response = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "my_predecessor": self.predecessor_id
            },
            type="PREDECESSOR_RESPONSE"
        )

        await self.messaging.send_message_direct(sender_id, response.to_dict())

    async def handle_predecessor_response(self, msg: Message):
        """Handle response to our GET_PREDECESSOR query."""
        their_pred = msg.payload.get("my_predecessor")

        # If their predecessor is not me and not None, it might belong between us
        if their_pred and their_pred != self.node.id and their_pred != self.successor_id:
            # Update successor to the intermediate node
            old_successor = self.successor_id
            self.successor_id = their_pred
            self.logger.info(
                f"Stabilization: updated successor from {old_successor} to {their_pred}"
            )

        # Notify our (possibly updated) successor that we are its predecessor
        await self._notify_successor()

    async def _notify_successor(self):
        """Notify successor that we think we are its predecessor."""
        if not self.successor_id or self.successor_id == self.node.id:
            return

        notify = Message(
            msg_id=str(uuid.uuid4()),
            sender_id=self.node.id,
            payload={
                "claiming_predecessor": self.node.id
            },
            type="NOTIFY"
        )

        try:
            await self.messaging.send_message_direct(self.successor_id, notify.to_dict())
        except Exception as e:
            self.logger.debug(f"Failed to notify successor: {e}")

    async def handle_notify(self, msg: Message):
        """Handle NOTIFY from a node claiming to be our predecessor."""
        claimed_pred = msg.payload["claiming_predecessor"]

        # If we have no predecessor, accept the claim
        if self.predecessor_id is None:
            self.predecessor_id = claimed_pred
            self.logger.info(
                f"Accepted {claimed_pred} as predecessor (was None)")
            return

        # If the claiming node should be between our current predecessor and us, update
        # For simplicity, we accept any valid node from our peer list
        if claimed_pred in self.messaging.peers:
            self.predecessor_id = claimed_pred
            self.logger.info(
                f"Updated predecessor to {claimed_pred} via NOTIFY")

    # ------------------------------------------------------------------
    # Failure detection and recovery
    # ------------------------------------------------------------------
    async def _liveness_check_loop(self):
        """Periodically check if successor is alive."""
        await asyncio.sleep(10)  # Wait for initial ring formation

        while True:
            try:
                await self._check_successor_alive()
                await asyncio.sleep(self.ping_timeout)
            except Exception as e:
                self.logger.debug(f"Liveness check error: {e}")
                await asyncio.sleep(1)

    async def _check_successor_alive(self):
        """Ping successor to check if it's alive."""
        if not self.successor_id or self.successor_id == self.node.id:
            return

        # Check if successor is still in peer list (discovery handles this)
        if self.successor_id not in self.messaging.peers:
            self.logger.warning(
                f"Successor {self.successor_id} not in peer list")
            self.successor_failures += 1

            if self.successor_failures >= self.max_ping_failures:
                await self._fix_successor()
                self.successor_failures = 0
        else:
            self.successor_failures = 0

    async def _fix_successor(self):
        """Find a new successor when current one fails."""
        self.logger.info(f"Attempting to fix successor. Available peers: {list(self.messaging.peers.keys())}")
        self.logger.info(f"Current successor_id: {self.successor_id}")
        
        if not self.messaging.peers:
            # We're alone
            self.successor_id = None
            self.predecessor_id = None
            self.ring_established = False
            self.logger.info("No peers available, leaving ring")
            return

        # Find any available peer as new successor
        # In a production system, we'd ask the failed successor's successor
        # For now, pick any peer that's not us
        for peer_id in self.messaging.peers:
            self.logger.debug(f"Checking peer {peer_id[:8]}... (self_uuid={self.node.uuid[:8]}, old_successor={self.successor_id[:8] if self.successor_id else 'None'})")
            # Compare UUIDs, not short IDs!
            if peer_id != self.node.uuid and peer_id != self.successor_id:
                old_successor = self.successor_id
                self.successor_id = peer_id
                self.logger.info(
                    f"Fixed successor: {old_successor[:8] if old_successor else 'None'}... -> {peer_id[:8]}...")

                # Notify new successor
                await self._notify_successor()
                return

        # If we only have one peer (the failed one), or no valid replacement found
        # Clear the ring state since we're effectively alone
        self.logger.warning(f"Could not find replacement successor among {len(self.messaging.peers)} peers")
        self.successor_id = None
        self.predecessor_id = None
        self.ring_established = False
        self.logger.info("No valid successor found - leaving ring")

    async def _fix_predecessor(self):
        """Find a new predecessor when current one fails."""
        self.logger.info(f"Attempting to fix predecessor. Available peers: {list(self.messaging.peers.keys())}")
        self.logger.info(f"Current predecessor_id: {self.predecessor_id}")
        
        if not self.messaging.peers:
            # We're alone
            self.successor_id = None
            self.predecessor_id = None
            self.ring_established = False
            self.logger.info("No peers available, leaving ring")
            return

        # Find any available peer as new predecessor
        # For now, pick any peer that's not us and not our current successor
        for peer_id in self.messaging.peers:
            self.logger.debug(f"Checking peer {peer_id[:8]}... for predecessor (self_uuid={self.node.uuid[:8]}, successor={self.successor_id[:8] if self.successor_id else 'None'})")
            # Compare UUIDs, not short IDs!
            # Don't pick ourselves or our successor as predecessor
            if peer_id != self.node.uuid and peer_id != self.predecessor_id and peer_id != self.successor_id:
                old_predecessor = self.predecessor_id
                self.predecessor_id = peer_id
                self.logger.info(
                    f"Fixed predecessor: {old_predecessor[:8] if old_predecessor else 'None'}... -> {peer_id[:8]}...")
                return

        # If we can't find anyone else, our successor becomes our predecessor (2-node ring)
        if self.successor_id and self.successor_id != self.predecessor_id:
            old_predecessor = self.predecessor_id
            self.predecessor_id = self.successor_id
            self.logger.info(
                f"Using successor as predecessor (2-node ring): {old_predecessor[:8] if old_predecessor else 'None'}... -> {self.successor_id[:8]}...")
            return
        
        # If we only have one peer (the failed one), or no valid replacement found
        # Clear the ring state since we're effectively alone
        self.logger.warning(f"Could not find replacement predecessor among {len(self.messaging.peers)} peers")
        self.successor_id = None
        self.predecessor_id = None
        self.ring_established = False
        self.logger.info("No valid predecessor found - leaving ring")

    # ------------------------------------------------------------------
    # Stability tracking for elections
    # ------------------------------------------------------------------
    def _mark_topology_change(self, reason: str = "unknown"):
        """Mark that the ring topology has changed."""
        self._last_topology_change = time.time()
        self.logger.info(f"Ring topology changed: {reason}")

    def is_ring_stable(self) -> bool:
        """
        Check if the ring has been stable (no topology changes) for the minimum duration.
        Used by LeaderElectionService to gate elections.
        """
        if not self.ring_established or not self.successor_id or not self.predecessor_id:
            return False
        
        # Verify neighbors are actually alive in peers table
        if self.successor_id not in self.messaging.peers:
            return False
        if self.predecessor_id not in self.messaging.peers:
            return False
            
        elapsed = time.time() - self._last_topology_change
        is_stable = elapsed >= self._min_stable_duration
        
        if not is_stable:
            self.logger.debug(f"Ring not stable yet: {elapsed:.1f}s < {self._min_stable_duration}s")
        
        return is_stable

    async def wait_for_stable_ring(self, timeout: float = 10.0) -> bool:
        """
        Wait for the ring to become stable.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            bool: True if ring became stable, False if timeout reached
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.is_ring_stable():
                self.logger.info(f"Ring stabilized after {time.time() - start_time:.1f}s")
                return True
            await asyncio.sleep(0.5)
        
        self.logger.warning(f"Ring did not stabilize within {timeout}s timeout")
        return False

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------
    def get_ring_neighbors(self) -> Tuple[Optional[str], Optional[str]]:
        """Return (predecessor, successor) tuple."""
        return (self.predecessor_id, self.successor_id)

    def is_in_ring(self) -> bool:
        """Check if this node is part of a ring."""
        return self.ring_established and self.successor_id is not None

    def get_ring_info(self) -> dict:
        """Get current ring state for debugging."""
        return {
            "node_id": self.node.id,
            "predecessor": self.predecessor_id,
            "successor": self.successor_id,
            "ring_established": self.ring_established,
            "in_ring": self.is_in_ring()
        }
