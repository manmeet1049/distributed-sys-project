# Low-Level Architecture

Distributed Emergency Alert System
Single Python Node | Service-Based Design | AsyncIO + UDP

---

## Overview

Each node is a single Python process (main.py) implementing a fully decentralized P2P emergency alert system.
- Services are composed into the Node (not inherited)
- Each service manages its own logic and state
- All I/O is async via asyncio
- Communication is UDP-based (multicast + unicast)

---

## Project Structure

distributed-sys-project/
├── main.py                      # Node class & CLI entry point
├── Data/
│   └── message.py               # Message class with vector clock
├── services/
│   ├── discovery.py             # DiscoveryService - UDP multicast peer discovery
│   ├── messaging_service.py     # MessagingService - P2P messaging with vector clocks
│   ├── ring_service.py          # RingService - Ring topology management
│   ├── leader_election.py       # LeaderElectionService - HS algorithm
│   ├── causal_buffer.py         # CausalBuffer - Message ordering (sequence-based)
│   └── vector_causal_buffer.py  # VectorCausalBuffer - Causal ordering via vector clocks

---

## Node Structure (main.py)

class Node:
    - id: "{name}:{uuid}" format
    - uuid: Unique identifier (used for election comparison)
    - name: Human-readable name
    - host, port: Network address for unicast
    - leader_id, is_leader: Current leader state

Services initialized:
    - discovery_service: Peer discovery via multicast
    - messaging: Message sending/receiving with vector clocks
    - ring_service: Ring topology (successor/predecessor)
    - leader_election: Hirschberg-Sinclair election

Startup (asyncio.gather):
    - discovery_service.start()
    - messaging.start()
    - ring_service.start()
    - leader_election.start()
    - _handle_input() (CLI)

---

## Communication Protocol

All communication uses UDP:

1. UDP Multicast (224.0.0.251:50000)
   - HELLO messages for peer discovery
   - APP messages for broadcast alerts
   - All nodes join this multicast group

2. UDP Unicast (node's port)
   - Direct peer-to-peer messages
   - Control messages (ring, election)
   - NACK for retransmission requests

Message Framing:
   - 4-byte length prefix (big-endian)
   - JSON payload

---

## Services

### DiscoveryService
Purpose: Peer discovery via UDP multicast
- Broadcasts HELLO every 5 seconds
- Maintains known_peers with last_seen timestamps
- Removes peers after 15 seconds timeout
- Triggers on_peer_discovered / on_peer_lost callbacks

### MessagingService
Purpose: Message routing and vector clocks
- Maintains peers dict from discovery callbacks
- send_to(peer_id, payload): Unicast with vector clock
- multicast_message(payload): Broadcast with vector clock
- send_message_direct(peer_id, msg_dict): Control messages (no vector clock)
- Routes incoming messages to appropriate handlers
- Manages sent_history for NACK-based retransmission

### RingService
Purpose: Logical ring topology for leader election
- Maintains successor_id and predecessor_id
- Join protocol: JOIN_REQUEST -> JOIN_RESPONSE -> UPDATE_PREDECESSOR
- Stabilization loop every 5 seconds (Chord-style)
- Liveness check every 2 seconds
- Failure recovery: _fix_successor() / _fix_predecessor()

### LeaderElectionService
Purpose: Leader election using Hirschberg-Sinclair algorithm
- Elects node with highest UUID
- Bidirectional: sends ELECTION messages both clockwise and counter-clockwise
- Phases: Phase k sends messages up to 2^k hops
- Heartbeat: Leader sends every 3s, timeout 10s triggers re-election
- Handles concurrent elections naturally (only highest UUID wins)

### VectorCausalBuffer
Purpose: Causal ordering of APP messages
- Each node maintains vector clock: {node_id: sequence_number}
- On send: increment own entry, attach to message
- On receive: buffer if causally not ready, deliver when ready
- Delivery condition: all entries in msg clock <= local clock (except sender)

---

## Message Types

Discovery:
- HELLO: Periodic announcement (uuid, name, host, port)

Application:
- APP: User messages with vector clock for causal ordering

Ring Protocol:
- JOIN_REQUEST: New node requesting to join
- JOIN_RESPONSE: Response with successor/predecessor info
- UPDATE_PREDECESSOR: Notify node of new predecessor
- UPDATE_SUCCESSOR: Notify node of new successor
- GET_PREDECESSOR: Query for predecessor (stabilization)
- PREDECESSOR_RESPONSE: Response to GET_PREDECESSOR
- NOTIFY: Claim to be predecessor

Election Protocol:
- ELECTION: HS algorithm probe (initiator_id, hop_count, max_hops, direction, phase)
- ELECTION_REPLY: Response to election probe (success or rejection)
- LEADER_ANNOUNCEMENT: Winner broadcasts to all peers
- LEADER_HEARTBEAT: Periodic heartbeat from leader

Reliability:
- NACK: Request retransmission of missed message

---

## Key Design Decisions

1. UDP over TCP
   - Lower latency for real-time alerts
   - Multicast support for efficient broadcast
   - Reliability via application-layer NACK

2. Ring Topology
   - Required for Hirschberg-Sinclair election algorithm
   - Not used for message propagation (multicast handles that)

3. Vector Clocks
   - Attached to APP messages only
   - Control messages don't need causal ordering

4. Highest UUID Wins
   - Deterministic leader election
   - No coordination needed for tie-breaking

5. Dual Detection
   - Discovery timeout: 15 seconds (slow, catches all failures)
   - Heartbeat timeout: 10 seconds (fast, catches leader failure)

---

## CLI Commands

peers          - List discovered peers
ring           - Show ring topology (successor/predecessor)
leader         - Show current leader info
election       - Manually trigger leader election
msg <text>     - Broadcast message to all peers
exit           - Shutdown node

---

## Startup Flow

1. Node created with name, host, port
2. UUID generated (used for election comparison)
3. Services instantiated (discovery, messaging, ring, election)
4. asyncio.gather() starts all services concurrently
5. Discovery broadcasts HELLO, discovers peers
6. MessagingService learns peers via callbacks
7. RingService joins ring when peers discovered
8. LeaderElectionService elects leader when ring stable

---

## Failure Handling

Peer Failure:
- Discovery detects via HELLO timeout (15s)
- Triggers on_peer_lost callback
- RingService repairs topology
- If failed peer was leader: election triggered

Leader Failure:
- Heartbeat timeout (10s) triggers faster detection
- Ring repaired
- New election started

Node Isolation:
- If no peers left, node declares itself leader
- Ring marked as not established

---
