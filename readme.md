# Distributed Emergency Alert System — One-Page Project Proposal

## Project Idea

Build a **fully decentralized emergency alert system** where regional nodes (servers) broadcast critical alerts (e.g., earthquakes, evacuations) to all others in a P2P network. Nodes dynamically join/leave, elect a leader to sequence alerts, and ensure **total order and reliable delivery** — even if some nodes crash or act maliciously. The system runs **without central servers**, using only standard libraries.

---

## How We Cover the 5 Core Properties

### 1. **Architectural Description (Binary)**

- **P2P Overlay**: Every node is both client and server.
- **No central point of failure** — system works with 1+ active node.
- **Modules**: AlertProcessor, Discovery, Election, Multicast, FaultTolerance.

---

### 2. **Dynamic Discovery of Hosts (Binary)**

- **UDP Multicast Heartbeats** (`239.0.0.1:9999`) every 5s.
- New node sends `"join"` → receives `"hello"` responses.
- Timeout after 15s → remove dead nodes.
- **Binary**: All live nodes known; no registry.

---

### 3. **Fault Tolerance (Fail-Stop, Crash, Byzantine)**

- **Crash/Fail-Stop**: 3 missed heartbeats → mark failed. Alerts replicated on 3+ nodes.
- **Byzantine**: Use **digital signatures (RSA/SHA-256)** + **simplified BFT voting** (quorum 3f+1).
- Only majority-approved alerts accepted.
- **Binary**: Correct alerts delivered despite 1 malicious node.

---

### 4. **Election (Correctness/Robustness)**

- **Paxos-inspired leader election**.
- Nodes propose with `(ballot = node_id + timestamp)`.
- Majority accepts highest ballot → leader elected.
- Re-elect on leader failure (10s timeout).
- **Binary**: Exactly one leader per partition.

---

### 5. **Ordered Reliable Multicast**

- Leader assigns **global sequence number** per alert.
- Nodes buffer out-of-order; deliver only in sequence.
- **ACK + retransmit** missing messages.
- New leader resumes from logs.
- **Binary**: All nodes see alerts in same order.

---

## Implementation Plan

- **Language**: Python (`socket`, `asyncio`, `cryptography`)
- **Start Small**: 5–10 nodes → scale to 20
- **Test**: Crash nodes, inject fake alerts, delay messages
- **Deliver**: Working prototype + 1-page report proving **all 5 properties met**

---

**Goal**: A reliable, self-healing alert network — ready for real-world disasters.
