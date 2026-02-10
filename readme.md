# Distributed Emergency Alert System

## Overview

A **decentralized peer-to-peer emergency alert system** where nodes automatically discover each other via UDP multicast, self-organize into a ring topology, and elect a leader using the Hirschberg-Sinclair algorithm. The system tolerates crash faults through heartbeat-based failure detection with automatic ring repair, and ensures causal message ordering using vector clocks with NACK-based retransmission.

## Features

- **Dynamic Discovery**: UDP multicast (`224.0.0.251:50000`) for automatic peer discovery
- **Ring Topology**: Self-organizing ring with successor/predecessor pointers
- **Leader Election**: Hirschberg-Sinclair algorithm (highest UUID wins)
- **Fault Tolerance**: Heartbeat monitoring (3s interval, 10s timeout) with automatic recovery
- **Causal Ordering**: Vector clocks + NACK-based retransmission

## Project Structure

```
├── main.py                 # Node entry point
├── Data/
│   └── message.py          # Message class with vector clocks
├── services/
│   ├── discovery.py        # UDP multicast peer discovery
│   ├── messaging_service.py # UDP messaging + causal buffer
│   ├── ring_service.py     # Ring topology management
│   ├── leader_election.py  # HS leader election
│   └── vector_causal_buffer.py # Causal ordering logic
└── logs/                   # Per-node log files
```

## Usage

```bash
python main.py <port>
```

Nodes on the same network will automatically discover each other, form a ring, and elect a leader.
