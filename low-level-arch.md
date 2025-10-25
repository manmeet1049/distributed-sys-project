# Low-Level Architecture

**Distributed Emergency Alert System**  
_Single Python Node | Service-Based Layered Design | AsyncIO + Sockets_

---

## Overview

Each node is a **single Python process** (`node.py`) implementing a **fully decentralized P2P emergency alert system**. The architecture uses **modular services** (classes) with **strict abstraction**: internal logic is hidden, and services communicate only via defined interfaces. Only **logging** is shared across services for observability.

> **Why "Services"?**
>
> - Encapsulates responsibility (e.g., discovery, election).
> - Enables independent testing and future replacement (e.g., swap UDP discovery for gossip).
> - Aligns with microservice-like thinking in a single process.

---

## Node Structure (`node.py`)

```python
class Node:
    def __init__(self, node_id, ip, port):
        self.id = node_id
        self.addr = (ip, port)
        self.logger = logging.getLogger(f"Node-{node_id}")

        self.discovery_service = DiscoveryService(self)
        self.election_service  = ElectionService(self)
        self.multicast_service = MulticastService(self)
        self.fault_service     = FaultToleranceService(self)
        self.storage_service   = StorageService(self)

    async def start(self):
        await asyncio.gather(
            self.discovery_service.start(),
            self.election_service.start_monitor(),
            self.multicast_service.start_listener(),
            self._handle_input(),
            self._periodic_cleanup()
        )
```
