# Low-Level Architecture# Low-Level Architecture



**Distributed Emergency Alert System**  **Distributed Emergency Alert System**  

Single Python Node | Service-Based Modular Design | AsyncIO + Dual Protocol (UDP + TCP) Single Python Node | Service-Based Layered Design | AsyncIO + Sockets_



------



## Overview## Overview



Each node is a **single Python process** (`main.py`) implementing a **fully decentralized P2P emergency alert system**. The architecture uses **modular services** (composition pattern) with **strict encapsulation**: each service manages its own logic and state, communicating with the Node only through defined interfaces. Services do **NOT inherit** from Node; instead, they receive a reference to the Node and operate independently.Each node is a **single Python process** (`node.py`) implementing a **fully decentralized P2P emergency alert system**. The architecture uses **modular services** (classes) with **strict abstraction**: internal logic is hidden, and services communicate only via defined interfaces. Only **logging** is shared across services for observability.



> **Why "Services"?**> **Why "Services"?**

>>

> - **Encapsulation**: Each service owns its domain (discovery, election, multicast, etc.)> - Encapsulates responsibility (e.g., discovery, election).

> - **Independence**: Services can be tested, debugged, and replaced independently> - Enables independent testing and future replacement (e.g., swap UDP discovery for gossip).

> - **Composition**: Services are helpers that the Node orchestrates, not extensions> - Aligns with microservice-like thinking in a single process.

> - **Scalability**: New services can be added without modifying existing ones

> - **Concurrency**: All services run concurrently via `asyncio.gather()`---



---## Node Structure (`node.py`)



## Current Architecture```python

class Node:

### Project Structure    def __init__(self, node_id, ip, port):

        self.id = node_id

```        self.addr = (ip, port)

distributed-sys-project/        self.logger = logging.getLogger(f"Node-{node_id}")

├── main.py                    # Node class & entry point

├── services/        self.discovery_service = DiscoveryService(self)

│   ├── __init__.py           # Service exports        self.election_service  = ElectionService(self)

│   ├── discovery.py          # DiscoveryService (UDP broadcast)        self.multicast_service = MulticastService(self)

│   ├── election.py           # [TODO] ElectionService        self.fault_service     = FaultToleranceService(self)

│   ├── multicast.py          # [TODO] MulticastService        self.storage_service   = StorageService(self)

│   ├── fault_tolerance.py    # [TODO] FaultToleranceService

│   └── storage.py            # [TODO] StorageService    async def start(self):

├── readme.md        await asyncio.gather(

├── low-level-arch.md            self.discovery_service.start(),

└── pyproject.toml            self.election_service.start_monitor(),

```            self.multicast_service.start_listener(),

            self._handle_input(),

### Node Structure (`main.py`)            self._periodic_cleanup()

        )

```python```

class Node:
    def __init__(self, node_id: str, host: str, port: int):
        self.id = node_id
        self.host = host
        self.port = port
        self.addr = (host, port)
        self.logger = logging.getLogger(f"Node-{node_id}")
        
        # Network state (shared with services)
        self.peers = {}  # {node_id: (host, port)}
        self.server_socket = None  # TCP server socket
        self.running = False
        
        # Initialize services (composition pattern)
        self.discovery_service = DiscoveryService(self)
        # Future services:
        # self.election_service = ElectionService(self)
        # self.multicast_service = MulticastService(self)
        # self.fault_service = FaultToleranceService(self)
        # self.storage_service = StorageService(self)

    async def start(self):
        """Start node with all services running concurrently."""
        # Setup TCP server socket (reliable peer-to-peer messaging)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.addr)
        self.server_socket.listen(5)
        self.server_socket.setblocking(False)
        
        self.running = True
        
        # Run all tasks concurrently
        await asyncio.gather(
            self.discovery_service.start(),      # UDP discovery
            self._accept_connections(),          # TCP server
            self._handle_input(),                # CLI input
            # Future services:
            # self.election_service.start(),
            # self.multicast_service.start(),
            # self.fault_service.start(),
        )
```

---

## Dual Protocol Design

Each node operates with **two independent communication protocols** running concurrently:

### 1. **UDP Broadcast (Discovery Layer)**
- **Port**: 9999 (shared by all nodes)
- **Address**: 255.255.255.255 (broadcast address)
- **Purpose**: Auto-discovery and peer announcement
- **Managed by**: `DiscoveryService`

**Flow:**
```
Node1 broadcasts:   HELLO (node_id, host, port)
                           ↓
                    [Network Broadcast]
                           ↓
Node2 receives:     HELLO → Updates peers list
Node3 receives:     HELLO → Updates peers list
```

### 2. **TCP Streaming (Messaging Layer)**
- **Port**: Dynamic (each node binds to its own port, e.g., 5000, 5001, 5002)
- **Purpose**: Reliable peer-to-peer message delivery
- **Managed by**: Node class + future `MulticastService`

**Flow:**
```
Node1 (5000) ──TCP──► Node2 (5001)  [Direct connection]
             ◄─TCP──┐
                    └─ Reliable message delivery
```

### **Concurrent Execution Model:**

```
┌──────────────────────────────────────────────────────┐
│              asyncio.gather() - Event Loop           │
└─────┬────────────────┬─────────────────┬────────────┘
      │                │                 │
      ▼                ▼                 ▼
  DiscoveryService  TCP Accept          Handle CLI
  (UDP Port 9999)   (TCP Port 5000)     (Stdin)
  
  Tasks:            Tasks:              Tasks:
  - Broadcast HELLO - Accept peers      - Read user input
  - Listen for HELLO- Handle incoming   - Parse commands
  - Cleanup timeout - Send responses    - Update UI
```

---

## Service Interface Pattern

Each service follows this interface:

```python
class ServiceName:
    def __init__(self, node):
        """Store reference to parent node for state access."""
        self.node = node
        self.logger = logging.getLogger(...)
        self.running = False
    
    async def start(self):
        """Main entry point - run all service tasks concurrently."""
        try:
            self.running = True
            await asyncio.gather(
                self._task_1(),
                self._task_2(),
                self._task_n(),
            )
        except Exception as e:
            self.logger.error(f"Error: {e}")
            await self.shutdown()
    
    async def shutdown(self):
        """Graceful shutdown - cleanup resources."""
        self.running = False
        # Close sockets, save state, etc.
```

### **Service Responsibilities:**

| Service | Status | Purpose | Protocol |
|---------|--------|---------|----------|
| **DiscoveryService** | ✅ Implemented | Auto-discover peers | UDP Broadcast |
| **ElectionService** | ⏳ TODO | Leader election (Paxos) | TCP |
| **MulticastService** | ⏳ TODO | Ordered alert delivery | TCP |
| **FaultToleranceService** | ⏳ TODO | Byzantine & crash tolerance | TCP + crypto |

---

## Data Flow Example: Node Startup

```
1. User: python main.py node1 5000
   └─ Creates Node("node1", "127.0.0.1", 5000)

2. Node.__init__()
   ├─ Create TCP server socket (Port 5000)
   └─ Instantiate DiscoveryService(self)

3. Node.start()
   └─ asyncio.gather() runs concurrently:
      ├─ DiscoveryService.start()
      │  ├─ Setup UDP sockets
      │  ├─ Broadcast HELLO immediately
      │  └─ Loop: broadcast every 5s, listen for HELLOs
      │
      ├─ Node._accept_connections()
      │  └─ Loop: accept TCP connections, handle peers
      │
      └─ Node._handle_input()
         └─ Loop: read CLI commands (connect, peers, exit)

4. When another node joins:
   ├─ New node broadcasts HELLO
   └─ Existing nodes' listeners catch it
      └─ Update node.peers automatically

5. User types: connect 127.0.0.1 5001 node2
   └─ Node establishes TCP connection to peer
      └─ Can now send/receive messages via TCP
```

---

## Key Design Principles

✅ **Composition over Inheritance** - Services use/reference Node, don't extend it  
✅ **Async-First** - All I/O is non-blocking via `asyncio`  
✅ **Loose Coupling** - Services communicate via Node's shared state  
✅ **High Cohesion** - Each service has one clear responsibility  
✅ **Graceful Shutdown** - All services can be stopped cleanly  

---

## Future Services

When adding new services, follow this pattern:

```python
# services/election.py
class ElectionService:
    def __init__(self, node):
        self.node = node
        # ... initialization
    
    async def start(self):
        # Your election logic here
        pass
    
    async def shutdown(self):
        # Cleanup
        pass

# Then in Node.start(), add to gather():
await asyncio.gather(
    self.discovery_service.start(),
    self.election_service.start(),      # ← Add new service
    ...
)
```

---
