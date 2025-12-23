#!/usr/bin/env python3
"""
Simple test to verify RingService implementation.
Run multiple instances to test ring formation.

Usage:
  Terminal 1: python3 test_ring.py --port 5000
  Terminal 2: python3 test_ring.py --port 5001
  Terminal 3: python3 test_ring.py --port 5002
  
Commands:
  ring       - Show ring topology
  peers      - Show discovered peers
  message <peer_id> <text> - Send message to peer
  exit       - Shutdown node
"""

import sys
import argparse

# Add the project root to the path
sys.path.insert(0, '/home/manmeet/Desktop/icodesometimes/dist-sys-proj/distributed-sys-project')

from main import Node
import asyncio
import uuid


async def main():
    parser = argparse.ArgumentParser(description='Test Ring Service')
    parser.add_argument('--port', type=int, required=True, help='Port to listen on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    args = parser.parse_args()
    
    # Generate unique node ID
    node_id = f"Node-{args.port}"
    
    print(f"\n{'='*60}")
    print(f"Starting {node_id} on port {args.port}")
    print(f"{'='*60}")
    print("Commands:")
    print("  ring       - Show ring topology (abbreviated UUIDs)")
    print("  uuid       - Show full UUID and node information")
    print("  peers      - Show discovered peers")
    print("  message <peer_uuid> <text> - Send message to peer")
    print("  multicast <text> - Broadcast to all peers")
    print("  exit       - Shutdown node")
    print(f"{'='*60}")
    print("Note: Detailed logs are written to logs/{}.log".format(node_id))
    print("Console shows only warnings/errors to keep it clean")
    print("Each node has a unique UUID for election purposes")
    print(f"{'='*60}\n")
    
    # Create and start node
    node = Node(node_id, args.host, args.port)
    
    try:
        await node.start()
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        await node.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
