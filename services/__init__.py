"""
Services module for distributed emergency alert system.

Contains modular service classes for:
- Discovery: Auto-discovery of nodes via broadcast
- Election: Leader election
- Multicast: Ordered alert distribution
- FaultTolerance: Crash and Byzantine handling
"""

from .discovery import DiscoveryService

__all__ = ["DiscoveryService"]
