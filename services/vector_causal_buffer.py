# services/vector_causal_buffer.py
import asyncio
from typing import Awaitable, Dict, Callable, List, Optional
from Data.message import Message

class VectorCausalBuffer:
    def __init__(self, node,on_causal_gap: Optional[Callable[[str, dict], Awaitable[None]]] = None):
        self.node = node
        self.holdback: List[Message] = []   # simple list is enough
        self.on_causal_gap = on_causal_gap  # callback to send NACK

    def handle_nack(self, sender_id: str, expected_seq: int, count, msg: Message):
        # Placeholder for NACK handling logic
        print(f"NACK sent to {sender_id} for expected seq {expected_seq}")
        # Create missing info
        missing_info = {
                "sender": sender_id,
                "expected_next": expected_seq,
                "received": count
            }
        if self.on_causal_gap:
            self.on_causal_gap(missing_info)

    def _can_deliver(self, msg: Message) -> bool:
        sender = msg.sender_id
        vc = msg.vector_clock
        for node_id, count in vc.items():
            local = self.node.vector_clock.get(node_id, 0)
            if node_id == msg.sender_id:
                if count != local + 1:           # must be exactly next from sender
                    self.handle_nack(msg.sender_id, local + 1, count, msg)
                    return False
            else:
                if count > local:                # must have seen all prior from others
                    return False
        return True

    def _merge(self, msg: Message):
        for node_id, count in msg.vector_clock.items():
            self.node.vector_clock[node_id] = max(
                self.node.vector_clock.get(node_id, 0), count
            )

    async def deliver(self, msg: Message, user_cb: Callable):
        if not self._can_deliver(msg):
            self.holdback.append(msg)
            return

        await user_cb(msg)
        self._merge(msg)

        # Drain the buffer as much as possible
        i = 0
        while i < len(self.holdback):
            held = self.holdback[i]
            if self._can_deliver(held):
                await user_cb(held)
                self._merge(held)
                self.holdback.pop(i)   # remove delivered
                # do NOT increment i — next item shifts into place
            else:
                i += 1