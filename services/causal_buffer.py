import asyncio
from collections import defaultdict
from typing import Dict, Tuple
from Data.message import Message

class CausalBuffer:
    def __init__(self):
        self.next_seq: Dict[str, int] = defaultdict(int)   # sender → expected seq
        self.holdback: Dict[Tuple[str, int], Message] = {} # (sender,seq) → msg

    async def deliver(self, msg: Message, user_cb):
        print(f"next sequence:",self.next_seq)
        print(f"holdback buffer:",self.holdback)
        key = (msg.sender_id, msg.seq)
        if msg.seq == self.next_seq[msg.sender_id]:
            await user_cb(msg)
            self.next_seq[msg.sender_id] += 1
            self._flush(msg.sender_id, user_cb)
        elif msg.seq > self.next_seq[msg.sender_id]:
            self.holdback[key] = msg
        # else: duplicate / old → ignore

    def _flush(self, sender: str, user_cb):
        while (sender, self.next_seq[sender]) in self.holdback:
            m = self.holdback.pop((sender, self.next_seq[sender]))
            asyncio.create_task(user_cb(m))
            self.next_seq[sender] += 1