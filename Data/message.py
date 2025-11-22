from typing import Optional, Dict, Any
import uuid

class Message:
    """
    Immutable container that carries everything needed for causal ordering.
    """
    __slots__ = ("msg_id", "sender_id", "vector_clock", "payload", "type")

    def __init__(self, *, msg_id: str, sender_id: str, 
                 vector_clock: Optional[Dict[str,int]] = None,
                 payload: dict, type: str = "APP"):
        self.msg_id    = msg_id      # unique per message (UUID)
        self.sender_id = sender_id   # node that created the message
        self.vector_clock      = vector_clock         # per-sender monotonic counter
        self.payload   = payload
        self.type      = type        # "APP", "NACK", ...

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_id":       self.msg_id,
            "sender_id":    self.sender_id,
            "vector_clock": self.vector_clock,
            "type":         self.type,
            "payload":      self.payload,
        }
    @classmethod
    def from_dict(cls, data:Dict[str,Any]) -> "Message":
        return cls(
            msg_id    = data["msg_id"],
            sender_id = data["sender_id"],
            seq       = data["vector_clock"],
            payload   = data["payload"],
            type      = data.get("type", "APP"),
        )
    from_dict = classmethod(from_dict)