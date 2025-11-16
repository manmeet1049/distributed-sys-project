class Message:
    """
    Immutable container that carries everything needed for causal ordering.
    """
    __slots__ = ("msg_id", "sender_id", "seq", "payload", "type")

    def __init__(self, *, msg_id: str, sender_id: str, seq: int,
                 payload: dict, type: str = "APP"):
        self.msg_id    = msg_id      # unique per message (UUID)
        self.sender_id = sender_id   # node that created the message
        self.seq       = seq         # per-sender monotonic counter
        self.payload   = payload
        self.type      = type        # "APP", "NACK", ...

    def to_dict(self) -> dict:
        return {
            "msg_id":    self.msg_id,
            "sender_id": self.sender_id,
            "seq":       self.seq,
            "type":      self.type,
            "payload":   self.payload,
        }
    def from_dict(cls, data: dict) -> "Message":
        return cls(
            msg_id    = data["msg_id"],
            sender_id = data["sender_id"],
            seq       = data["seq"],
            payload   = data["payload"],
            type      = data.get("type", "APP"),
        )
    from_dict = classmethod(from_dict)