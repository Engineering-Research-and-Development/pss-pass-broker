from dataclasses import dataclass, field
from typing import Any
from datetime import datetime, timezone


@dataclass(frozen=True)
class Message:
    """
    Represents a standardized, immutable message within the bridge.
    Encapsulating all data and metadata of an event.
    """

    source_id: str
    topic: str
    payload: bytes

    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)
