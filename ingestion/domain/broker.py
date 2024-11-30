from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class Event:
    event_type: str
    payload: dict[str, object]
    timestamp: Optional[datetime]
