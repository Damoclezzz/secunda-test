from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class ClaimedOutboxEvent:
    event_id: UUID
    payment_id: UUID
    payload: dict[str, Any]
    claim_token: UUID
