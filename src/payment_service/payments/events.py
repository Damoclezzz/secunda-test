from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class PaymentCreatedEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: UUID
    event_type: Literal["payment.created"] = "payment.created"
    schema_version: Literal[1] = 1
    occurred_at: datetime
    payment_id: UUID
