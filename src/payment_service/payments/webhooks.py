from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from payment_service.payments.models import Currency, PaymentStatus


class PaymentResultWebhook(BaseModel):
    model_config = ConfigDict(frozen=True)

    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime
