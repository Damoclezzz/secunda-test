from typing import Protocol
from uuid import UUID

from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.models import Payment


class PaymentRepository(Protocol):
    async def insert(self, payment: Payment, event: PaymentCreatedEvent) -> None: ...

    async def get_by_id(self, payment_id: UUID) -> Payment | None: ...

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None: ...
