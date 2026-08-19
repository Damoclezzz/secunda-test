from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.infrastructure.db.models import OutboxRecord, PaymentRecord
from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.models import (
    DuplicateIdempotencyKeyError,
    Payment,
)

IDEMPOTENCY_CONSTRAINT = "uq_payments_idempotency_key"


class SqlAlchemyPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert(self, payment: Payment, event: PaymentCreatedEvent) -> None:
        insert_payment = (
            insert(PaymentRecord)
            .values(
                id=payment.id,
                amount=payment.amount,
                currency=payment.currency.value,
                description=payment.description,
                payment_metadata=payment.metadata,
                status=payment.status.value,
                idempotency_key=payment.idempotency_key,
                webhook_url=payment.webhook_url,
                created_at=payment.created_at,
            )
            .on_conflict_do_nothing(constraint=IDEMPOTENCY_CONSTRAINT)
            .returning(PaymentRecord.id)
        )
        outbox_record = OutboxRecord(
            id=event.event_id,
            payment_id=payment.id,
            payload=event.model_dump(mode="json"),
            available_at=event.occurred_at,
        )

        async with self.session.begin():
            inserted_payment_id = await self.session.scalar(insert_payment)
            if inserted_payment_id is None:
                raise DuplicateIdempotencyKeyError

            self.session.add(outbox_record)

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        result = await self.session.execute(
            select(PaymentRecord).where(PaymentRecord.id == payment_id)
        )
        record = result.scalar_one_or_none()

        return record.to_payment() if record is not None else None

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self.session.execute(
            select(PaymentRecord).where(PaymentRecord.idempotency_key == idempotency_key)
        )
        record = result.scalar_one_or_none()

        return record.to_payment() if record is not None else None
