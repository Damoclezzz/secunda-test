from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import PaymentRecord
from payment_service.infrastructure.db.payment_repository import SqlAlchemyPaymentRepository
from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.models import Currency, Payment, PaymentStatus


async def test_mismatched_outbox_payment_id_rolls_back_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = uuid4()
    created_at = datetime.now(UTC)
    payment = Payment(
        id=payment_id,
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description=None,
        metadata={},
        status=PaymentStatus.PENDING,
        idempotency_key="atomicity-key",
        webhook_url="https://example.com/webhooks/payments",
        created_at=created_at,
    )
    invalid_event = PaymentCreatedEvent(
        event_id=uuid4(),
        occurred_at=created_at,
        payment_id=uuid4(),
    )

    async with session_factory() as session:
        repository = SqlAlchemyPaymentRepository(session)
        with pytest.raises(IntegrityError):
            await repository.insert(payment, invalid_event)

    async with session_factory() as session:
        stored_payment = await session.scalar(
            select(PaymentRecord).where(PaymentRecord.id == payment_id)
        )

    assert stored_payment is None
