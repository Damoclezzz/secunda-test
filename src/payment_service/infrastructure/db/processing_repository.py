from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import PaymentRecord
from payment_service.payments.models import ClaimedPayment, Currency, PaymentStatus


class SqlAlchemyPaymentProcessingRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def claim(self, payment_id: UUID, claim_seconds: int) -> ClaimedPayment | None:
        now = datetime.now(UTC)
        processing_token = uuid4()
        statement = (
            update(PaymentRecord)
            .where(
                PaymentRecord.id == payment_id,
                PaymentRecord.webhook_delivered_at.is_(None),
                or_(
                    PaymentRecord.processing_token.is_(None),
                    PaymentRecord.processing_claimed_until < now,
                ),
            )
            .values(
                processing_token=processing_token,
                processing_claimed_until=now + timedelta(seconds=claim_seconds),
            )
            .returning(PaymentRecord)
        )

        async with self.session_factory() as session, session.begin():
            record = await session.scalar(statement)

        if record is None:
            return None

        return ClaimedPayment(
            id=record.id,
            amount=record.amount,
            currency=Currency(record.currency),
            status=PaymentStatus(record.status),
            webhook_url=record.webhook_url,
            processed_at=record.processed_at,
            processing_token=processing_token,
        )

    async def exists(self, payment_id: UUID) -> bool:
        async with self.session_factory() as session:
            payment_exists = await session.scalar(
                select(PaymentRecord.id).where(PaymentRecord.id == payment_id)
            )

        return payment_exists is not None

    async def is_webhook_delivered(self, payment_id: UUID) -> bool:
        async with self.session_factory() as session:
            delivered_at = await session.scalar(
                select(PaymentRecord.webhook_delivered_at).where(PaymentRecord.id == payment_id)
            )

        return delivered_at is not None

    async def store_result(
        self,
        payment_id: UUID,
        processing_token: UUID,
        status: PaymentStatus,
    ) -> datetime | None:
        processed_at = datetime.now(UTC)
        statement = (
            update(PaymentRecord)
            .where(
                PaymentRecord.id == payment_id,
                PaymentRecord.processing_token == processing_token,
                PaymentRecord.status == PaymentStatus.PENDING.value,
            )
            .values(status=status.value, processed_at=processed_at)
            .returning(PaymentRecord.processed_at)
        )

        async with self.session_factory() as session, session.begin():
            stored_at = await session.scalar(statement)

        return stored_at

    async def renew_claim(
        self,
        payment_id: UUID,
        processing_token: UUID,
        claim_seconds: int,
    ) -> bool:
        statement = (
            update(PaymentRecord)
            .where(
                PaymentRecord.id == payment_id,
                PaymentRecord.processing_token == processing_token,
                PaymentRecord.webhook_delivered_at.is_(None),
            )
            .values(processing_claimed_until=datetime.now(UTC) + timedelta(seconds=claim_seconds))
            .returning(PaymentRecord.id)
        )

        async with self.session_factory() as session, session.begin():
            updated_payment_id = await session.scalar(statement)

        return updated_payment_id is not None

    async def mark_webhook_delivered(
        self,
        payment_id: UUID,
        processing_token: UUID,
        attempt: int,
    ) -> bool:
        statement = (
            update(PaymentRecord)
            .where(
                PaymentRecord.id == payment_id,
                PaymentRecord.processing_token == processing_token,
                PaymentRecord.status.in_(
                    (PaymentStatus.SUCCEEDED.value, PaymentStatus.FAILED.value)
                ),
                PaymentRecord.webhook_delivered_at.is_(None),
            )
            .values(
                webhook_delivered_at=datetime.now(UTC),
                webhook_attempts=attempt,
                webhook_last_error=None,
                processing_token=None,
                processing_claimed_until=None,
            )
            .returning(PaymentRecord.id)
        )

        async with self.session_factory() as session, session.begin():
            updated_payment_id = await session.scalar(statement)

        return updated_payment_id is not None

    async def record_webhook_failure(
        self,
        payment_id: UUID,
        processing_token: UUID,
        attempt: int,
        error: str,
    ) -> bool:
        statement = (
            update(PaymentRecord)
            .where(
                PaymentRecord.id == payment_id,
                PaymentRecord.processing_token == processing_token,
                PaymentRecord.webhook_delivered_at.is_(None),
            )
            .values(
                webhook_attempts=attempt,
                webhook_last_error=error[:500],
            )
            .returning(PaymentRecord.id)
        )

        async with self.session_factory() as session, session.begin():
            updated_payment_id = await session.scalar(statement)

        return updated_payment_id is not None

    async def release_claim(self, payment_id: UUID, processing_token: UUID) -> bool:
        statement = (
            update(PaymentRecord)
            .where(
                PaymentRecord.id == payment_id,
                PaymentRecord.processing_token == processing_token,
            )
            .values(
                processing_token=None,
                processing_claimed_until=None,
            )
            .returning(PaymentRecord.id)
        )

        async with self.session_factory() as session, session.begin():
            updated_payment_id = await session.scalar(statement)

        return updated_payment_id is not None
