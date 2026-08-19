from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import OutboxRecord
from payment_service.outbox.models import ClaimedOutboxEvent


class SqlAlchemyOutboxRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def claim_batch(
        self,
        batch_size: int,
        claim_seconds: int,
    ) -> list[ClaimedOutboxEvent]:
        now = datetime.now(UTC)
        statement = (
            select(OutboxRecord)
            .where(
                OutboxRecord.published_at.is_(None),
                OutboxRecord.available_at <= now,
                or_(
                    OutboxRecord.claim_token.is_(None),
                    OutboxRecord.claimed_until < now,
                ),
            )
            .order_by(OutboxRecord.available_at, OutboxRecord.id)
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        )

        async with self.session_factory() as session, session.begin():
            records = list(await session.scalars(statement))
            events = []
            for record in records:
                claim_token = uuid4()
                record.claim_token = claim_token
                record.claimed_until = now + timedelta(seconds=claim_seconds)
                record.publish_attempts += 1
                events.append(
                    ClaimedOutboxEvent(
                        event_id=record.id,
                        payment_id=record.payment_id,
                        payload=record.payload,
                        claim_token=claim_token,
                    )
                )

        return events

    async def mark_published(self, event_id: UUID, claim_token: UUID) -> bool:
        statement = (
            update(OutboxRecord)
            .where(
                OutboxRecord.id == event_id,
                OutboxRecord.claim_token == claim_token,
                OutboxRecord.published_at.is_(None),
            )
            .values(
                published_at=datetime.now(UTC),
                claim_token=None,
                claimed_until=None,
                last_error=None,
            )
            .returning(OutboxRecord.id)
        )

        async with self.session_factory() as session, session.begin():
            updated_event_id = await session.scalar(statement)

        return updated_event_id is not None

    async def mark_failed(
        self,
        event_id: UUID,
        claim_token: UUID,
        error: str,
        retry_delay: float,
    ) -> bool:
        now = datetime.now(UTC)
        statement = (
            update(OutboxRecord)
            .where(
                OutboxRecord.id == event_id,
                OutboxRecord.claim_token == claim_token,
                OutboxRecord.published_at.is_(None),
            )
            .values(
                available_at=now + timedelta(seconds=retry_delay),
                claim_token=None,
                claimed_until=None,
                last_error=error[:1000],
            )
            .returning(OutboxRecord.id)
        )

        async with self.session_factory() as session, session.begin():
            updated_event_id = await session.scalar(statement)

        return updated_event_id is not None
