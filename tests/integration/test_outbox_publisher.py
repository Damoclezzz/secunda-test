import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from aio_pika import DeliveryMode
from faststream.rabbit import RabbitBroker
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import OutboxRecord, PaymentRecord
from payment_service.infrastructure.db.outbox_repository import SqlAlchemyOutboxRepository
from payment_service.infrastructure.messaging.publisher import RabbitEventPublisher
from payment_service.infrastructure.messaging.topology import payments_new_queue
from payment_service.outbox.publisher import OutboxPublisher, OutboxPublisherOptions
from payment_service.payments.models import Currency, PaymentStatus


@dataclass(frozen=True, slots=True)
class StoredOutboxEvent:
    event_id: UUID
    payment_id: UUID
    payload: dict[str, Any]


async def store_outbox_event(
    session_factory: async_sessionmaker[AsyncSession],
    claim_token: UUID | None = None,
    claimed_until: datetime | None = None,
) -> StoredOutboxEvent:
    event_id = uuid4()
    payment_id = uuid4()
    created_at = datetime.now(UTC)
    payload = {
        "event_id": str(event_id),
        "event_type": "payment.created",
        "schema_version": 1,
        "occurred_at": created_at.isoformat(),
        "payment_id": str(payment_id),
    }
    payment = PaymentRecord(
        id=payment_id,
        amount=Decimal("100.00"),
        currency=Currency.RUB.value,
        description=None,
        payment_metadata={},
        status=PaymentStatus.PENDING.value,
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhooks/payments",
        created_at=created_at,
    )
    outbox = OutboxRecord(
        id=event_id,
        payment_id=payment_id,
        payload=payload,
        available_at=created_at,
        claim_token=claim_token,
        claimed_until=claimed_until,
    )

    async with session_factory() as session, session.begin():
        session.add(payment)
        await session.flush()
        session.add(outbox)

    return StoredOutboxEvent(event_id, payment_id, payload)


def make_publisher_options(batch_size: int = 10) -> OutboxPublisherOptions:
    return OutboxPublisherOptions(
        batch_size=batch_size,
        poll_interval=0.01,
        claim_seconds=15,
        retry_delay=1,
    )


async def expire_claim(
    session_factory: async_sessionmaker[AsyncSession],
    event_id: UUID,
) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            update(OutboxRecord)
            .where(OutboxRecord.id == event_id)
            .values(claimed_until=datetime.now(UTC) - timedelta(seconds=1))
        )


async def test_concurrent_outbox_claims_do_not_overlap(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    stored_events = [await store_outbox_event(session_factory) for _ in range(4)]
    first_repository = SqlAlchemyOutboxRepository(session_factory)
    second_repository = SqlAlchemyOutboxRepository(session_factory)

    first_claim, second_claim = await asyncio.gather(
        first_repository.claim_batch(4, 15),
        second_repository.claim_batch(4, 15),
    )

    first_ids = {event.event_id for event in first_claim}
    second_ids = {event.event_id for event in second_claim}
    assert first_ids.isdisjoint(second_ids)
    assert first_ids | second_ids == {event.event_id for event in stored_events}


async def test_concurrent_publishers_send_each_live_event_once(
    session_factory: async_sessionmaker[AsyncSession],
    rabbit_broker: RabbitBroker,
) -> None:
    stored_events = [await store_outbox_event(session_factory) for _ in range(4)]
    event_publisher = RabbitEventPublisher(rabbit_broker, timeout=5)
    first_publisher = OutboxPublisher(
        SqlAlchemyOutboxRepository(session_factory),
        event_publisher.publish,
        make_publisher_options(batch_size=2),
    )
    second_publisher = OutboxPublisher(
        SqlAlchemyOutboxRepository(session_factory),
        event_publisher.publish,
        make_publisher_options(batch_size=2),
    )

    processed_counts = await asyncio.gather(
        first_publisher.publish_batch(),
        second_publisher.publish_batch(),
    )

    queue = await rabbit_broker.declare_queue(payments_new_queue)
    messages = [await queue.get(timeout=5) for _ in stored_events]
    extra_message = await queue.get(timeout=0.1, fail=False)
    message_ids = set()
    for message in messages:
        assert message is not None
        assert message.message_id is not None
        message_ids.add(UUID(message.message_id))
        await message.ack()

    assert sum(processed_counts) == len(stored_events)
    assert set(processed_counts) == {len(stored_events) // 2}
    assert message_ids == {event.event_id for event in stored_events}
    assert extra_message is None


async def test_expired_outbox_claim_is_reclaimed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    old_claim_token = uuid4()
    stored_event = await store_outbox_event(
        session_factory,
        claim_token=old_claim_token,
        claimed_until=datetime.now(UTC) - timedelta(seconds=1),
    )
    repository = SqlAlchemyOutboxRepository(session_factory)

    claimed_event = (await repository.claim_batch(1, 15))[0]

    assert claimed_event.event_id == stored_event.event_id
    assert claimed_event.claim_token != old_claim_token


async def test_stale_claim_cannot_mark_event_published(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    stored_event = await store_outbox_event(session_factory)
    repository = SqlAlchemyOutboxRepository(session_factory)
    [first_claim] = await repository.claim_batch(1, 15)
    await expire_claim(session_factory, stored_event.event_id)
    [second_claim] = await repository.claim_batch(1, 15)

    stale_marked = await repository.mark_published(
        first_claim.event_id,
        first_claim.claim_token,
    )
    current_marked = await repository.mark_published(
        second_claim.event_id,
        second_claim.claim_token,
    )

    assert stale_marked is False
    assert current_marked is True


async def test_failed_publication_releases_claim_for_retry(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    stored_event = await store_outbox_event(session_factory)
    repository = SqlAlchemyOutboxRepository(session_factory)
    publish_event = AsyncMock(side_effect=ConnectionError("broker unavailable"))
    publisher = OutboxPublisher(repository, publish_event, make_publisher_options())

    processed_count = await publisher.publish_batch()

    async with session_factory() as session:
        outbox = await session.get(OutboxRecord, stored_event.event_id)

    assert processed_count == 1
    assert outbox is not None
    assert outbox.published_at is None
    assert outbox.claim_token is None
    assert outbox.claimed_until is None
    assert outbox.publish_attempts == 1
    assert outbox.last_error == "ConnectionError: broker unavailable"


async def test_unroutable_event_is_not_marked_published(
    session_factory: async_sessionmaker[AsyncSession],
    rabbit_broker: RabbitBroker,
) -> None:
    stored_event = await store_outbox_event(session_factory)
    queue = await rabbit_broker.declare_queue(payments_new_queue)
    await queue.delete(if_unused=False, if_empty=False)
    repository = SqlAlchemyOutboxRepository(session_factory)
    event_publisher = RabbitEventPublisher(rabbit_broker, timeout=5)
    publisher = OutboxPublisher(repository, event_publisher.publish, make_publisher_options())

    await publisher.publish_batch()

    async with session_factory() as session:
        outbox = await session.get(OutboxRecord, stored_event.event_id)

    assert outbox is not None
    assert outbox.published_at is None
    assert outbox.claim_token is None
    assert outbox.last_error is not None


async def test_publisher_sends_persistent_event_and_marks_outbox(
    session_factory: async_sessionmaker[AsyncSession],
    rabbit_broker: RabbitBroker,
) -> None:
    stored_event = await store_outbox_event(session_factory)
    repository = SqlAlchemyOutboxRepository(session_factory)
    event_publisher = RabbitEventPublisher(rabbit_broker, timeout=5)
    publisher = OutboxPublisher(repository, event_publisher.publish, make_publisher_options())

    processed_count = await publisher.publish_batch()

    queue = await rabbit_broker.declare_queue(payments_new_queue)
    message = await queue.get(timeout=5)
    assert message is not None
    assert processed_count == 1
    assert json.loads(message.body) == stored_event.payload
    assert message.message_id == str(stored_event.event_id)
    assert message.correlation_id == str(stored_event.payment_id)
    assert message.headers["x-attempt"] == 1
    assert message.content_type == "application/json"
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    await message.ack()

    async with session_factory() as session:
        outbox = await session.get(OutboxRecord, stored_event.event_id)

    assert outbox is not None
    assert outbox.published_at is not None
    assert outbox.claim_token is None
    assert outbox.claimed_until is None
