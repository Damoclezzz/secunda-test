import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import PaymentRecord
from payment_service.infrastructure.db.processing_repository import (
    SqlAlchemyPaymentProcessingRepository,
)
from payment_service.infrastructure.messaging.consumer import MAX_ATTEMPTS, PaymentMessageHandler
from payment_service.infrastructure.messaging.payment_retry_publisher import (
    RabbitPaymentRetryPublisher,
)
from payment_service.infrastructure.messaging.topology import payments_dlq, payments_new_queue
from payment_service.infrastructure.webhook import WebhookClient
from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.models import Currency, PaymentStatus
from payment_service.payments.processor import PaymentProcessor
from payment_service.payments.simulator import PaymentProcessingSimulator

FIRST_RETRY_LOWER_BOUND = 0.9
SECOND_RETRY_LOWER_BOUND = 1.9


async def test_rabbit_message_is_acknowledged_after_webhook_commit(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    rabbit_broker: RabbitBroker,
) -> None:
    payment_id = await store_payment(session_factory)
    webhook = RecordingWebhook()
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        processor = make_processor(session_factory, client)
        handler = PaymentMessageHandler(
            processor,
            RabbitPaymentRetryPublisher(rabbit_broker, timeout=5),
        )
        consumer_broker = RabbitBroker(
            rabbitmq_url,
            default_channel=Channel(prefetch_count=1),
        )
        consumer_broker.subscriber(
            payments_new_queue,
            ack_policy=AckPolicy.MANUAL,
        )(handler.handle)
        await consumer_broker.start()

        try:
            event_id = uuid4()
            await rabbit_broker.publish(
                make_event_payload(event_id, payment_id),
                queue=payments_new_queue,
                mandatory=True,
                persist=True,
                message_id=str(event_id),
            )
            payment = await wait_for_webhook_delivery(session_factory, payment_id)
        finally:
            await consumer_broker.stop()

    queue = await rabbit_broker.declare_queue(payments_new_queue)
    message = await queue.get(timeout=0.1, fail=False)
    assert payment.webhook_delivered_at is not None
    assert message is None


async def test_failed_webhook_is_attempted_three_times_then_dead_lettered(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    rabbit_broker: RabbitBroker,
) -> None:
    payment_id = await store_payment(session_factory)
    simulator = CountingPaymentProcessingSimulator()
    webhook = FailingWebhook(status_code=503)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        processor = make_processor(session_factory, client, simulator)
        handler = PaymentMessageHandler(
            processor,
            RabbitPaymentRetryPublisher(rabbit_broker, timeout=5),
        )
        consumer_broker = RabbitBroker(
            rabbitmq_url,
            default_channel=Channel(prefetch_count=1),
        )
        consumer_broker.subscriber(
            payments_new_queue,
            ack_policy=AckPolicy.MANUAL,
        )(handler.handle)
        await consumer_broker.start()

        event_id = uuid4()
        try:
            await rabbit_broker.publish(
                make_event_payload(event_id, payment_id),
                queue=payments_new_queue,
                mandatory=True,
                persist=True,
                correlation_id=str(payment_id),
                headers={"x-attempt": 1},
                message_id=str(event_id),
            )
            queue = await rabbit_broker.declare_queue(payments_dlq)
            dead_letter = await wait_for_message(queue)
        finally:
            await consumer_broker.stop()

    await dead_letter.ack()

    async with session_factory() as session:
        payment = await session.get(PaymentRecord, payment_id)

    assert payment is not None
    assert payment.status == PaymentStatus.SUCCEEDED.value
    assert payment.webhook_delivered_at is None
    assert payment.webhook_attempts == MAX_ATTEMPTS
    assert payment.webhook_last_error == "Webhook returned HTTP 503"
    assert payment.processing_token is None
    assert payment.processing_claimed_until is None
    assert simulator.calls == 1
    assert len(webhook.request_times) == MAX_ATTEMPTS
    assert webhook.request_times[1] - webhook.request_times[0] >= FIRST_RETRY_LOWER_BOUND
    assert webhook.request_times[2] - webhook.request_times[1] >= SECOND_RETRY_LOWER_BOUND
    assert dead_letter.message_id == str(event_id)
    assert dead_letter.correlation_id == str(payment_id)
    assert dead_letter.headers["x-attempt"] == MAX_ATTEMPTS
    assert "x-death" in dead_letter.headers


async def test_failed_retry_publication_keeps_current_message_recoverable(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await store_payment(session_factory)
    webhook = FailingWebhook(status_code=503)
    retry_publisher = AsyncMock(spec=RabbitPaymentRetryPublisher)
    retry_publisher.publish.side_effect = ConnectionError("broker unavailable")
    message = AsyncMock()
    message.headers = {"x-attempt": 1}
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        occurred_at=datetime.now(UTC),
        payment_id=payment_id,
    )

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        handler = PaymentMessageHandler(
            make_processor(session_factory, client),
            retry_publisher,
        )

        with pytest.raises(ConnectionError, match="broker unavailable"):
            await handler.handle(event, message)

    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()

    async with session_factory() as session:
        payment = await session.get(PaymentRecord, payment_id)

    assert payment is not None
    assert payment.webhook_attempts == 1
    assert payment.webhook_last_error == "Webhook returned HTTP 503"
    assert payment.processing_token is not None
    assert payment.processing_claimed_until is not None


class CountingPaymentProcessingSimulator(PaymentProcessingSimulator):
    def __init__(self) -> None:
        super().__init__(min_delay=0, max_delay=0, success_rate=1)
        self.calls = 0

    async def simulate(self) -> PaymentStatus:
        self.calls += 1

        return PaymentStatus.SUCCEEDED


class RecordingWebhook:
    async def respond(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)


class FailingWebhook:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.request_times: list[float] = []

    async def respond(self, request: httpx.Request) -> httpx.Response:
        self.request_times.append(time.monotonic())

        return httpx.Response(self.status_code)


async def store_payment(session_factory: async_sessionmaker[AsyncSession]) -> UUID:
    payment_id = uuid4()
    payment = PaymentRecord(
        id=payment_id,
        amount=Decimal("100.00"),
        currency=Currency.RUB.value,
        description=None,
        payment_metadata={},
        status=PaymentStatus.PENDING.value,
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhooks/payments",
        created_at=datetime.now(UTC),
    )

    async with session_factory() as session, session.begin():
        session.add(payment)

    return payment_id


def make_processor(
    session_factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    simulator: PaymentProcessingSimulator | None = None,
) -> PaymentProcessor:
    return PaymentProcessor(
        SqlAlchemyPaymentProcessingRepository(session_factory),
        simulator or CountingPaymentProcessingSimulator(),
        WebhookClient(client),
        claim_seconds=15,
        claim_poll_interval=0.01,
    )


def make_event_payload(event_id: UUID, payment_id: UUID) -> dict[str, str | int]:
    return {
        "event_id": str(event_id),
        "event_type": "payment.created",
        "schema_version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "payment_id": str(payment_id),
    }


async def wait_for_webhook_delivery(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: UUID,
) -> PaymentRecord:
    async with asyncio.timeout(5):
        while True:
            async with session_factory() as session:
                payment = await session.get(PaymentRecord, payment_id)

            if payment is not None and payment.webhook_delivered_at is not None:
                return payment

            await asyncio.sleep(0.01)


async def wait_for_message(queue: AbstractQueue) -> AbstractIncomingMessage:
    async with asyncio.timeout(8):
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                return message

            await asyncio.sleep(0.05)
