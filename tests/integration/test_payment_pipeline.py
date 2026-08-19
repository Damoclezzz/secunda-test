import asyncio
from http import HTTPStatus
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import OutboxRecord, PaymentRecord
from payment_service.infrastructure.db.outbox_repository import SqlAlchemyOutboxRepository
from payment_service.infrastructure.db.processing_repository import (
    SqlAlchemyPaymentProcessingRepository,
)
from payment_service.infrastructure.messaging.consumer import PaymentMessageHandler
from payment_service.infrastructure.messaging.payment_retry_publisher import (
    RabbitPaymentRetryPublisher,
)
from payment_service.infrastructure.messaging.publisher import RabbitEventPublisher
from payment_service.infrastructure.messaging.topology import payments_new_queue
from payment_service.infrastructure.webhook import WebhookClient
from payment_service.outbox.publisher import OutboxPublisher, OutboxPublisherOptions
from payment_service.payments.models import PaymentStatus
from payment_service.payments.processor import PaymentProcessor
from payment_service.payments.simulator import PaymentProcessingSimulator


async def test_payment_pipeline_runs_from_api_through_outbox_to_webhook(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_url: str,
    rabbit_broker: RabbitBroker,
) -> None:
    simulator = AsyncMock(spec=PaymentProcessingSimulator)
    simulator.simulate.return_value = PaymentStatus.SUCCEEDED
    webhook = AsyncMock(return_value=httpx.Response(204))
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http_client:
        processor = PaymentProcessor(
            SqlAlchemyPaymentProcessingRepository(session_factory),
            simulator,
            WebhookClient(http_client),
            claim_seconds=15,
            claim_poll_interval=0.01,
        )
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
            create_response = await client.post(
                "/api/v1/payments",
                headers=make_api_headers(),
                json=make_payment_body(),
            )
            payment_id = UUID(create_response.json()["payment_id"])
            publisher = OutboxPublisher(
                SqlAlchemyOutboxRepository(session_factory),
                RabbitEventPublisher(rabbit_broker, timeout=5).publish,
                OutboxPublisherOptions(
                    batch_size=10,
                    poll_interval=0.01,
                    claim_seconds=15,
                    retry_delay=1,
                ),
            )
            published_count = await publisher.publish_batch()
            payment = await wait_for_webhook_delivery(session_factory, payment_id)
        finally:
            await consumer_broker.stop()

    get_response = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers=make_api_headers(idempotency_key=None),
    )
    async with session_factory() as session:
        outbox = await session.scalar(
            select(OutboxRecord).where(OutboxRecord.payment_id == payment_id)
        )

    queue = await rabbit_broker.declare_queue(payments_new_queue)
    remaining_message = await queue.get(timeout=0.1, fail=False)

    assert create_response.status_code == HTTPStatus.ACCEPTED
    assert published_count == 1
    assert payment.status == PaymentStatus.SUCCEEDED.value
    assert payment.webhook_delivered_at is not None
    assert payment.webhook_attempts == 1
    assert get_response.status_code == HTTPStatus.OK
    assert get_response.json()["status"] == PaymentStatus.SUCCEEDED.value
    assert outbox is not None
    assert outbox.published_at is not None
    simulator.simulate.assert_awaited_once()
    webhook.assert_awaited_once()
    assert remaining_message is None


def make_api_headers(idempotency_key: str | None = "pipeline-payment-key") -> dict[str, str]:
    headers = {"X-API-Key": "test-api-key"}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key

    return headers


def make_payment_body() -> dict[str, Any]:
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Pipeline payment",
        "metadata": {"source": "integration"},
        "webhook_url": "https://example.com/webhooks/payments",
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
