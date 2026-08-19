import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import PaymentRecord
from payment_service.infrastructure.db.processing_repository import (
    SqlAlchemyPaymentProcessingRepository,
)
from payment_service.infrastructure.webhook import WebhookClient
from payment_service.payments.models import Currency, PaymentStatus
from payment_service.payments.processor import PaymentProcessor
from payment_service.payments.simulator import PaymentProcessingSimulator


async def test_concurrent_payment_claims_have_one_owner(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await store_payment(session_factory)
    first_repository = SqlAlchemyPaymentProcessingRepository(session_factory)
    second_repository = SqlAlchemyPaymentProcessingRepository(session_factory)

    claims = await asyncio.gather(
        first_repository.claim(payment_id, 15),
        second_repository.claim(payment_id, 15),
    )

    assert sum(claim is not None for claim in claims) == 1


async def test_concurrent_processing_runs_simulation_and_webhook_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await store_payment(session_factory)
    simulator = BlockingPaymentProcessingSimulator()
    webhook = RecordingWebhook()
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        first_processor = make_processor(session_factory, simulator, client)
        second_processor = make_processor(session_factory, simulator, client)
        first_processing = asyncio.create_task(first_processor.process(payment_id))
        await simulator.started.wait()
        second_processing = asyncio.create_task(second_processor.process(payment_id))
        await asyncio.sleep(0.05)

        assert simulator.calls == 1
        assert webhook.requests == []

        simulator.release.set()
        await asyncio.gather(first_processing, second_processing)

    assert simulator.calls == 1
    assert len(webhook.requests) == 1


async def test_successful_processing_delivers_webhook_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await store_payment(session_factory)
    simulator = CountingPaymentProcessingSimulator()
    webhook = RecordingWebhook()
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        processor = make_processor(session_factory, simulator, client)
        await processor.process(payment_id)
        await processor.process(payment_id)

    async with session_factory() as session:
        payment = await session.get(PaymentRecord, payment_id)

    assert payment is not None
    assert payment.status == PaymentStatus.SUCCEEDED.value
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert payment.webhook_attempts == 1
    assert payment.processing_token is None
    assert payment.processing_claimed_until is None
    assert simulator.calls == 1
    assert len(webhook.requests) == 1
    assert json.loads(webhook.requests[0].content) == {
        "payment_id": str(payment_id),
        "status": "succeeded",
        "amount": "100.00",
        "currency": "RUB",
        "processed_at": payment.processed_at.isoformat().replace("+00:00", "Z"),
    }


async def test_terminal_result_resumes_without_repeated_simulation(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    processed_at = datetime.now(UTC)
    payment_id = await store_payment(
        session_factory,
        status=PaymentStatus.FAILED,
        processed_at=processed_at,
    )
    simulator = CountingPaymentProcessingSimulator()
    webhook = RecordingWebhook()
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        processor = make_processor(session_factory, simulator, client)
        await processor.process(payment_id)

    assert simulator.calls == 0
    assert len(webhook.requests) == 1
    assert json.loads(webhook.requests[0].content)["status"] == "failed"


class CountingPaymentProcessingSimulator(PaymentProcessingSimulator):
    def __init__(self, status: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        super().__init__(min_delay=0, max_delay=0, success_rate=1)
        self.status = status
        self.calls = 0

    async def simulate(self) -> PaymentStatus:
        self.calls += 1

        return self.status


class BlockingPaymentProcessingSimulator(CountingPaymentProcessingSimulator):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def simulate(self) -> PaymentStatus:
        self.calls += 1
        self.started.set()
        await self.release.wait()

        return self.status


class RecordingWebhook:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)

        return httpx.Response(204)


async def store_payment(
    session_factory: async_sessionmaker[AsyncSession],
    status: PaymentStatus = PaymentStatus.PENDING,
    processed_at: datetime | None = None,
) -> UUID:
    payment_id = uuid4()
    payment = PaymentRecord(
        id=payment_id,
        amount=Decimal("100.00"),
        currency=Currency.RUB.value,
        description=None,
        payment_metadata={},
        status=status.value,
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhooks/payments",
        created_at=datetime.now(UTC),
        processed_at=processed_at,
    )

    async with session_factory() as session, session.begin():
        session.add(payment)

    return payment_id


def make_processor(
    session_factory: async_sessionmaker[AsyncSession],
    simulator: PaymentProcessingSimulator,
    client: httpx.AsyncClient,
) -> PaymentProcessor:
    return PaymentProcessor(
        SqlAlchemyPaymentProcessingRepository(session_factory),
        simulator,
        WebhookClient(client),
        claim_seconds=15,
        claim_poll_interval=0.01,
    )
