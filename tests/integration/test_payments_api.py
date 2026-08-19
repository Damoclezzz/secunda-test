import asyncio
from http import HTTPStatus
from typing import Any
from uuid import UUID

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.db.models import OutboxRecord, PaymentRecord


def make_api_headers(
    api_key: str = "test-api-key",
    idempotency_key: str | None = "payment-key",
) -> dict[str, str]:
    headers = {"X-API-Key": api_key}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key

    return headers


def make_payment_body(amount: str = "100.50") -> dict[str, Any]:
    return {
        "amount": amount,
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://example.com/webhooks/payments",
    }


async def test_create_and_retrieve_payment(
    client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(),
        json=make_payment_body(),
    )

    assert response.status_code == HTTPStatus.ACCEPTED
    accepted_payment = response.json()
    assert accepted_payment["status"] == "pending"

    payment_id = accepted_payment["payment_id"]
    response = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers=make_api_headers(idempotency_key=None),
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "id": payment_id,
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "status": "pending",
        "webhook_url": "https://example.com/webhooks/payments",
        "created_at": accepted_payment["created_at"],
        "processed_at": None,
    }

    async with session_factory() as session:
        result = await session.scalars(
            select(OutboxRecord).where(OutboxRecord.payment_id == UUID(payment_id))
        )
        outbox = result.one()

    assert str(outbox.id) == outbox.payload["event_id"]
    assert str(outbox.payment_id) == payment_id
    assert outbox.payload["payment_id"] == payment_id
    assert outbox.payload["event_type"] == "payment.created"
    assert outbox.payload["schema_version"] == 1


async def test_idempotency_replay_returns_existing_payment(
    client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_response = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(),
        json=make_payment_body(),
    )
    replay_response = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(),
        json=make_payment_body(),
    )

    assert first_response.status_code == HTTPStatus.ACCEPTED
    assert replay_response.status_code == HTTPStatus.ACCEPTED
    assert replay_response.json() == first_response.json()

    async with session_factory() as session:
        payment_count = await session.scalar(select(func.count()).select_from(PaymentRecord))
        outbox_count = await session.scalar(select(func.count()).select_from(OutboxRecord))

    assert payment_count == 1
    assert outbox_count == 1


async def test_idempotency_conflict_rejects_different_request(
    client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_response = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(),
        json=make_payment_body(),
    )
    conflict_response = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(),
        json=make_payment_body(amount="200.00"),
    )

    assert first_response.status_code == HTTPStatus.ACCEPTED
    assert conflict_response.status_code == HTTPStatus.CONFLICT

    async with session_factory() as session:
        payment_count = await session.scalar(select(func.count()).select_from(PaymentRecord))
        outbox_count = await session.scalar(select(func.count()).select_from(OutboxRecord))

    assert payment_count == 1
    assert outbox_count == 1


async def test_concurrent_requests_create_one_payment(
    client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    responses = await asyncio.gather(
        *(
            client.post(
                "/api/v1/payments",
                headers=make_api_headers(),
                json=make_payment_body(),
            )
            for _ in range(8)
        )
    )

    assert {response.status_code for response in responses} == {HTTPStatus.ACCEPTED}
    assert len({response.json()["payment_id"] for response in responses}) == 1

    async with session_factory() as session:
        payment_count = await session.scalar(select(func.count()).select_from(PaymentRecord))
        outbox_count = await session.scalar(select(func.count()).select_from(OutboxRecord))

    assert payment_count == 1
    assert outbox_count == 1


async def test_payment_endpoints_require_valid_api_key(client: AsyncClient) -> None:
    missing_key = await client.post("/api/v1/payments", json={})
    invalid_key = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(api_key="invalid"),
        json=make_payment_body(),
    )
    unauthenticated_get = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")

    assert missing_key.status_code == HTTPStatus.UNAUTHORIZED
    assert invalid_key.status_code == HTTPStatus.UNAUTHORIZED
    assert unauthenticated_get.status_code == HTTPStatus.UNAUTHORIZED


async def test_create_payment_rejects_invalid_request(client: AsyncClient) -> None:
    missing_idempotency_key = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(idempotency_key=None),
        json=make_payment_body(),
    )
    invalid_amount = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(),
        json=make_payment_body(amount="1.001"),
    )
    blank_idempotency_key = await client.post(
        "/api/v1/payments",
        headers=make_api_headers(idempotency_key="   "),
        json=make_payment_body(),
    )

    assert missing_idempotency_key.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert invalid_amount.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert blank_idempotency_key.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_unknown_payment_returns_not_found(client: AsyncClient) -> None:
    unknown_payment = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000",
        headers=make_api_headers(idempotency_key=None),
    )
    invalid_payment_id = await client.get(
        "/api/v1/payments/not-a-uuid",
        headers=make_api_headers(idempotency_key=None),
    )

    assert unknown_payment.status_code == HTTPStatus.NOT_FOUND
    assert invalid_payment_id.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
