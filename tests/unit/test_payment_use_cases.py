from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.models import (
    Currency,
    DuplicateIdempotencyKeyError,
    IdempotencyConflictError,
    Payment,
    PaymentInput,
    PaymentStatus,
)
from payment_service.payments.use_cases import create_payment


class FakePaymentRepository:
    def __init__(self, existing_payment: Payment | None = None) -> None:
        self.existing_payment = existing_payment
        self.inserted_event: PaymentCreatedEvent | None = None

    async def insert(self, payment: Payment, event: PaymentCreatedEvent) -> None:
        if self.existing_payment is not None:
            raise DuplicateIdempotencyKeyError

        self.existing_payment = payment
        self.inserted_event = event

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        if self.existing_payment is not None and self.existing_payment.id == payment_id:
            return self.existing_payment

        return None

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        if (
            self.existing_payment is not None
            and self.existing_payment.idempotency_key == idempotency_key
        ):
            return self.existing_payment

        return None


def make_payment_input(amount: str = "100.00") -> PaymentInput:
    return PaymentInput(
        amount=Decimal(amount),
        currency=Currency.RUB,
        description="Order 42",
        metadata={"order_id": 42},
        webhook_url="https://example.com/webhooks/payments",
    )


def make_existing_payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Order 42",
        metadata={"order_id": 42},
        status=PaymentStatus.PENDING,
        idempotency_key="payment-key",
        webhook_url="https://example.com/webhooks/payments",
        created_at=datetime.now(UTC),
    )


async def test_create_payment_produces_matching_event() -> None:
    repository = FakePaymentRepository()

    payment = await create_payment(repository, make_payment_input(), "payment-key")

    assert repository.inserted_event is not None
    assert repository.inserted_event.payment_id == payment.id
    assert repository.inserted_event.occurred_at == payment.created_at


async def test_create_payment_replays_matching_request() -> None:
    stored_payment = make_existing_payment()
    repository = FakePaymentRepository(stored_payment)

    payment = await create_payment(repository, make_payment_input(), "payment-key")

    assert payment == stored_payment


async def test_create_payment_rejects_different_request() -> None:
    repository = FakePaymentRepository(make_existing_payment())

    with pytest.raises(IdempotencyConflictError):
        await create_payment(repository, make_payment_input("200.00"), "payment-key")
