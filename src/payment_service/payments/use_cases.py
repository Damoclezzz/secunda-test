from datetime import UTC, datetime
from uuid import uuid4

from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.models import (
    DuplicateIdempotencyKeyError,
    IdempotencyConflictError,
    Payment,
    PaymentInput,
    PaymentStatus,
)
from payment_service.payments.repository import PaymentRepository


async def create_payment(
    repository: PaymentRepository,
    payment_input: PaymentInput,
    idempotency_key: str,
) -> Payment:
    created_at = datetime.now(UTC)
    payment = Payment(
        id=uuid4(),
        amount=payment_input.amount,
        currency=payment_input.currency,
        description=payment_input.description,
        metadata=payment_input.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        webhook_url=payment_input.webhook_url,
        created_at=created_at,
    )
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        occurred_at=created_at,
        payment_id=payment.id,
    )

    try:
        await repository.insert(payment, event)
    except DuplicateIdempotencyKeyError:
        existing_payment = await repository.get_by_idempotency_key(idempotency_key)
        if existing_payment is None:
            raise

        if not existing_payment.matches_creation_input(payment_input):
            raise IdempotencyConflictError from None

        return existing_payment

    return payment
