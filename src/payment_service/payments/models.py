from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class PaymentInput:
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    webhook_url: str


@dataclass(frozen=True, slots=True)
class Payment:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None = None

    def matches_creation_input(self, payment_input: PaymentInput) -> bool:
        return (
            self.amount == payment_input.amount
            and self.currency == payment_input.currency
            and self.description == payment_input.description
            and self.metadata == payment_input.metadata
            and self.webhook_url == payment_input.webhook_url
        )


@dataclass(frozen=True, slots=True)
class ClaimedPayment:
    id: UUID
    amount: Decimal
    currency: Currency
    status: PaymentStatus
    webhook_url: str
    processed_at: datetime | None
    processing_token: UUID


class DuplicateIdempotencyKeyError(Exception):
    pass


class IdempotencyConflictError(Exception):
    pass


class PaymentNotFoundError(Exception):
    pass


class ProcessingClaimLostError(Exception):
    pass
