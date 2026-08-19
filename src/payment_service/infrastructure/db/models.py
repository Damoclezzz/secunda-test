from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payment_service.payments.models import Currency, Payment, PaymentStatus


class Base(DeclarativeBase):
    pass


class PaymentRecord(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name="ck_payments_status",
        ),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="ck_payments_processed_at",
        ),
        CheckConstraint(
            "(processing_token IS NULL) = (processing_claimed_until IS NULL)",
            name="ck_payments_processing_claim",
        ),
        CheckConstraint("webhook_attempts >= 0", name="ck_payments_webhook_attempts"),
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )

    id: Mapped[UUID] = mapped_column(postgresql.UUID(as_uuid=True), primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str | None] = mapped_column(String(500))
    payment_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        postgresql.JSONB,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    status: Mapped[str] = mapped_column(String(16))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_token: Mapped[UUID | None] = mapped_column(postgresql.UUID(as_uuid=True))
    processing_claimed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_attempts: Mapped[int] = mapped_column(
        SmallInteger,
        default=0,
        server_default=text("0"),
    )
    webhook_last_error: Mapped[str | None] = mapped_column(Text)

    def convert_to_payment(self) -> Payment:
        return Payment(
            id=self.id,
            amount=self.amount,
            currency=Currency(self.currency),
            description=self.description,
            metadata=self.payment_metadata,
            status=PaymentStatus(self.status),
            idempotency_key=self.idempotency_key,
            webhook_url=self.webhook_url,
            created_at=self.created_at,
            processed_at=self.processed_at,
        )


class OutboxRecord(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint(
            "(claim_token IS NULL) = (claimed_until IS NULL)",
            name="ck_outbox_claim",
        ),
        CheckConstraint(
            "payload ->> 'event_id' = id::text",
            name="ck_outbox_payload_event_id",
        ),
        CheckConstraint(
            "payload ->> 'payment_id' = payment_id::text",
            name="ck_outbox_payload_payment_id",
        ),
        Index("ix_outbox_payment_id", "payment_id"),
        Index("ix_outbox_available_at", "available_at"),
        Index("ix_outbox_published_at", "published_at"),
        Index("ix_outbox_claimed_until", "claimed_until"),
    )

    id: Mapped[UUID] = mapped_column(postgresql.UUID(as_uuid=True), primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        ForeignKey("payments.id", ondelete="RESTRICT"),
    )
    payload: Mapped[dict[str, Any]] = mapped_column(postgresql.JSONB)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    claim_token: Mapped[UUID | None] = mapped_column(postgresql.UUID(as_uuid=True))
    claimed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
