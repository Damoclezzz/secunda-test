import asyncio
import logging
from uuid import UUID

from payment_service.infrastructure.db.processing_repository import (
    SqlAlchemyPaymentProcessingRepository,
)
from payment_service.infrastructure.webhook import WebhookClient
from payment_service.payments.models import (
    ClaimedPayment,
    PaymentNotFoundError,
    PaymentStatus,
    ProcessingClaimLostError,
)
from payment_service.payments.simulator import PaymentProcessingSimulator
from payment_service.payments.webhooks import PaymentResultWebhook

logger = logging.getLogger(__name__)


class PaymentProcessor:
    def __init__(
        self,
        repository: SqlAlchemyPaymentProcessingRepository,
        simulator: PaymentProcessingSimulator,
        webhook_client: WebhookClient,
        claim_seconds: int,
        claim_poll_interval: float,
    ) -> None:
        self.repository = repository
        self.simulator = simulator
        self.webhook_client = webhook_client
        self.claim_seconds = claim_seconds
        self.claim_poll_interval = claim_poll_interval

    async def process(self, payment_id: UUID) -> None:
        payment = await self.wait_for_claim(payment_id)
        if payment is None:
            logger.info("Payment webhook was already delivered payment_id=%s", payment_id)

            return

        if payment.status == PaymentStatus.PENDING:
            status = await self.simulator.simulate()
            processed_at = await self.repository.store_result(
                payment.id,
                payment.processing_token,
                status,
            )
            if processed_at is None:
                raise ProcessingClaimLostError(payment.id)
        else:
            status = payment.status
            processed_at = payment.processed_at
            if processed_at is None:
                raise RuntimeError("Terminal payment has no processing timestamp")

        renewed = await self.repository.renew_claim(
            payment.id,
            payment.processing_token,
            self.claim_seconds,
        )
        if not renewed:
            raise ProcessingClaimLostError(payment.id)

        await self.webhook_client.deliver(
            payment.webhook_url,
            PaymentResultWebhook(
                payment_id=payment.id,
                status=status,
                amount=payment.amount,
                currency=payment.currency,
                processed_at=processed_at,
            ),
        )
        marked = await self.repository.mark_webhook_delivered(
            payment.id,
            payment.processing_token,
        )
        if not marked:
            raise ProcessingClaimLostError(payment.id)

        logger.info("Payment webhook delivered payment_id=%s status=%s", payment.id, status)

    async def wait_for_claim(self, payment_id: UUID) -> ClaimedPayment | None:
        while True:
            payment = await self.repository.claim(payment_id, self.claim_seconds)
            if payment is not None:
                return payment
            if not await self.repository.exists(payment_id):
                raise PaymentNotFoundError(payment_id)
            if await self.repository.is_webhook_delivered(payment_id):
                return None

            await asyncio.sleep(self.claim_poll_interval)
