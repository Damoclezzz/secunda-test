from collections.abc import Mapping
from typing import Any

from faststream.rabbit.annotations import RabbitMessage

from payment_service.infrastructure.messaging.payment_retry_publisher import (
    RabbitPaymentRetryPublisher,
)
from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.models import WebhookAttemptFailedError
from payment_service.payments.processor import PaymentProcessor

MAX_ATTEMPTS = 3


def parse_attempt(headers: Mapping[str, Any]) -> int:
    raw_attempt = headers.get("x-attempt", 1)
    if isinstance(raw_attempt, int) and not isinstance(raw_attempt, bool):
        attempt = raw_attempt
    elif isinstance(raw_attempt, str) and raw_attempt.isdecimal():
        attempt = int(raw_attempt)
    else:
        raise ValueError(f"x-attempt must be an integer from 1 to {MAX_ATTEMPTS}")

    if attempt not in range(1, MAX_ATTEMPTS + 1):
        raise ValueError(f"x-attempt must be an integer from 1 to {MAX_ATTEMPTS}")

    return attempt


class PaymentMessageHandler:
    def __init__(
        self,
        processor: PaymentProcessor,
        retry_publisher: RabbitPaymentRetryPublisher,
    ) -> None:
        self.processor = processor
        self.retry_publisher = retry_publisher

    async def handle(self, event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        try:
            attempt = parse_attempt(message.headers)
        except ValueError:
            await message.reject(requeue=False)

            return

        try:
            await self.processor.process(event.payment_id, attempt)
        except WebhookAttemptFailedError as error:
            if attempt == MAX_ATTEMPTS:
                await self.processor.release_claim(error)
                await message.reject(requeue=False)

                return

            try:
                await self.retry_publisher.publish(event, attempt + 1)
            except Exception:
                await message.nack(requeue=True)

                raise

            await self.processor.release_claim(error)
            await message.ack()

            return

        await message.ack()
