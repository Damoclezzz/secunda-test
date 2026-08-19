import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass

from payment_service.infrastructure.db.outbox_repository import SqlAlchemyOutboxRepository
from payment_service.outbox.models import ClaimedOutboxEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OutboxPublisherOptions:
    batch_size: int
    poll_interval: float
    claim_seconds: int
    retry_delay: float


class OutboxPublisher:
    def __init__(
        self,
        repository: SqlAlchemyOutboxRepository,
        publish: Callable[[ClaimedOutboxEvent], Awaitable[None]],
        options: OutboxPublisherOptions,
    ) -> None:
        self.repository = repository
        self.publish = publish
        self.options = options
        self.task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self.run(), name="outbox-publisher")

    async def stop(self) -> None:
        if self.task is None:
            return

        self.task.cancel()
        with suppress(asyncio.CancelledError):
            await self.task

    async def run(self) -> None:
        while True:
            try:
                processed_count = await self.publish_batch()
            except Exception:
                logger.exception("Outbox publisher iteration failed")
                await asyncio.sleep(self.options.retry_delay)

                continue

            if processed_count == 0:
                await asyncio.sleep(self.options.poll_interval)

    async def publish_batch(self) -> int:
        events = await self.repository.claim_batch(
            self.options.batch_size,
            self.options.claim_seconds,
        )
        for event in events:
            await self.publish_event(event)

        return len(events)

    async def publish_event(self, event: ClaimedOutboxEvent) -> None:
        try:
            await self.publish(event)
        except Exception as error:
            await self.repository.mark_failed(
                event.event_id,
                event.claim_token,
                f"{type(error).__name__}: {error}",
                self.options.retry_delay,
            )
            logger.warning(
                "Outbox event publication failed event_id=%s payment_id=%s error=%s",
                event.event_id,
                event.payment_id,
                type(error).__name__,
            )

            return

        marked = await self.repository.mark_published(event.event_id, event.claim_token)
        if marked:
            logger.info(
                "Published outbox event event_id=%s payment_id=%s",
                event.event_id,
                event.payment_id,
            )
        else:
            logger.warning(
                "Published outbox event lost its claim event_id=%s payment_id=%s",
                event.event_id,
                event.payment_id,
            )
