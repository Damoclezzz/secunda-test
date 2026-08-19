from faststream.rabbit import RabbitBroker

from payment_service.infrastructure.messaging.topology import payments_new_queue
from payment_service.outbox.models import ClaimedOutboxEvent


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, timeout: float) -> None:
        self.broker = broker
        self.timeout = timeout

    async def publish(self, event: ClaimedOutboxEvent) -> None:
        confirmation = await self.broker.publish(
            event.payload,
            queue=payments_new_queue,
            mandatory=True,
            persist=True,
            timeout=self.timeout,
            correlation_id=str(event.payment_id),
            headers={"x-attempt": 1},
            content_type="application/json",
            message_id=str(event.event_id),
            message_type=str(event.payload["event_type"]),
        )
        if confirmation is None:
            raise RuntimeError("RabbitMQ did not confirm the published message")
