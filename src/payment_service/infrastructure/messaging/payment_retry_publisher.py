from faststream.rabbit import RabbitBroker, RabbitQueue

from payment_service.infrastructure.messaging.topology import (
    payments_retry_1s_queue,
    payments_retry_2s_queue,
)
from payment_service.payments.events import PaymentCreatedEvent

retry_queues: dict[int, RabbitQueue] = {
    2: payments_retry_1s_queue,
    3: payments_retry_2s_queue,
}


class RabbitPaymentRetryPublisher:
    def __init__(self, broker: RabbitBroker, timeout: float) -> None:
        self.broker = broker
        self.timeout = timeout

    async def publish(self, event: PaymentCreatedEvent, attempt: int) -> None:
        queue = retry_queues[attempt]
        confirmation = await self.broker.publish(
            event.model_dump(mode="json"),
            queue=queue,
            mandatory=True,
            persist=True,
            timeout=self.timeout,
            correlation_id=str(event.payment_id),
            headers={"x-attempt": attempt},
            content_type="application/json",
            message_id=str(event.event_id),
            message_type=event.event_type,
        )
        if confirmation is None:
            raise RuntimeError("RabbitMQ did not confirm the retry message")
