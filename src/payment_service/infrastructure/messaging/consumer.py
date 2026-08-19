from faststream.rabbit.annotations import RabbitMessage

from payment_service.payments.events import PaymentCreatedEvent
from payment_service.payments.processor import PaymentProcessor


class PaymentMessageHandler:
    def __init__(self, processor: PaymentProcessor) -> None:
        self.processor = processor

    async def handle(self, event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        await self.processor.process(event.payment_id)
        await message.ack()
