import asyncio
import random

from payment_service.payments.models import PaymentStatus


class PaymentProcessingSimulator:
    def __init__(self, min_delay: float, max_delay: float, success_rate: float) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.success_rate = success_rate

    async def simulate(self) -> PaymentStatus:
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        if random.random() < self.success_rate:
            return PaymentStatus.SUCCEEDED

        return PaymentStatus.FAILED
