import httpx

from payment_service.payments.webhooks import PaymentResultWebhook


class WebhookClient:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def deliver(self, url: str, payload: PaymentResultWebhook) -> None:
        response = await self.client.post(url, json=payload.model_dump(mode="json"))
        response.raise_for_status()
