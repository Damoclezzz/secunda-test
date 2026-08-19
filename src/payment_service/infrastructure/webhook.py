import httpx

from payment_service.payments.webhooks import PaymentResultWebhook, WebhookDeliveryError


class WebhookClient:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def deliver(self, url: str, payload: PaymentResultWebhook) -> None:
        try:
            response = await self.client.post(url, json=payload.model_dump(mode="json"))
            response.raise_for_status()
        except httpx.TimeoutException as error:
            raise WebhookDeliveryError("Webhook request timed out") from error
        except httpx.HTTPStatusError as error:
            raise WebhookDeliveryError(
                f"Webhook returned HTTP {error.response.status_code}"
            ) from error
        except httpx.RequestError as error:
            raise WebhookDeliveryError(f"Webhook request failed: {type(error).__name__}") from error
