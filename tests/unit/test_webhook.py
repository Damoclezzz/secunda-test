from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from payment_service.infrastructure.webhook import WebhookClient
from payment_service.payments.models import Currency, PaymentStatus
from payment_service.payments.webhooks import PaymentResultWebhook, WebhookDeliveryError


async def test_webhook_timeout_has_safe_failure_context() -> None:
    webhook = TimingOutWebhook()

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        with pytest.raises(WebhookDeliveryError) as raised_error:
            await WebhookClient(client).deliver(
                "https://example.com/webhook?token=secret",
                make_webhook_payload(),
            )

    assert raised_error.value.reason == "Webhook request timed out"


async def test_webhook_connection_error_has_safe_failure_context() -> None:
    webhook = UnavailableWebhook()

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook.respond)) as client:
        with pytest.raises(WebhookDeliveryError) as raised_error:
            await WebhookClient(client).deliver(
                "https://example.com/webhook?token=secret",
                make_webhook_payload(),
            )

    assert raised_error.value.reason == "Webhook request failed: ConnectError"


class TimingOutWebhook:
    async def respond(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("request URL contains sensitive data", request=request)


class UnavailableWebhook:
    async def respond(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("request URL contains sensitive data", request=request)


def make_webhook_payload() -> PaymentResultWebhook:
    return PaymentResultWebhook(
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        processed_at=datetime.now(UTC),
    )
