import asyncio
import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.infrastructure.db.processing_repository import (
    SqlAlchemyPaymentProcessingRepository,
)
from payment_service.infrastructure.db.session import create_engine
from payment_service.infrastructure.messaging.consumer import PaymentMessageHandler
from payment_service.infrastructure.messaging.topology import RabbitTopology, payments_new_queue
from payment_service.infrastructure.webhook import WebhookClient
from payment_service.payments.processor import PaymentProcessor
from payment_service.payments.simulator import PaymentProcessingSimulator
from payment_service.settings import Settings, get_settings


def create_consumer_app(settings: Settings | None = None) -> FastStream:
    resolved_settings = settings or get_settings()
    engine = create_engine(resolved_settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    broker = RabbitBroker(
        resolved_settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )
    topology = RabbitTopology(broker)
    http_client = httpx.AsyncClient(timeout=resolved_settings.webhook_timeout)
    processor = PaymentProcessor(
        SqlAlchemyPaymentProcessingRepository(session_factory),
        PaymentProcessingSimulator(
            resolved_settings.payment_processing_min_delay,
            resolved_settings.payment_processing_max_delay,
            resolved_settings.payment_processing_success_rate,
        ),
        WebhookClient(http_client),
        resolved_settings.processing_claim_seconds,
        resolved_settings.processing_claim_poll_interval,
    )
    message_handler = PaymentMessageHandler(processor)
    broker.subscriber(
        payments_new_queue,
        ack_policy=AckPolicy.MANUAL,
    )(message_handler.handle)

    return FastStream(
        broker,
        after_startup=(topology.declare,),
        on_shutdown=(http_client.aclose, engine.dispose),
    )


async def run() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    await create_consumer_app(settings).run(
        log_level=logging.getLevelNamesMapping()[settings.log_level]
    )


if __name__ == "__main__":
    asyncio.run(run())
