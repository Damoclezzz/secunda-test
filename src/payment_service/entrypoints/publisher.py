import asyncio
import logging

from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.infrastructure.db.outbox_repository import SqlAlchemyOutboxRepository
from payment_service.infrastructure.db.session import create_engine
from payment_service.infrastructure.messaging.publisher import RabbitEventPublisher
from payment_service.infrastructure.messaging.topology import RabbitTopology
from payment_service.outbox.publisher import OutboxPublisher, OutboxPublisherOptions
from payment_service.settings import Settings, get_settings


def create_publisher_app(settings: Settings | None = None) -> FastStream:
    resolved_settings = settings or get_settings()
    engine = create_engine(resolved_settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    broker = RabbitBroker(
        resolved_settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    event_publisher = RabbitEventPublisher(broker, resolved_settings.outbox_publish_timeout)
    outbox_publisher = OutboxPublisher(
        SqlAlchemyOutboxRepository(session_factory),
        event_publisher.publish,
        OutboxPublisherOptions(
            batch_size=resolved_settings.outbox_batch_size,
            poll_interval=resolved_settings.outbox_poll_interval,
            claim_seconds=resolved_settings.outbox_claim_seconds,
            retry_delay=resolved_settings.outbox_retry_delay,
        ),
    )
    topology = RabbitTopology(broker)

    return FastStream(
        broker,
        after_startup=(topology.declare, outbox_publisher.start),
        on_shutdown=(outbox_publisher.stop, engine.dispose),
    )


async def run() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    await create_publisher_app(settings).run(
        log_level=logging.getLevelNamesMapping()[settings.log_level]
    )


if __name__ == "__main__":
    asyncio.run(run())
