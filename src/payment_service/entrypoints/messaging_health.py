import asyncio

from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from sqlalchemy import select

from payment_service.infrastructure.db.session import create_engine
from payment_service.infrastructure.messaging.topology import RabbitTopology
from payment_service.settings import get_settings


async def check() -> None:
    settings = get_settings()
    engine = create_engine(settings)
    broker = RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        timeout=2,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )

    try:
        async with engine.connect() as connection:
            await connection.execute(select(1))

        await broker.start()
        await RabbitTopology(broker).declare()
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(check())
