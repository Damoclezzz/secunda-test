import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payment_service.entrypoints.api import create_app
from payment_service.infrastructure.db.models import OutboxRecord, PaymentRecord
from payment_service.infrastructure.messaging.topology import RabbitTopology, queues
from payment_service.settings import Settings


@pytest.fixture(scope="session")
def database_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if value is None:
        pytest.skip("TEST_DATABASE_URL is not set")

    return value


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    value = os.getenv("TEST_RABBITMQ_URL")
    if value is None:
        pytest.skip("TEST_RABBITMQ_URL is not set")

    return value


@pytest_asyncio.fixture
async def database_engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(database_url)

    async with engine.begin() as connection:
        await connection.execute(delete(OutboxRecord))
        await connection.execute(delete(PaymentRecord))

    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(
    database_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(database_engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def rabbit_broker(rabbitmq_url: str) -> AsyncIterator[RabbitBroker]:
    broker = RabbitBroker(
        rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    await broker.start()
    await RabbitTopology(broker).declare()
    for queue in queues:
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.purge()

    yield broker
    await broker.stop()


@pytest_asyncio.fixture
async def client(database_url: str, database_engine: AsyncEngine) -> AsyncIterator[AsyncClient]:
    settings = Settings(
        environment="test",
        api_key=SecretStr("test-api-key"),
        database_url=SecretStr(database_url),
        rabbitmq_url=SecretStr("amqp://unused"),
    )
    app = create_app(settings)

    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as test_client,
    ):
        yield test_client
