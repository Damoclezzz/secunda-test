from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment_service.entrypoints.health import router as health_router
from payment_service.infrastructure.db.session import create_engine
from payment_service.payments.api import router as payments_router
from payment_service.settings import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    engine = create_engine(resolved_settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await engine.dispose()

    app = FastAPI(title="Payment processing service", lifespan=lifespan)
    if settings is not None:
        app.dependency_overrides[get_settings] = lambda: resolved_settings

    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.include_router(health_router)
    app.include_router(payments_router)

    return app
