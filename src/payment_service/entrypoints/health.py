from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.authentication import require_api_key
from payment_service.infrastructure.db.session import get_session

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/health")
async def check_health(session: Annotated[AsyncSession, Depends(get_session)]) -> dict[str, str]:
    await session.execute(select(1))

    return {"status": "ok"}
