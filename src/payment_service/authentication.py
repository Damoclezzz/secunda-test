from secrets import compare_digest
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from payment_service.settings import Settings, get_settings


async def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    expected_api_key = settings.api_key.get_secret_value()
    if api_key is None or not compare_digest(api_key, expected_api_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
