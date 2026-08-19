from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.authentication import require_api_key
from payment_service.infrastructure.db.payment_repository import SqlAlchemyPaymentRepository
from payment_service.infrastructure.db.session import get_session
from payment_service.payments.models import IdempotencyConflictError, PaymentInput
from payment_service.payments.schemas import (
    CreatePaymentRequest,
    PaymentAcceptedResponse,
    PaymentResponse,
)
from payment_service.payments.use_cases import create_payment

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post("", response_model=PaymentAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def accept_payment(
    payment_request: CreatePaymentRequest,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
            pattern=r".*\S.*",
        ),
    ],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentAcceptedResponse:
    repository = SqlAlchemyPaymentRepository(session)
    payment_input = PaymentInput(
        amount=payment_request.amount,
        currency=payment_request.currency,
        description=payment_request.description,
        metadata=payment_request.metadata,
        webhook_url=str(payment_request.webhook_url),
    )

    try:
        payment = await create_payment(repository, payment_input, idempotency_key)
    except IdempotencyConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key is already used for another request",
        ) from error

    return PaymentAcceptedResponse.create_from_payment(payment)


@router.get("/{payment_id}", response_model=PaymentResponse)
async def retrieve_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentResponse:
    repository = SqlAlchemyPaymentRepository(session)
    payment = await repository.get_by_id(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")

    return PaymentResponse.create_from_payment(payment)
