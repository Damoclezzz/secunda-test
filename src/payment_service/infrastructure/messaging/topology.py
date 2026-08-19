from faststream.rabbit import QueueType, RabbitBroker, RabbitQueue
from faststream.rabbit.schemas.queue import QuorumQueueArgs

PAYMENTS_NEW = "payments.new"
PAYMENTS_RETRY_1S = "payments.retry.1s"
PAYMENTS_RETRY_2S = "payments.retry.2s"
PAYMENTS_DLQ = "payments.dlq"


def dead_letter_arguments(routing_key: str, message_ttl: int | None = None) -> QuorumQueueArgs:
    arguments: QuorumQueueArgs = {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": routing_key,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-max-length": 10_000,
    }
    if message_ttl is not None:
        arguments["x-message-ttl"] = message_ttl

    return arguments


payments_new_queue = RabbitQueue(
    PAYMENTS_NEW,
    queue_type=QueueType.QUORUM,
    arguments=dead_letter_arguments(PAYMENTS_DLQ),
)
payments_retry_1s_queue = RabbitQueue(
    PAYMENTS_RETRY_1S,
    queue_type=QueueType.QUORUM,
    arguments=dead_letter_arguments(PAYMENTS_NEW, message_ttl=1000),
)
payments_retry_2s_queue = RabbitQueue(
    PAYMENTS_RETRY_2S,
    queue_type=QueueType.QUORUM,
    arguments=dead_letter_arguments(PAYMENTS_NEW, message_ttl=2000),
)
dlq_arguments: QuorumQueueArgs = {
    "x-overflow": "reject-publish",
    "x-max-length": 10_000,
}
payments_dlq = RabbitQueue(
    PAYMENTS_DLQ,
    queue_type=QueueType.QUORUM,
    arguments=dlq_arguments,
)

queues = (
    payments_new_queue,
    payments_retry_1s_queue,
    payments_retry_2s_queue,
    payments_dlq,
)


class RabbitTopology:
    def __init__(self, broker: RabbitBroker) -> None:
        self.broker = broker

    async def declare(self) -> None:
        for queue in queues:
            await self.broker.declare_queue(queue)
