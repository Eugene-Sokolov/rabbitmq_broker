import logging
from uuid import UUID

from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import BaseModel

from consumer.services.payments import PaymentNotFoundError, PaymentProcessing
from consumer.topology import ATTEMPT_HEADER, PAYMENTS_QUEUE, RetryPolicy, retry_queue


logger = logging.getLogger(__name__)


class PaymentCreatedMessage(BaseModel):
    payment_id: UUID


def register_handlers(
    broker: RabbitBroker,
    processor: PaymentProcessing,
    retry: RetryPolicy,
    *,
    prefetch_count: int,
) -> None:
    @broker.subscriber(PAYMENTS_QUEUE, channel=Channel(prefetch_count=prefetch_count))
    async def handle_payment_created(message: PaymentCreatedMessage, raw: RabbitMessage) -> None:
        attempt = int(raw.headers.get(ATTEMPT_HEADER, 1))
        try:
            await processor.process(message.payment_id)
        except PaymentNotFoundError:
            logger.exception("Dead-lettering message for unknown payment")
            raise
        except Exception:
            if not retry.can_retry(attempt):
                logger.exception(
                    f"Payment {message.payment_id} failed on attempt {attempt}, dead-lettering"
                )
                raise
            queue = retry_queue(attempt)
            logger.exception(
                f"Payment {message.payment_id} failed on attempt {attempt}, "
                f"retrying via {queue.name}"
            )
            await broker.publish(
                message.model_dump(mode="json"),
                queue=queue,
                headers={ATTEMPT_HEADER: attempt + 1},
                message_id=raw.message_id,
                message_type=raw.raw_message.type,
                timestamp=raw.raw_message.timestamp,
                persist=True,
            )
