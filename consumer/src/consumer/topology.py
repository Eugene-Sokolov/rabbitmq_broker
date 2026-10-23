from data.queues import PAYMENTS_NEW_QUEUE
from faststream.rabbit import RabbitQueue
from pydantic import BaseModel, ConfigDict


ATTEMPT_HEADER = "x-attempt"

PAYMENTS_QUEUE = RabbitQueue(PAYMENTS_NEW_QUEUE, declare=False)

PAYMENTS_DEAD_LETTER_QUEUE = RabbitQueue(f"{PAYMENTS_NEW_QUEUE}.dlq", declare=False)


def retry_queue(attempt: int) -> RabbitQueue:
    return RabbitQueue(f"{PAYMENTS_NEW_QUEUE}.retry.{attempt}", declare=False)


class RetryPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_attempts: int

    def can_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts

    def queues(self) -> list[RabbitQueue]:
        return [retry_queue(attempt) for attempt in range(1, self.max_attempts)]
