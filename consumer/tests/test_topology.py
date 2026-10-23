import itertools
import json
import re
from pathlib import Path
from typing import Any

from consumer.settings import Settings
from consumer.topology import PAYMENTS_DEAD_LETTER_QUEUE, PAYMENTS_QUEUE, RetryPolicy, retry_queue


DEFINITIONS = json.loads((Path(__file__).parents[2] / "rabbitmq" / "definitions.json").read_text())
DEFAULT_RETRY = RetryPolicy(max_attempts=Settings.model_fields["max_attempts"].default)


def _effective_policy(queue_name: str) -> dict[str, Any]:
    matching = [
        policy
        for policy in DEFINITIONS["policies"]
        if policy["apply-to"] in {"queues", "all"} and re.search(policy["pattern"], queue_name)
    ]
    assert matching, f"no policy applies to {queue_name}"
    return max(matching, key=lambda policy: policy["priority"])["definition"]


def test_consumer_queues_are_passively_declared() -> None:
    queues = [PAYMENTS_QUEUE, PAYMENTS_DEAD_LETTER_QUEUE, *DEFAULT_RETRY.queues()]

    assert not any(queue.declare for queue in queues)


def test_retry_queues_are_named_by_attempt() -> None:
    assert [queue.name for queue in RetryPolicy(max_attempts=3).queues()] == [
        "payments.new.retry.1",
        "payments.new.retry.2",
    ]
    assert retry_queue(5).name == "payments.new.retry.5"


def test_retry_policy_allows_retries_until_max_attempts() -> None:
    policy = RetryPolicy(max_attempts=3)

    assert [policy.can_retry(attempt) for attempt in (1, 2, 3)] == [True, True, False]


def test_single_attempt_policy_has_no_retry_queues() -> None:
    assert RetryPolicy(max_attempts=1).queues() == []


def test_definitions_declare_every_queue_the_consumer_uses() -> None:
    defined = {queue["name"]: queue for queue in DEFINITIONS["queues"]}
    used = [PAYMENTS_QUEUE, PAYMENTS_DEAD_LETTER_QUEUE, *DEFAULT_RETRY.queues()]

    for queue in used:
        assert queue.name in defined, f"{queue.name} missing from definitions.json"
        assert defined[queue.name]["durable"]


def test_definitions_dead_letter_payments_queue_to_dlq() -> None:
    assert _effective_policy(PAYMENTS_QUEUE.name) == {
        "dead-letter-exchange": "",
        "dead-letter-routing-key": PAYMENTS_DEAD_LETTER_QUEUE.name,
    }


def test_definitions_return_retries_to_payments_queue_with_increasing_delay() -> None:
    policies = [_effective_policy(queue.name) for queue in DEFAULT_RETRY.queues()]

    for policy in policies:
        assert policy["dead-letter-exchange"] == ""
        assert policy["dead-letter-routing-key"] == PAYMENTS_QUEUE.name
    delays = [policy["message-ttl"] for policy in policies]
    assert delays[0] > 0
    assert all(later > earlier for earlier, later in itertools.pairwise(delays))


def test_dead_letter_queue_has_no_expiry_or_forwarding() -> None:
    assert not any(
        re.search(policy["pattern"], PAYMENTS_DEAD_LETTER_QUEUE.name)
        for policy in DEFINITIONS["policies"]
    )
