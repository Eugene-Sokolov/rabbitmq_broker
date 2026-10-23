import argparse
import asyncio
import logging
import secrets
import statistics
import time
import uuid
from collections import Counter

import httpx


logger = logging.getLogger("load_test")

CURRENCIES = ("RUB", "USD", "EUR")
RETRY_QUEUES = {"retry.1": "payments.new.retry.1", "retry.2": "payments.new.retry.2"}
STATS_INTERVAL_SECONDS = 5


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load test the payments service.")
    parser.add_argument("--total", type=int, default=300, help="payments to create")
    parser.add_argument("--concurrency", type=int, default=20, help="parallel requests")
    parser.add_argument(
        "--fail-share", type=float, default=0.05, help="share of always failing webhooks"
    )
    parser.add_argument("--watch", type=int, default=60, help="seconds to watch queues afterwards")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--api-key", default="example-api-key")
    parser.add_argument("--webhook-url", default="http://webhook-sink:8080")
    parser.add_argument("--rabbitmq-url", default="http://localhost:15672")
    parser.add_argument("--rabbitmq-user", default="guest")
    parser.add_argument("--rabbitmq-password", default="guest")
    return parser.parse_args()


def _payment(index: int, webhook_url: str, fail_share: float) -> dict[str, object]:
    always_fails = secrets.randbelow(10_000) < fail_share * 10_000
    return {
        "amount": f"{secrets.randbelow(1_000_000) / 100 + 1:.2f}",
        "currency": secrets.choice(CURRENCIES),
        "description": f"Load test payment #{index}",
        "metadata": {"load_test": True, "index": index},
        "webhook_url": f"{webhook_url}/{'fail' if always_fails else 'webhook'}",
    }


async def _create(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    body: dict[str, object],
) -> tuple[int, float]:
    async with semaphore:
        started = time.perf_counter()
        try:
            response = await client.post(
                "/api/v1/payments",
                json=body,
                headers={"Idempotency-Key": str(uuid.uuid4())},
            )
        except httpx.HTTPError:
            return 0, time.perf_counter() - started
        return response.status_code, time.perf_counter() - started


async def _send_payments(args: argparse.Namespace) -> None:
    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient(
        base_url=args.api_url,
        headers={"X-API-Key": args.api_key},
        timeout=30,
    ) as client:
        started = time.perf_counter()
        results = await asyncio.gather(
            *(
                _create(client, semaphore, _payment(index, args.webhook_url, args.fail_share))
                for index in range(args.total)
            )
        )
        elapsed = time.perf_counter() - started

    statuses = Counter(status for status, _ in results)
    latencies = sorted(latency * 1000 for _, latency in results)
    logger.info(f"Sent {args.total} payments in {elapsed:.1f}s ({args.total / elapsed:.0f} req/s)")
    responses = ", ".join(
        f"{status or 'connection error'}: {count}" for status, count in statuses.items()
    )
    logger.info(f"Responses: {responses}")
    logger.info(
        f"Latency: p50 {statistics.median(latencies):.0f} ms, "
        f"p95 {latencies[int(len(latencies) * 0.95) - 1]:.0f} ms, max {latencies[-1]:.0f} ms"
    )


async def _watch_queues(args: argparse.Namespace) -> None:
    async with httpx.AsyncClient(
        base_url=args.rabbitmq_url,
        auth=(args.rabbitmq_user, args.rabbitmq_password),
        timeout=5,
    ) as client:
        deadline = time.monotonic() + args.watch
        while time.monotonic() < deadline:
            response = await client.get("/api/queues/%2F")
            response.raise_for_status()
            queues = {queue["name"]: queue for queue in response.json()}
            main = queues.get("payments.new", {})
            retries = "  ".join(
                f"{label}={queues.get(name, {}).get('messages', 0)}"
                for label, name in RETRY_QUEUES.items()
            )
            logger.info(
                f"waiting={main.get('messages_ready', 0)}  "
                f"processing={main.get('messages_unacknowledged', 0)}  "
                f"{retries}  "
                f"dlq={queues.get('payments.new.dlq', {}).get('messages', 0)}"
            )
            await asyncio.sleep(STATS_INTERVAL_SECONDS)


async def _run(args: argparse.Namespace) -> None:
    await _send_payments(args)
    if args.watch > 0:
        logger.info(f"Queue depths for the next {args.watch}s (Ctrl+C to stop):")
        await _watch_queues(args)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(_run(_parse_args()))


if __name__ == "__main__":
    main()
