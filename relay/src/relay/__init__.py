import asyncio
import logging
import signal

from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from relay.relay import OutboxRelay
from relay.settings import Settings


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run(Settings()))


def create_broker(rabbitmq_url: str) -> RabbitBroker:
    return RabbitBroker(rabbitmq_url, default_channel=Channel(on_return_raises=True))


async def _run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signal_number, stop.set)

    engine = create_async_engine(str(settings.database_url))
    broker = create_broker(str(settings.rabbitmq_url))
    relay = OutboxRelay(
        async_sessionmaker(engine, expire_on_commit=False),
        broker,
        batch_size=settings.batch_size,
        poll_interval=settings.poll_interval,
        max_backoff=settings.max_backoff,
        cleanup_interval=settings.cleanup_interval,
        retention=settings.outbox_retention,
    )
    try:
        async with broker:
            await relay.run(stop)
    finally:
        await engine.dispose()
