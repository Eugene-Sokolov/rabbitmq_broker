import httpx
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from consumer.gateway import EmulatedPaymentGateway
from consumer.handlers import register_handlers
from consumer.services.payments import PaymentProcessor
from consumer.settings import Settings
from consumer.topology import PAYMENTS_DEAD_LETTER_QUEUE, RetryPolicy
from consumer.webhooks import WebhookNotifier


def create_app(settings: Settings) -> FastStream:
    engine = create_async_engine(str(settings.database_url))
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    broker = RabbitBroker(
        str(settings.rabbitmq_url), default_channel=Channel(on_return_raises=True)
    )
    retry = RetryPolicy(max_attempts=settings.max_attempts)
    processor = PaymentProcessor(
        async_sessionmaker(engine, expire_on_commit=False),
        EmulatedPaymentGateway(
            success_rate=settings.gateway_success_rate,
            min_seconds=settings.gateway_min_seconds,
            max_seconds=settings.gateway_max_seconds,
        ),
        WebhookNotifier(http_client),
    )
    register_handlers(broker, processor, retry, prefetch_count=settings.prefetch_count)
    app = FastStream(broker)

    @app.on_startup
    async def check_queues_exist() -> None:
        await broker.connect()
        for queue in (PAYMENTS_DEAD_LETTER_QUEUE, *retry.queues()):
            await broker.declare_queue(queue)

    @app.after_shutdown
    async def close_clients() -> None:
        await http_client.aclose()
        await engine.dispose()

    return app
