import asyncio
import logging

from consumer.app import create_app
from consumer.settings import Settings


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(create_app(Settings()).run())
