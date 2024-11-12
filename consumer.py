from typing import TYPE_CHECKING
import logging
import time
from config import (
    configure_logging,
    get_connection,
    MQ_EXCHANGE,
    MQ_ROUTING_KEY,
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

log = logging.getLogger(__name__)


def consume_messages(channel: "BlockingChannel") -> None:


def main():
    configure_logging(level=logging.DEBUG)
    with get_connection() as connection:
        log.info("Created connection: %s", connection)
        with connection.channel() as channel:
            log.info("Created channel: %s", channel)
            produce_message(channel=channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
