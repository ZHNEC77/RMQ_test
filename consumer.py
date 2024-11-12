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
    from pika.spec import Basic, BasicProperties


log = logging.getLogger(__name__)


def process_new_message(
    ch: "BlockingChannel",
    method: "Basic.Deliver",
    properties: "BasicProperties",
    body: bytes,
):
    log.info("ch: %s", )
    log.info("method %s")
    log.info()
    log.info()


def consume_messages(channel: "BlockingChannel") -> None:
    channel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,

    )


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
