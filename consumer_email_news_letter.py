from typing import TYPE_CHECKING
import logging
import time
from config import (
    configure_logging,
)

from rabbit.common import EmailUpdatesRabbit

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
    log.debug("ch: %s", ch)
    log.debug("method %s", method)
    log.debug("properties %s", properties)
    log.debug("body %s", body)

    log.warning("[ ] Update user email for newsletters %r", body)

    start_time = time.time()
    time.sleep(1)
    end_time = time.time()
    log.info("Finished processing message %r, sending ack!", body)

    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.warning(
        "[X]Updated user email in %.f2s message %r ok",
        end_time - start_time,
        body,
    )


def main():
    configure_logging(level=logging.WARNING)
    with EmailUpdatesRabbit() as rabbit:
        rabbit.consume_messages(
            message_callback=process_new_message,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
