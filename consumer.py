from typing import TYPE_CHECKING
import logging
import time
from config import (
    configure_logging,
    get_connection,
    MQ_ROUTING_KEY,
)

from rabbit import RabbitBase

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

    log.info("[ ] Start processing message %r", body)
    start_time = time.time()

    number = int(body[-2:])
    is_odd = number % 2
    ...
    time.sleep(1 + is_odd * 2)
    ...
    end_time = time.time()
    log.info("Finished processing message %r, sending ack!", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.warning(
        "[X]Finished in %.f2s processing message %r",
        end_time - start_time,
        body,
    )


def consume_messages(channel: "BlockingChannel") -> None:
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(MQ_ROUTING_KEY)
    channel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,
        # auto_ack=True,
    )
    log.warning("Waiting for messages...")
    channel.start_consuming()


def main():
    configure_logging(level=logging.WARNING)
    with RabbitBase() as rabbit:
        consume_messages(channel=rabbit.channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
