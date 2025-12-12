import time
from typing import TYPE_CHECKING
import logging
from config import (get_connection, 
                    configure_logging)


log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties

def process_new_message(ch: 'BlockingChannel', 
                        method: 'Basic', 
                        property: 'BasicProperties', 
                        body: bytes):
    log.info('ch %s', ch)
    log.info('method %s', method)
    log.info('property %s', property)
    log.info('body %s', body)

def consume_message(channel: 'BlockingChannel'):
    channel.basic_consume(
        queue='news',
        on_message_callback=process_new_message,
        auto_ack=True,
    )
    log.info('waitinf for message')
    channel.start_consuming()




def main():
    configure_logging(level=logging.DEBUG)
    with get_connection() as conn:
        log.info('Create connection: %s', conn)
        with conn.channel() as channel:
            log.info('Create connection: %s', channel)
            consume_message(channel)



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning('Bye!')