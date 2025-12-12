import time
from typing import TYPE_CHECKING
import logging
from config import (get_connection, 
                    configure_logging)


log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

def produce_message(channel: 'BlockingChannel'):
    queue = channel.queue_declare(queue='news')
    message_body = f'Hello world! {time.time()}'
    log.debug('send message %s', message_body)
    channel.basic_publish(
        exchange='',
        routing_key='news',
        body=message_body,
        )
    log.debug('published message %s', message_body)




def main():
    configure_logging(level=logging.DEBUG)
    with get_connection() as conn:
        log.info('Create connection: %s', conn)
        with conn.channel() as channel:
            log.info('Create connection: %s', channel)
            produce_message(channel)



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning('Bye!')