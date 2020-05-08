from confluent_kafka import Consumer, OFFSET_BEGINNING
import time

import logging
import logging.config

logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

class KafkaConsumer:

    def __init__(
        self,
        topic_name,
        group_id,
        offset_earliest=False,
        consume_timeout=1.0,
        sleep_secs=0.5
    ):
        self.topic_name = topic_name
        self.offset_earliest = offset_earliest
        self.consume_timeout = consume_timeout
        self.sleep_secs = sleep_secs

        # create consumer with assigned properties
        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://localhost:9092',
            'group.id': group_id
        }
        if offset_earliest:
            self.broker_properties['auto.offset.reset'] = 'earliest'

        logger.info(f'create consumer with conf {self.broker_properties}')
        self.consumer = Consumer(self.broker_properties)
        self.consumer.subscribe([self.topic_name], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    def consume(self):
        try:
            message = self.consumer.poll(self.consume_timeout)
        except Exception as e:
            logger.exception(f'exception from consumer {e}')

        if message is None:
            logger.debug('no message received by consumer')
        elif message.error():
            logger.debug(f'error from consumer {message.error()}')
        else:
            logger.info(f'consumed message {message.key()}: {message.value()}')

    def close(self):
        logger.info('closing consumer')
        self.consumer.close()

    def run(self):
        try:
            while True:
                self.consume()
                time.sleep(self.sleep_secs)
        except KeyboardInterrupt as e:
            logger.info('shutting down consumer')
            self.close()

def main():
    consumer = KafkaConsumer('police.department.calls', 'test-consumer', True)
    consumer.run()

if __name__ == '__main__':
    main()
