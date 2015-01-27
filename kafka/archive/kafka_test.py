__author__ = 'aouyang1'

import threading, logging, time

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

class Producer1(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        producer = SimpleProducer(client)

        msgID = 0
        while True:
            producer.send_messages('94303',  "msgID {}: Let's meet up at 2PM at Greer Park!".format(msgID))
            msgID += 2
            time.sleep(1)

class Producer2(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        producer = SimpleProducer(client)

        msgID = 1
        while True:
            producer.send_messages('94301', "msgID {}: Taking a walk at 11AM!".format(msgID))
            msgID += 2
            time.sleep(1)


class Consumer1(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = SimpleConsumer(client, "puppy_group", "94303")
        for message in consumer:
            print(message)


class Consumer2(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = SimpleConsumer(client, "puppy_group", "94301")
        for message in consumer:
            print(message)

class Consumer3(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = SimpleConsumer(client, "puppy_group", "94301")
        for message in consumer:
            print(message)



def main():
    threads = [Producer1(), Producer2(), Consumer1(), Consumer2(), Consumer3()]


    for t in threads:
        t.start()

    time.sleep(5)


if __name__ == "__main__":
    """
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
    )
    """
    main()
