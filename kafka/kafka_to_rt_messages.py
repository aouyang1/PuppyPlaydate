__author__ = 'aouyang1'

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer


class Consumer(object):

    def __init__(self, addr, group, topic):
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic)


    def sim_msg_consume(self):
        print self.consumer.__repr__()
        self.consumer.seek(0, 2)
        for message in self.consumer:
            print(message.offset, message.message.value)


cons = Consumer(addr="localhost:9092", group="rt", topic="messages")
cons.sim_msg_consume()