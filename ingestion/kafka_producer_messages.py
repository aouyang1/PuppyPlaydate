__author__ = 'aouyang1'

import time
import numpy as np
import IngestionUtilities as IngUt

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from faker import Factory

fake = Factory.create()
NUM_USERS = 1000000

class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)
        self.county_and_state_list = IngUt.parse_county_list('county_list.txt')


    def sim_msg_stream(self):

        msg_cnt = 0

        while True:
            county, state = IngUt.select_random_county(self.county_state_list)

            timestamp = list(time.localtime()[0:6])

            message_info = IngUt.create_json_message(county=county,
                                                     state=state,
                                                     rank=0,
                                                     timestamp=timestamp,
                                                     creatorID=np.random.randint(NUM_USERS),
                                                     senderID=np.random.randint(NUM_USERS),
                                                     messageID=msg_cnt,
                                                     message=fake.text())

            self.producer.send_messages('messages', message_info)

            msg_cnt += 1

            
if __name__ == "__main__":
    prod = Producer("localhost:9092")
    prod.sim_msg_stream()
