__author__ = 'aouyang1'

import time
import random
import IngestionUtilities as IngUt
import sys

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from faker import Factory

fake = Factory.create()
NUM_USERS = 1000000


class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)
        self.county_state_list = IngUt.parse_county_list('ingestion/county_list.txt')

    def sim_msg_stream(self, sleep_time=0.25):

        msg_cnt = 0

        while True:
            print len(self.county_state_list)
            county, state = IngUt.select_random_county(self.county_state_list)

            timestamp = list(time.localtime()[0:6])

            message_info = IngUt.create_json_message(county=county,
                                                     state=state,
                                                     rank=0,
                                                     timestamp=timestamp,
                                                     creator_id=random.randint(0, NUM_USERS-1),
                                                     sender_id=random.randint(0, NUM_USERS-1),
                                                     message_id=msg_cnt,
                                                     message=fake.text())

            self.producer.send_messages('messages', message_info)
            print timestamp

            if sleep_time != 0:
                time.sleep(sleep_time)

            msg_cnt += 1

            
if __name__ == "__main__":
    prod = Producer("localhost:9092")
    args = sys.argv
    print args[1]
    if len(args) == 1:
        sleep_time = 0.0
    elif args[1] == "+":
        sleep_time = 0.0
    else:
        sleep_time = float(args[1])

    prod.sim_msg_stream(sleep_time)
