__author__ = 'aouyang1'

import time
import json
import numpy as np
from datetime import datetime
import string
import random

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer


class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)
        self.county_and_state_list = self.parse_county_list('county_list.txt')
        self.num_counties = len(self.county_and_state_list)
        self.num_users = 1000000

    def parse_county_list(self, filename):
        cnt = 0
        county_state_list = []
        with open(filename) as f:
            for line in f:
                if 6 <= cnt <= 45085:
                    county_state = line.split('|')[3]

                    parsed_county_state = [county_state_row.strip() for county_state_row in county_state.split(',')]
                    if len(parsed_county_state) == 1:
                        parsed_county_state = parsed_county_state.append("DC")

                    if parsed_county_state:
                        county_state_list.append(parsed_county_state)

                cnt += 1

        return county_state_list


    def sim_msg_stream(self):

        msg_id = 0

        while True:
            county_random_index = np.random.randint(self.num_counties)
            message_info = {"county": self.county_and_state_list[county_random_index][0],
 			                "state": self.county_and_state_list[county_random_index][1],
                            "rank": 0,
                            "timestamp": list(time.localtime()[0:6]),
                            "creatorID": np.random.randint(self.num_users),
                            "messageID": msg_id,
                            "senderID": np.random.randint(self.num_users),
                            "message": "".join([random.choice(string.letters) for i in xrange(15)])}

            json_message = json.dumps(message_info)

            self.producer.send_messages('messages', json_message)

            msg_id += 1

            
if __name__ == "__main__":
    prod = Producer("localhost:9092")
    prod.sim_msg_stream()
