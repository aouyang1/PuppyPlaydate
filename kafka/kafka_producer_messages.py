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
        self.cs_list = self.parse_county_list()
        self.num_counties = len(self.cs_list)
        self.num_users = 1000000

    
    def parse_county_list(self):
        cnt = 0
        cs_list = []
        with open('county_list.txt') as f:
            for line in f:
                if 6 <= cnt <= 45085:
                    split_line = line.split('|')

                    county_state = split_line[3]

                    cs_list.append([ls.strip() for ls in county_state.split(',')])

                cnt += 1

        return cs_list


    def sim_msg_stream(self):

        msg_id = 0

        while True:
            
            message_info = {"county": self.cs_list[np.random.randint(self.num_counties)],
                            "rank": 0,
                            "timestamp": list(time.localtime()[0:6]),
                            "creatorID": np.random.randint(self.num_users),
                            "messageID": msg_id,
                            "senderID": np.random.randint(self.num_users),
                            "message": "".join([random.choice(string.letters) for i in xrange(15)])}

            json_message = json.dumps(message_info)

            self.producer.send_messages('messages', json_message)

            msg_id += 1

            

prod = Producer("localhost:9092")
prod.sim_msg_stream()
