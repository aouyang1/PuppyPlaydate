__author__ = 'aouyang1'

import time
import json
import numpy as np
from datetime import datetime

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer


class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)

    def sim_msg_stream(self):

        counties = ['santa cruz',
                    'san francisco',
                    'san mateo',
                    'santa clara']

        num_counties = len(counties)
        num_users = 10

        msg_id = 0

        while True:
            dt = datetime.utcnow()
            message_info = {"county": counties[np.random.randint(num_counties)],
                            "rank": 0,
                            "timestamp": [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second],
                            "creatorID": np.random.randint(num_users),
                            "messageID": msg_id,
                            "senderID": np.random.randint(num_users),
                            "message": "This is a message"}

            json_message = json.dumps(message_info)

            self.producer.send_messages('messages', json_message)

            msg_id += 1

            time.sleep(1)

prod = Producer("localhost:9092")
prod.sim_msg_stream()
