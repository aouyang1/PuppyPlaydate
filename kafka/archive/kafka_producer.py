__author__ = 'aouyang1'

import time
import json
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer


class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)

    def sim_msg_stream(self):

        while True:
            dt = datetime.utcnow()
            message_info = {"county": "dallas",
                            "rank": 0,
                            "timestamp": [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second],
                            "creatorID": 0,
                            "messageID": 0,
                            "senderID": 0,
                            "message": "This is a message"}

            json_message = json.dumps(message_info)

            self.producer.send_messages('messages', json_message )

            time.sleep(1)

    def sim_new_users(self):

        while True:
            user_info = {"name": "Kottbulle",
                         "id": 0,
                         "county": "dallas"}

            json_user = json.dumps(user_info)

            self.producer.send_messages('new_users', json_user)
            time.sleep(5)


prod1 = Producer("localhost:9092")
prod1.sim_msg_stream()