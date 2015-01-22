__author__ = 'aouyang1'

import time
import json
import numpy as np
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer


class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)

    def sim_new_users(self):

        counties = ['santa cruz',
                    'san francisco',
                    'san mateo',
                    'santa clara']

        user_names = ['Kottbulle',
                      'Nalle',
                      'Buddy',
                      'Winny',
                      'Poo',
                      'Xinny',
                      'Ronny',
                      'Pig',
                      'Nimbus',
                      'Bronson']

        num_counties = len(counties)
        num_users = len(user_names)

        for user_id in range(num_users):
            user_info = {"name": user_names[np.random.randint(num_users)],
                         "id": user_id,
                         "county": counties[np.random.randint(num_counties)]}

            json_user = json.dumps(user_info)

            self.producer.send_messages('new_users', json_user)

            time.sleep(5)


prod = Producer("localhost:9092")
prod.sim_new_users()
