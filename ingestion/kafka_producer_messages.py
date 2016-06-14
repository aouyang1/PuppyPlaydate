__author__ = 'aouyang1'

import os
import json
import time
import random
import IngestionUtilities as IngUt
import sys

from kafka import KafkaProducer
from faker import Factory

fake = Factory.create()
NUM_USERS = 1000000


class Producer(object):
    """Kafka producer class with functions to send messages.

    Messages are sent to a single kafka topic "messages" as a json formatted
    string

    Attributes:
        client: string representing IP:port of the kafka broker
        producer: Producer object using the previously specified kafka client
        county_state_list: a list of lists containing the counties and their
            associated state abbreviation
    """
    def __init__(self, addr):
        """Initialize Producer with address of the kafka broker ip address."""
        self.producer = KafkaProducer(bootstrap_servers=["52.36.254.68:9092","52.25.10.25:9092","52.35.40.35:9092","52.39.200.249:9092"], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.county_state_list = IngUt.parse_county_list('ingestion/county_list.txt')

    def sim_msg_stream(self):
        """Sends a stream of messages to the Kafka topic "messages".

        Args:
            sleep_time: float number in seconds representing the rate messages
                should be sent to the Kafka topic

        Returns:
            None
        """
        msg_cnt = 0

        while True:
            county, state = IngUt.select_random_county(self.county_state_list)

            timestamp = list(time.localtime()[0:6])
	    message_info = {"county": county,
			    "state": state,
			    "rank": 0,
			    "timestamp": timestamp,
			    "creatorID": random.randint(0, NUM_USERS-1),
			    "sender_id": random.randint(0, NUM_USERS-1),
			    "message": fake.text()}


            self.producer.send('messages', message_info)
            if msg_cnt % 10000 == 0:
                print timestamp

            msg_cnt += 1

            
if __name__ == "__main__":
    kafka_url = "{}:9092".format(os.environ["KAFKA_BROKER_URL"]) 
    print(kafka_url)
    prod = Producer(kafka_url)
    args = sys.argv
    print args[1]
    if len(args) == 1:
        sleep_time = 0.0
    elif args[1] == "+":
        sleep_time = 0.0
    else:
        sleep_time = float(args[1])

    prod.sim_msg_stream()
