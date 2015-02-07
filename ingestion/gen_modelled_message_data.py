__author__ = 'aouyang1'

import time
import numpy as np
import os
from datetime import datetime
import IngestionUtilities as IngUt
from faker import Factory

fake = Factory.create()
NUM_MESSAGES = 375000
NUM_USERS = 1000000
hadoop_remote_path = "/user/PuppyPlaydate/history/"

county_state_list = IngUt.parse_county_list("county_list.txt")


for rep in range(200):
    msg_data = []
    local_filename = "messages_{}.txt".format(datetime.now().strftime('%y-%m-%d_%H-%M-%S'))

    file_writer = open(local_filename, 'w')

    for msg_cnt in range(NUM_MESSAGES):

        # print progress to console
        if msg_cnt % 100000 == 0:
            print "rep: {} is {} complete".format(rep, msg_cnt/float(NUM_MESSAGES))

        county, state = IngUt.select_random_county(county_state_list)

        random_timestamp_arr = IngUt.gen_modelled_date()

        message_info = IngUt.create_json_message(county=county,
                                                 state=state,
                                                 rank=0,
                                                 timestamp=random_timestamp_arr,
                                                 creator_id=np.random.randint(NUM_USERS),
                                                 sender_id=np.random.randint(NUM_USERS),
                                                 message_id=msg_cnt,
                                                 message=fake.text())

        file_writer.write(message_info + "\n")

    file_writer.close()
    os.system("sudo -u hdfs hdfs dfs -put {local} {remote}{local}"
              .format(local=local_filename, remote=hadoop_remote_path))




