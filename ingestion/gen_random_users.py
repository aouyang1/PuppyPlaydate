__author__ = 'aouyang1'

import time
import numpy as np
import os
from datetime import datetime
import IngestionUtilities as IngUt
from faker import Factory

fake = Factory.create()
NUM_USERS = 1000000
hadoop_remote_path = "/user/PuppyPlaydate/users/"

county_state_list = IngUt.parse_county_list("county_list.txt")

msg_data = []
local_filename = "random_users.txt"
file_writer = open(local_filename, 'w')

for user_cnt in range(NUM_USERS):

    # print progress to console
    if user_cnt % 100000 == 0:
        print "{}".format(user_cnt/float(NUM_USERS))

    county, state = IngUt.select_random_county(county_state_list)

    message_info = IngUt.create_json_user(name=fake.name(),
                                          county=county,
                                          state=state,
                                          userID=user_cnt)

    file_writer.write(message_info + "\n")

file_writer.close()
os.system("sudo -u hdfs hdfs dfs -put {local} {remote}{local}"
          .format(local=local_filename, remote=hadoop_remote_path))




