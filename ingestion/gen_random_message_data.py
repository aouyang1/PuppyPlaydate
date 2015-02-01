__author__ = 'aouyang1'

import time
import numpy as np
import os
from datetime import datetime
import IngestionUtilities as IngUt
from faker import Factory

fake = Factory.create()
NUM_MESSAGES = 10000000
NUM_USERS = 1000000
hadoop_remote_path = "/user/PuppyPlaydate/history/"

county_state_list = IngUt.parse_county_list("county_list.txt")

start_dtt = datetime(2000, 1, 1, 0, 0, 0).timetuple()
end_dtt = datetime(2015, 1, 1, 0, 0, 0).timetuple()
start_ts = time.mktime(start_dtt)
end_ts = time.mktime(end_dtt)



msg_data = []
local_filename = "random_messages.txt"
file_writer = open(local_filename, 'w')

for msg_cnt in range(NUM_MESSAGES):

    # print progress to console
    if msg_cnt % 100000 == 0:
        print "{}".format(msg_cnt/float(NUM_MESSAGES))

    county, state = IngUt.select_random_county(county_state_list)

    random_timestamp_arr = IngUt.gen_random_date_between(start_date=start_ts, end_date=end_ts)

    message_info = IngUt.create_json_message(county=county,
                                             state=state,
                                             rank=0,
                                             timestamp=random_timestamp_arr,
                                             creatorID=np.random.randint(NUM_USERS),
                                             senderID=np.random.randint(NUM_USERS),
                                             messageID=msg_cnt,
                                             message=fake.text())

    file_writer.write(message_info + "\n")

file_writer.close()
os.system("sudo -u hdfs hdfs dfs -put {local} {remote}{local}"
          .format(local=local_filename, remote=hadoop_remote_path))




