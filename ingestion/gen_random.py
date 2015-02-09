__author__ = 'aouyang1'

import time
from datetime import datetime
import IngestionUtilities as IngUt
from faker import Factory

fake = Factory.create()
NUM_MESSAGES = 375000
NUM_USERS = 1000000
NUM_REPLICATIONS = 75
hadoop_remote_path = "/user/PuppyPlaydate/history/"

county_state_list = IngUt.parse_county_list("county_list.txt")

start_dtt = datetime(2013, 1, 1, 0, 0, 0).timetuple()
end_dtt = datetime(2015, 2, 4, 0, 0, 0).timetuple()
start_ts = time.mktime(start_dtt)
end_ts = time.mktime(end_dtt)

IngUt.gen_random_messages(county_state_list, start_ts, end_ts, reps=NUM_REPLICATIONS,
                          num_messages=NUM_MESSAGES, num_users=NUM_USERS,
                          date_model=IngUt.gen_random_date)

