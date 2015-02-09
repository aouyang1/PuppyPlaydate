__author__ = 'aouyang1'

import IngestionUtilities as IngUt
from faker import Factory

fake = Factory.create()
NUM_MESSAGES = 375000
NUM_USERS = 1000000
NUM_REPLICATIONS = 1000
hadoop_remote_path = "/user/PuppyPlaydate/history/"

county_state_list = IngUt.parse_county_list("county_list.txt")

IngUt.gen_random_messages(county_state_list, 0, 0, reps=NUM_REPLICATIONS,
                          num_messages=NUM_MESSAGES, num_users=NUM_USERS,
                          date_model=IngUt.gen_modelled_date)
