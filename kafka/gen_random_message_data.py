__author__ = 'aouyang1'

import random
import time
import datetime
import numpy as np
import string
import json
import os

def parse_county_list():
    cnt = 0
    county_state_list = []
    with open('county_list.txt') as f:
        for line in f:
            if 6 <= cnt <= 45085:
                county_state = line.split('|')[3]
		
		parsed_county_state = [county_state_row.strip() for county_state_row in county_state.split(',')]
		if len(parsed_county_state) == 1:
		    parsed_county_state = parsed_county_state.append("DC")
		
		if parsed_county_state:
	            county_state_list.append(parsed_county_state)

            cnt += 1

    return county_state_list


NUM_MESSAGES = 10000000
NUM_USERS = 1000000
remote_path = "/user/PuppyPlaydate/history/"

county_state_list = parse_county_list()

num_counties = len(county_state_list)

start_dtt = datetime.datetime(2000, 1, 1, 0, 0, 0).timetuple()
end_dtt = datetime.datetime(2015, 1, 1, 0, 0, 0).timetuple()

start_ts = time.mktime(start_dtt)
end_ts = time.mktime(end_dtt)

for rep in range(5):
    msg_data = []
    local_filename = "random_messages_{}.txt".format(rep)
    f = open(local_filename, 'w')
    for i in range(NUM_MESSAGES):
        rand_ts = random.random()*(end_ts - start_ts) + start_ts
 	county_random_index = np.random.randint(num_counties)
        #print county_state_list[county_random_index]
        msg = {"county": county_state_list[county_random_index][0],
               "state": county_state_list[county_random_index][1],
               "rank": 0,
               "timestamp": list(datetime.datetime.fromtimestamp(rand_ts).timetuple()[0:6]),
               "creatorID": np.random.randint(NUM_USERS),
               "messageID": i,
               "senderID": np.random.randint(NUM_USERS),
               "message": "".join([random.choice(string.letters) for i in xrange(15)])}

        f.write(json.dumps(msg) + "\n")
        if i % 100000 == 0:
            print "{}".format(i/float(num_messages))

    f.close()
    os.system("sudo -u hdfs hdfs dfs -put {local} {remote}{local}".format(local=local_filename, remote=remote_path))
    print "REPETITION: {} DONE!".format(rep)



