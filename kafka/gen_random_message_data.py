__author__ = 'aouyang1'

import random
import time
import datetime
import numpy as np
import string
import json


def parse_county_list():
    cnt = 0
    cs_list = []
    with open('county_list.txt') as f:
        for line in f:
            if 6 <= cnt <= 45085:
                split_line = line.split('|')

                county_state = split_line[3]

                cs_list.append([ls.strip() for ls in county_state.split(',')])

            cnt += 1

    return cs_list


num_messages = 10000000

cs_list = parse_county_list()

num_counties = len(cs_list)
num_users = 1000000

start_dtt = datetime.datetime(2000, 1, 1, 0, 0, 0).timetuple()
end_dtt = datetime.datetime(2015, 1, 1, 0, 0, 0).timetuple()

start_ts = time.mktime(start_dtt)
end_ts = time.mktime(end_dtt)

for rep in range(25):
    msg_data = []
    f = open('random_messages_' + str(rep) + '.txt', 'w')
    for i in range(num_messages):
        rand_ts = random.random()*(end_ts - start_ts) + start_ts

        msg = {"county": cs_list[np.random.randint(num_counties)][0],
               "rank": 0,
               "timestamp": list(datetime.datetime.fromtimestamp(rand_ts).timetuple()[0:6]),
               "creatorID": np.random.randint(num_users),
               "messageID": i,
               "senderID": np.random.randint(num_users),
               "message": "".join([random.choice(string.letters) for i in xrange(15)])}

        f.write(json.dumps(msg) + "\n")
        if i % 100000 == 0:
            print i/float(num_messages)

    f.close()
    print "DONE!"
