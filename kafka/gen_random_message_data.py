__author__ = 'aouyang1'

import random
import time
import datetime
import numpy as np
import string
import json


def parse_county_list():
    # make the file name (here county_list.txt) as input to this function
    # so it is reusable in the future?
    cnt = 0
    # spell out the full name "counter" for easier time reading it
    cs_list = []
    # use full words for cs (counties?) for more readability
    with open('county_list.txt') as f:
        for line in f:
            if 6 <= cnt <= 45085:
                # this is kinda ghetto, is there a better way to trigger this?
                split_line = line.split('|')

                county_state = split_line[3]
                # put the above two lines into one for better readability

                cs_list.append([ls.strip() for ls in county_state.split(',')])
                # whats ls?

            cnt += 1

    return cs_list


num_messages = 10000000
# use all caps for constants?

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
    # use .format()
    for i in range(num_messages):
        rand_ts = random.random()*(end_ts - start_ts) + start_ts

        msg = {"county": cs_list[np.random.randint(num_counties)][0],
               "rank": 0,
               # line length should be less than 80 characters (you have 92)
               "timestamp": list(datetime.datetime.fromtimestamp(rand_ts).timetuple()[0:6]),
               "creatorID": np.random.randint(num_users),
               "messageID": i,
               "senderID": np.random.randint(num_users),
               "message": "".join([random.choice(string.letters) for i in xrange(15)])}
        # why u using python2 :P?
        # line length should be less than 80 characters (you have 87)
        # i think you should make msg into a separate function it would be more
        # clear.

        f.write(json.dumps(msg) + "\n")
        # use .format() brehhh
        if i % 100000 == 0:
            print i/float(num_messages)

    f.close()
    print "DONE!"

# after more modularization wrap the main executable def in
# __name__ = "__main__" so the functions you write can get exported to be used
# in other codes or for testing
