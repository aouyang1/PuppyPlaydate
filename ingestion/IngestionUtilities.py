__author__ = 'aouyang1'

import json
import numpy as np
import random
from datetime import datetime

def parse_county_list(filename):
    cnt = 0
    pop_cnt = 0
    county_state_list = []
    with open(filename) as f:
        for line in f:
            if 6 <= cnt <= 45085:
		segmented_line = line.split('|')
                county_state = segmented_line[3]
		population = max(int(segmented_line[5].strip().replace(',',''))/1000,1)		

                parsed_county_state = [county_state_row.strip() for county_state_row in county_state.split(',')]
                if len(parsed_county_state) == 1:
                    parsed_county_state = parsed_county_state.append("DC")

                if parsed_county_state:
                    county_state_list += [parsed_county_state]*population

            cnt += 1
    
    return county_state_list


def select_random_county(county_state_list):
    county_random_index = np.random.randint(len(county_state_list))
    county = county_state_list[county_random_index][0]
    state = county_state_list[county_random_index][1]

    return county, state


def gen_random_date_between(start_date, end_date):
    random_timestamp = random.random()*(end_date - start_date) + start_date
    random_timestamp_tup = datetime.fromtimestamp(random_timestamp).timetuple()
    random_timestamp_arr = list(random_timestamp_tup[0:6])
    return random_timestamp_arr


def create_json_message(county, state, rank, timestamp, creator_id, message_id, sender_id, message):
    message_info = {"county": county,
                    "state": state,
                    "rank": rank,
                    "timestamp": timestamp,
                    "creatorID": creator_id,
                    "messageID": message_id,
                    "senderID": sender_id,
                    "message": message}

    return json.dumps(message_info)


def create_json_user(name, county, state, user_id):
    message_info = {"name": name,
                    "county": county,
                    "state": state,
                    "userID": user_id}

    return json.dumps(message_info)

