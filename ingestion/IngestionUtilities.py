__author__ = 'aouyang1'

import json
import random
import datetime 
from calendar import monthrange


DOWNSAMPLE = 1000


def parse_county_list(filename):
    line_count = 0
    county_state_list = []

    with open(filename) as f:
        for line in f:
	    
            if 6 <= line_count <= 45085:
                segmented_line = line.split('|')

                # extract county state information "Dallas County, TX"
                county = segmented_line[3]

                county_state = [row.strip() for row in county.split(',')]

                # checks for District of Columbia with no state
                if not county_state or len(county_state) == 1:
                    county_state = [county.strip(), "DC"]

                # extract labor force field
                population = int(segmented_line[5].strip().replace(',', ''))
                population_downsized = max(population/DOWNSAMPLE, 1)

                county_state_list += [county_state]*population_downsized

            line_count += 1
    
    return county_state_list


def select_random_county(county_state_list):
    county_state = random.choice(county_state_list)
    county = county_state[0]
    try:
        state = county_state[1]
    except:
        print county_state

    return county, state


def gen_random_date_between(start_date, end_date):
    random_timestamp = random.random()*(end_date - start_date) + start_date
    random_timestamp_tup = datetime.datetime.fromtimestamp(random_timestamp).\
                           timetuple()
    random_timestamp_arr = list(random_timestamp_tup[0:6])
    return random_timestamp_arr


def gen_modelled_date():

              # 2012, 2013, 2014
    year_model = [1,    2,    4]
    year_model = reduce(lambda x, y: x+y, [[year]*freq for year, freq in
                                           zip(range(2012, 2015), year_model)])
    rand_year = random.choice(year_model)


                 # J  F  M  A  M  J  J  A  S  O   N   D
    month_model = [1, 4, 8, 9, 7, 5, 4, 6, 8, 12, 10, 6]
    month_model = reduce(lambda x, y: x+y, [[month]*freq for month, freq in
                                            zip(range(1, 13), month_model)])
    rand_month = random.choice(month_model)

    week_dict = {0: [], 1: [], 2: [], 3: [], 4: [], 5: [], 6: []} 	
    num_days_in_month = monthrange(rand_year, rand_month)[1]

    for day in range(1, num_days_in_month+1):
        week_dict[datetime.date(rand_year, rand_month, day).weekday()] += [day]   
 

                # M  T  W  R  F  S  S
    week_model = [2, 1, 1, 2, 4, 8, 3]
    week_model = reduce(lambda x, y: x+y, [[week]*freq for week, freq in
                                           zip(range(7), week_model)])
    rand_day = random.choice(week_dict[random.choice(week_model)])

    # 0  1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17 18  19  20
    # 21  22  23
    hour_model = [1, 1, 1, 1, 1, 1, 2, 9, 7, 5, 2,  1,  1,  2,  2,  3,  4,  14,
                  10, 8,  6,  3,  1,  1]
    hour_model = reduce(lambda x, y: x+y, [[hour]*freq for hour, freq in
                                           zip(range(24), hour_model)])
    rand_hour = random.choice(hour_model)
 
    rand_minute = random.choice(range(60))

    rand_second = random.choice(range(60))
    
    random_timestamp_arr = [rand_year, rand_month, rand_day, rand_hour,
                            rand_minute, rand_second]
    return random_timestamp_arr


def create_json_message(county, state, rank, timestamp, creator_id, message_id,
                        sender_id, message):
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

