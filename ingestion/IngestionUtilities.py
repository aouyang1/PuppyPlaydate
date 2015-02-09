__author__ = 'aouyang1'

import json
import random
import datetime 
from calendar import monthrange


DOWNSAMPLE = 1000   # amount of downsampling needed on the Civilian Labor Force
                    # field to minimize parse county list length, but maintain
                    # the frequency distribution


def parse_county_list(filename):
    """ Parses the county list from http://www.bls.gov/lau/laucntycur14.txt
        for the county name and state abbreviation. The Civilian Labor Force
        field is also used to increase the likelihood of a county with a higher
        labor force to be chosen.

    Civilian Labor Force helps to keep the map distribution non-uniform due to
    random sampling. Since District of Columbia does not have an associated
    state, "DC" was associated with the county name.

    Args:
        filename: string representing the path of the county list to be parsed

    Returns:
        A list of lists where each nested list is a county name and state
        abbreviation
        example:

        [["San Mateo County", "CA"], ["Dallas County", "TX"], ...]
    """
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
    """ Uniformly selects a random county from the parsed county list

    Args:
        county_state_list: List of lists where each nested list is of size 2
            with the county name in the 0th index and state abbreviation in
            the 1st index

    Returns:
        The county name and state name randomly chosen from the
        county_state_list
    """
    county_state = random.choice(county_state_list)
    county = county_state[0]
    state = county_state[1]

    return county, state


def gen_random_date_between(start_date, end_date):
    """Generates a random date between a specified start and end date. Dates
        are uniformly distributed across the time range.

    inputs must be in seconds from epoch

    Args:
        start_date: integer representing the start date in seconds from epoch
        end_date: integer representing the end date in seconds from epoch

    Returns:
        A list representing the randomly generated date
        example:

        [2015, 2, 9, 12, 13, 59]
    """
    random_timestamp = random.random()*(end_date - start_date) + start_date
    random_timestamp_tup = datetime.datetime.fromtimestamp(random_timestamp).\
                           timetuple()
    random_timestamp_arr = list(random_timestamp_tup[0:6])
    return random_timestamp_arr


def gen_modelled_date():
    """Generates a random date between 2012 and 2015 using the designated
        model as follows. Each year after 2012 will have a higher likelihood
        of being selected. Two peak months in April and October with October
        being the annual peak. Weekly patterns with peak activity on Saturdays.

    No input arguments since model is fixed. May change in the future for more
    dynamic models

    Args:
        None

    Returns:
        A list representing the randomly generated date
        example:

        [2015, 2, 9, 12, 13, 59]
    """
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
    """Takes various message fields in and converts them to JSON format.

    All input arguments must be present. Has not been designed to handle
    missing fields

    Args:
        county: string representing the county name for the message
            ("San Mateo County"
        state: string represent the abbreviation of the state ("CA")
        rank: an integer representing the order of the message in the message
            thread where 0 is the oldest message
        timestamp: a list of integers representing the time the message was
            sent in the format [year month day hour minute second]
        creator_id: integer representing the creator of this message thread
        message_id: integer representing the message ID for this message thread
        sender_id: integer representing the user ID of the sender of this
            message
        message: string representing the message to be sent

    Returns:
        A json formatted string
        example:

        {'county': "San Mateo County",
         'state': "CA",
         'rank': 0,
         'timestamp': [2015, 2, 8, 12, 13, 49],
         'creator_id': 103845
         'message_id': 1947462
         'sender_id': 462847
         'message': "Hey! Let's meet up at Greer Park at 2PM today!"}
    """
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
    """Takes various user fields in and converts them to JSON format.

    All input arguments must be present. Has not been designed to handle
    missing fields

    Args:
        name: string representing the pet name
        county: string representing the county name for the message
        state: string representing the abbreviation of the state
        userID: integer representing the user ID of the pet

    Returns:
        A json formatted string
        example:

        {'name': "kottbulle",
         'county': "San Mateo County",
         'state': "CA",
         'userID': 103845}
    """
    message_info = {"name": name,
                    "county": county,
                    "state": state,
                    "userID": user_id}

    return json.dumps(message_info)


