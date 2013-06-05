__author__ = 'krystal'

# import pycassa
import json
# from tweepy import Status, API
from django.utils.encoding import smart_str


# TWEETFIELDS = ['contributors', 'coordinates', 'created_at', 'entities', 'favorite_count', 'favorited',
#                'filter_level', 'in_reply_to_screen_name', 'in_reply_to_status_id_str', 'in_reply_to_user_id_str',
#                'place', 'source', 'text', 'truncated', 'user']


def insertToCass(cf, data):
    if 'delete' not in data:    # Check if is new tweet
        # Insert all tweet data into Cassandra
        data = json.loads(data)
        key = data['id_str']
        for k, v in data.iteritems():
            if v is not None:
                cf.insert(key, {smart_str(k): smart_str(v)})    # using Django lib to decode Unicode


        # Insert specific tweet data into Cassandra

        # data = json.loads(data)
        # data = Status.parse(API(), data)
        # key = data['id_str']
        # for k in TWEETFIELDS:
        #     v = getattr(data, k)
        #     if v:
        #         print v
        #         cf.insert(key, {k: smart_str(v)})


# cf.insert('foo', {'column1': 'val1'})