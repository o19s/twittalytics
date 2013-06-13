from django.utils.encoding import smart_str
import json

import pycassa

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from credentials import *

# === OAuth Authentication ===

def twitterAuth():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


# === Connect to Cassandra database ===
# TODO: Set up Twittalytics Keyspace

# Defaults to connecting to the server at 'localhost:9160'

# === Start streaming ===
class TweetListener(StreamListener):
    def on_data(self, data):
        print(data)
        insertToCass(cf, data)
        return True

    def on_error(self, status):
        print status


def insertToCass(cf, data):
    if 'delete' not in data:    # Check if is new tweet
        # Insert all tweet data into Cassandra
        data = json.loads(data)
        key = data['id_str']
        for k, v in data.iteritems():
            if v is not None:
                cf.insert(key, {smart_str(k): smart_str(v)})    # using Django lib to decode Unicode


def get_followers_param():
    f = open('user_ids.txt', 'r')
    data = f.readline().split(', ')
    return data


if __name__ == '__main__':

    conn = pycassa.ConnectionPool('Twittalytics')  
    cf = pycassa.ColumnFamily(conn, 'Tweets')   # get column family 'Tweets'

    l = TweetListener()
    auth = twitterAuth()

    stream = Stream(auth, l)

    #   Stream filtered to ~5000 people
    stream.filter(follow=get_followers_param())

    #   1% of fire-hose
    stream.sample()

    # stream.disconnect()