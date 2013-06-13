from django.utils.encoding import smart_str
import json
import sys

import pycassa

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from credentials import HOSTS, consumer_key, consumer_secret, \
                        access_token, access_token_secret

COUNT = 0


class TweetListener(StreamListener):
    """Listener class to stream in Twitter data."""
    def on_data(self, data):
        self.insertToCass(cf, data)
        return True

    def on_error(self, status):
        print status

    def insertToCass(self, cf, data):
        global COUNT
        # Check for new tweet
        if 'delete' not in data:
            # Insert all tweet data into Cassandra
            data = json.loads(data)
            key = data['id_str']
            for k, v in data.iteritems():
                if v is not None:
                    # Use Django lib to decode Unicode
                    cf.insert(key, {smart_str(k): smart_str(v)})
                    COUNT += 1
                    sys.stdout.write("\rIndexed %s rows." % COUNT)
                    sys.stdout.flush()
                    

def twitterAuth():
    """OAuth authentication."""
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


def get_followers_param():
    f = open('user_ids.txt', 'r')
    data = f.readline().split(', ')
    return data


if __name__ == '__main__':
    # Connect to host, and start streaming in data.
    # Assumes Keyspace and CF both exist.
    print("Connecting to %s" % HOSTS)
    conn = pycassa.ConnectionPool("Twittalytics", server_list=HOSTS)
    cf = pycassa.ColumnFamily(conn, 'Tweets')   # get column family 'Tweets'

    l = TweetListener()
    auth = twitterAuth()

    stream = Stream(auth, l)

    #   Stream filtered to ~5000 people
    #stream.filter(follow=get_followers_param())

    #   1% of fire-hose
    stream.sample()

    # stream.disconnect()
