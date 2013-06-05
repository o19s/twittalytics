from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import cass
import pycassa

# === OAuth Authentication ===

consumer_key = "czUFT245RTJtUr994Pwg0g"
consumer_secret = "QJ4zZw9EPs77p7nylbqobif1GzKrECYwlNOadHScHY"

access_token = "98714423-TbMwpOiVQ6RG1FSxs6jthklm8evLiuCs3u8gAca0"
access_token_secret = "zg2KPWYqkyEBCbSpxrrUVmr9aAdpguRn3eUArJeupA"


def twitterAuth():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


# === Connect to Cassandra database ===

conn = pycassa.ConnectionPool('Tweetielytics')  # Defaults to connecting to the server at 'localhost:9160'
cf = pycassa.ColumnFamily(conn, 'Tweet')   # get column family 'Tweets'


# === Start streaming ===

class TweetListener(StreamListener):
    def on_data(self, data):
        cass.insertToCass(cf, data)
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    l = TweetListener()
    auth = twitterAuth()

    stream = Stream(auth, l)
    # stream.filter(track=['basketball'])

    stream.sample()




    # stream.disconnect()

    # api = tweepy.API(auth)

    #     print api.me().name


    # If the application settings are set for "Read and Write" then
    # this line should tweet out the message to your account's
    # timeline. The "Read and Write" setting is on https://dev.twitter.com/apps
    # api.update_status('Updating using OAuth authentication via Tweepy!')