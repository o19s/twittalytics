__author__ = 'krystal'

import tweetielytics
import tweepy
from tweepy import Cursor, User


def getFollowers():

    auth = tweetielytics.twitterAuth()

    api = tweepy.API(auth)

    # user = api.get_user('dep4b')

    cursor = Cursor(api.followers_ids, id='dep4b')

    f = open('followers.txt', 'a')

    for follower in cursor.items():
        f.write(str(follower) + '\n')

    f.close()


def getFollowersLv2(num):
    auth = tweetielytics.twitterAuth()

    api = tweepy.API(auth)

    f = open('followers.txt', 'r')
    g = open('followers3.txt', 'a')

    lines = f.readlines()

    counter = num   # 30

    while counter:
    # try:
        cursor = Cursor(api.followers, id=lines[counter])
        print str(counter) + ': ' + str(lines[counter])
        for c in cursor.items():    # iterate through followers
            g.write(c.id_str + '|' + str(c.created_at) + '|' + str(c.favourites_count) + '|' +
                    str(c.followers_count) + '|' + str(c.friends_count) + '|' + str(c.screen_name) + '\n')
        counter += 1
    # except BaseException:
    #     print "Stopped at " + str(counter)

    g.close()
    f.close()


def extract_ids():
    f = open('followers3.txt', 'r')
    g = open('user_ids.txt', 'a')

    d = set()

    for line in f:
        l = line.split('|')
        d.add(l[0])

    f.close()

    f = open('followers.txt', 'r')

    for line in f:
        l = line.split('\n')
        d.add(l[0])

    for i in d:
        g.write(i + ',')

    f.close()
    g.close()

    print len(d)