import datetime
import logging
import os
from itertools import count
from time import sleep

import tweepy
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
                                        StaleElementReferenceException)
from sqlitedict import SqliteDict

from .exception import WebBrowserError, WebDriverError
from .utils import sentiment, split_text


def _format_date(date):
    day = '0' + str(date.day) if len(str(date.day)) == 1 else str(date.day)
    month = '0' + str(date.month)\
            if len(str(date.month)) == 1 else str(date.month)
    year = str(date.year)
    return '-'.join([year, month, day])


def _form_url(since, until, user):
    search_url = f'https://twitter.com/search?q=from%3A{user}%20%20since%3A'\
                 f'{since}%20until%3A{until}%20include%3Anativeretweets&src='\
                 'typed_query'
    return search_url


def _increment_day(date, i):
    return date + datetime.timedelta(days=i)


class Scrape(object):
    def __init__(self, user):
        self.user = user

    def tweet_ids(self, start_date=None, end_date=None, delay_time=1,
                  web_browser='chrome', path_to_driver=None):
        if start_date is None:
            start_date = datetime.datetime(2010, 1, 1)

        if end_date is None:
            end_date = datetime.datetime.now()

        if end_date < start_date:
            raise ValueError("Incorrect start and end dates.")

        if start_date == end_date:
            raise ValueError("Date must be at least an earlier date.")

        if web_browser == 'chrome':
            if path_to_driver is None:
                path_to_driver = "chromedriver"
            driver = webdriver.Chrome(executable_path=path_to_driver)
        elif web_browser == 'safari':
            if path_to_driver is None:
                path_to_driver = "/usr/bin/safaridriver"
            driver = webdriver.Safari(executable_path=path_to_driver)
        elif web_browser == 'firefox':
            if path_to_driver is None:
                path_to_driver = "geckodriver"
            driver = webdriver.Firefox(executable_path=path_to_driver)
        else:
            raise WebBrowserError("Web Browser can only be Safari, "
                                  "Firefox or Chrome")

        if not os.path.exists(path_to_driver):
            raise WebDriverError(f'{web_browser} driver not found '
                                 f'in {path_to_driver}.')

        days = (end_date - start_date).days + 1
        id_selector = '.time a.tweet-timestamp'
        tweet_selector = 'li.js-stream-item'
        user = self.user.lower()

        for _ in range(days):
            d1 = _format_date(_increment_day(start_date, 0))
            d2 = _format_date(_increment_day(start_date, 1))
            url = _form_url(d1, d2, user)
            print(url)
            print(d1)
            driver.get(url)
            sleep(delay_time)

            try:
                found_tweets = driver.find_elements_by_css_selector(tweet_selector)
                increment = 10

                while len(found_tweets) >= increment:
                    print('scrolling down to load more tweets')
                    driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                    sleep(delay_time)
                    found_tweets = driver.find_elements_by_css_selector(tweet_selector)
                    increment += 10

                print('{} tweets found'.format(len(found_tweets)))

                for tweet in found_tweets:
                    try:
                        id = tweet.find_element_by_css_selector(id_selector).get_attribute('href').split('/')[-1]
                        yield (id)
                    except StaleElementReferenceException:
                        print('lost element reference', tweet)

            except NoSuchElementException:
                print('no tweets on this day')

            start_date = _increment_day(start_date, 1)
        driver.close()

    # todo: might need a rewrite
    def write_series_to_csv(self, tweet_ids_filename, series):
        with open(tweet_ids_filename, 'a') as outfile:
            series.to_csv(outfile, index=False)


class API(object):
    def __init__(self, consumer_key, consumer_secret, access_token,
                 access_token_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret

    def __call__(self):
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        return tweepy.API(
            auth,
            wait_on_rate_limit=True,
            wait_on_rate_limit_notify=True,
            retry_count=3,
            retry_delay=5,
            retry_errors=set([401, 404, 500, 503]))


class Tweet(object):
    counter = 0

    def __init__(self, status_json):
        self.tweet = status_json
        Tweet.counter += 1

    def __repr__(self):
        return "Tweet({0.tweet!r})".format(self)

    def __str__(self):
        return "{0.tweet!s}".format(self)

    @property
    def tweet(self):
        return self._tweet

    @tweet.setter
    def tweet(self, value):
        # make sure only a dict can be assigned to the tweet object self.tweet
        if not isinstance(value, dict):
            raise TypeError(
                "Expected a dict but got {}.".format(type(value)))
        self._tweet = value

    @property
    def id(self):
        """Get the tweet's id

        Returns:
            String -- The string representation of the unique identifier
                for this Tweet
        """

        return self.tweet['id_str']

    @property
    def text(self):
        """Tweet text

        Returns:
            String -- The actual UTF-8 text of the status update
        """

        return self.tweet['text']

    @property
    def created_at(self):
        """[summary]

        Returns:
            [type] -- [description]
        """
        return datetime.datetime.strptime(
            self.tweet['created_at'], "%a %b %d %H:%M:%S %z %Y")

    @property
    def keywords(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        # todo: consider returning a generator instead?
        return split_text(self.text)

    @property
    def users_mentioned(self):
        """Fetch users mentioned in tweet.

        Returns early if one of the following occurs:
            - entities objects is not found in the tweet object.
            - there are not users mentioned in tweet.

        Returns:
            [type] -- [description]
        """

        entities = self.tweet.get('entities', {})
        if not entities:
            return entities

        users_mentions = entities.get('user_mentions', [])
        if not users_mentions:
            return users_mentions

        # makes sure the user is not included in the returned users_mentioned
        return [user['id_str']
                for user in users_mentions if user['id_str'] != self.id]

    @property
    def owner_description(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return self.tweet.get('user').get("description", "")

    @property
    def owner_id(self):
        """Returns the id to the owner of this tweet

        Returns:
            [type] -- [description]
        """

        return self.tweet['user']['id_str']

    @property
    def original_owner_id(self):
        """ this method should be called if the tweet is either a retweet or
        quoted.

        checks for possible tweet owners using two engagement styles:
                (i) retweet_status and
                (ii)quoted status.
        using retweet status, if a tweet contains RT @username,
        then its a rewteet of the original user @username

        using quoted status, if its a quoted status then we can get the
        original tweet id and get the original owner

        Arguments:
            user_id {[type]} -- [description]
            tweet {[type]} -- [description]
        """

        if self.is_retweeted_tweet:
            tweet_ = self.tweet['retweeted_status']
            return tweet_['user']['id_str']

        if self.is_quoted_tweet:
            tweet_ = self.tweet['quoted_status']
            return tweet_['user']['id_str']

        if self.tweet['in_reply_to_user_id_str'] is not None:
            return self.tweet['in_reply_to_user_id_str']

        return self.owner_id

    @property
    def is_retweeted_tweet(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return True if 'retweeted_status' in self.tweet else False

    @property
    def is_quoted_tweet(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return True if 'quoted_status' in self.tweet else False

    @property
    def hashtags(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        entities = self.tweet.get('entities', {})
        if not entities:
            return entities

        hashtags = entities.get('hashtags', [])

        return hashtags

    @property
    def urls(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        entities = self.tweet.get('entities', {})
        if not entities:
            return entities

        urls = entities.get('urls', [])

        return urls

    @property
    def media(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        entities = self.tweet.get('entities', {})
        if not entities:
            return entities

        media = entities.get('media', [])

        return media

    @property
    def retweet_count(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return self.tweet['retweet_count']

    @property
    def is_retweeted(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return True if self.retweet_count else False

    @property
    def is_others_mentioned(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return True if self.users_mentioned else False

    @property
    def is_favourited(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return True if self.tweet['favorite_count'] else False

    @property
    def is_positive_sentiment(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        d = sentiment(self.text)
        return True if d == 'positive' else False

    @property
    def owner_followers_count(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return self.tweet['user']['followers_count']

    @property
    def owner_friends_count(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        return self.tweet['user']['friends_count']


def get_user_tweets_and_build_tables(api, user_id, days, database_file_path,
                                     tablename='tweet-objects'):

    """Fetch all tweets by a user....(((( we may need to include keywords to
    fetch only tweets with related keywords"""

    today = datetime.datetime.today()
    print(">>  Processing {}'s tweets....".format(user_id))

    # there is still need for test for users who have not tweeted within the
    # specified number of days
    with SqliteDict(database_file_path, tablename=tablename) as tweets_table:
        for counter, status in zip(count(), tweepy.Cursor(api.user_timeline,
                                                          id=user_id).items()):
            # process status here
            if counter % 100 == 0 and counter != 0:
                print(f'>>  Over {counter} tweets have been retrieved so far'
                      f'..USER ID: {user_id}')

            difference = (today - status.created_at).days

            if difference >= days + 1:
                break
            else:
                tweet_id = status.id_str
                # if this doesn't wort, try its private json extension
                tweets_table[tweet_id] = status
                tweets_table.commit()

    logging.info(f'Total number of tweets retrieved from {user_id}: {counter}')
    return counter


def get_all_tweets_in_network_and_build_tables(api, user_ids, days,
                                               database_file_path,
                                               tablename='tweet-objects'):
    total = len(user_ids)
    counter = 0
    error_ids = set()
    for i, user_id in zip(count(start=1), enumerate(user_ids)):
        logging.info(f"PROCESSING {i} OF {total} USERS")
        try:
            ct = get_user_tweets_and_build_tables(
                api, user_id, days, database_file_path, tablename=tablename)

            if not ct:
                raise tweepy.TweepError('0 tweet was fetched and would add '
                                        f'{user_id} to error ids')
            counter += ct
        except tweepy.TweepError as e:
            print("XXXX Skipped {}, {}.\n".format(user_id, e))
            error_ids.add(user_id)

    return counter, error_ids
