import datetime
import logging
from itertools import count

import progressbar
import tweepy
from pymongo.errors import DuplicateKeyError
import json

from indiff.utils import sentiment, split_text


def auth(consumer_key, consumer_secret, access_token, access_token_secret):
    auth_ = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth_.set_access_token(access_token, access_token_secret)
    return tweepy.API(
        auth_,
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

        return self.tweet['id']

    @property
    def text(self):
        """Tweet text

        Returns:
            String -- The actual UTF-8 text of the status update
        """

        if 'full_text' in self.tweet:
            return self.tweet['full_text']

        return self.tweet['text']

    @property
    def created_at(self):
        """[summary]

        Returns:
            [type] -- [description]
        """
        date = self.tweet['created_at']
        if isinstance(date, datetime.datetime):
            return date

        try:
            return datetime.datetime.strptime(str(date), "%a %b %d %H:%M:%S %z %Y")
        except:
            pass

        try:
            return datetime.datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S")
        except:
            pass

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
        if not entities or isinstance(entities, float) or entities == 'None':
            return []

        # Entities are stored as a JSON string for event tweets
        if isinstance(entities, str):
            entities = json.loads(entities.replace('\'', '\"'))

        users_mentions = entities.get('mentions', [])
        if users_mentions and not isinstance(users_mentions, float) and not users_mentions == 'None':
            return list([mention['username'] for mention in users_mentions])

        return []

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

        return self.tweet['author_id']

    def original_owner_id(self, tweet_collection):
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
            if 'referenced_tweets' in self.tweet:
                # Wrap in try in case of weird formatting
                try:
                    for referenced in self.tweet['referenced_tweets']:
                        if referenced['type'] == 'retweeted':
                            # TODO Expand data collection to find these
                            tweet_ = tweet_collection.find_one({"id": referenced['id']})
                            if tweet_:
                                return tweet_['author_id']
                except TypeError:
                    pass

        if self.is_quoted_tweet:
            if 'referenced_tweets' in self.tweet:
                # Wrap in try in case of weird formatting
                try:
                    for referenced in self.tweet['referenced_tweets']:
                        if referenced['type'] == 'quoted':
                            # TODO Expand data collection to find thse
                            tweet_ = tweet_collection.find_one({"id": referenced['id']})
                            if tweet_:
                                return tweet_['author_id']
                except TypeError:
                    pass

        # For old tweet format
        if 'retweeted_status' in self.tweet:
            return self.tweet['retweeted_status']['user']['id_str']
        if 'quoted_status' in self.tweet:
            return self.tweet['quoted_status']['user']['id_str']

        if 'in_reply_to_user_id' in self.tweet:
            return self.tweet['in_reply_to_user_id']

        return self.owner_id

    @property
    def original_tweet_id(self):
        """ Returns the original id this tweet is in response to """

        # Wrap in try in case of weird formatting
        try:
            if 'referenced_tweets' in self.tweet:
                for referenced in self.tweet['referenced_tweets']:
                    if referenced['type'] == 'retweeted':
                        return referenced['id']
                    if referenced['type'] == 'quoted':
                        return referenced['id']
                    if referenced['type'] == 'replied_to':
                        return referenced['id']
        except TypeError:
            pass

        # For old tweet format
        if 'retweeted_status' in self.tweet:
            return self.tweet['retweeted_status']['id_str']
        if 'quoted_status' in self.tweet:
            return self.tweet['quoted_status']['id_str']

        return None

    @property
    def is_retweeted_tweet(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        if 'referenced_tweets' in self.tweet:
            # Wrap in try in case of weird formatting
            try:
                for referenced in self.tweet['referenced_tweets']:
                    if referenced['type'] == 'retweeted':
                        return True
            except TypeError:
                pass
        return False

    @property
    def is_quoted_tweet(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        if 'referenced_tweets' in self.tweet:
            # Wrap in try in case of weird formatting
            try:
                for referenced in self.tweet['referenced_tweets']:
                    if referenced['type'] == 'quoted':
                        return True
            except TypeError:
                pass
        return False

    @property
    def is_response_tweet(self):
        """ Determines whther this tweet is a response (retweet, quote, or reply) """

        if self.is_retweeted_tweet:
            return True

        if self.is_quoted_tweet:
            return True

        if 'in_reply_to_user_id' in self.tweet:
            return True

        return False

    @property
    def hashtags(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        entities = self.tweet.get('entities', {})
        if not entities or isinstance(entities, float) or entities == 'None':
            return entities

        if isinstance(entities, str):
            entities = json.loads(entities.replace('\'', '\"'))

        hashtags = entities.get('hashtags', [])

        text_tags = []
        for hashtag in hashtags:
            # Might be two different property names, for some reason
            if 'tag' in hashtag:
                text_tags.append(hashtag['tag'])
            elif 'text' in hashtag:
                text_tags.append(hashtag['text'])

        return text_tags

    @property
    def urls(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        entities = self.tweet.get('entities', {})
        if not entities or isinstance(entities, float) or entities == 'None':
            return entities

        if isinstance(entities, str):
            entities = json.loads(entities.replace('\'', '\"'))

        urls = entities.get('urls', [])

        return urls

    @property
    def media(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        attachments = self.tweet.get('attachments', {})
        if not attachments or isinstance(attachments, float) or attachments == 'None':
            return attachments

        if isinstance(attachments, str):
            attachments = json.loads(attachments.replace('\'', '\"'))

        media = attachments.get('media_keys', [])

        return media

    @property
    def retweet_count(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        metrics = self.tweet['public_metrics']

        # Metrics are stored as a JSON string for event tweets
        if isinstance(metrics, str):
            metrics = json.loads(metrics.replace('\'', '\"'))

        return metrics['retweet_count']

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

        return True if len(self.users_mentioned) > 0 else False

    @property
    def is_favourited(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        metrics = self.tweet['public_metrics']

        if metrics == 'None':
            return

        # Metrics are stored as a JSON string for event tweets
        if isinstance(metrics, str):
            metrics = json.loads(metrics.replace('\'', '\"'))

        return True if metrics['like_count'] else False

    @property
    def is_positive_sentiment(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        d = sentiment(self.text)
        return True if d == 'positive' else False

    @property
    def is_negative_sentiment(self):
        """[summary]

        Returns:
            [type] -- [description]
        """

        d = sentiment(self.text)
        return True if d == 'negative' else False


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


def get_user_tweets_in_network(api=None, users=None, collection=None,
                               n_tweets=5000):
    """Fetches users' tweets into database.

    Keyword Arguments:
        api {Tweepy} -- wrapper for the API as provided by Twitter
        (default: {None})
        users {iterable} -- a list or set of twitter usernames or ids
        (default: {None})
        collection {str} -- database collection name (default: {None})
        n_tweets{int} -- number of tweets to fetch (default: {5000})

    Raises:
        tweepy.TweepError: raised if a given username or user ID does not exist

    Returns:
        int, set -- total number of tweets retrieved, a set of error username
        or user IDs
    """
    total = len(users)
    error_ids = []

    for i, user in zip(count(start=1), users):
        logging.info(f"PROCESSING {i} OF {total} USERS")
        try:
            get_user_tweets(api=api, user=user, collection=collection,
                            n_tweets=n_tweets)

        except tweepy.TweepError as e:
            logging.error("Skipped {}, {}.\n".format(user, e))
            error_ids.append(user)

    return error_ids


def get_user_tweets(api=None, user=None, collection=None, n_tweets=5000):
    """Fetches a user tweets into database.

    Keyword Arguments:
        api {Tweepy} -- wrapper for the API as provided by Twitter
        (default: {None})
        user {str} -- twitter username or ID (default: {None})
        collection {str} -- database collection name (default: {None})
        n_tweets{int} -- number of tweets to fetch (default: {5000})

    Returns:
        int -- number of user tweets
    """

    logging.info("Fetching {}'s tweets....".format(user))

    tweet_count = 0

    bar = progressbar.ProgressBar()
    cursor = tweepy.Cursor(api.user_timeline, id=user,
                           tweet_mode='extended').items(n_tweets)
    for status in bar(cursor):
        tweet_count += 1
        tweet = status._json
        id_ = {"_id": tweet['id_str']}
        new_document = {**id_, **tweet}
        try:
            collection.insert_one(new_document)
        except DuplicateKeyError:
            logging.info(f"found duplicate key: {tweet['id_str']}")
            continue

    if not tweet_count:
        raise tweepy.TweepError('User has no tweet.')
