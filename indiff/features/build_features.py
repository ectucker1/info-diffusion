from collections import ChainMap, Counter

import numpy as np
import pandas as pd
import progressbar

from indiff.twitter import Tweet


class Features(object):
    def __init__(self, src_user=None, dest_user=None, keywords=None,
                 node_collection=None, tweet_collection=None, retweets_collection=None, event_tweets_collection=None,
                 users_collection=None, user=None, replies_collection=None):
        self.src_user = src_user
        self.dest_user = dest_user
        self.keywords = keywords
        self.node_collection = node_collection
        self.tweet_collection = tweet_collection
        self.retweets_collection=retweets_collection
        self.replies_collection=replies_collection
        self.event_tweets_collection=event_tweets_collection
        self.users_collection=users_collection
        self.user = user

    def activity_index(self, user_id, e=30.4*24):
        """Expresses user's volume of tweets.
        The activity is computed as the average amount of tweets emitted per
        hour bounded by 1

        Arguments:
            user_id {str} -- user ID

        Keyword Arguments:
            e {float} -- hourly frequency (default: {30.4*24})

        Returns:
            float -- average amount of tweets
        """
        number_of_user_messages = len(get_user_published_tweets(
            user_id, self.node_collection))

        if number_of_user_messages < e:
            return number_of_user_messages / e
        else:
            return 1

    def dTR(self, user_id):
        """Computes ratio of directed tweets for a user.
        This provides an idea about the role she plays in the spread
        of information.

        Arguments:
            user_id {str} -- User ID

        Returns:
            float -- user's ratio of directed tweets
        """
        n_dv = number_of_tweets_with_user_mentions(user_id,
                                                   self.node_collection)
        n_mv = len(get_user_published_tweets(user_id, self.node_collection))

        if n_mv > 0:
            return n_dv / n_mv
        else:
            return 0

    def h(self):
        """Computes social homogeneity index for vx ∈ V and vy ∈ V.
        This reflects the overlap of the sets of users they interact with.
        It is computed with the Jaccard similarity index that is defined as the
        size of the intersection of the sets divided by the size of their
        union.

        Returns:
            float -- social homogeneity index
        """
        src_user_mv = users_ever_mentioned(self.src_user, self.node_collection)
        dest_user_mv = users_ever_mentioned(self.dest_user,
                                            self.node_collection)

        x = src_user_mv.intersection(dest_user_mv)
        y = src_user_mv.union(dest_user_mv)

        if len(y):
            return len(x) / len(y)
        else:
            return 0

    def hM(self):
        """Computes a boolean value for each user regarding the mentioning
        behaviour to capture the existence of an active interaction in the
        past. This feature can be somehow regarded as a “friendship” indicator
        in the case where both users have a positive value.

        Returns:
            boolean -- mentioning behaviour
        """
        mvx = users_ever_mentioned(self.src_user, self.node_collection)

        if self.dest_user in mvx:
            return 1
        else:
            return 0

    def mR(self, user_id, meu=200):
        """Computes the volume of directed tweets received by a user.

        Arguments:
            user_id {str} -- User ID

        Keyword Arguments:
            meu {int} -- value chosen based on empirical observation of the
            distribution of the mention rates (default: {200})

        Returns:
            [type] -- [description]
        """
        n_tmv = len(tweets_mentioned_in(user_id, self.node_collection))

        if n_tmv < meu:
            return n_tmv / meu
        else:
            return 1

    def src_num_directed_dest(self):
        """Computes the number of tweets directed from the source to target user"""
        mentions = tweets_mentioned_in(self.dest_user, self.node_collection)

        num_directed = 0

        # For each tweet which mentions the target user
        for tweet in expanded_tweets(mentions, self.tweet_collection):
            # If that tweet was sent by the src user
            if tweet.owner_id == self.src_user:
                num_directed += 1

        return num_directed

    def src_avg_positive_sentiment_directed_dest(self):
        """Computes the avg positive sentiment of the tweets directed from the source to target user"""
        mentions = tweets_mentioned_in(self.dest_user, self.node_collection)

        num_directed = 0
        num_positive = 0

        # For each tweet which mentions the target user
        for tweet in expanded_tweets(mentions, self.tweet_collection):
            # If that tweet was sent by the src user
            if tweet.owner_id == self.src_user:
                num_directed += 1
                # If that tweet is positive
                if tweet.is_positive_sentiment:
                    num_positive += 1

        if num_directed == 0:
            return 0
        return num_positive / num_directed

    def src_avg_negative_sentiment_directed_dest(self):
        """Computes the avg negative sentiment of the tweets directed from the source to target user"""
        mentions = tweets_mentioned_in(self.dest_user, self.node_collection)

        num_directed = 0
        num_negative = 0

        # For each tweet which mentions the target user
        for tweet in expanded_tweets(mentions, self.tweet_collection):
            # If that tweet was sent by the src user
            if tweet.owner_id == self.src_user:
                num_directed += 1
                # If that tweet is positive
                if tweet.is_negative_sentiment:
                    num_negative += 1

        if num_directed == 0:
            return 0
        return num_negative / num_directed

    def hK(self, user_id):
        """Has the user tweeted about the given topic?

        Arguments:
            user_id {str} -- User ID

        Returns:
            int -- 0 if False or 1 if True
        """
        user_tweets_keywords = get_keywords_from_user_tweets(
            user_id, self.node_collection)

        if not self.keywords.isdisjoint(user_tweets_keywords):
            return 1
        else:
            return 0

    def A(self, user_id):
        """Computes temporal dimension so that fluctuation of users attention
        through time can be captured.

        Arguments:
            user_id {str} -- User ID

        Returns:
            list -- receptivity level of a user over 6 bins of 4 hours each
        """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        if attr['A']:
            return attr['A']
        # TODO Verify this is correct empty value
        return [0, 0, 0, 0, 0, 0]

    def y(self):
        """Checks whether diffusion exists between two users.

        Returns:
            int -- Returns 0 if False, 1 if True
        """
        query = {'_id': self.dest_user}
        attr = self.node_collection.find_one(query)

        if self.src_user in set(attr['all_possible_original_tweet_owners']):
            return 1
        else:
            return 0

    def ratio_of_retweets_to_tweets(self, user_id):
        """ notation 7 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)

        total_number_of_tweets_retweeted = attr['retweeted_count']
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return total_number_of_tweets_retweeted / total_number_of_tweets

    def avg_number_of_tweets_with_hastags(self, user_id):
        """ notation 8 (ii) (new) """
        n_tweets_with_hashtags = number_of_tweets_with_hashtags(
            user_id, self.node_collection)
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return n_tweets_with_hashtags / total_number_of_tweets

    def avg_number_of_retweets_with_hastags(self, user_id):
        """ notation 8 (i) (new) """
        n_retweets_with_hashtags = retweets_with_hashtags(user_id,
                                                          self.node_collection)
        n_retweeted_tweets = number_of_retweeted_tweets(user_id,
                                                        self.node_collection)

        if n_retweeted_tweets == 0:
            return 0
        if n_retweeted_tweets:
            return n_retweets_with_hashtags / n_retweeted_tweets
        else:
            return 0

    def avg_number_of_retweets(self, user_id):
        """ number 9 (new) """
        n_retweeted_tweets = number_of_retweeted_tweets(user_id,
                                                        self.node_collection)
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return n_retweeted_tweets / total_number_of_tweets

    def avg_number_of_tweets(self, user_id):
        """ number 10 (new) """
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))
        n_days = get_user_number_of_tweet_days(user_id, self.node_collection)

        if n_days == 0:
            n_days = 1

        avg = total_number_of_tweets / n_days

        if avg > 1:
            avg = 1

        return avg

    def total_number_of_tweets(self, user_id):
        """The number of Tweets sent by the given user in the database"""
        total_number_of_tweets = len(get_user_published_tweets(user_id, self.node_collection))

        return total_number_of_tweets

    def avg_number_of_mentions_not_including_retweets(self, user_id):
        """ number 11 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)

        count = attr['tweets_with_others_mentioned_count']
        + attr['quoted_tweets_with_others_mentioned_count']
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return count / total_number_of_tweets

    def avg_number_followers(self, user_id):
        """ number 12 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        n_followers = attr['followers_count']

        avg = n_followers / 707

        # bound by 1
        if avg > 1:
            avg = 1

        return avg

    def raw_number_followers(self, user_id):
        # Get the number of followers for the given user id
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        n_followers = attr['followers_count']

        return n_followers

    def avg_number_friends(self, user_id):
        """ number 13 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        n_friends = attr['friends_count']

        avg = n_friends / 707

        # bound by 1
        if avg > 1:
            avg = 1

        return avg

    def raw_number_friends(self, user_id):
        # Get the number of friends (following) for the given user
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        n_friends = attr['friends_count']

        return n_friends

    def follower_friends_ratio(self, user_id):
        # Get the ratio of the number of followers a user has to the number of friends
        num_followers = self.raw_number_followers(user_id)
        num_friends = self.raw_number_friends(user_id)

        if num_friends == 0:
            return 0
        return  num_followers / num_friends

    def avg_number_of_mentions(self):
        """ number 14 (new) """
        raise NotImplementedError

    def variance_tweets_per_day(self):
        """ number 15 (new) """
        raise NotImplementedError

    def ratio_of_mentions_to_tweet(self, user_id):
        """ number 16 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)

        all_tweets_with_mentions_count = attr['tweets_with_others_mentioned_count']
        + attr['retweets_with_others_mentioned_count']
        + attr['quoted_tweets_with_others_mentioned_count']

        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return all_tweets_with_mentions_count / total_number_of_tweets

    def avg_url_per_retweet(self, user_id):
        """ number 17 (i) new """
        n_retweeted_tweets_with_url = retweeted_tweets_with_urls(
            user_id, self.node_collection)
        n_retweeted_tweets = number_of_retweeted_tweets(user_id,
                                                        self.node_collection)

        if n_retweeted_tweets == 0:
            return 0
        if n_retweeted_tweets:
            return n_retweeted_tweets_with_url / n_retweeted_tweets
        else:
            return 0

    def avg_url_per_tweet(self, user_id):
        """ number 17 (ii) new """
        n_tweets_with_url = number_of_tweets_with_urls(user_id,
                                                       self.node_collection)
        + retweeted_tweets_with_urls(user_id, self.node_collection)
        # get_quoted_tweets_with_url
        + quoted_tweets_with_urls(user_id, self.node_collection)

        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return n_tweets_with_url / total_number_of_tweets

    def avg_number_of_media_in_retweets(self, user_id):
        """ number 18 (i) new """
        n_retweets_with_media = retweeted_tweets_with_media(
            user_id, self.node_collection)
        n_retweeted_tweets = number_of_retweeted_tweets(user_id,
                                                        self.node_collection)

        if n_retweeted_tweets == 0:
            return 0
        if n_retweeted_tweets:
            return n_retweets_with_media / n_retweeted_tweets
        else:
            return 0

    def avg_number_of_media_in_tweets(self, user_id):
        """ number 18 (ii) new """
        n_tweets_with_media = number_of_tweets_with_media(user_id,
                                                          self.node_collection)
        # get_retweeted_tweets_with_media + len(quoted_tweets_with_media(self))
        + retweeted_tweets_with_media(user_id, self.node_collection)

        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return n_tweets_with_media / total_number_of_tweets

    def description(self, user_id):
        """ number 19 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        if attr['description']:
            return 1
        else:
            return 0

    def ratio_of_follower_to_friends(self, user_id):
        """ number 20 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)

        number_of_followers = len(attr['followers_ids'])
        number_of_friends = len(attr['friends_ids'])

        if number_of_friends == 0:
            return 0

        ratio = number_of_followers / number_of_friends

        if ratio > 1:
            ratio = 1

        return ratio

    def ratio_of_favorited_to_tweet(self, user_id):
        """ number 21 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)

        number_of_favorited_tweets = attr['favorite_tweets_count']
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return number_of_favorited_tweets / total_number_of_tweets

    def avg_time_before_retweet_quote_favorite(self):
        """ number 23 (new) """
        raise NotImplementedError

    def avg_positive_sentiment_of_tweets(self, user_id):
        """ number 24 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)

        number_of_positive_sentiments = attr['positive_sentiment_count']
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return number_of_positive_sentiments / total_number_of_tweets

    def avg_negative_sentiment_of_tweets(self, user_id):
        """ number 24 (new) """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)

        number_of_negative_sentiments = attr['negative_sentiment_count']
        total_number_of_tweets = len(get_user_published_tweets(
            user_id, self.node_collection))

        if total_number_of_tweets == 0:
            return 0
        return number_of_negative_sentiments / total_number_of_tweets

    def ratio_of_tweet_per_time_period(self, user_id):
        """ separate tweets in 4 periods using the hour attribute

            number 25 (new)
        Arguments:
            user_id {[type]} -- [description]
            node_collection {[type]} -- [description]
        """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        return attr['ratio_of_tweet_per_time_period']

    def ratio_of_tweets_that_got_retweeted_per_time_period(self, user_id):
        """ separate tweets in 4 periods using the hour attribute

            number 26 (new)
        Arguments:
            user_id {[type]} -- [description]
            node_collection {[type]} -- [description]
        """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        return attr['ratio_of_tweets_that_got_retweeted_per_time_period']

    def ratio_of_retweet_per_time_period(self, user_id):
        """ separate tweets in 4 periods using the hour attribute

            number 27 (new)
        Arguments:
            user_id {[type]} -- [description]
            node_collection {[type]} -- [description]
        """
        query = {'_id': user_id}
        attr = self.node_collection.find_one(query)
        return attr['ratio_of_retweet_per_time_period']

    def dest_num_responses_to_src(self):
        """ Returns the number of responses (retweets, quotes, or replies) given from the target to the source user """
        count = 0
        for response in get_responses(self.dest_user, self.node_collection, self.tweet_collection, self.retweets_collection, self.replies_collection):
            if response.original_owner_id(self.tweet_collection) == self.src_user:
                count += 1
        return count

    def dest_num_responses_to_mentions(self):
        """ Returns the number of responses (retweets, quotes, or replies) given from the target when mentioned """
        count = 0
        for response in get_responses(self.dest_user, self.node_collection, self.tweet_collection, self.retweets_collection, self.replies_collection):
            if response.users_mentioned == self.dest_user:
                count += 1
        return count

    def dest_avg_positive_sentiment_responses(self):
        """ Computes the avg positive sentiment of the responses sent by the target """
        num_responses = 0
        num_positive = 0

        # For each response
        for tweet in get_responses(self.dest_user, self.node_collection, self.tweet_collection, self.retweets_collection, self.replies_collection):
            num_responses += 1
            # If that tweet is positive
            if tweet.is_positive_sentiment:
                num_positive += 1

        if num_responses == 0:
            return 0
        return num_positive / num_responses

    def dest_avg_negative_sentiment_responses(self):
        """ Computes the avg negative sentiment of the responses sent by the target """
        num_responses = 0
        num_negative = 0

        # For each response
        for tweet in get_responses(self.dest_user, self.node_collection, self.tweet_collection, self.retweets_collection, self.replies_collection):
            num_responses += 1
            # If that tweet is negative
            if tweet.is_negative_sentiment:
                num_negative += 1

        if num_responses == 0:
            return 0
        return num_negative / num_responses

    def dest_num_responses_to_media(self):
        """ Returns the number of responses (retweets, quotes, or replies) given from the target to tweets containing media """
        count = 0
        for response in get_responses(self.dest_user, self.node_collection, self.tweet_collection, self.retweets_collection, self.replies_collection):
            if response.media:
                count += 1
        return count

    def dest_num_responses_to_hashtags(self):
        """ Returns the number of responses (retweets, quotes, or replies) given from the target to tweets containing hashtags """
        count = 0
        for response in get_responses(self.dest_user, self.node_collection, self.tweet_collection, self.retweets_collection, self.replies_collection):
            if response.hashtags:
                count += 1
        return count

    def dest_num_responses_to_urls(self):
        """ Returns the number of responses (retweets, quotes, or replies) given from the target to tweets containing urls """
        count = 0
        for response in get_responses(self.dest_user, self.node_collection, self.tweet_collection, self.retweets_collection, self.replies_collection):
            if response.urls:
                count += 1
        return count

    def dest_follows_src(self):
        """ Returns 1 if the target user follows the source, 0 otherwise """
        for id in get_following(self.dest_user, self.users_collection):
            if id == self.src_user:
                return 1

        return 0

    def event_is_positive(self, user_id):
        """ Returns 1 if the event tweet for the given user is positive, 0 otherwise """
        event = get_event_tweet(user_id, self.event_tweets_collection)

        if event is None:
            return 0

        if event.is_positive_sentiment:
            return 1
        else:
            return 0

    def event_is_negative(self, user_id):
        """ Returns 1 if the event tweet for the given user is negative, 0 otherwise """
        event = get_event_tweet(user_id, self.event_tweets_collection)

        if event is None:
            return 0

        if event.is_negative_sentiment:
            return 1
        else:
            return 0

    def event_is_directed_to(self, src_id, target_id):
        """ Returns 1 if the event tweet for the given user is directed to the target """
        event = get_event_tweet(src_id, self.event_tweets_collection)

        if event is None:
            return 0

        # TODO Verify mentions are working right
        if target_id in event.users_mentioned:
            return 1
        else:
            return 0

    def event_has_hashtags(self, user_id):
        """ Returns 1 if the event tweet for the given user has a hashtag """
        event = get_event_tweet(user_id, self.event_tweets_collection)

        if event is None:
            return 0

        if event.hashtags:
            return 1
        else:
            return 0

    def event_has_media(self, user_id):
        """ Returns 1 if the event tweet for the given user has media """
        event = get_event_tweet(user_id, self.event_tweets_collection)

        if event is None:
            return 0

        if event.media:
            return 1
        else:
            return 0

    def event_has_url(self, user_id):
        """ Returns 1 if the event tweet for the given user has media """
        event = get_event_tweet(user_id, self.event_tweets_collection)

        if event is None:
            return 0

        if event.urls:
            return 1
        else:
            return 0

    def num_event_responses(self, user_id, responder_id):
        """ Returns 1 if one of the event tweets for the given user has a response, false otherwise """
        response_count = 0
        for event in get_event_tweets(user_id, self.event_tweets_collection):
            for response in get_responses(responder_id, self.node_collection, self.tweet_collection,
                                          self.retweets_collection, self.replies_collection):
                if response.original_tweet_id == event.id:
                    response_count += 1

        return response_count

    def event_response_time(self, user_id, responder_id):
        """ Returns 1 if the event tweet for the given user has a response, false otherwise """
        event = get_event_tweet(user_id, self.event_tweets_collection)

        if event is None:
            return 0

        found_response = None
        for response in get_responses(responder_id, self.node_collection, self.tweet_collection,
                                      self.retweets_collection, self.replies_collection):
            if response.original_tweet_id == event.id:
                found_response = response
                break

        if found_response is None:
            return 0
        else:
            return (found_response.created_at.replace(tzinfo=None) - event.created_at.replace(tzinfo=None)).total_seconds()

    def additional_features(self, user_id, user=None):
        return {
            f'{user}_I': self.activity_index(user_id),
            f'{user}_dTR': self.dTR(user_id),
            f'{user}_mR': self.mR(user_id),
            f'{user}_hK': self.hK(user_id),
            f'{user}_A_1': self.A(user_id)[0],
            f'{user}_A_2': self.A(user_id)[1],
            f'{user}_A_3': self.A(user_id)[2],
            f'{user}_A_4': self.A(user_id)[3],
            f'{user}_A_5': self.A(user_id)[4],
            f'{user}_A_6': self.A(user_id)[5],
            f'{user}_ratio_of_retweets_to_tweets':
                self.ratio_of_retweets_to_tweets(user_id),
            f'{user}_avg_number_of_tweets_with_hastags':
                self.avg_number_of_tweets_with_hastags(user_id),
            f'{user}_avg_number_of_retweets_with_hastags':
                self.avg_number_of_retweets_with_hastags(user_id),
            f'{user}_avg_number_of_retweets':
                self.avg_number_of_retweets(user_id),
            f'{user}_avg_number_of_tweets':
                self.avg_number_of_tweets(user_id),
            f'{user}_avg_number_of_mentions_not_including_retweets':
                self.avg_number_of_mentions_not_including_retweets(user_id),
            f'{user}_ratio_of_mentions_to_tweet':
                self.ratio_of_mentions_to_tweet(user_id),
            f'{user}_avg_url_per_retweet':
                self.avg_url_per_retweet(user_id),
            f'{user}_avg_url_per_tweet':
                self.avg_url_per_tweet(user_id),
            f'{user}_avg_number_of_media_in_retweets':
                self.avg_number_of_media_in_retweets(user_id),
            f'{user}_avg_number_of_media_in_tweets':
                self.avg_number_of_media_in_tweets(user_id),
            f'{user}_description':
                self.description(user_id),
            f'{user}_ratio_of_favorited_to_tweet':
                self.ratio_of_favorited_to_tweet(user_id),
            f'{user}_avg_positive_sentiment_of_tweets':
                self.avg_positive_sentiment_of_tweets(user_id),
            f'{user}_avg_negative_sentiment_of_tweets':
                self.avg_negative_sentiment_of_tweets(user_id),
            f'{user}_ratio_of_tweet_per_time_period_1':
                self.ratio_of_tweet_per_time_period(user_id)['1']
                if '1' in self.ratio_of_tweet_per_time_period(user_id) else 0,
            f'{user}_ratio_of_tweet_per_time_period_2':
                self.ratio_of_tweet_per_time_period(user_id)['2']
                if '2' in self.ratio_of_tweet_per_time_period(user_id)
                else 0,
            f'{user}_ratio_of_tweet_per_time_period_3':
                self.ratio_of_tweet_per_time_period(user_id)['3']
                if '3' in self.ratio_of_tweet_per_time_period(user_id)
                else 0,
            f'{user}_ratio_of_tweet_per_time_period_4':
                self.ratio_of_tweet_per_time_period(user_id)['4']
                if '4' in self.ratio_of_tweet_per_time_period(user_id)
                else 0,
            f'{user}_'
            'ratio_of_tweets_that_got_retweeted_per_time_period_1':
                self.ratio_of_tweets_that_got_retweeted_per_time_period(
                    user_id)['1']
                if '1' in self.ratio_of_tweets_that_got_retweeted_per_time_period(user_id)
                else 0,
            f'{user}_'
            'ratio_of_tweets_that_got_retweeted_per_time_period_2':
                self.ratio_of_tweets_that_got_retweeted_per_time_period(
                    user_id)['2']
                if '2' in self.ratio_of_tweets_that_got_retweeted_per_time_period(
                    user_id)
                else 0,
            f'{user}_'
            'ratio_of_tweets_that_got_retweeted_per_time_period_3':
                self.ratio_of_tweets_that_got_retweeted_per_time_period(
                    user_id)['3']
                if '3' in self.ratio_of_tweets_that_got_retweeted_per_time_period(
                    user_id)
                else 0,
            f'{user}_'
            'ratio_of_tweets_that_got_retweeted_per_time_period_4':
                self.ratio_of_tweets_that_got_retweeted_per_time_period(
                    user_id)['4']
                if '4' in self.ratio_of_tweets_that_got_retweeted_per_time_period(
                    user_id)
                else 0,
            f'{user}_ratio_of_retweet_per_time_period_1':
                self.ratio_of_retweet_per_time_period(user_id)['1']
            if '1' in self.ratio_of_retweet_per_time_period(user_id) else 0,
            f'{user}_ratio_of_retweet_per_time_period_2':
                self.ratio_of_retweet_per_time_period(user_id)['2']
                if '2' in self.ratio_of_retweet_per_time_period(user_id)
                else 0,
            f'{user}_ratio_of_retweet_per_time_period_3':
                self.ratio_of_retweet_per_time_period(user_id)['3']
                if '3' in self.ratio_of_retweet_per_time_period(user_id)
                else 0,
            f'{user}_ratio_of_retweet_per_time_period_4':
                self.ratio_of_retweet_per_time_period(user_id)['4']
                if '4' in self.ratio_of_retweet_per_time_period(user_id)
                else 0,
            f'{user}_avg_number_followers': self.avg_number_followers(user_id),
            f'{user}_avg_number_friends': self.avg_number_friends(user_id),
            f'{user}_ratio_of_follower_to_friends':
                self.ratio_of_follower_to_friends(user_id),
            }

    def generic_user_features(self, user_id, user=None):
        """Creates a dictionary of features which apply to both the source and target.
        Part of the model for Adaeze's project.

        Arguments:
            user_id {str} -- User ID

        Keyword Arguments:
            user {str} -- Name used to prefix users in the returned dictionary

        Returns:
            {dict} -- mapping of feature names to values
        """
        return {
            f'{user}_num_followers': self.raw_number_followers(user_id),
            f'{user}_num_friends': self.raw_number_friends(user_id),
            f'{user}_follower_friends_ratio': self.follower_friends_ratio(user_id),
            f'{user}_total_tweets': self.total_number_of_tweets(user_id),
            f'{user}_avg_positive_sentiment_of_tweets': self.avg_positive_sentiment_of_tweets(user_id),
            f'{user}_avg_negative_sentiment_of_tweets': self.avg_negative_sentiment_of_tweets(user_id)
        }

    def src_user_features(self):
        """Creates a dictionary of features which apply only to the source user.
        Part of the model for Adaeze's project.

        Returns:
            {dict} -- mapping of feature names to values
        """
        return {
            'src_num_directed_dest': self.src_num_directed_dest(),
            'src_avg_positive_sentiment_directed_dest': self.src_avg_positive_sentiment_directed_dest(),
            'src_avg_negative_sentiment_directed_dest': self.src_avg_negative_sentiment_directed_dest(),
            'src_num_with_media': number_of_tweets_with_media(self.src_user, self.node_collection),
            'src_num_with_hashtags': number_of_tweets_with_hashtags(self.src_user, self.node_collection),
            'src_num_with_urls': number_of_tweets_with_urls(self.src_user, self.node_collection),
            'event_is_positive': self.event_is_positive(self.src_user),
            'event_is_negative': self.event_is_negative(self.src_user),
            'event_is_directed': self.event_is_directed_to(self.src_user, self.dest_user),
            'event_has_hashtags': self.event_has_hashtags(self.src_user),
            'event_has_media': self.event_has_media(self.src_user),
            'event_has_url': self.event_has_url(self.src_user),
            'num_event_responses': self.num_event_responses(self.src_user, self.dest_user),
            'event_response_time': self.event_response_time(self.src_user, self.dest_user)
        }

    def target_user_features(self):
        """Creates a dictionary of features which apply only to the target.
        Part of the model for Adaeze's project.

        Returns:
            {dict} -- mapping of feature names to values
        """
        return {
            'dest_num_responses_to_src': self.dest_num_responses_to_src(),
            'dest_num_responses_to_mentions': self.dest_num_responses_to_mentions(),
            'dest_avg_positive_sentiment_responses': self.dest_avg_positive_sentiment_responses(),
            'dest_avg_negative_sentiment_responses': self.dest_avg_negative_sentiment_responses(),
            'dest_num_responses_to_media': self.dest_num_responses_to_media(),
            'dest_num_responses_to_hashtags': self.dest_num_responses_to_hashtags(),
            'dest_num_responses_to_urls': self.dest_num_responses_to_urls(),
            'dest_follows_src': self.dest_follows_src()
        }

    def to_dict(self):
        """Calculates all features for the current pair of src and destination.
        Currently setup for the model for Adaeze's project.

        Returns:
            {dict} -- mapping of feature names to values
        """
        default_features = {
            # NB: The src_id and dest_id features are not in the range 0-1, and should probably not actually be input to a model
            'src_id': self.src_user,
            'dest_id': self.dest_user
        }

        # Calculate features common to both the source and target users
        src_generic_features = self.generic_user_features(user_id=self.src_user, user='src')
        dest_generic_features = self.generic_user_features(user_id=self.dest_user, user='dest')

        # Calculate features specific to the source user
        src_features = self.src_user_features()

        # Calculate features specific to the target user
        dest_features = self.target_user_features()

        return ChainMap(default_features, src_generic_features, src_features,
                        dest_generic_features, dest_features)


def get_user_published_tweets(user_id, node_collection):
    """notation 6"""
    # all_tweets = {}

    query = {'_id': user_id}
    attr = node_collection.find_one(query)

    # list of tweet ids
    tweets = attr['tweets']
    # all_tweets.update(tweets)

    retweeted_tweets = attr['retweeted_tweets']
    # all_tweets.update(retweeted_tweets)

    quoted_tweets = attr['quoted_tweets']
    # all_tweets.update(quoted_tweets)

    return tweets + retweeted_tweets + quoted_tweets


def get_responses(user_id, node_collection, tweets_collection, retweets_collection, replies_collection):
    # Find cached user attributes
    query = {'_id': user_id}
    attr = node_collection.find_one(query)

    returned_ids = set()

    # Find retweets in retweets collection
    query = {'user.id_str': user_id}
    for retweet in retweets_collection.find(query):
        parsed = Tweet(retweet)
        if parsed.id not in returned_ids:
            returned_ids.add(parsed.id)
            yield parsed

    # Find replies in replies collection
    query = {'author_id': user_id}
    for reply in replies_collection.find(query):
        parsed = Tweet(reply)
        if parsed.id not in returned_ids:
            returned_ids.add(parsed.id)
            yield parsed
    query = {'user.id_str': user_id}
    for reply in replies_collection.find(query):
        parsed = Tweet(reply)
        if parsed.id not in returned_ids:
            returned_ids.add(parsed.id)
            yield parsed

    # For each quoted tweet
    for quoted_tweets in expanded_tweets(attr['quoted_tweets'], tweets_collection):
        yield quoted_tweets

    # For each original tweet
    for tweet in expanded_tweets(attr['tweets'], tweets_collection):
        if tweet.is_response_tweet:
            yield tweet


def get_event_tweet(user_id, event_tweets_collection):
    """ Gets the event tweet (the earliest tweet sent by the user in this database) """
    min_tweet = None
    for tweet in get_event_tweets(user_id, event_tweets_collection):
        if min_tweet is None:
            min_tweet = tweet
        elif tweet.created_at < min_tweet.created_at:
            min_tweet = tweet

    return min_tweet


def get_event_tweets(user_id, event_tweets_collection):
    """ Gets all tweets from the user in the event collection """
    query = {'author_id': user_id}

    returned_ids = set()

    for tweet in event_tweets_collection.find(query):
        parsed = Tweet(tweet)
        if parsed.id not in returned_ids:
            returned_ids.add(parsed.id)
            yield parsed


def get_following(user_id, users_collection):
    """ Gets list of users the given user is following """
    query = {'id': user_id}

    user = users_collection.find_one(query)
    if 'following_ids' in user:
        return user['following_ids']
    elif 'following' in user:
        return user['following']

    # No following data; return empty list
    return []


def expanded_tweets(tweet_ids, tweets_collection):
    """ Get tweets from the database using the given list of ids """
    for id_str in tweet_ids:
        query = {'id': id_str}
        found = tweets_collection.find_one(query)
        if found is not None:
            yield Tweet(found)


def users_ever_mentioned(user_id, node_collection):  # get_users_mentioned_in
    """notation 7"""
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return set(attr['users_mentioned_in_all_my_tweets'])


def number_of_tweets_with_user_mentions(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_tweets_with_user_mentions']


def tweets_mentioned_in(user_id, node_collection):
    """notation 9"""
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return set(attr['mentioned_in'])


def get_keywords_from_user_tweets(user_id, node_collection):
    """notation 12"""
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return set(attr['keywords_in_all_my_tweets'])


def calculate_network_diffusion(edges, keywords, node_collection,
                                tweet_collection, retweets_collection, event_tweets_collection, users_collection,
                                replies_collection,
                                *, additional_attr=False,
                                do_not_add_sentiment=False, n_days=30):
    # todo: turn this into a generator and see if its contents will only be
    # consumed once. this will require removing counter and search for another
    # way of knowing the number of things calculated
    # changed results.append to yield

    widgets = ['Computing Diffusion, ',
               progressbar.Counter('Processed %(value)02d'),
               ' edges (', progressbar.Timer(), ')']
    bar = progressbar.ProgressBar(widgets=widgets)
    for src_user, dest_user in bar(edges):
        features = Features(src_user=src_user, dest_user=dest_user,
                            keywords=keywords, node_collection=node_collection,
                            tweet_collection=tweet_collection,
                            retweets_collection=retweets_collection,
                            replies_collection=replies_collection,
                            users_collection=users_collection,
                            event_tweets_collection=event_tweets_collection)

        yield(features.to_dict())


def number_of_retweeted_tweets(user_id, node_collection):
    """---"""
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return len(attr['retweeted_tweets'])


def retweet_count(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['retweet_count']


def number_of_tweets_with_hashtags(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_tweets_with_hashtags']


def retweets_with_hashtags(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_retweeted_tweets_with_hashtags']


def get_user_number_of_tweet_days(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    tweet_max_date = attr['tweet_max_date']
    tweet_min_date = attr['tweet_min_date']
    diff = tweet_max_date - tweet_min_date

    if diff == 0:
        return 0
    return diff.days


def number_of_tweets_with_urls(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_tweets_with_urls']


def retweeted_tweets_with_urls(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_retweeted_tweets_with_urls']


def quoted_tweets_with_urls(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_quoted_tweets_with_urls']


def number_of_tweets_with_media(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_tweets_with_media']

# get_retweeted_tweets_with_media


def retweeted_tweets_with_media(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_retweeted_tweets_with_media']


def quoted_tweets_with_media(user_id, node_collection):
    query = {'_id': user_id}
    attr = node_collection.find_one(query)
    return attr['n_quoted_tweets_with_media']


def compute_ratio_of_tweet_per_time_period(user):
    periods = Counter()

    all_tweets_dates = user['tweets_dates'] + \
        user['retweeted_tweets_dates'] + user['quoted_tweets_dates']

    n_all_tweets_dates = len(all_tweets_dates)

    for tweet_date in all_tweets_dates:
        h = tweet_date.hour

        if h in range(0, 24):
            period = h // 6 + 1
            periods[str(period)] += 1

    for key, value in periods.items():
        periods[key] = value / n_all_tweets_dates

    user['ratio_of_tweet_per_time_period'] = periods


def compute_ratio_of_tweets_that_got_retweeted_per_time_period(user):
    periods = Counter()

    all_tweets_dates = user['tweets_dates'] + \
        user['retweeted_tweets_dates'] + user['quoted_tweets_dates']

    n_all_tweets_dates = len(all_tweets_dates)

    retweeted_tweets_dates = user['retweeted_tweets_dates']

    for tweet_date in retweeted_tweets_dates:
        h = tweet_date.hour

        if h in range(0, 24):
            period = h // 6 + 1
            periods[str(period)] += 1

    for key, value in periods.items():
        periods[key] = value / n_all_tweets_dates

    user['ratio_of_tweets_that_got_retweeted_per_time_period'] = periods


def compute_ratio_of_retweet_per_time_period(user):
    periods = Counter()
    retweeted_tweets_dates = user['retweeted_tweets_dates']
    n_retweeted_tweets_dates = len(retweeted_tweets_dates)

    for tweet_date in retweeted_tweets_dates:
        h = tweet_date.hour

        if h in range(0, 24):
            period = h // 6 + 1
            periods[str(period)] += 1

    for key, value in periods.items():
        if n_retweeted_tweets_dates:
            periods[key] = value / n_retweeted_tweets_dates
        else:
            periods[key] = 0

    user['ratio_of_retweet_per_time_period'] = periods


def compute_A(user):
    tweet_freq_table = {}
    all_tweets_dates = user['tweets_dates'] + \
        user['retweeted_tweets_dates'] + user['quoted_tweets_dates']

    n_all_tweets_dates = len(all_tweets_dates)

    for tweet_date in all_tweets_dates:
        tweet_date_and_time = tweet_date
        tweet_date = tweet_date_and_time.date()
        tweet_hour = tweet_date_and_time.hour
        hour_bin = tweet_hour // 4

        tweet_freq_table.setdefault(tweet_date, {0: 0,
                                                 1: 0,
                                                 2: 0,
                                                 3: 0,
                                                 4: 0,
                                                 5: 0})[hour_bin] += 1

    results = pd.DataFrame(list(tweet_freq_table.values()))
    results = results / n_all_tweets_dates
    results = results.values
    sum_ = np.sum(results, axis=0)

    user['A'] = sum_.tolist()
