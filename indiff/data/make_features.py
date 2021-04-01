# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime
from itertools import count
from pathlib import Path

import click
import networkx as nx
import pandas as pd
import progressbar
import pymongo
from dotenv import find_dotenv, load_dotenv

from indiff import utils
from indiff.features import build_features
from indiff.twitter import Tweet


def compute_user_attribs(user_attribs, user_tweets, users_collection, tweet_collection, tweet_mentions_collection):
    """ Computes a user's attributes

    Arguments:
        user_attribs {dict} -- user attributes
        user_tweets {collection} -- all tweets by user
        users_collection {collection} -- collection with all users
        tweet_mentions_collection {collection} -- tweets with user mentions
    """
    user_id = user_attribs['_id']

    user = users_collection.find_one({'id': user_id})

    user_attribs['username'] = user['username']

    if not user_attribs['followers_count']:
        user_attribs['followers_count'] = user['public_metrics']['followers_count']

    if not user_attribs['friends_count']:
        user_attribs['friends_count'] = user['public_metrics']['following_count']

    if not user_attribs['description']:
        user_attribs['description'] = user['description']

    bar = progressbar.ProgressBar(prefix=f"Computing {user_id}'s "
                                  "Attributes: ")
    for user_tweet in bar(user_tweets):
        tweet = Tweet(user_tweet)

        orig_owner_id = tweet.original_owner_id(tweet_collection)
        if orig_owner_id != user_id:
            user_attribs['all_possible_original_tweet_owners'].append(
                orig_owner_id)

        if tweet.is_retweeted_tweet:
            user_attribs['retweeted_tweets'].append(tweet.id)

            if tweet.hashtags:
                user_attribs['n_retweeted_tweets_with_hashtags'] += 1
            if tweet.urls:
                user_attribs['n_retweeted_tweets_with_urls'] += 1
            if tweet.media:
                user_attribs['n_retweeted_tweets_with_media'] += 1

            if tweet.is_others_mentioned:
                user_attribs['retweets_with_others_mentioned_count'] += 1

            # fetch tweet dates
            user_attribs['retweeted_tweets_dates'].append(tweet.created_at)
        elif tweet.is_quoted_tweet:
            user_attribs['quoted_tweets'].append(tweet.id)

            if tweet.hashtags:
                user_attribs['n_quoted_tweets_with_hashtags'] += 1
            if tweet.urls:
                user_attribs['n_quoted_tweets_with_urls'] += 1
            if tweet.media:
                user_attribs['n_quoted_tweets_with_media'] += 1

            if tweet.is_others_mentioned:
                user_attribs['quoted_tweets_with_others_mentioned_count'] += 1

            # fetch tweet dates
            user_attribs['quoted_tweets_dates'].append(tweet.created_at)
        else:
            user_attribs['tweets'].append(tweet.id)

            if tweet.hashtags:
                user_attribs['n_tweets_with_hashtags'] += 1
            if tweet.urls:
                user_attribs['n_tweets_with_urls'] += 1
            if tweet.media:
                user_attribs['n_tweets_with_media'] += 1

            users_mentioned_in_tweet = tweet.users_mentioned
            if users_mentioned_in_tweet:
                user_attribs['users_mentioned_in_all_my_tweets'].extend(
                    users_mentioned_in_tweet)

                # write tweet-user-mentioned to db
                tweet_users_doc = {
                    "_id": tweet.id,
                    "users": users_mentioned_in_tweet
                }

                tweet_mentions_collection.insert_one(
                    tweet_users_doc)

            if tweet.is_others_mentioned:
                user_attribs['tweets_with_others_mentioned_count'] += 1

            # fetch tweet dates
            user_attribs['tweets_dates'].append(tweet.created_at)

        if tweet.is_response_tweet:
            user_attribs['responses'].append(tweet.id)

        if tweet.is_favourited:
            user_attribs['favorite_tweets_count'] += 1

        user_attribs['retweet_count'] += tweet.retweet_count

        if tweet.is_retweeted:
            user_attribs['retweeted_count'] += 1

        user_attribs['keywords_in_all_my_tweets'].extend(tweet.keywords)

        if user_attribs['tweet_min_date'] == 0:
            user_attribs['tweet_min_date'] = tweet.created_at

        if user_attribs['tweet_max_date'] == 0:
            user_attribs['tweet_max_date'] = tweet.created_at

        if user_attribs['tweet_min_date'] > tweet.created_at:
            user_attribs['tweet_min_date'] = tweet.created_at

        if user_attribs['tweet_max_date'] < tweet.created_at:
            user_attribs['tweet_max_date'] = tweet.created_at

        # external_owner_id = tweet.original_owner_id
        # if external_owner_id:
        #     user['all_possible_original_tweet_owners'].add(
        #         external_owner_id)

        # TODO: recalculate for neutral
        if tweet.is_positive_sentiment:
            user_attribs['positive_sentiment_count'] += 1
        else:
            user_attribs['negative_sentiment_count'] += 1

        # calculate n_tweets_with_user_mentions
        if tweet.users_mentioned:
            user_attribs['n_tweets_with_user_mentions'] += 1


def process_user_attribs(users, tweet_collection, tweet_mentions_collection,
                         users_collection, user_attribs_collection):
    """ Computes user attributes for multiple users

    Arguments:
        users {list} -- user ids
        tweet_collection {collection} -- tweets
        tweet_mentions_collection {collection} -- tweets with user mentions
        users_collection {collection} -- user data objects
        user_attribs_collection {collection} -- user attributes
    """
    n_user_ids = len(users)

    # Create an index so that user mentions will be efficent
    user_attribs_collection.create_index('username')

    for i, user_id in zip(count(start=1), users):
        logging.info(f"PROCESSING NODE ATTR FOR {user_id}: "
                     f"{i} OF {n_user_ids} USERS")
        user_attribs = {'_id': user_id,
                        'username': '',
                        'tweets': [],
                        'n_tweets_with_hashtags': 0,
                        'n_tweets_with_urls': 0,
                        'n_tweets_with_media': 0,
                        'tweets_with_others_mentioned_count': 0,
                        'mentioned_in': [],
                        'users_mentioned_in_all_my_tweets': [],
                        'keywords_in_all_my_tweets': [],
                        'all_possible_original_tweet_owners': [],
                        'retweeted_tweets': [],
                        'n_retweeted_tweets_with_hashtags': 0,
                        'n_retweeted_tweets_with_urls': 0,
                        'n_retweeted_tweets_with_media': 0,
                        'retweets_with_others_mentioned_count': 0,
                        'retweet_count': 0,
                        'retweeted_count': 0,
                        'quoted_tweets': [],
                        'n_quoted_tweets_with_hashtags': 0,
                        'n_quoted_tweets_with_urls': 0,
                        'n_quoted_tweets_with_media': 0,
                        'quoted_tweets_with_others_mentioned_count': 0,
                        'description': None,
                        'favorite_tweets_count': 0,
                        'positive_sentiment_count': 0,
                        'negative_sentiment_count': 0,
                        'followers_count': 0,
                        'friends_count': 0,
                        'followers_ids': [],
                        'friends_ids': [],
                        'tweet_min_date': 0,
                        'tweet_max_date': 0,
                        'n_tweets_with_user_mentions': 0,
                        'tweets_dates': [],
                        'retweeted_tweets_dates': [],
                        'quoted_tweets_dates': [],
                        'responses': []
                        }

        query = {"author_id": user_id}
        user_tweets = tweet_collection.find(query)

        # compute user atribs
        compute_user_attribs(
            user_attribs=user_attribs,
            user_tweets=user_tweets,
            users_collection=users_collection,
            tweet_collection=tweet_collection,
            tweet_mentions_collection=tweet_mentions_collection
            )

        update_user_attribs(user_attribs=user_attribs)
        # write user attributes as document to database
        try:
            user_attribs_collection.insert_one(user_attribs)
        except pymongo.errors.DuplicateKeyError:
            logging.info(f'updating node attribute for {user_id}')
            user_attribs_collection.replace_one({'_id': user_id}, user_attribs)
        except pymongo.errors.InvalidDocument as err:
            logging.error('found an invalid document')
            logging.error(err)

    compute_mentioned_in(tweet_mentions_collection, user_attribs_collection)


def update_user_attribs(user_attribs):
    """ Updates a user's attributes with four additional features

    Arguments:
        user_attribs {dict} -- user attributes
    """

    # compute ratio_of_tweet_per_time_period
    build_features.compute_ratio_of_tweet_per_time_period(user_attribs)

    # compute ratio_of_tweets_that_got_retweeted_per_time_period
    build_features.compute_ratio_of_tweets_that_got_retweeted_per_time_period(
        user_attribs)

    # compute ratio_of_retweet_per_time_period
    build_features.compute_ratio_of_retweet_per_time_period(user_attribs)

    # compute get_A
    build_features.compute_A(user_attribs)


def compute_mentioned_in(tweet_mentions_collection, user_attribs_collection):
    """ Computes tweets a user is mentioned in

    Arguments:
        tweet_mentions_collection {collection} -- a collection of tweet and
        user mentions
        user_attribs_collection {collection} -- a collection of user attributes
    """
    # calculate extra attributes
    # TODO: look for a way to make this computationally effecient since
    # we can now query a database and just change a particular part of the
    # database.
    # get all tweets in database
    n_tweets = tweet_mentions_collection.count_documents({})
    if n_tweets:
        logging.info('update user attribs with tweets mentioned in')

        tweets = tweet_mentions_collection.find({})
        bar = progressbar.ProgressBar(maxlen=n_tweets)
        for tweet_document in bar(tweets):
            tweet_id = tweet_document['_id']
            users_mentioned = tweet_document['users']

            for user in users_mentioned:
                # check if user exists in user_attribs_collection
                query_user_attr = {'username': user}
                document_count = user_attribs_collection.count_documents(
                    query_user_attr)
                if document_count:
                    user_attr_document = user_attribs_collection.find_one(
                        query_user_attr)

                    # update the document
                    mentioned_in = user_attr_document['mentioned_in']
                    mentioned_in.append(tweet_id)
                    new_values = {"$set": {
                        "mentioned_in": mentioned_in
                        }}

                    user_attribs_collection.update_one(
                        query_user_attr, new_values)


@click.command()
@click.argument('topic')
@click.argument('keywords_filepath', type=click.Path(exists=True))
def main(topic, keywords_filepath):
    """ Runs feature extraction scripts to generate raw data.
    """
    logger = logging.getLogger(__name__)
    current_date_and_time = datetime.now()

    # root directories
    root_dir = Path(__file__).resolve().parents[2]
    data_root_dir = os.path.join(root_dir, 'data')
    raw_data_root_dir = os.path.join(data_root_dir, 'raw')
    topic_raw_data_dir = os.path.join(raw_data_root_dir, topic)

    db_name = "RPE_twitteranniv"
    event_db_name = "RPE_twitteranniv"
    client = None

    try:
        if not os.path.exists(topic_raw_data_dir):
            raise FileExistsError(f'Dataset for {topic} does not exists.')

        client = pymongo.MongoClient(host='localhost', port=27017,
                                     appname=__file__)
        db = client[db_name]
        event_db = client[event_db_name]
        tweet_collection = db[topic]
        user_attribs_collection = db[topic + "-user-attribs"]
        users_collection = db[topic + "-users"]
        retweets_collection = db[topic + "-retweets"]
        tweet_mentions_collection = db[topic + "-mentions"]
        event_tweets_collection = event_db[topic + "-event_tweets"]

        if db_name not in client.list_database_names():
            raise ValueError(f"Database does not exist: {db_name}.")

        if topic not in db.list_collection_names():
            raise ValueError(f"Collection does not exist: {topic}.")

        topic_raw_data_dir = Path(topic_raw_data_dir)

        social_network_filepath = list(topic_raw_data_dir.glob('*.adjlist'))[0]

        # build initial graph from file
        social_network = nx.read_adjlist(social_network_filepath,
                                         delimiter=',',
                                         create_using=nx.DiGraph)
    except (ValueError, FileNotFoundError, FileExistsError, KeyError) as error:
        logger.error(error)
    else:
        # Remove all nodes without users in the database
        widgets = ['Filtering users not in database, ',
                   progressbar.Counter('Processed %(value)02d'),
                   ' users (', progressbar.Timer(), ')']
        bar = progressbar.ProgressBar(widgets=widgets)
        for user in bar(list(social_network.nodes)):
            if not users_collection.find_one({'id': user}):
                social_network.remove_node(user)

        social_network.name = topic

        # reports file path
        parts = list(topic_raw_data_dir.parts)
        if parts[-2] != 'raw':
            raise ValueError(f'Not an expected file path. Expected value: raw')
        _ = parts.pop(-2)
        parts[-2] = 'reports'
        topic_reports_dir = Path(*parts)

        # initialise node attributes to have desired info from dataset
        user_ids = nx.nodes(social_network)
        process_user_attribs(
            users=user_ids, tweet_collection=tweet_collection,
            tweet_mentions_collection=tweet_mentions_collection,
            users_collection=users_collection,
            user_attribs_collection=user_attribs_collection
            )
        keywords = utils.get_keywords_from_file(keywords_filepath)

        # prepare table for dataframe
        results = build_features.calculate_network_diffusion(
            nx.edges(social_network), keywords,
            node_collection=user_attribs_collection,
            tweet_collection=tweet_collection,
            retweets_collection=retweets_collection,
            event_tweets_collection=event_tweets_collection,
            additional_attr=True,
            do_not_add_sentiment=False
            )

        df = pd.DataFrame(results)

        # save processed dataset to hdf file
        key = utils.generate_random_id(15)

        # save features to a centralised raw directory
        raw_dataset_dir = topic_raw_data_dir.parent
        processed_saveas = os.path.join(raw_dataset_dir, 'dataset.h5')
        logger.info(f'saving computed features to "{processed_saveas}"')
        df.to_hdf(processed_saveas, key=key)

        # save key to reports directory
        if not os.path.exists(topic_reports_dir):
            os.makedirs(topic_reports_dir)
        key_saveas = os.path.join(topic_reports_dir.parent, 'dataset.keys')
        logger.info(f'saving dataset key to "{key_saveas}"')

        mode = 'a'
        if not os.path.exists(key_saveas):
            mode = 'w'

        with open(key_saveas, mode) as f:
            f.write('\n***\n\nmake_dataset.py '
                    f'started at {current_date_and_time}')
            f.write(f'\nNetwork path: {topic_raw_data_dir}')
            f.write(f'\nTopic: {topic}')
            f.write(f'\nKey: {key}\n\n')
    finally:
        if client is not None:
            logger.info('ending all server sessions')
            client.close()


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
