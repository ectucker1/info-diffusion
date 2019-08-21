# -*- coding: utf-8 -*-
import logging
import os
from collections import Counter
from datetime import datetime
from itertools import count
from pathlib import Path

import click
import networkx as nx
import numpy as np
import pandas as pd
import progressbar
import pymongo
from dotenv import find_dotenv, load_dotenv

from indiff import utils
from indiff.features import build_features
from indiff.twitter import Tweet


@click.command()
@click.argument('topic')
@click.argument('keywords_filepath', type=click.Path(exists=True))
def main(topic, keywords_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    current_date_and_time = datetime.now()

    # root directories
    root_dir = Path(__file__).resolve().parents[2]
    data_root_dir = os.path.join(root_dir, 'data')
    raw_data_root_dir = os.path.join(data_root_dir, 'raw')
    topic_raw_data_dir = os.path.join(raw_data_root_dir, topic)

    db_name = "info-diffusion"
    client = None

    try:
        if not os.path.exists(topic_raw_data_dir):
            raise FileExistsError(f'Dataset for {topic} does not exists.')

        client = pymongo.MongoClient(host='localhost', port=27017,
                                     appname=__file__)
        db = client[db_name]
        tweet_collection = db[topic]
        user_attribs_collection = db[topic + "-user-attr"]
        tweet_mentions_collection = db[topic + "-mentions"]

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
        n_user_ids = len(user_ids)

        for i, user_id in zip(count(start=1), user_ids):
            logging.info(f"PROCESSING NODE ATTR FOR {user_id}: "
                         f"{i} OF {n_user_ids} USERS")
            user = {'tweets': [],
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
                    'ratio_of_tweet_per_time_period': None,
                    'ratio_of_tweets_that_got_retweeted_per_time_period': None,
                    'ratio_of_retweet_per_time_period': None,
                    'A': None,
                    }

            query = {"user.id_str": user_id}
            user_tweets = tweet_collection.find(query)

            bar = progressbar.ProgressBar(prefix=f"Computing {user_id}'s "
                                          "Attributes: ")
            for user_tweet in bar(user_tweets):
                tweet = Tweet(user_tweet)

                if not user['followers_count']:
                    user['followers_count'] = tweet.owner_followers_count

                if not user['friends_count']:
                    user['friends_count'] = tweet.owner_friends_count

                user_description = tweet.owner_description
                if user_description and user['description'] is not None:
                    user['description'] = user_description

                orig_owner_id = tweet.original_owner_id
                if orig_owner_id != user_id:
                    user['all_possible_original_tweet_owners'].append(
                        orig_owner_id)

                if tweet.is_retweeted_tweet:
                    user['retweeted_tweets'].append(tweet.id)

                    if tweet.hashtags:
                        user['n_retweeted_tweets_with_hashtags'] += 1
                    if tweet.urls:
                        user['n_retweeted_tweets_with_urls'] += 1
                    if tweet.media:
                        user['n_retweeted_tweets_with_media'] += 1

                    if tweet.is_others_mentioned:
                        user['retweets_with_others_mentioned_count'] += 1

                    # fetch tweet dates
                    user['retweeted_tweets_dates'].append(tweet.created_at)
                elif tweet.is_quoted_tweet:
                    user['quoted_tweets'].append(tweet.id)

                    if tweet.hashtags:
                        user['n_quoted_tweets_with_hashtags'] += 1
                    if tweet.urls:
                        user['n_quoted_tweets_with_urls'] += 1
                    if tweet.media:
                        user['n_quoted_tweets_with_media'] += 1

                    if tweet.is_others_mentioned:
                        user['quoted_tweets_with_others_mentioned_count'] += 1

                    # fetch tweet dates
                    user['quoted_tweets_dates'].append(tweet.created_at)
                else:
                    user['tweets'].append(tweet.id)

                    if tweet.hashtags:
                        user['n_tweets_with_hashtags'] += 1
                    if tweet.urls:
                        user['n_tweets_with_urls'] += 1
                    if tweet.media:
                        user['n_tweets_with_media'] += 1

                    users_mentioned_in_tweet = tweet.users_mentioned
                    if users_mentioned_in_tweet:
                        user['users_mentioned_in_all_my_tweets'].extend(
                            users_mentioned_in_tweet)

                        # write tweet-user-mentioned to db
                        tweet_users_doc = {
                            "_id": tweet.id,
                            "users": users_mentioned_in_tweet
                        }

                        tweet_mentions_collection.insert_one(
                            tweet_users_doc)

                    if tweet.is_others_mentioned:
                        user['tweets_with_others_mentioned_count'] += 1

                    # fetch tweet dates
                    user['tweets_dates'].append(tweet.created_at)

                if tweet.is_favourited:
                    user['favorite_tweets_count'] += 1

                user['retweet_count'] += tweet.retweet_count

                if tweet.is_retweeted:
                    user['retweeted_count'] += 1

                user['keywords_in_all_my_tweets'].extend(tweet.keywords)

                if user['tweet_min_date'] == 0:
                    user['tweet_min_date'] = tweet.created_at

                if user['tweet_max_date'] == 0:
                    user['tweet_max_date'] = tweet.created_at

                if user['tweet_min_date'] > tweet.created_at:
                    user['tweet_min_date'] = tweet.created_at

                if user['tweet_max_date'] < tweet.created_at:
                    user['tweet_max_date'] = tweet.created_at

                # external_owner_id = tweet.original_owner_id
                # if external_owner_id:
                #     user['all_possible_original_tweet_owners'].add(
                #         external_owner_id)

                # TODO: recalculate for neutral
                if tweet.is_positive_sentiment:
                    user['positive_sentiment_count'] += 1
                else:
                    user['negative_sentiment_count'] += 1

                # calculate n_tweets_with_user_mentions
                if tweet.users_mentioned:
                    user['n_tweets_with_user_mentions'] += 1

            # compute ratio_of_tweet_per_time_period
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

            # compute ratio_of_tweets_that_got_retweeted_per_time_period
            periods = Counter()

            retweeted_tweets_dates = user['retweeted_tweets_dates']
            n_retweeted_tweets_dates = len(retweeted_tweets_dates)

            for tweet_date in retweeted_tweets_dates:
                h = tweet_date.hour

                if h in range(0, 24):
                    period = h // 6 + 1
                    periods[str(period)] += 1

            for key, value in periods.items():
                periods[key] = value / n_all_tweets_dates

            user['ratio_of_tweets_that_got_retweeted_per_time_period'] = periods

            # compute ratio_of_retweet_per_time_period
            periods = Counter()

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

            # compute get_A
            tweet_freq_table = {}

            for tweet_date in all_tweets_dates:
                tweet_date_and_time = tweet_date
                tweet_date = tweet_date_and_time.date
                tweet_hour = tweet_date_and_time.hour
                hour_bin = tweet_hour // 4

                tweet_freq_table.setdefault(
                    tweet_date, {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
                )[hour_bin] += 1

            results = pd.DataFrame(list(tweet_freq_table.values()))
            results = results / n_all_tweets_dates
            results = results.values
            sum_ = np.sum(results, axis=0)

            user['A'] = sum_.tolist()

            # write node attributes as document to database
            id_ = {"_id": user_id}
            new_document = {**id_, **user}

            try:
                user_attribs_collection.insert_one(new_document)
            except pymongo.errors.DuplicateKeyError:
                logger.info(f'updating node attribute for {user_id}')
                user_attribs_collection.replace_one(id_, new_document)
            except pymongo.errors.InvalidDocument as err:
                logger.error('found an invalid document')
                logger.error(err)

        # calculate extra attributes
        # TODO: look for a way to make this computationally effecient since
        # we can now query a database and just change a particular part of the
        # database.
        # get all tweets in database
        n_tweets = tweet_mentions_collection.count_documents({})
        if n_tweets:
            logger.info('update user attribs with tweets mentioned in')
            tweets = tweet_mentions_collection.find({})
            bar = progressbar.ProgressBar(maxlen=n_tweets)
            for tweet_document in bar(tweets):
                tweet_id = tweet_document['_id']
                users_mentioned = tweet_document['users']

                for user in users_mentioned:
                    # check if user exists in user_attribs_collection
                    query_user_attr = {"_id": user}
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

        keywords = utils.get_keywords_from_file(keywords_filepath)

        # prepare table for dataframe
        results = build_features.calculate_network_diffusion(
            nx.edges(social_network), keywords,
            node_collection=user_attribs_collection,
            tweet_collection=tweet_collection,
            additional_attr=True,
            do_not_add_sentiment=False)

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
