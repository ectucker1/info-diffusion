# -*- coding: utf-8 -*-
import datetime
import logging
import os
from itertools import count
from pathlib import Path

import click
import networkx as nx
import pandas as pd
import progressbar
import pymongo
import requests
from dotenv import find_dotenv, load_dotenv

from indiff import utils
from indiff.features import build_features
from indiff.twitter import API, Tweet, get_user_tweets_in_network


@click.command()
@click.argument('network_filepath', type=click.Path(exists=True))
@click.argument('keywords_filepath', type=click.Path(exists=True))
def main(network_filepath, keywords_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    current_date_and_time = datetime.datetime.now()

    topic, _ = os.path.splitext(os.path.basename(network_filepath))

    # root directories
    root_dir = Path(__file__).resolve().parents[2]
    data_root_dir = os.path.join(root_dir, 'data')
    raw_data_root_dir = os.path.join(data_root_dir, 'raw')
    topic_raw_data_dir = os.path.join(raw_data_root_dir, topic)

    url = "http://example.com/"
    timeout = 5

    db_name = "info-diffusion"

    try:
        # test internet conncetivity is active
        req = requests.get(url, timeout=timeout)
        req.raise_for_status()

        # prepare credentials for accessing twitter API
        consumer_key = os.environ.get('CONSUMER_KEY')
        consumer_secret = os.environ.get('CONSUMER_SECRET')
        access_token = os.environ.get('ACCESS_TOKEN')
        access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')

        auth = API(consumer_key=consumer_key,
                   consumer_secret=consumer_secret,
                   access_token=access_token,
                   access_token_secret=access_token_secret)

        api = auth()

        myclient = pymongo.MongoClient('localhost', 27017)
        db = myclient[db_name]
        col = db[topic]
        node_collection = db[topic + "-nodes"]

        # build initial graph from file
        social_network = nx.read_edgelist(network_filepath, delimiter=',',
                                          create_using=nx.DiGraph())
    except (ValueError, FileNotFoundError, FileExistsError, KeyError) as error:
        logger.error(error)
    except requests.HTTPError as e:
        logger.error("Checking internet connection failed, "
                     f"status code {e.response.status_code}")
    except requests.ConnectionError:
        logger.error("No internet connection available.")
    else:

        if not os.path.exists(topic_raw_data_dir):
            os.makedirs(topic_raw_data_dir)

        user_ids = nx.nodes(social_network)

        logger.info('downloading data set from raw data')
        tweet_count, error_ids = get_user_tweets_in_network(api=api,
                                                            users=user_ids,
                                                            collection=col,
                                                            n_tweets=1000000)

        logger.info('removing ids with error from graph')
        social_network.remove_nodes_from(error_ids)

        # free memory holding error ids
        del error_ids

        social_network.name = topic

        topic_raw_data_dir = Path(topic_raw_data_dir)

        # processed file path
        parts = list(topic_raw_data_dir.parts)
        _ = parts.pop()
        parts[-1] = 'processed'
        processed_root_dir = Path(*parts)

        # reports file path
        parts = list(topic_raw_data_dir.parts)
        if parts[-2] != 'raw':
            raise ValueError(f'Not an expected file path. Expected value: raw')
        _ = parts.pop(-2)
        parts[-2] = 'reports'
        topic_reports_dir = Path(*parts)

        # initialise node attributes to have desired info from dataset
        n_user_ids = len(user_ids)
        for i, user_id in zip(count(start=1), user_ids):
            logging.info(f"PROCESSING NODE ATTR FOR {i} OF {n_user_ids} USERS")
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
                    }

            query = {"user.id_str": user_id}
            user_tweets = col.find(query)

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
                else:
                    user['tweets'].append(tweet.id)

                    if tweet.hashtags:
                        user['n_tweets_with_hashtags'] += 1
                    if tweet.urls:
                        user['n_tweets_with_urls'] += 1
                    if tweet.media:
                        user['n_tweets_with_media'] += 1

                    users_mentioned_in_tweet = tweet.users_mentioned
                    user['users_mentioned_in_all_my_tweets'].extend(
                        users_mentioned_in_tweet)

                    if tweet.is_others_mentioned:
                        user['tweets_with_others_mentioned_count'] += 1

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

            # write node attributes as document to database
            id_ = {"_id": user_id}
            new_document = {**id_, **user}

            try:
                node_collection.insert_one(new_document)
            except pymongo.errors.DuplicateKeyError:
                logger.info(f'updating node attribute for {user_id}')
                node_collection.replace_one(id_, new_document)

        # free user memory from previous ieration
        del user

        # calculate extra attributes
        # TODO: look for a way to make this computationally effecient since
        # we can now query a database and just change a particular part of the
        # database.
        logger.info('computing mentioned in')
        bar = progressbar.ProgressBar(maxlen=n_user_ids,
                                      prefix=f"Computing {user_id}'s "
                                      "Attributes: ")
        for user_id in bar(user_ids):
            query = {"user.id_str": user_id}
            user_tweets = col.find(query)

            for user_tweet in user_tweets:
                tweet = Tweet(user_tweet)
                users_mentioned_in_tweet = tweet.users_mentioned
                if users_mentioned_in_tweet:
                    for other_user in users_mentioned_in_tweet:
                        if other_user in social_network:
                            query = {'_id': other_user}
                            attr = node_collection.find_one(query)
                            attr['mentioned_in'].append(tweet.id)
                            node_collection.replace_one(query, attr)

        keywords = utils.get_keywords_from_file(keywords_filepath)

        # prepare table for dataframe
        results = build_features.calculate_network_diffusion(
            nx.edges(social_network), keywords,
            node_collection=node_collection, tweet_collection=col,
            additional_attr=True,
            do_not_add_sentiment=False)

        df = pd.DataFrame(results)

        # save processed dataset to hdf file
        key = utils.generate_random_id(15)
        if not os.path.exists(processed_root_dir):
            os.makedirs(processed_root_dir)
        processed_saveas = os.path.join(processed_root_dir, 'dataset.h5')
        df.to_hdf(processed_saveas, key=key)

        # save key to reports directory
        if not os.path.exists(topic_reports_dir):
            os.makedirs(topic_reports_dir)
        key_saveas = os.path.join(topic_reports_dir.parent, 'dataset.keys')
        with open(key_saveas, 'a') as f:
            f.write('\n***\n\nmake_dataset.py '
                    f'started at {current_date_and_time}')
            f.write(f'\nNetwork path: {network_filepath}')
            f.write(f'\nTopic: {topic}')
            f.write(f'\nKey: {key}\n\n')

        nx.write_adjlist(social_network,
                         os.path.join(topic_raw_data_dir, f'{topic}.adjlist'),
                         delimiter=',')

        graph_info_saveas = os.path.join(topic_reports_dir,
                                         f'{topic}-crawl-stats.txt')
        with open(graph_info_saveas, 'w') as f:
            f.write(f'###* Info for {topic}, started at '
                    f'{current_date_and_time}.\n#\n#\n')
            f.write(nx.info(social_network))
            f.write(f'\nNumber of tweets: {tweet_count}')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
