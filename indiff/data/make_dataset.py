# -*- coding: utf-8 -*-
import datetime
import logging
import os
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

    # root directories
    root_dir = Path(__file__).resolve().parents[2]
    datastore_root_dir = os.path.join(root_dir, 'data', 'raw')
    filename, _ = os.path.splitext(os.path.basename(network_filepath))
    dataset_dir = os.path.join(datastore_root_dir, filename)

    url = "http://example.com/"
    timeout = 5

    db_name = "info_diffusion"

    try:
        if os.path.exists(dataset_dir):
            raise FileExistsError(f'Dataset for {filename} already exists.')

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

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        db = myclient[db_name]
        col = db[filename]

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

        if not os.path.exists(dataset_dir):
            os.makedirs(dataset_dir)

        nodes = social_network.nodes()

        logger.info('downloading data set from raw data')
        tweet_count, error_ids = get_user_tweets_in_network(api=api,
                                                            users=nodes,
                                                            collection=col)

        logger.info('removing ids with error from graph')
        social_network.remove_nodes_from(error_ids)

        # free memory holding error ids
        del error_ids

        social_network.name = filename

        dataset_filepath = Path(dataset_dir)
        parts = list(dataset_filepath.parts)
        parts[6] = 'reports'
        parts.pop(7)
        reports_filepath = Path(*parts)
        if not os.path.exists(reports_filepath):
            os.makedirs(reports_filepath)

        logger.info("Building Features")
        network_filepath = Path(dataset_dir)
        if not network_filepath.is_absolute():
            raise ValueError('Expected an absolute path.')
        if not network_filepath.is_dir():
            raise ValueError('network_filepath path is not a directory.')

        parts = list(network_filepath.parts)
        parts[7] = 'processed'
        processed_path = Path(*parts)
        if not os.path.exists(processed_path):
            os.makedirs(processed_path)

        # infer path to write corresponding reports
        # change 'data' to 'reports'
        # reomve 'raw' from original path
        parts = list(network_filepath.parts)
        parts[6] = 'reports'
        parts.pop(7)
        reports_filepath = Path(*parts)

        # initialise node attributes to have desired info from dataset
        for user_id in nx.nodes(social_network):
            attr = {user_id: {'tweets': dict(),
                              'tweets_with_hashtags': dict(),
                              'tweets_with_urls': dict(),
                              'tweets_with_media': dict(),
                              'users_who_mentioned_me': set(),
                              'tweets_with_others_mentioned_count': 0,
                              'mentioned_in': set(),
                              'users_mentioned_in_all_my_tweets': set(),
                              'keywords_in_all_my_tweets': set(),
                              'all_possible_original_tweet_owners': set(),
                              'retweeted_tweets': dict(),
                              'retweeted_tweets_with_hashtags': dict(),
                              'retweeted_tweets_with_urls': dict(),
                              'retweeted_tweets_with_media': dict(),
                              'users_mentioned_in_all_my_retweets': set(),
                              'retweets_with_others_mentioned_count': 0,
                              'retweet_count': 0,
                              'retweeted_count': 0,
                              'quoted_tweets': dict(),
                              'quoted_tweets_with_hashtags': dict(),
                              'quoted_tweets_with_urls': dict(),
                              'quoted_tweets_with_media': dict(),
                              'users_mentioned_in_all_my_quoted_tweets': set(),
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
                    }

            nx.set_node_attributes(social_network, attr)

        # iterate over the tweets dataset to fetch desired result for nodes
        bar = progressbar.ProgressBar(prefix='Computing Node Attributes: ')
        for tweet in bar(col.find()):
            tweet = Tweet(tweet)
            user_id = tweet.owner_id

            user = social_network._node[tweet.owner_id]

            user['followers_count'] = tweet.owner_followers_count
            user['friends_count'] = tweet.owner_friends_count

            orig_owner_id = tweet.original_owner_id
            if orig_owner_id != user_id:
                user['all_possible_original_tweet_owners'].add(
                    orig_owner_id)

            user_description = tweet.owner_description

            if user_description:
                user['description'] = user_description

            if tweet.is_retweeted_tweet:
                user['retweeted_tweets'][tweet.id] = tweet

                if tweet.hashtags:
                    user['retweeted_tweets_with_hashtags'][tweet.id] = tweet
                if tweet.urls:
                    user['retweeted_tweets_with_urls'][tweet.id] = tweet
                if tweet.media:
                    user['retweeted_tweets_with_media'][tweet.id] = tweet

                users_mentioned_in_tweet = tweet.users_mentioned
                user['users_mentioned_in_all_my_retweets'].update(
                    users_mentioned_in_tweet)

                if tweet.is_others_mentioned:
                    user['retweets_with_others_mentioned_count'] += 1

            elif tweet.is_quoted_tweet:
                user['quoted_tweets'][tweet.id] = tweet

                if tweet.hashtags:
                    user['quoted_tweets_with_hashtags'][tweet.id] = tweet
                if tweet.urls:
                    user['quoted_tweets_with_urls'][tweet.id] = tweet
                if tweet.media:
                    user['quoted_tweets_with_media'][tweet.id] = tweet

                users_mentioned_in_tweet = tweet.users_mentioned
                user['users_mentioned_in_all_my_quoted_tweets'].update(
                    users_mentioned_in_tweet)

                if tweet.is_others_mentioned:
                    user['quoted_tweets_with_others_mentioned_count'] += 1

            else:
                user['tweets'][tweet.id] = tweet

                if tweet.hashtags:
                    user['tweets_with_hashtags'][tweet.id] = tweet
                if tweet.urls:
                    user['tweets_with_urls'][tweet.id] = tweet
                if tweet.media:
                    user['tweets_with_media'][tweet.id] = tweet

                users_mentioned_in_tweet = tweet.users_mentioned
                user['users_mentioned_in_all_my_tweets'].update(
                    users_mentioned_in_tweet)

                if tweet.is_others_mentioned:
                    user['tweets_with_others_mentioned_count'] += 1

            if tweet.is_favourited:
                user['favorite_tweets_count'] += 1

            if users_mentioned_in_tweet:
                for other_user in users_mentioned_in_tweet:
                    if other_user in social_network:
                        social_network._node[other_user]['users_who_mentioned_me'].update(
                            user_id)
                        social_network._node[other_user]['mentioned_in'].update(tweet.id)

            user['retweet_count'] += tweet.retweet_count

            if tweet.is_retweeted:
                user['retweeted_count'] += 1

            user['keywords_in_all_my_tweets'].update(tweet.keywords)

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

        keywords = utils.get_keywords_from_file(keywords_filepath)

        # prepare table for dataframe
        results = build_features.calculate_network_diffusion(
            nx.edges(social_network), keywords, graph=social_network,
            additional_attr=True, do_not_add_sentiment=False)

        df = pd.DataFrame(results)

        # save processed dataset to hdf file
        key = filename
        processed_saveas = os.path.join(processed_path, 'dataset.h5')
        df.to_hdf(processed_saveas, key=filename)

        # save key to reports directory
        key_saveas = os.path.join(reports_filepath, 'dataset.keys')
        with open(key_saveas, 'a') as f:
            f.write('\n***\n\nmake_dataset.py '
                    f'started at {current_date_and_time}')
            f.write(f'\nKey: {filename}\n\n')

        nx.write_adjlist(social_network,
                         os.path.join(dataset_dir, f'{filename}.adjlist'),
                         delimiter=',')

        graph_info_saveas = os.path.join(reports_filepath,
                                         f'{filename}-crawl-stats.txt')
        with open(graph_info_saveas, 'w') as f:
            f.write(f'###* Info for {filename}, started at '
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
