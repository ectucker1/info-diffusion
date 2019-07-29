# -*- coding: utf-8 -*-
import datetime
import logging
import os
from pathlib import Path

import click
import networkx as nx
import pandas as pd
import progressbar
import requests
from dotenv import find_dotenv, load_dotenv
from sqlitedict import SqliteDict

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
    current_date_and_time = datetime.datetime.now()

    # root directories
    root_dir = Path(__file__).resolve().parents[2]
    datastore_root_dir = os.path.join(root_dir, 'data', 'raw')
    filename = topic
    dataset_dir = os.path.join(datastore_root_dir, filename)

    try:
        if not os.path.exists(dataset_dir):
            raise FileExistsError(f'Dataset for {filename} does not already \
                                  exists.')

    except (ValueError, FileNotFoundError, FileExistsError, KeyError) as error:
        logger.error(error)
    except requests.HTTPError as e:
        logger.error("Checking internet connection failed, "
                     f"status code {e.response.status_code}")
    except requests.ConnectionError:
        logger.error("No internet connection available.")
    else:

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

        database_filepath = list(network_filepath.glob('*.sqlite'))[0]
        social_network_filepath = list(network_filepath.glob('*.adjlist'))[0]

        social_network = nx.read_adjlist(social_network_filepath,
                                         delimiter=',',
                                         create_using=nx.DiGraph)

        # infer path to write corresponding reports
        # change 'data' to 'reports'
        # reomve 'raw' from original path
        parts = list(network_filepath.parts)
        parts[6] = 'reports'
        parts.pop(7)
        reports_filepath = Path(*parts)

        # get tweet ids that fulfil the date range of interest
        with SqliteDict(filename=database_filepath.as_posix(),
                        tablename='tweet-objects') as tweets:

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
            bar = progressbar.ProgressBar(
                maxval=len(tweets),
                prefix='Computing Node Attributes ').start()
            for i, (tweet_id, status) in enumerate(tweets.items()):
                tweet = Tweet(status._json)
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
                    user['retweeted_tweets'][tweet_id] = tweet

                    if tweet.hashtags:
                        user['retweeted_tweets_with_hashtags'][tweet_id] = tweet
                    if tweet.urls:
                        user['retweeted_tweets_with_urls'][tweet_id] = tweet
                    if tweet.media:
                        user['retweeted_tweets_with_media'][tweet_id] = tweet

                    users_mentioned_in_tweet = tweet.users_mentioned
                    user['users_mentioned_in_all_my_retweets'].update(
                        users_mentioned_in_tweet)

                    if tweet.is_others_mentioned:
                        user['retweets_with_others_mentioned_count'] += 1

                elif tweet.is_quoted_tweet:
                    user['quoted_tweets'][tweet_id] = tweet

                    if tweet.hashtags:
                        user['quoted_tweets_with_hashtags'][tweet_id] = tweet
                    if tweet.urls:
                        user['quoted_tweets_with_urls'][tweet_id] = tweet
                    if tweet.media:
                        user['quoted_tweets_with_media'][tweet_id] = tweet

                    users_mentioned_in_tweet = tweet.users_mentioned
                    user['users_mentioned_in_all_my_quoted_tweets'].update(
                        users_mentioned_in_tweet)

                    if tweet.is_others_mentioned:
                        user['quoted_tweets_with_others_mentioned_count'] += 1

                else:
                    user['tweets'][tweet_id] = tweet

                    if tweet.hashtags:
                        user['tweets_with_hashtags'][tweet_id] = tweet
                    if tweet.urls:
                        user['tweets_with_urls'][tweet_id] = tweet
                    if tweet.media:
                        user['tweets_with_media'][tweet_id] = tweet

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
                            social_network._node[other_user]['mentioned_in'].update(tweet_id)

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
                bar.update(i)
            bar.finish()

        keywords = utils.get_keywords_from_file(keywords_filepath)

        # prepare table for dataframe
        results = build_features.calculate_network_diffusion(
            nx.edges(social_network), keywords, graph=social_network,
            additional_attr=True, do_not_add_sentiment=False)

        df = pd.DataFrame(results)

        # save processed dataset to hdf file
        key = utils.generate_random_id(length=15)
        processed_saveas = os.path.join(processed_path, 'dataset.h5')
        if os.path.exists(processed_saveas):
            os.remove(processed_saveas)
            print(f'Removed {processed_saveas} for a new one.')
        df.to_hdf(processed_saveas, key=key)

        # save key to reports directory
        key_saveas = os.path.join(reports_filepath, 'dataset.keys')

        if os.path.exists(key_saveas):
            os.remove(key_saveas)
            print(f'Removed {key_saveas} for a new one.')

        with open(key_saveas, 'w') as f:
            f.write('\n***\n\nmake_dataset.py '
                    f'started at {current_date_and_time}')
            f.write(f'\nKey: {key}\n\n')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
