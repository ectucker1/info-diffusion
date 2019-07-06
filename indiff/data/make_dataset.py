# -*- coding: utf-8 -*-
import datetime
import logging
import os
from pathlib import Path

import click
import networkx as nx
import pandas as pd
import progressbar
from dotenv import find_dotenv, load_dotenv
from sqlitedict import SqliteDict

from indiff import utils
from indiff.exception import ZeroTweetError
from indiff.features import build_features
from indiff.twitter import Tweet


@click.command()
@click.option('--input', type=click.Path(exists=True))
@click.option('--keywords', type=click.Path(exists=True))
@click.option('--start', required=True, help='start date for tweet crawl')
@click.option('--end', required=True, help='end date for tweet crawl')
def main(input, keywords, start, end):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """

    current_date_and_time = datetime.datetime.now()

    if start.isdigit() and end.isdigit():
        raise ValueError("Both start and end dates cannot be digits.")

    if start.isdigit():
        # start is therefore the number of days from end date
        diff = datetime.timedelta(int(start))
        end = utils.construct_datetime_from_str(end)
        start = end - diff
    elif end.isdigit():
        diff = datetime.timedelta(int(end))
        start = utils.construct_datetime_from_str(start)
        end = start + diff
    else:
        start = utils.construct_datetime_from_str(start)
        end = utils.construct_datetime_from_str(end)

    # get input and create result destination dir from it
    # input_path = Path(input)
    input = Path(input)
    if not input.is_absolute():
        raise ValueError('Expected an absolute path.')
    if not input.is_dir():
        raise ValueError('input path is not a directory.')

    parts = list(input.parts)
    parts[-6] = 'processed'
    processed_path = Path(*parts)
    if not os.path.exists(processed_path):
        os.makedirs(processed_path)

    graph_filepath = list(input.glob('*.adjlist'))[0]
    database_filepath = list(input.glob('*.sqlite'))[0]

    # infer path to write corresponding reports
    # change 'data' to 'reports'
    # reomve 'raw' from original path
    parts = list(input.parts)
    parts[-7] = 'reports'
    parts.pop(-6)
    reports_filepath = Path(*parts)

    stats_filepath = list(input.glob('*.txt'))[0]

    # make sure the dates provided here are with the range of avaliable dataset
    with open(stats_filepath) as f:
        stats = f.readlines()
        stats = [s.strip('\n') for s in stats]
        stats = stats[-3:]

    file_start_date = stats[0].split(' ')
    file_start_date = utils.construct_datetime_from_str(file_start_date[-2])

    file_end_date = stats[1].split(' ')
    file_end_date = utils.construct_datetime_from_str(file_end_date[-2])

    n_tweets = stats[2].split(' ')
    n_tweets = int(n_tweets[-1])

    if n_tweets == 0:
        raise ZeroTweetError('There are zero tweets in the database.')

    n_min_tweets = 2
    if n_tweets < n_min_tweets:
        raise ValueError(f'Found {n_tweets} tweets in database. '
                         f'Get more data (>{n_min_tweets}).')

    if not file_start_date <= start <= file_end_date:
        raise ValueError(
            f'Start date "{start.date()}" is out of range'
            f'({str(file_start_date.date())}, '
            f'{str((file_end_date + datetime.timedelta(1)).date())}).')

    if not file_start_date <= end <= file_end_date:
        raise ValueError(
            f'End date "{end.date()}" is out of range'
            f'({str(file_start_date.date())}, '
            f'{str((file_end_date + datetime.timedelta(1)).date())}).')

    # get tweet ids that fulfil the date range of interest
    with SqliteDict(filename=database_filepath.as_posix(),
                    tablename='tweet-objects') as tweets:
        sub_tweet_ids = {
            tweet_id for tweet_id, tweet in tweets.items()
            if start <= tweet.created_at <= end + datetime.timedelta(1)
            }

        logging.info("Load graph from file")
        sn = nx.read_adjlist(graph_filepath.as_posix(), delimiter=',',
                             create_using=nx.DiGraph())

        # shkrink graph by removing users who dont fulfil the date of interest
        valid_ids = {tweets[tweet_id].user.id_str
                     for tweet_id in sub_tweet_ids}
        error_ids = sn.nodes() - valid_ids
        error_len = len(error_ids)
        logging.info(f"Removing {error_len} error ids from social network")
        sn.remove_nodes_from(error_ids)

        # initialise node attributes to have desired info from dataset
        for user_id in nx.nodes(sn):
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
                              'friends_ids': []}
                    }

            nx.set_node_attributes(sn, attr)

        keywords = utils.get_keywords_from_file(keywords)

        # iterate over the tweets dataset to fetch desired result for nodes
        bar = progressbar.ProgressBar(
            maxval=len(sub_tweet_ids),
            prefix='Computing Node Attributes ').start()
        for i, (tweet_id) in enumerate(sub_tweet_ids):
            status = tweets[tweet_id]
            tweet = Tweet(status._json)
            user_id = tweet.owner_id

            user = sn._node[tweet.owner_id]

            orig_owner_id = tweet.original_owner_id(tweets)

            user['followers_count'] = tweet.owner_followers_count
            user['friends_count'] = tweet.owner_friends_count

            if orig_owner_id and orig_owner_id != user_id:
                user['all_possible_original_tweet_owners'].add(orig_owner_id)

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
                    if other_user in sn:
                        sn._node[other_user]['users_who_mentioned_me'].update(
                            user_id)
                        sn._node[other_user]['mentioned_in'].update(tweet_id)

            user['retweet_count'] += tweet.retweet_count

            if tweet.is_retweeted:
                user['retweeted_count'] += 1

            user['keywords_in_all_my_tweets'].update(tweet.keywords)

            external_owner_id = tweet.original_owner_id(tweets)
            if external_owner_id:
                user['all_possible_original_tweet_owners'].add(
                    external_owner_id)

            # TODO: recalculate for neutral
            if tweet.is_positive_sentiment:
                user['positive_sentiment_count'] += 1
            else:
                user['negative_sentiment_count'] += 1
            bar.update(i)
        bar.finish()

    n_days = (end - start).days

    # prepare table for dataframe
    results = build_features.calculate_network_diffusion(
        nx.edges(sn), n_days, keywords, graph=sn,
        additional_attr=True, do_not_add_sentiment=False)

    df = pd.DataFrame(results)

    # save processed dataset to hdf file
    key = utils.generate_random_id(length=15)
    processed_saveas = os.path.join(processed_path, 'dataset.h5')
    df.to_hdf(processed_saveas, key=key)

    if not os.path.exists(reports_filepath):
        os.makedirs(reports_filepath)

    # save key to reports directory
    key_saveas = os.path.join(reports_filepath, 'dataset.keys')
    with open(key_saveas, 'a') as f:
        f.write(f'\n***\n\nmake_dataset.py started at {current_date_and_time}')
        f.write(f'\nn_days: {n_days}')
        f.write(f'\nStart Date: {str(start)}')
        f.write(f'\nEnd Date: {str(end)}')
        f.write(f'\nKey: {key}\n\n')

    graph_info_saveas = os.path.join(reports_filepath, 'network-stats.txt')
    with open(graph_info_saveas, 'a') as f:
        f.write(f'\n###\n\nmake_dataset.py started at {current_date_and_time}')
        f.write(f'\nStart Date: {str(start)}')
        f.write(f'\nEnd Date: {str(end)}')
        f.write(f'\nn_days: {n_days}')
        f.write(f'\nNumber of tweets: {len(sub_tweet_ids)}')
        f.write(f'\n{nx.info(sn)}\n\n')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
