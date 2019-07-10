# -*- coding: utf-8 -*-
import datetime
import logging
import os
from pathlib import Path

import click
import networkx as nx
import requests
from dotenv import find_dotenv, load_dotenv

from indiff.twitter import API, get_all_tweets_in_network_and_build_tables


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
def main(input_filepath):
    """ Download tweets given a network file (saved in ../../data/raw.)
    """
    # additional data to be used for save
    current_date_and_time = datetime.datetime.now()

    # root directories
    root_dir = Path(__file__).resolve().parents[2]
    datastore_root_dir = os.path.join(root_dir, 'data', 'raw')
    filename, _ = os.path.splitext(os.path.basename(input_filepath))

    # make directories of year/month/day-of-crawl/time-of-crawl=hr-mins
    # todo: correctly break the next line
    # additional_filename = f'{current_date_and_time.year}/'\
    #    f'{current_date_and_time.month}/{current_date_and_time.day}/'\
    #    f'{current_date_and_time.hour}-{current_date_and_time.minute}'

    dataset_dir = os.path.join(datastore_root_dir, filename)
    database_file_path = os.path.join(dataset_dir, f'{filename}-tweets.sqlite')

    logger = logging.getLogger(__name__)
    logger.info('downloading data set from raw data')

    url = "http://example.com/"
    timeout = 5

    try:
        # test internet conncetivity is active
        req = requests.get(url, timeout=timeout)
        req.raise_for_status()

        # prepare credentials for accessing twitter API
        consumer_key = os.environ.get('CONSUMER_KEY')
        consumer_secret = os.environ.get('CONSUMER_SECRET')
        access_token = os.environ.get('ACCESS_TOKEN')
        access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')

        auth = API(consumer_key=consumer_key, consumer_secret=consumer_secret,
                   access_token=access_token,
                   access_token_secret=access_token_secret)

        api = auth()

        # build initial graph from file
        social_network = nx.read_edgelist(input_filepath, delimiter=',',
                                          create_using=nx.DiGraph())
    except (ValueError, FileNotFoundError, KeyError) as error:
        logging.error(error)
    except requests.HTTPError as e:
        logging.error(
            "Checking internet connection failed, status code {0}".format(
                e.response.status_code
                )
            )
    except requests.ConnectionError:
        logging.error("No internet connection available.")
    else:
        nodes = social_network.nodes()

        if not os.path.exists(dataset_dir):
            os.makedirs(dataset_dir)

        tweet_count, error_ids = get_all_tweets_in_network_and_build_tables(
            api, user_ids=nodes, database_file_path=database_file_path)

        social_network.remove_nodes_from(error_ids)

        nx.write_adjlist(social_network,
                         os.path.join(dataset_dir, f'{filename}.adjlist'),
                         delimiter=',')

        social_network.name = filename

        graph_info_saveas = os.path.join(dataset_dir,
                                         f'{filename}-crawl-stats.txt')
        with open(graph_info_saveas, 'w') as f:
            f.write(f'###* Info for {filename}, started at '
                    f'{current_date_and_time}.\n#\n#\n')
            f.write(f'\nNumber of tweets: {tweet_count}')
            f.write(f'\nNumber of users: {len(nodes)}')

        print('\n\n\n')
        logging.info(f'Use {dataset_dir} as an input_filepath to make_dataset.py')
        print('\n\n\n')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
