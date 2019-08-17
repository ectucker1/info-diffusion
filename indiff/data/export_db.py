# -*- coding: utf-8 -*-
import logging
import os
from pathlib import Path

import click
import progressbar
import pymongo
from dotenv import find_dotenv, load_dotenv
from sqlitedict import SqliteDict


@click.command()
@click.argument('topic')
def main(topic):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)

    # root directories
    root_dir = Path(__file__).resolve().parents[2]
    datastore_root_dir = os.path.join(root_dir, 'data', 'raw')
    filename = topic
    dataset_dir = os.path.join(datastore_root_dir, filename)

    try:
        if not os.path.exists(dataset_dir):
            raise FileExistsError(f'Dataset for {filename} does not exists.')

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    except (ValueError, FileNotFoundError, FileExistsError, KeyError) as error:
        logger.error(error)
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
        # social_network_filepath = list(network_filepath.glob('*.adjlist'))[0]

        # infer path to write corresponding reports
        # change 'data' to 'reports'
        # reomve 'raw' from original path
        parts = list(network_filepath.parts)
        parts[6] = 'reports'
        parts.pop(7)
        reports_filepath = Path(*parts)

        mydb = myclient["infodiffusion"]
        mycol = mydb[topic]

        # get tweet ids that fulfil the date range of interest
        with SqliteDict(filename=database_filepath.as_posix(),
                        tablename='tweet-objects') as tweets:
            # iterate over the tweets dataset to fetch desired result for nodes
            bar = progressbar.ProgressBar(maxval=len(tweets),
                                          prefix='Exporting Tweets: ')
            for tweet_id in bar(tweets):
                # export to mongodb
                tweet_ = tweets[tweet_id]._json
                _ = mycol.insert_one(tweet_)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
