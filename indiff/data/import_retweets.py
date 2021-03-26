import click
import json
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


@click.command()
@click.argument('retweets_file', type=click.Path(exists=True))
@click.argument('database_name', type=str)
@click.argument('collection_name', type=str)
def main(retweets_file, database_name, collection_name):

    # Create a list to hold the retweets temporarily
    retweets = []

    # Connect to MongoDB and get our database and collections
    client = connect_database()
    database = client[database_name]
    tweets_collection = database[collection_name]

    # Create an index so we can query this later
    tweets_collection.create_index("user.id_str")

    # For each retweet
    for retweet in load_retweets(retweets_file):
        # For old tweet format: Retweets store retweeted tweets recursively
        if 'retweeted_status' not in retweet:
            print('Not a valid retweet')
        # If we have a valid retweet
        else:
            retweets.append(retweet)

            # Print to show progress
            if len(retweets) % 1000 == 0:
                print('Saving batch of 1000 retweets')
                tweets_collection.insert_many(retweets)

    print('Saving final batch of retweets')
    tweets_collection.insert_many(retweets)


# Loads all the retweets parsed from JSON at a given path
def load_retweets(path):
    with open(path, 'r') as file:
        for retweet in file:
            retweet = retweet.strip()
            if len(retweet) > 0:
                yield json.loads(retweet)


# Connect to the MongoDB database and return the client
def connect_database():
    try:
        client = MongoClient('localhost:27017', serverSelectionTimeoutMS=10)
        client.server_info()
        return client
    except ServerSelectionTimeoutError:
        print('Could not connect to database.')


if __name__ == '__main__':
    main()
