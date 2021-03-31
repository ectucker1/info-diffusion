import pymongo


def main():
    # Script parameters
    db_name = "RPE_twitteranniv"
    event_db_name = "RPE_twitteranniv"
    topic = "twitt_anniv"

    # Connect to database server
    client = pymongo.MongoClient(host='localhost', port=27017, appname=__file__)

    # Get all the collections
    db = client[db_name]
    event_db = client[event_db_name]
    tweet_collection = db[topic]
    users_collection = db[topic + "-users"]
    retweets_collection = db[topic + "-retweets"]
    event_tweets_collection = event_db[topic + "-event_tweets"]

    # Create indices for tweets collection
    print("Creating indices for tweet collection")
    tweet_collection.create_index("id")
    tweet_collection.create_index("author_id")

    # Create indices for users collections
    print("Creating indices for users collection")
    users_collection.create_index("id")
    users_collection.create_index("username")

    # Create indices for retweets collection (old format)
    print("Creating indices for retweets collection")
    retweets_collection.create_index("id_str")
    retweets_collection.create_index("user.id_str")

    # Create indices for event tweets collection
    print("Creating indices for event tweets collection")
    event_tweets_collection.create_index("id")
    event_tweets_collection.create_index("author_id")


if __name__ == '__main__':
    main()
