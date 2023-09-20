from Tool_Pack import tools
from collections import defaultdict
import os
from pymongo import MongoClient
import time


class ContentGraph:

    def __init__(self, year):
        self.distance_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\" \
                             r"Tweet_Documents_Distance\\"
        self.tweets_per_date_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Indexes\\" \
                                    r"tweet_ids\date_dict_with_parents"
        self.year = year
        # Pymongo init
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.Climate_Change_Tweets
        self.collection = self.db.tweet_documents

    def calculate_distances(self):
        date_tweets = tools.load_pickle(self.tweets_per_date_path)








