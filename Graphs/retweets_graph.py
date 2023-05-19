import os
from pymongo import MongoClient, ASCENDING, errors
# import tweepy is needed because we load tweepy objects/pickles
import tweepy
from Tool_Pack import tools
import networkx as nx
from collections import Counter


class RTGraph:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"
        self.output_path = self.path + r"I_O\\"
        self.directed_graph = nx.DiGraph()
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.Climate_Change_Tweets
        self.col_tweets = self.db.tweet_documents
        self.col_superdocs = self.db.super_documents

    def seek_retweets(self):
        users_that_retweeted = set()
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    if hasattr(tweet_obj, "retweeted_status"):
                        users_that_retweeted.add(tweet_obj.author.id)
                        self.mongo_array_updater(tweet_obj)
        tools.save_pickle(self.output_path + r"bin\all_users_that_retweeted", users_that_retweeted)

    def mongo_array_updater(self, tweeter_object):
        author_id = tweeter_object.author.id
        retweeted_tweet_id = tweeter_object.retweeted_status.id
        retweeted_author = tweeter_object.retweeted_status.author.id
        author_record = self.col_superdocs.find_one({"author_id": author_id})
        rt_tweets_list = list()
        rt_authors_list = list()
        if "rt_tweets_list" in author_record:
            rt_tweets_list = author_record["rt_tweets_list"]
            rt_tweets_list.append(retweeted_tweet_id)
        else:
            rt_tweets_list.append(retweeted_tweet_id)
        if "rt_authors_list" in author_record:
            rt_authors_list = author_record["rt_authors_list"]
            rt_authors_list.append(retweeted_author)
        else:
            rt_authors_list.append(retweeted_author)

        self.col_superdocs.update_one({"author_id": author_id},
                                      {"$set": {"rt_tweets_list": rt_tweets_list,
                                                "rt_authors_list": rt_authors_list}})

    def populate_network(self):
        all_users_that_retweeted = tools.load_pickle(self.output_path + r"bin\all_users_that_retweeted")
        for user_id in all_users_that_retweeted:
            user_document = self.col_superdocs.find_one({"author_id": user_id})
            for rt_user_id in user_document["rt_authors_list"]:
                self.directed_graph.add_edge(user_id, rt_user_id)
            print()

    def create_csv_file(self):
        all_users_that_retweeted = tools.load_pickle(self.output_path + r"bin\all_users_that_retweeted")
        list_of_pairs = list()
        toral_l = len(all_users_that_retweeted)
        previous_ = 0
        for idx, user_id in enumerate(all_users_that_retweeted):
            user_document = self.col_superdocs.find_one({"author_id": user_id})
            if int(idx/toral_l) != previous_:
                print(int(idx/toral_l))
            for rt_user_id in user_document["rt_authors_list"]:
                list_of_pairs.append((user_id, rt_user_id))
        tools.save_pickle(self.output_path + r"bin\list_of_rt_edges_with_duplicates", list_of_pairs)
        exit()
        for x, y in Counter(list_of_pairs).items():
            print()

        with open(self.output_path + r"Graph_files\retweet_network_all.csv", "w") as csv_file:
            csv_file.write("source, target, weight\n")

    def digraph_test(self):
        for i in range(10):
            # self.directed_graph.add_weighted_edges_from([(1,4, 1), (1,2, 1), (5, 6, 1)], weight="weight")
            self.directed_graph.add_edge(1, 2, weight=10)
        print()

    def mongo_tests(self):
        dumy_list = [1, 2, 3, 4, 5, 6]
        db = self.client.Climate_Change_Tweets
        collection = db.test_base
        collection.insert_one({"author_id": 1, "author_username": "gg", "id_list": dumy_list})


if __name__ == "__main__":
    climate_rt_graph = RTGraph()
    # climate_rt_graph.seek_retweets()
    # climate_rt_graph.mongo_tests()
    # climate_rt_graph.populate_network()
    climate_rt_graph.create_csv_file()

    print()

