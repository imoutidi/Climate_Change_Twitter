import os
import re
from pymongo import MongoClient, ASCENDING, errors
# import tweepy is needed because we load tweepy objects/pickles
import tweepy
from Tool_Pack import tools
import networkx as nx
from collections import Counter
from operator import itemgetter

import backboning


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
        # This part of code creates pairs of source user and retweeted user.
        # /////////////////////////////////////////////////
        # all_users_that_retweeted = tools.load_pickle(self.output_path + r"bin\all_users_that_retweeted")
        # list_of_pairs = list()
        # for idx, user_id in enumerate(all_users_that_retweeted):
        #     user_document = self.col_superdocs.find_one({"author_id": user_id})
        #     for rt_user_id in user_document["rt_authors_list"]:
        #         list_of_pairs.append((user_id, rt_user_id))
        # tools.save_pickle(self.output_path + r"bin\list_of_rt_edges_with_duplicates", list_of_pairs)

        # list_of_pairs = tools.load_pickle(self.output_path + r"bin\list_of_rt_edges_with_duplicates")
        list_of_top_pairs = tools.load_pickle(self.output_path + r"bin\list_of_top_pairs_10_percent_no_self_rt")
        # We will keep only alphanumerics
        regex = re.compile('[^a-zA-Z0-9]')
        with open(self.output_path + r"Graph_files\retweet_network_top_10_with_author_names_no_retweets.csv",
                  "w", encoding='utf-8') as csv_file:
            csv_file.write("source, target, weight\n")
            counter = 0
            for rt_pair, num_of_rts in list_of_top_pairs.items():
                print(counter)
                counter += 1
                if self.col_tweets.find_one({"author_id": rt_pair[0]}):
                    name_pair0 = self.col_tweets.find_one({"author_id": rt_pair[0]})["author_username"]
                    name_pair0 = regex.sub('', name_pair0)
                    # if the regex cleared all character and the username is now blank we
                    # insert the author_id as label.
                    if len(name_pair0) == 0:
                        name_pair0 = str(rt_pair[0])
                    if self.col_tweets.find_one({"author_id": rt_pair[1]}):
                        name_pair1 = self.col_tweets.find_one({"author_id": rt_pair[1]})["author_username"]
                        name_pair1 = regex.sub('', name_pair1)
                        # if the regex cleared all character and the username is now blank we
                        # insert the author_id as label.
                        if len(name_pair1) == 0:
                            name_pair1 = str(rt_pair[1])
                        csv_file.write(name_pair0 + ", " + name_pair1 + ", " + str(num_of_rts) + "\n")

    # Keeping retweets only between the top X percent of users based on the number of retweets they have received
    # in-degree in the retweet di_graph
    def connections_of_authorities(self):
        set_of_all_nodes = set()
        list_of_pairs = tools.load_pickle(self.output_path + r"bin\list_of_rt_edges_with_duplicates")
        for rt_pair in list_of_pairs:
            set_of_all_nodes.add(rt_pair[0])
            set_of_all_nodes.add(rt_pair[1])
        counted_list_of_pairs = Counter(list_of_pairs)
        for rt_pair, num_of_rts in counted_list_of_pairs.items():
            self.directed_graph.add_edge(rt_pair[0], rt_pair[1], weight=num_of_rts)
        in_degrees = list(self.directed_graph.in_degree)
        # This is 5% of all the nodes on the retweet network.
        top_in_degrees = sorted(in_degrees, key=itemgetter(1), reverse=True)[:180]
        # indexing for lookups
        top_users = {s[0] for s in top_in_degrees}
        list_of_top_pairs = dict()
        for idx, top_user in enumerate(top_in_degrees):
            print(idx)
            for rt_pair, num_of_rts in counted_list_of_pairs.items():
                if top_user[0] == rt_pair[0] and rt_pair[1] in top_users:
                    list_of_top_pairs[rt_pair] = num_of_rts
        # removing self retweets.
        pairs_to_be_removed = list()
        for top_pair in list_of_top_pairs:
            if top_pair[0] == top_pair[1]:
                pairs_to_be_removed.append(top_pair)
        print()
        for t_pair in pairs_to_be_removed:
            del list_of_top_pairs[t_pair]
        # tools.save_pickle(self.output_path + r"bin\list_of_top_pairs_10_percent_no_self_rt", list_of_top_pairs)

    def creation_of_digraph(self):
        list_of_top_pairs = tools.load_pickle(self.output_path + r"bin\list_of_top_pairs_1_percent_no_self_rt")
        for rt_pair, num_of_rts in list_of_top_pairs.items():
            self.directed_graph.add_edge(rt_pair[0], rt_pair[1], weight=num_of_rts)


    def mongo_tests(self):
        dumy_list = [1, 2, 3, 4, 5, 6]
        db = self.client.Climate_Change_Tweets
        collection = db.test_base
        collection.insert_one({"author_id": 1, "author_username": "gg", "id_list": dumy_list})

    def backboning_graph(self):
        table, nnodes, nnedges = backboning.read("/path/to/input", "column_of_interest")
        nc_table = backboning.noise_corrected(table)
        nc_backbone = backboning.thresholding(nc_table, threshold_value)
        backboning.write(nc_backbone, "network_name", "nc", "/path/to/output")
        print()



if __name__ == "__main__":
    climate_rt_graph = RTGraph()
    # climate_rt_graph.seek_retweets()
    # climate_rt_graph.mongo_tests()
    # climate_rt_graph.populate_network()
    climate_rt_graph.connections_of_authorities()
    # climate_rt_graph.create_csv_file()
    # climate_rt_graph.creation_of_digraph()
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\I_O\\"
    #                       r"bin\list_of_top_pairs_5_percent_no_self_rt")

    print()

