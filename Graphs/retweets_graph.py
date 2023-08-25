import os
import csv
from pymongo import MongoClient, ASCENDING, errors
# import tweepy is needed because we load tweepy objects/pickles
import tweepy
from Tool_Pack import tools
import networkx as nx
from collections import Counter
from operator import itemgetter

from Back_Boning import backboning
import graph_tools


class RTGraph:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"
        self.output_path = self.path + r"I_O\\"
        self.directed_graph = nx.DiGraph()
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.Climate_Change_Tweets
        self.collection_tweets = self.db.tweet_documents
        self.collection_superdocs = self.db.super_documents

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
        author_record = self.collection_superdocs.find_one({"author_id": author_id})
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

        self.collection_superdocs.update_one({"author_id": author_id}, {"$set": {"rt_tweets_list": rt_tweets_list,
                                             "rt_authors_list": rt_authors_list}})

    def populate_network(self):
        all_users_that_retweeted = tools.load_pickle(self.output_path + r"bin\all_users_that_retweeted")
        for user_id in all_users_that_retweeted:
            user_document = self.collection_superdocs.find_one({"author_id": user_id})
            for rt_user_id in user_document["rt_authors_list"]:
                self.directed_graph.add_edge(user_id, rt_user_id)
            print()

    def create_graph_files(self, percentage):
        list_of_top_pairs = tools.load_pickle(self.output_path + r"bin\list_of_top_pairs_" + str(percentage) +
                                              r"_percent_no_self_rt")
        user_indexes = tools.load_pickle(self.output_path + r"Indexes\user_id_to_screen_name")
        # nodes that are part on the top percentage based in in-degree.
        percent_nodes = set()

        # We create first the edge csv file because we also create an index, so we can know which
        # nodes are part of the X percent network we are working at the given time.
        # Creating the edge file
        with open(self.output_path + r"Graph_files\\" + str(percentage) + r"_percent\\" + str(percentage)
                  + r"p_edges.csv", "w") as edge_file:
            edge_file.write("Source,Target,Weight\n")
            counter = 0
            for rt_pair, num_of_rts in list_of_top_pairs.items():
                percent_nodes.add(rt_pair[0])
                percent_nodes.add(rt_pair[1])
                print(counter)
                counter += 1
                edge_file.write(str(rt_pair[0]) + "," + str(rt_pair[1]) + "," + str(num_of_rts) + "\n")

        # Creating the node file
        with open(self.output_path + r"Graph_files\\" + str(percentage) + r"_percent\\" + str(percentage)
                  + r"p_nodes.csv", "w") as node_file:
            node_file.write("id,label\n")
            for node in percent_nodes:
                if node in user_indexes:
                    node_file.write(str(node) + "," + user_indexes[node] + "\n")
                else:
                    node_file.write(str(node) + "," + str(node) + "\n")

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
        # This is X% of all the nodes on the retweet network.
        top_in_degrees = sorted(in_degrees, key=itemgetter(1), reverse=True)[:18000]
        # indexing for lookups
        top_users = {s[0] for s in top_in_degrees}
        list_of_top_pairs = dict()
        for idx, top_user in enumerate(top_in_degrees):
            print(idx)
            for rt_pair, num_of_rts in counted_list_of_pairs.items():
                # this checks if both users of the edge ar in the top X percent of the in-degree users.
                if top_user[0] == rt_pair[0] and rt_pair[1] in top_users:
                    list_of_top_pairs[rt_pair] = num_of_rts

        # removing self retweets.
        pairs_to_be_removed = list()
        for top_pair in list_of_top_pairs:
            if top_pair[0] == top_pair[1]:
                pairs_to_be_removed.append(top_pair)
        for t_pair in pairs_to_be_removed:
            del list_of_top_pairs[t_pair]
        tools.save_pickle(self.output_path + r"bin\list_of_top_pairs_10_percent_no_self_rt", list_of_top_pairs)
        # tools.save_pickle(self.output_path + r"bin\list_of_all_pairs_no_self_rt", list_of_top_pairs)

    def creation_of_digraph(self):
        list_of_top_pairs = tools.load_pickle(self.output_path + r"bin\list_of_all_pairs_no_self_rt")
        for rt_pair, num_of_rts in list_of_top_pairs.items():
            self.directed_graph.add_edge(rt_pair[0], rt_pair[1], weight=num_of_rts)

        # Extract degree lists
        in_degree_list = list()
        out_degree_list = list()
        degree_list = list()
        for degree_tuple in self.directed_graph.in_degree:
            in_degree_list.append(degree_tuple)
        print()
        for degree_tuple in self.directed_graph.out_degree:
            out_degree_list.append(degree_tuple)
        for degree_tuple in self.directed_graph.degree:
            degree_list.append(degree_tuple)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                          r"Backbone_Degrees\Raw\in_degrees", in_degree_list)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                          r"Backbone_Degrees\Raw\out_degrees", out_degree_list)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                          r"Backbone_Degrees\Raw\degrees", degree_list)
        print()

    def create_graph_file_for_backboning(self):
        list_of_pairs = tools.load_pickle(self.output_path + r"bin\list_of_all_pairs_no_self_rt")
        with open(self.output_path + r"Graph_files\retweet_network_all_for_backboning_no_self_retweets.csv",
                  "w", encoding='utf-8') as csv_file:
            csv_file.write("src\ttrg\tweight\n")
            for pair, weight in list_of_pairs.items():
                csv_file.write(str(pair[0]) + "\t" + str(pair[1]) + "\t" + str(weight) + "\n")

    def backboning_graph(self):
        threshold = 13000
        table, nnodes, nnedges = backboning.read(self.output_path
                                                 + r"\Graph_files\\"
                                                   r"retweet_network_all_for_backboning_no_self_retweets.csv",
                                                 "weight")
        nc_table = backboning.noise_corrected(table)
        nc_backbone = backboning.thresholding(nc_table, threshold)
        backboning.write(nc_backbone, "retweet_network_backbone_" + str(threshold),
                         "nc", self.output_path + r"\Graph_files\\")

    def convert_backbone_format_to_gephi(self):
        user_indexes = tools.load_pickle(self.output_path + r"Indexes\user_id_to_screen_name")
        backbone_users = set()
        with open(self.output_path + r"Graph_files\BB_files_from_nc_method\retweet_network_backbone_3000_nc.csv") \
                as bone_file:
            # Creating the edge file
            with open(self.output_path + r"Graph_files\Back_Bones\threshold_3k\\"
                                         r"edges_threshold_3k.csv", "w") as edge_file:
                edge_file.write("Source,Target,Weight\n")
                reader = csv.reader(bone_file, delimiter=",", quotechar='"')
                # Skipping the header
                next(reader, None)
                for edge_info in reader:
                    # we don't include the last field of the back boning algorithm yet, it is a significance score.
                    split_info = edge_info[0].split("\t")
                    backbone_users.add(int(split_info[0]))
                    backbone_users.add(int(split_info[1]))
                    edge_file.write(split_info[0] + "," + split_info[1] + "," + split_info[2] + "\n")
        # Creating the node file.
        with open(self.output_path + r"Graph_files\Back_Bones\threshold_3k\\"
                                     r"nodes_threshold_3k.csv", "w") as node_file:
            node_file.write("id,label\n")
            for author_id in backbone_users:
                if author_id in user_indexes:
                    node_file.write(str(author_id) + "," + user_indexes[author_id] + "\n")
                else:
                    node_file.write(str(author_id) + "," + str(author_id) + "\n")


if __name__ == "__main__":
    climate_rt_graph = RTGraph()
    # climate_rt_graph.seek_retweets()
    # climate_rt_graph.mongo_tests()
    # climate_rt_graph.populate_network()
    # climate_rt_graph.connections_of_authorities()

    # climate_rt_graph.create_graph_files(percentage=100)
    climate_rt_graph.creation_of_digraph()

    # climate_rt_graph.create_graph_file_for_backboning()
    # climate_rt_graph.backboning_graph()
    # climate_rt_graph.convert_backbone_format_to_gephi()
    # tools.line_count(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\I_O\Graph_files\climate_retweet_network_backbone_nc.csv")

