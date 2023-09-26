from Tool_Pack import tools
from collections import defaultdict
import os
import csv
import statistics
import networkx as nx
from pymongo import MongoClient
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter
import time


class ContentGraph:

    def __init__(self, year):
        self.graph_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Graphs\\"
        self.distance_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\" \
                             r"Tweet_Documents_Distance\distances_of_"
        self.tweets_per_date_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Indexes\\" \
                                    r"tweet_ids\date_dict_with_parents"
        self.year = year
        # Pymongo init
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.Climate_Change_Tweets
        self.collection = self.db.tweet_documents
        # Graph init
        self.random_seed = 123
        self.content_coms = list()
        self.top_community_nodes = list()
        self.content_graph = nx.Graph()

    def create_graph_files(self):
        num_of_distances_per_tweet = 400
        if not os.path.exists(self.graph_path + str(self.year)):
            os.makedirs(self.graph_path + str(self.year))
        date_tweets = tools.load_pickle(self.tweets_per_date_path)
        distances = tools.load_pickle(self.distance_path + str(self.year))

        year_tweets = date_tweets[self.year]

        # max distance for normalization
        max_distance = 0
        # Calculating standard deviation and mean to prune out insignificant distances
        all_distances = list()

        # Checking for node loops if two nodes are pointing to each other.
        loop_counter = 0
        loop_checking_set = set()

        for idx, distance_tuple in enumerate(distances):
            # print(idx)
            # Converting numpy arrays to lists
            if distance_tuple[0] is not None:
                d_indexes = distance_tuple[0][:1].tolist()[0]
                d_distances = distance_tuple[1][:1].tolist()[0]
                for doc_index, doc_distance in zip(d_indexes[1:num_of_distances_per_tweet],
                                                   d_distances[1:num_of_distances_per_tweet]):
                    all_distances.append(doc_distance)
                    if doc_distance > max_distance:
                        max_distance = doc_distance

        all_distances = self.remove_outliers(all_distances)
        self.distribution_plot(all_distances, self.year)
        exit()

        standard_dev = statistics.pstdev(all_distances)
        distance_mean = statistics.mean(all_distances)
        print()

        with open(self.graph_path + str(self.year) + r"\edges.csv", "w") as edge_file:
            edge_file.write("Source,Target,Weight\n")
            for idx, distance_tuple in enumerate(distances):
                if distance_tuple[0] is not None:
                    print(idx)
                    # Converting numpy arrays to lists
                    d_indexes = distance_tuple[0][:1].tolist()[0]
                    d_distances = distance_tuple[1][:1].tolist()[0]
                    for doc_index, doc_distance in zip(d_indexes[1:400], d_distances[1:400]):
                        if (year_tweets[idx], year_tweets[doc_index]) not in loop_checking_set:
                            # To avoid loops I insert also the reversed couple in the set
                            loop_checking_set.add((year_tweets[idx], year_tweets[doc_index]))
                            loop_checking_set.add((year_tweets[doc_index], year_tweets[idx]))
                            # I save the similarities for the graph.
                            if 0.0 < doc_distance < distance_mean:
                                edge_file.write(str(year_tweets[idx]) + "," + str(year_tweets[doc_index]) +
                                                "," + str(1 - (doc_distance/max_distance)) + "\n")

    def create_graph_object(self):
        with open(self.graph_path + str(self.year) + r"\edges.csv") as edge_file:
            edge_reader = csv.reader(edge_file, delimiter=",")
            # Skipping the headers
            next(edge_reader)
            for edge_info in edge_reader:
                self.content_graph.add_edge(int(edge_info[0]), int(edge_info[1]), weight=float(edge_info[2]))

        degree_list = list()
        for degree_tuple in self.content_graph.degree:
            degree_list.append(degree_tuple)
        print("Graph created.")
        print("Number of nodes: " + str(len(self.content_graph.nodes)))
        print("Number of edges: " + str(len(self.content_graph.edges)))

    # Detects communities on the graph using the Louvain community detection algorithm.
    # The communities are saved only in the object.
    def community_detection(self, com_size_threshold=0.01):
        communities = nx.community.louvain_communities(self.content_graph, weight="weight", resolution=1,
                                                       seed=self.random_seed)
        self.content_coms = communities
        # Calculating the actual number of X% of the graphs nodes.
        com_threshold_size = len(self.content_graph) * com_size_threshold
        for idx, com_nodes in enumerate(communities):
            # Sorting the nodes based on their in degree on the retweet graph.
            com_nodes = sorted(com_nodes, key=lambda x: self.content_graph.degree[x], reverse=True)
            if len(com_nodes) > com_threshold_size:
                self.top_community_nodes.append(com_nodes)
        # We sort the list of the top communities based on their size
        self.top_community_nodes = sorted(self.top_community_nodes, key=lambda x: len(x[1]), reverse=True)

    @staticmethod
    def distribution_plot(dist_list, d_year):
        sns.histplot(dist_list, kde=True)  # You can also use sns.distplot() for older versions of Seaborn
        # # Add labels and title
        plt.xlabel('Values')
        plt.ylabel('Frequency')
        plt.title('Distribution Plot')
        # Show the plot
        # plt.show()
        plt.savefig(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\"
                    r"Tests\Distance_Distributions\Dist_of_" + str(d_year))

    @staticmethod
    def remove_outliers(dist_list):
        # Calculate the IQR (Interquartile Range)
        Q1 = np.percentile(dist_list, 25)
        Q3 = np.percentile(dist_list, 75)
        IQR = Q3 - Q1

        # Define the lower and upper bounds for outliers
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # Remove outliers from the list
        filtered_data = [x for x in dist_list if lower_bound <= x <= upper_bound]
        return filtered_data


if __name__ == "__main__":
    c_graph = ContentGraph(2014)
    c_graph.create_graph_files()
    # c_graph.create_graph_object()
    # c_graph.community_detection(0.02)
    print()


# to check tweets on twitter https://twitter.com/twitter/status/1234903157580296192



