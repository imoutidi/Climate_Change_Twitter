import re
import csv
# Package name is community but refer to python-louvain on pypi
import community
# import infomap
import networkx as nx

from operator import itemgetter
from collections import defaultdict
from pymongo import MongoClient
from Tool_Pack import tools


class CommunityPipeline:

    def __init__(self, percentage):
        self.percent = percentage
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\I_O\\"
        self.input_path = self.path + r"Graph_files\\" + str(self.percent) + "_percent\\"
        self.output_path = self.path + r"Communities\\" + str(self.percent) + "_percent\\"
        self.author_ids_to_labels = dict()
        self.directed_graph = nx.DiGraph()
        # Tuple with author_usernames and their twitter ids that are part of a detected community in the graph.
        # Both sorted based on their in_degree descending.
        self.top_com_nodes = list()
        self.allcoms = list()

    def creation_of_digraph(self):
        # Creating author_id to author username/label in Twitter
        with open(self.input_path + str(self.percent) + "p_nodes.csv") as node_file:
            node_reader = csv.reader(node_file, delimiter=",")
            # Skipping the headers
            next(node_reader)
            for node_info in node_reader:
                self.author_ids_to_labels[int(node_info[0])] = node_info[1]
        with open(self.input_path + str(self.percent) + "p_edges.csv") as edge_file:
            edge_reader = csv.reader(edge_file, delimiter=",")
            # Skipping the headers
            next(edge_reader)
            for edge_info in edge_reader:
                self.directed_graph.add_edge(int(edge_info[0]), int(edge_info[1]), weight=int(edge_info[2]))

    def community_detection(self, com_size_threshold=0.01):
        # communities = community.best_partition(self.directed_graph, weight="weight", resolution=1.)
        communities = nx.community.louvain_communities(self.directed_graph, weight="weight", resolution=1, seed=123)
        self.allcoms = communities
        # Calculating the actual number of X% of the graphs nodes.
        com_threshold_size = len(self.directed_graph) * com_size_threshold
        for idx, com_nodes in enumerate(communities):
            com_labels = list()
            # Sorting the nodes based on their in degree on the retweet graph.
            com_nodes = sorted(com_nodes, key=lambda x: self.directed_graph.in_degree[x], reverse=True)
            for node_id in com_nodes:
                com_labels.append(self.author_ids_to_labels[node_id])
            if len(com_nodes) > com_threshold_size:
                self.top_com_nodes.append((com_labels, com_nodes))
            print()
        # We sort the list of the top communities based on their size
        self.top_com_nodes = sorted(self.top_com_nodes, key=lambda x: len(x[1]), reverse=True)


if __name__ == "__main__":
    rts_pipeline = CommunityPipeline(percentage=1)
    rts_pipeline.creation_of_digraph()
    rts_pipeline.community_detection()
    print()
