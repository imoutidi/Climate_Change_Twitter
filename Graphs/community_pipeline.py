import os
import time
import csv
# Package name is community but refer to python-louvain on pypi
import community
# import infomap
import networkx as nx
import matplotlib.pyplot as plt

import numpy as np
from wordcloud import WordCloud
from PIL import Image
from Tool_Pack import tools


class CommunityPipeline:

    def __init__(self, percentage, random_seed, threshold):
        self.percent = percentage
        self.random_seed = random_seed
        self.threshold = threshold
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"I_O\Graph_files\Back_Bones\\" + "threshold_" + self.threshold + "\\"
        self.output_path = self.path + r"I_O\Communities\\" + "threshold_" + self.threshold + "\\"
        self.author_ids_to_labels = dict()
        self.directed_graph = nx.DiGraph()
        # Tuple with author_usernames and their twitter ids that are part of a detected community in the graph.
        # Both sorted based on their in_degree descending.
        self.top_com_nodes = list()
        self.allcoms = list()
        self.color_dict = {0: "#ff5793", 1: "#91cda2", 2: "#00996d", 3: "#e2bd6a", 4: "#2a4b67", 5: "#00deeb",
                           6: "#97c8e6", 7: "#b44806", 8: "#8c58a6", 9: "#ff5053", 10: "#c7a6ac", 11: "#68f4d2",
                           12: "#00c4ff", 13: "#962f4b", 14: "#f9a200", 15: "#afc400", 16: "#ff7b00", 17: "#63b939",
                           18: "#602f4b", 19: "#008198", 20: "#402f2b", 21: "#afc400", 22: "#39e200", 23: "#ff7aff",
                           24: "#ff7a11"}

    def creation_of_digraph(self):
        # Creating author_id to author username/label in Twitter
        with open(self.input_path + "nodes_threshold_" + self.threshold + ".csv") as node_file:
            node_reader = csv.reader(node_file, delimiter=",")
            # Skipping the headers
            next(node_reader)
            for node_info in node_reader:
                self.author_ids_to_labels[int(node_info[0])] = node_info[1]
        with open(self.input_path + "edges_threshold_" + self.threshold + ".csv") as edge_file:
            edge_reader = csv.reader(edge_file, delimiter=",")
            # Skipping the headers
            next(edge_reader)
            for edge_info in edge_reader:
                self.directed_graph.add_edge(int(edge_info[0]), int(edge_info[1]), weight=int(edge_info[2]))

    # Detects communities on the graph using the Louvain community detection algorithm.
    # The communities are saved only in the object.
    def community_detection(self, com_size_threshold=0.01):
        # communities = community.best_partition(self.directed_graph, weight="weight", resolution=1.)
        communities = nx.community.louvain_communities(self.directed_graph, weight="weight", resolution=1,
                                                       seed=self.random_seed)
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
        # We sort the list of the top communities based on their size
        self.top_com_nodes = sorted(self.top_com_nodes, key=lambda x: len(x[1]), reverse=True)

    def create_worldclouds(self):
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        mask = np.array(Image.open(self.path + r"Images\c1.png"))
        input_dict = [{"a": 20, "b": 30, "c": 15, "d": 20, "e": 50, "f": 15, "g": 30,
                       "h": 20, "i": 10, "j": 10, "k": 17, "l": 30, "m": 40, "n": 20,
                       "o": 30, "p": 10, "q": 50, "r": 40, "s": 20, "t": 10, "w": 30}]
        list_of_input_dicts = self.convert_to_list_of_dict(self.top_com_nodes)

        # for idx, com in enumerate(anno_coms):
        for idx, community_dict in enumerate(list_of_input_dicts):
            wc = WordCloud(background_color=self.color_dict[idx], mask=mask, max_words=60, prefer_horizontal=1,
                           contour_width=70, collocations=False, margin=1, width=660, height=660, contour_color="black",
                           color_func=lambda *args, **kwargs: (0, 0, 0))
            wc.generate_from_frequencies(community_dict)
            wc.to_file(self.output_path + "community_" + str(idx) + ".png")

    def convert_to_list_of_dict(self, top_communities, number_of_users_to_display=50):
        all_com_usernames_dict_list = list()
        for t_community in top_communities:
            user_in_degree_dict = dict()
            for username, author_id in zip(t_community[0][:number_of_users_to_display],
                                           t_community[1][:number_of_users_to_display]):
                user_in_degree_dict[username] = self.directed_graph.in_degree[author_id]
            all_com_usernames_dict_list.append(user_in_degree_dict)
        return all_com_usernames_dict_list


if __name__ == "__main__":
    rts_pipeline = CommunityPipeline(percentage=100, random_seed=123, threshold="3k")
    rts_pipeline.creation_of_digraph()
    start_time = time.perf_counter()
    rts_pipeline.community_detection()
    print("Community detection processing time: " + str(time.perf_counter() - start_time) + " seconds")
    start_time = time.perf_counter()
    rts_pipeline.create_worldclouds()
    print("Wordclouds creation processing time: " + str(time.perf_counter() - start_time) + " seconds")
