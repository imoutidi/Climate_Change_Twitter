import os
import math
import networkx as nx
from operator import itemgetter
from Tool_Pack import tools
from collections import defaultdict
import time


class UserGraph:
    def __init__(self, current_year):
        self.main_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\Per_Year\\"
        self.u_graph = nx.Graph()
        self.year = current_year
        self.random_seed = 123
        self.content_coms = list()
        self.top_community_nodes = list()

    def populate_graph(self):
        user_similarities = tools.load_pickle(self.main_path + str(self.year)
                                              + r"\user_similarities_" + str(self.year))
        for user_tuple, similarity in user_similarities.items():
            self.u_graph.add_edge(user_tuple[0], user_tuple[1], weight=similarity)
        print(len(self.u_graph))
            # self.content_graph.add_edge(int(edge_info[0]), int(edge_info[1]), weight=float(edge_info[2]))

    def community_detection(self, com_size_threshold=0.01):
        if len(self.u_graph) == 0:
            print("This year has not enough important users to form a meaningful graph. Exiting!")
            return "Empty Graph"
        communities = nx.community.louvain_communities(self.u_graph, weight="weight", resolution=1,
                                                       seed=self.random_seed)
        self.content_coms = communities
        # Calculating the actual number of X% of the graphs nodes.
        com_threshold_size = len(self.u_graph) * com_size_threshold
        for idx, com_nodes in enumerate(communities):
            # Sorting the nodes based on their degree on the graph.
            com_nodes = sorted(com_nodes, key=lambda x: self.u_graph.degree[x], reverse=True)
            if len(com_nodes) > com_threshold_size:
                self.top_community_nodes.append(com_nodes)
        print()
        # We sort the list of the top communities based on their size
        self.top_community_nodes = sorted(self.top_community_nodes, key=lambda x: len(x), reverse=True)
        tools.save_pickle(self.main_path + str(self.year) + r"\top_communities_"
                          + str(self.year), self.top_community_nodes)
        return "ALL OK"

    def investigate_community_similarities(self):
        for current_year in range(2008, 2018):
            current_communities = tools.load_pickle(self.main_path + str(current_year)
                                                    + r"\top_communities_" + str(current_year))
            next_communities = tools.load_pickle(self.main_path + str(current_year + 1)
                                                 + r"\top_communities_" + str(current_year + 1))
            for outer_community in current_communities:
                for inner_community in next_communities:
                    com_intersection = set(outer_community).intersection(set(inner_community))
                    print()


if __name__ == "__main__":
    for c_year in range(2006, 2018):
        user_graph = UserGraph(c_year)
        # user_graph.populate_graph()
        # user_graph.community_detection()
        user_graph.investigate_community_similarities()
    # a = tools.load_pickle(
    #     r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\Per_Year\2010\top_communities_2010")
    # print()



