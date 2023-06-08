import csv
import time
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from Tool_Pack import tools
from Back_Boning import backboning


class BbTester:
    weak_ratios_list = list()
    strong_ratios_list = list()

    def __init__(self, threshold):
        self.threshold = threshold
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\\" \
                    r"Climate_Changed\I_O\Graph_files\\"
        self.input_file = self.path + r"retweet_network_all_for_backboning_no_self_retweets.csv"
        self.output_path = self.path + r"BB_files_from_nc_method\\"
        self.author_ids_to_labels = dict()
        self.directed_graph = nx.DiGraph()
        self.graph_number_of_nodes = 0

    def backboning(self):
        table, nnodes, nnedges = backboning.read(self.input_file, "weight")
        nc_table = backboning.noise_corrected(table)
        nc_backbone = backboning.thresholding(nc_table, self.threshold)
        backboning.write(nc_backbone, "retweet_network_backbone_" + str(self.threshold), "nc", self.output_path)

    def creation_of_digraph(self):
        with open(self.output_path + "retweet_network_backbone_" + str(self.threshold) + "_nc.csv") as edge_file:
            edge_reader = csv.reader(edge_file, delimiter="\t")
            # Skipping the headers
            next(edge_reader)
            for edge_info in edge_reader:
                self.directed_graph.add_edge(int(edge_info[0]), int(edge_info[1]), weight=int(edge_info[2]))

    def components_ratios(self):
        weak_comps = sorted(nx.weakly_connected_components(self.directed_graph), key=len, reverse=True)
        strong_comps = sorted(nx.strongly_connected_components(self.directed_graph), key=len, reverse=True)
        self.weak_ratios_list.append((len(weak_comps[0]) / self.directed_graph.size(), self.threshold))
        self.strong_ratios_list.append((len(strong_comps[0]) / self.directed_graph.size(), self.threshold))
        print()


if __name__ == "__main__":
    for i in range(7501000, 10001000, 1000):
        print(i)
        start_time = time.perf_counter()
        backbone_tester = BbTester(i)
        backbone_tester.backboning()
        backbone_tester.creation_of_digraph()
        backbone_tester.components_ratios()
        print("Backboning processing time: " + str(time.perf_counter() - start_time) + " seconds")
    backbone_tester = BbTester(1000)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\weak_ratios_list1"
                      , backbone_tester.weak_ratios_list)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                      r"strong_ratios_list1", backbone_tester.strong_ratios_list)

