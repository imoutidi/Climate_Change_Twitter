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
            print(len(self.directed_graph))
            print(self.directed_graph.size())

    def components_ratios(self):
        weak_comps = sorted(nx.weakly_connected_components(self.directed_graph), key=len, reverse=True)
        strong_comps = sorted(nx.strongly_connected_components(self.directed_graph), key=len, reverse=True)
        self.weak_ratios_list.append((len(weak_comps[0]) / len(self.directed_graph), self.threshold))
        self.strong_ratios_list.append((len(strong_comps[0]) / len(self.directed_graph), self.threshold))

    @staticmethod
    def merge_ratios():
        merged_strong = list()
        merged_weak = list()
        for i in range(1, 5):
            temp_strong = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                                            r"Backbone_Ratios\strong_ratios_list" + str(i))
            temp_weak = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                                          r"Backbone_Ratios\weak_ratios_list" + str(i))
            merged_strong += temp_strong
            merged_weak += temp_weak
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                          r"strong_ratios_list", merged_strong)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                          r"weak_ratios_list", merged_weak)

    @staticmethod
    def plot_ratios():
        strong = tools.load_pickle(
            r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\strong_ratios_list")
        weak = tools.load_pickle(
            r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\weak_ratios_list")
        print()

        # Extract x and y coordinates from the list of tuples

        x_values = [point[1] for point in weak[:200]]
        y_values = [point[0] for point in weak[:200]]

        fig, ax = plt.subplots(figsize=(8, 6))

        # Plot the data as scatter plot
        plt.scatter(x_values, y_values, color='b', marker='o')

        # Set plot labels and title
        ax.set_xlabel("Threshold")
        ax.set_ylabel("Weak components nodes ratio to all \n network nodes")
        ax.set_title("Ratio for thresholds from 0 to 200000")
        ax.ticklabel_format(style='plain')
        plt.savefig(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\Plots\\"
                    r"weak_first_200.png")


def run_backbone_fragments():
    for i in range(0, 10001000, 1000):
        print(i)
        start_time = time.perf_counter()
        backbone_tester = BbTester(i)
        # backbone_tester.backboning()
        backbone_tester.creation_of_digraph()
        backbone_tester.components_ratios()
        print("Backboning processing time: " + str(time.perf_counter() - start_time) + " seconds")
    backbone_tester = BbTester(1000)
    tools.save_pickle(
        r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\new_weak_ratios_list"
        , backbone_tester.weak_ratios_list)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                      r"new_strong_ratios_list", backbone_tester.strong_ratios_list)


if __name__ == "__main__":
    run_backbone_fragments()
    # backbone_tester = BbTester(0)
    # backbone_tester.merge_ratios()
    # backbone_tester.plot_ratios()
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
    #                       r"Backbone_Ratios\strong_ratios_list")
    # b = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
    #                       r"Backbone_Ratios\weak_ratios_list")


    print()

