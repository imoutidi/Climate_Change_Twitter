import csv
import time
import networkx as nx
import matplotlib.pyplot as plt
import multiprocessing
import numpy as np
from Tool_Pack import tools
from Back_Boning import backboning


class BbTester:
    weak_ratios_list = list()
    weak_ratios_2 = list()
    strong_ratios_list = list()
    strong_ratios_2 = list()
    number_of_edges_list = list()
    number_of_nodes_list = list()

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
        self.weak_ratios_list.append((len(weak_comps[0]) / len(self.directed_graph), self.threshold))
        self.weak_ratios_2.append((len(weak_comps[0]) / 1798123, self.threshold))
        self.strong_ratios_list.append((len(strong_comps[0]) / len(self.directed_graph), self.threshold))
        self.strong_ratios_2.append((len(strong_comps[0]) / 1798123, self.threshold))
        self.number_of_edges_list.append((len(self.directed_graph.edges), self.threshold))
        self.number_of_nodes_list.append((len(self.directed_graph.nodes), self.threshold))

    @staticmethod
    def merge_ratios():
        merged_strong = list()
        merged_weak = list()
        merged_nodes = list()
        merged_edges = list()
        for i in range(0, 8):
            temp_strong = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                                            r"Backbone_Ratios\new_strong_ratios_list_" + str(i))
            temp_weak = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                                          r"Backbone_Ratios\new_weak_ratios_list_" + str(i))
            temp_nodes = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                                           r"Backbone_Ratios\new_num_of_nodes_list_" + str(i))
            temp_edges = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                                           r"Backbone_Ratios\new_num_of_edges_list_" + str(i))

            merged_strong += temp_strong
            merged_weak += temp_weak
            merged_nodes += temp_nodes
            merged_edges += temp_edges
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                          r"strong_ratios_list", merged_strong)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                          r"weak_ratios_list", merged_weak)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                          r"Backbone_Ratios\num_of_nodes_list", merged_nodes)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\\"
                          r"Backbone_Ratios\num_of_edges_list", merged_edges)

    @staticmethod
    def plot_ratios():
        strong = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                                   r"Non_Parallel\_strong_ratios_list")
        strong2 = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                                    r"Non_Parallel\_strong_ratios_list_2")
        weak = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                                 r"Non_Parallel\_weak_ratios_list")
        weak2 = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                                  r"Non_Parallel\_weak_ratios_list_2")
        nodes = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                                  r"Non_Parallel\new_num_of_nodes_list_2")
        edges = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                                  r"Non_Parallel\new_num_of_edges_list_2")
        print()

        # Extract x and y coordinates from the list of tuples

        x_values = [point[1] for point in nodes]
        y_values = [point[0] for point in nodes]

        fig, ax = plt.subplots(figsize=(8, 6))

        # Plot the data as scatter plot
        plt.scatter(x_values, y_values, color='b', marker='o')

        # Set plot labels and title
        ax.set_xlabel("Threshold")
        ax.set_ylabel("Nodes")
        ax.set_title("Nodes per threshold")
        ax.ticklabel_format(style='plain')
        plt.savefig(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\Non_Parallel\\"
                    r"Plots\nodes.png")


def run_bb_frag_in_parallel():
    threshold_couples = list()
    for idx, i in enumerate(range(0, 10001000, 1250000)):
        threshold_couples.append((i, i+1250000, idx))
    print()

    # Create a multiprocessing pool with the desired number of processes
    pool = multiprocessing.Pool(processes=8)

    # Apply the function to each argument in parallel
    pool.map(run_backbone_fragments, threshold_couples)

    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()


def run_backbone_fragments(thresh_couple):
    for i in range(thresh_couple[0], thresh_couple[1], 1000):
        # print(i)
        # start_time = time.perf_counter()
        backbone_tester = BbTester(i)
        # backbone_tester.backboning()
        backbone_tester.creation_of_digraph()
        backbone_tester.components_ratios()
        # print("Backboning processing time: " + str(time.perf_counter() - start_time) + " seconds")
    backbone_tester = BbTester(1000)
    tools.save_pickle(
        r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\Non_Parallel\\"
        r"_weak_ratios_list_" + str(thresh_couple[2]), backbone_tester.weak_ratios_list)
    tools.save_pickle(
        r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\Non_Parallel\\"
        r"_weak_ratios_list_2" + str(thresh_couple[2]), backbone_tester.weak_ratios_2)

    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                      r"Non_Parallel\_strong_ratios_list_" + str(thresh_couple[2]), backbone_tester.strong_ratios_list)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                      r"Non_Parallel\_strong_ratios_list_2" + str(thresh_couple[2]), backbone_tester.strong_ratios_2)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                      r"Non_Parallel\new_num_of_edges_list_2" + str(thresh_couple[2]), backbone_tester.number_of_edges_list)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\\"
                      r"Non_Parallel\new_num_of_nodes_list_2" + str(thresh_couple[2]), backbone_tester.number_of_nodes_list)


if __name__ == "__main__":
    # run_backbone_fragments((0, 1001000, 0))
    # run_bb_frag_in_parallel()
    backbone_tester = BbTester(0)
    # backbone_tester.creation_of_digraph()
    # backbone_tester.components_ratios()
    # backbone_tester.merge_ratios()
    backbone_tester.plot_ratios()
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\new_strong_ratios_list_0")
    # b = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Backbone_Ratios\num_of_nodes_list")


    print()

