from Tool_Pack import tools
from collections import defaultdict
import os
from pymongo import MongoClient
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

    def calculate_distances(self):
        date_tweets = tools.load_pickle(self.tweets_per_date_path)
        distances = tools.load_pickle(self.distance_path + str(self.year))
        print()

    def create_graph_files(self, percentage):
        if not os.path.exists(self.graph_path + str(self.year)):
            os.makedirs(self.graph_path + str(self.year))
        date_tweets = tools.load_pickle(self.tweets_per_date_path)
        distances = tools.load_pickle(self.distance_path + str(self.year))

        # We create first the edge csv file because we also create an index, so we can know which
        # nodes are part of the X percent network we are working at the given time.
        # Creating the edge file
        with open(self.graph_path + str(self.year) + r"\edges.csv", "w") as edge_file:
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


if __name__ == "__main__":
    c_graph = ContentGraph(2009)
    c_graph.calculate_distances()



