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

    def create_graph_files(self):
        if not os.path.exists(self.graph_path + str(self.year)):
            os.makedirs(self.graph_path + str(self.year))
        date_tweets = tools.load_pickle(self.tweets_per_date_path)
        distances = tools.load_pickle(self.distance_path + str(self.year))

        year_tweets = date_tweets[self.year]
        print()

        # We create first the edge csv file because we also create an index, so we can know which
        # nodes are part of the X percent network we are working at the given time.
        # Creating the edge file
        with open(self.graph_path + str(self.year) + r"\edges.csv", "w") as edge_file:
            edge_file.write("Source,Target,Weight\n")
            counter = 0
            for idx, distance_tuple in enumerate(distances):
                # Converting numpy arrays to lists
                d_indexes = distance_tuple[0][:1].tolist()[0]
                d_distances = distance_tuple[1][:1].tolist()[0]
                # for doc_index, doc_distance in zip(distance_tuple[0][1:], distance_tuple[1][1:]):
                for doc_index, doc_distance in zip(d_indexes, d_distances):
                    edge_file.write(str(year_tweets[idx]) + "," + str(year_tweets[doc_index]) +
                                    "," + str(doc_distance) + "\n")
                break

            # for rt_pair, num_of_rts in list_of_top_pairs.items():
            #     percent_nodes.add(rt_pair[0])
            #     percent_nodes.add(rt_pair[1])
            #     print(counter)
            #     counter += 1
            #     edge_file.write(str(rt_pair[0]) + "," + str(rt_pair[1]) + "," + str(num_of_rts) + "\n")


if __name__ == "__main__":
    c_graph = ContentGraph(2009)
    # c_graph.calculate_distances()
    c_graph.create_graph_files()




# to check tweets on twitter https://twitter.com/twitter/status/1234903157580296192



