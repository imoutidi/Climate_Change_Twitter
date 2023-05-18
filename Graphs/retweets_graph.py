import os
import tweepy
from Tool_Pack import tools
import networkx as nx


class RTGraph:

    def __init__(self):
        self.input_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\\" \
                          r"Datasets\Climate_Changed\Downloaded_Tweets\\"
        self.directed_graph = nx.DiGraph()
        self.total_tweets = 0
        self.total_retweets = 0

    def parse_tweets(self):
        set_of_tweets = set()
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    self.total_tweets += 1
                    if hasattr(tweet_obj, "retweeted_status"):
                        self.total_retweets += 1
                    # if tweet_obj.id not in set_of_tweets:
                    #     set_of_tweets.add(tweet_obj.id)
        print("Total Tweets = " + str(self.total_tweets))
        print("Total Retweets = " + str(self.total_retweets))


if __name__ == "__main__":
    climate_rt_graph = RTGraph()
    climate_rt_graph.parse_tweets()
    print()

