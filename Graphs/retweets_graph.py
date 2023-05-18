import networkx as nx


class RTGraph():

    def __init__(self):
        self.input_path = r""
        self.directed_graph = nx.DiGraph()

    def parse_tweets(self):
        set_of_tweets = set()
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    if tweet_obj.id not in set_of_tweets:
                        set_of_tweets.add(tweet_obj.id)
                        self.mongo_insert(tweet_obj)

                        
if __name__ == "__main__":
    climate_rt_graph = RTGraph()
    print()

