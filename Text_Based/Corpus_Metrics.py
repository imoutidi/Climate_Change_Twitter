import os
from Tool_Pack import tools
from collections import defaultdict


class CorpusMaster:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"

    def parse_tweets(self):
        date_dictionary = defaultdict(int)
        bancars_tweets = list()
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    if "#bancars" in tweet_obj.full_text:
                        bancars_tweets.append(tweet_obj)

                    # date_dictionary[tweet_obj.created_at.year] += 1



if __name__ == "__main__":
    c_corpus = CorpusMaster()
    c_corpus.parse_tweets()