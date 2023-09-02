from collections import defaultdict
import os
from Tool_Pack import tools

class Snapshots:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"
    def parse_tweets(self):

        date_dictionary = defaultdict(list)
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    if hasattr(tweet_obj, 'retweeted_status'):

                        print()
                    else:
                        date_dictionary[tweet_obj.created_at.year].append(tweet_obj.id)

            print()
        # tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\"
        #                   r"Tests\tweets_per_year", dict(date_dictionary))


if __name__ == "__main__":
    snaps = Snapshots()
    snaps.parse_tweets()
