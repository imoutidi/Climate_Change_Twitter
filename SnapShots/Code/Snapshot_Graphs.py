from collections import defaultdict
import os
import numpy as np
from Tool_Pack import tools
from pymongo import MongoClient

class Snapshots:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"

    def parse_tweets(self):
        date_dictionary = defaultdict(list)
        child_key_rt_dict = defaultdict(list)
        parent_key_rt_dict = defaultdict(list)
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    if hasattr(tweet_obj, 'retweeted_status'):
                        # Keeping the retweets on this dictionary
                        child_key_rt_dict[tweet_obj.id].append(tweet_obj.retweeted_status.id)
                        parent_key_rt_dict[tweet_obj.retweeted_status.id].append(tweet_obj.id)
                        date_dictionary[tweet_obj.created_at.year].append(tweet_obj.id)
                    else:
                        # This dictionary does not contain retweets
                        date_dictionary[tweet_obj.created_at.year].append(tweet_obj.id)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Indexes\\"
                          r"tweet_ids\dates_to_ids", date_dictionary)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Indexes\\"
                          r"tweet_ids\child_retweet_to_parent", child_key_rt_dict)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Indexes\\"
                          r"tweet_ids\parent_retweet_to_childen", parent_key_rt_dict)

    def replace_retweets_with_parents(self):
        # Load indexes
        date_dictionary = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\\"
                                            r"I_O\Indexes\tweet_ids\dates_to_ids")
        child_key_rt_dict = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\\"
                                              r"I_O\Indexes\tweet_ids\child_retweet_to_parent")
        parent_key_rt_dict = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\\"
                                               r"I_O\Indexes\tweet_ids\parent_retweet_to_childen")
        date_dict_with_parents = defaultdict(set)
        validation = defaultdict(list)
        for year_idx in range(2009, 2020):
            for tweet_id in date_dictionary[year_idx]:
                if tweet_id in child_key_rt_dict:
                    date_dict_with_parents[year_idx].add(child_key_rt_dict[tweet_id][0])
                    validation[year_idx].append(child_key_rt_dict[tweet_id][0])
                else:
                    date_dict_with_parents[year_idx].add(tweet_id)
                    validation[year_idx].append(tweet_id)
        print()

    @staticmethod
    def format_data_for_indexing(tweets_ids):
        ids = np.arange(0, len(tweets_ids))
        client = MongoClient('localhost', 27017)
        db = client.Climate_Change_Tweets
        collection_tweets = db.tweet_documents
        # cursor = collection_tweets.find({})
        bert_vector_list = list()

        for t_id in tweets_ids:
            doc_record = collection_tweets.find_one({"tweet_id": t_id})
            bert_vector_list.append(np.array(doc_record["bert_vector"]))
        bert_array = np.vstack(bert_vector_list)
        return bert_array, ids


if __name__ == "__main__":
    snaps = Snapshots()
    # snaps.parse_tweets()
    snaps.replace_retweets_with_parents()
    print()
