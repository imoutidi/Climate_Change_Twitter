from collections import defaultdict
import os
import hnswlib
import numpy as np
from Tool_Pack import tools
from pymongo import MongoClient
import time

class Snapshots:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"
        self.output_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\"

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
        # tools.save_pickle(self.output_path + r"Indexes\tweet_ids\dates_to_ids", date_dictionary)
        # tools.save_pickle(self.output_path + r"Indexes\tweet_ids\child_retweet_to_parent", child_key_rt_dict)
        # tools.save_pickle(self.output_path + r"Indexes\tweet_ids\parent_retweet_to_childen", parent_key_rt_dict)

    def replace_retweets_with_parents(self):
        # Load indexes
        date_dictionary = tools.load_pickle(self.output_path + r"Indexes\tweet_ids\dates_to_ids")
        child_key_rt_dict = tools.load_pickle(self.output_path + r"Indexes\tweet_ids\child_retweet_to_parent")
        # parent_key_rt_dict = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\\"
        #                                        r"I_O\Indexes\tweet_ids\parent_retweet_to_childen")
        date_dict_with_parents = defaultdict(set)
        for year_idx in range(2009, 2020):
            for tweet_id in date_dictionary[year_idx]:
                if tweet_id in child_key_rt_dict:
                    date_dict_with_parents[year_idx].add(child_key_rt_dict[tweet_id][0])
                else:
                    date_dict_with_parents[year_idx].add(tweet_id)
        for year_idx in range(2009, 2020):
            date_dict_with_parents[year_idx] = list(date_dict_with_parents[year_idx])
        # tools.save_pickle(self.output_path + r"\Indexes\tweet_ids\date_dict_with_parents", date_dict_with_parents)

    def format_data_for_indexing(self, tweets_ids):
        parent_key_rt_dict = tools.load_pickle(self.output_path + r"Indexes\tweet_ids\parent_retweet_to_childen")
        ids = np.arange(0, len(tweets_ids))
        client = MongoClient('localhost', 27017)
        db = client.Climate_Change_Tweets
        collection_tweets = db.tweet_documents
        # cursor = collection_tweets.find({})
        bert_vector_list = list()

        for t_id in tweets_ids:
            doc_record = collection_tweets.find_one({"tweet_id": t_id})
            # if the parent id is not in the database we substitute it with one of its children.
            # Remember to retrieve also with child id when you get the distances.
            if doc_record is None:
                child_id = parent_key_rt_dict[t_id][0]
                d_record = collection_tweets.find_one({"tweet_id": child_id})
                bert_vector_list.append(np.array(d_record["bert_vector"]))
            else:
                bert_vector_list.append(np.array(doc_record["bert_vector"]))
        bert_array = np.vstack(bert_vector_list)
        return bert_array, ids

    def create_index(self, c_year):
        date_dict_with_parents = tools.load_pickle(self.output_path + r"Indexes\tweet_ids\date_dict_with_parents")
        print()
        dim = 768

        # Generating data
        vectors, element_ids = self.format_data_for_indexing(date_dict_with_parents[c_year])
        num_elements = len(vectors)

        # Declaring index
        nn_index = hnswlib.Index(space='l2', dim=dim)  # possible options are l2, cosine or ip

        # Initializing index - the maximum number of elements should be known beforehand
        nn_index.init_index(max_elements=num_elements, ef_construction=200, M=16)

        # Element insertion (can be called several times):
        nn_index.add_items(vectors, element_ids)

        # Controlling the recall by setting ef:
        nn_index.set_ef(num_elements)  # ef should always be > k

        # Query dataset, k - number of the closest elements (returns 2 numpy arrays)
        # labels, distances = nn_index.knn_query(bert_data, k=10)

        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Indexes\LSH\\"
                          r"tweets_index_" + str(c_year), nn_index)

    def index_query(self, c_year):
        labels, distances = 0, 0
        all_labels_and_distances_list = list()
        parent_key_rt_dict = tools.load_pickle(self.output_path + r"Indexes\tweet_ids\parent_retweet_to_childen")
        number_of_closest_neighbors = 200
        year_index = tools.load_pickle(self.output_path + r"Indexes\LSH\tweets_index_" + str(c_year))
        date_dict_with_parents = tools.load_pickle(self.output_path + r"Indexes\tweet_ids\date_dict_with_parents")
        # dates_to_ids = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
        #                                  r"SnapShots\I_O\Indexes\tweet_ids\dates_to_ids")
        client = MongoClient('localhost', 27017)
        db = client.Climate_Change_Tweets
        collection_tweets = db.tweet_documents
        bert_vector_list = list()
        counter = 0
        print(len(date_dict_with_parents[c_year]))
        for date_tweet_id in list(date_dict_with_parents[c_year]):
            doc_record = collection_tweets.find_one({"tweet_id": date_tweet_id})

            bert_vector_list.append(np.array(doc_record["bert_vector"]))
            bert_array = np.vstack(bert_vector_list)
            # Do that to not accumulate all the vectors on the list.
            bert_vector_list = list()

            # "Dirty" workaround for the hnswlib bug. If the number of results "k" is too big it throws
            # an RuntimeError exception. I catch it and reduce the size of "k"
            print(counter)
            while True:
                try:
                    if number_of_closest_neighbors < 2:
                        labels, distances = 0, 0
                        break
                    labels, distances = year_index.knn_query(bert_array, k=int(number_of_closest_neighbors))
                    break
                except RuntimeError as run_time:
                    number_of_closest_neighbors /= 2
            all_labels_and_distances_list.append((labels, distances))

            counter += 1

            # This part retrieves the tweet texts.
            # for vector_index in labels[0]:
            #     tweet_id = list(date_dict_with_parents[c_year])[vector_index]
            #     doc_record = collection_tweets.find_one({"tweet_id": tweet_id})
            #     # As mentioned in format_data_for_indexing() I replace the parent with the child of the retweet
            #     # since I got retweets but not their parents in the database.
            #     if doc_record is None:
            #         child_id = parent_key_rt_dict[tweet_id][0]
            #         doc_record = collection_tweets.find_one({"tweet_id": child_id})
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\"
                          r"Tweet_Documents_Distance\distances_of_" + str(c_year), all_labels_and_distances_list)
        # return all_labels_and_distances_list


if __name__ == "__main__":
    snaps = Snapshots()
    # snaps.parse_tweets()
    # snaps.replace_retweets_with_parents()
    # for y_idx in range(2009, 2020):
    #     print(y_idx)
    #     snaps.create_index(y_idx)
    start_time = time.perf_counter()
    snaps.index_query(2010)
    print("Community detection processing time: " + str(time.perf_counter() - start_time) + " seconds")
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\\"
    #                       r"I_O\Indexes\tweet_ids\date_dict_with_parents")
    print()
