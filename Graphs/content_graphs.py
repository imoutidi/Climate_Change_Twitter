from Tool_Pack import tools
from pymongo import MongoClient
from collections import defaultdict
import time
import numpy as np
import hnswlib
import pickle


# Querying everything
def index_query():
    all_tweets_ids = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\\"
                                       r"Climate_Changed\I_O\bin\all_tweets_ids")
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection_tweets = db.tweet_documents

    labels_distances_dict = defaultdict(list)

    for nn_index_id in range(20):
        print(nn_index_id)
        current_nn_index = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                                             r"I_O\Datasets\LSH_files\indexes\tweets_index_" + str(nn_index_id))
        for idx, tweet_id in enumerate(all_tweets_ids[:2000000]):
            print(idx)
            doc_record = collection_tweets.find_one({"tweet_id": tweet_id})
            bert_vector_list = list()
            bert_vector_list.append(np.array(doc_record["bert_vector"]))
            bert_array = np.vstack(bert_vector_list)
            except_counter = 0
            try:
                labels, distances = current_nn_index.knn_query(bert_array, k=2000)
                labels_distances_dict[tweet_id].append((labels, distances))
            except Exception as e:
                except_counter += 1
                print("Exception number: " + str(except_counter))
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\LSH_files\\"
                      r"Computations\labels_distances_dict_1")

    # Keep that to remember how to acces the actuall tweets ids and text.
    # print("Top closests matches:")
    # for vector_index in labels[0]:
    #     tweet_id = all_tweets_ids[vector_index]
    #     doc_record = collection_tweets.find_one({"tweet_id": tweet_id})
    #     print(doc_record["full_text"])


if __name__ == "__main__":
    print()
    index_query()