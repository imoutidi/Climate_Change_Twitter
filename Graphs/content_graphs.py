from Tool_Pack import tools
from pymongo import MongoClient
import time
import numpy as np
import hnswlib
import pickle


# START HERE
def index_query():
    nn_index = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                                 r"I_O\Datasets\LSH_files\indexes\test_index_4")
    all_tweets_ids = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\\"
                                       r"Climate_Changed\I_O\bin\all_tweets_ids")
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection_tweets = db.tweet_documents
    bert_vector_list = list()
    doc_record = collection_tweets.find_one({"tweet_id": all_tweets_ids[100]})
    print("Query full text:")
    print(doc_record["full_text"])
    bert_vector_list.append(np.array(doc_record["bert_vector"]))
    bert_array = np.vstack(bert_vector_list)
    labels, distances = nn_index.knn_query(bert_array, k=20)
    print("Top closests matches:")
    for vector_index in labels[0]:
        tweet_id = all_tweets_ids[vector_index]
        doc_record = collection_tweets.find_one({"tweet_id": tweet_id})
        print(doc_record["full_text"])