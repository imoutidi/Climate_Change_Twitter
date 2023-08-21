from Tool_Pack import tools
from pymongo import MongoClient
import time
import numpy as np
import hnswlib
import pickle


# I am querying all the tweets of the database using batches of 10000
# for each tweet i am querying all 20 indexes that have been created.
# from each index I keep the top 500 closest neighbors with the query
def index_query():
    nn_index = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                                 r"I_O\Datasets\LSH_files\indexes\test_index_4")
    all_tweets_ids = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\\"
                                       r"Climate_Changed\I_O\bin\all_tweets_ids")
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection_tweets = db.tweet_documents
    bert_vector_list = list()
    for tweet_id in all_tweets_ids:
        doc_record = collection_tweets.find_one({"tweet_id": tweet_id})
        bert_vector_list.append(np.array(doc_record["bert_vector"]))
    bert_array = np.vstack(bert_vector_list)
    labels, distances = nn_index.knn_query(bert_array, k=2000)
    print("Top closests matches:")
    for vector_index in labels[0]:
        tweet_id = all_tweets_ids[vector_index]
        doc_record = collection_tweets.find_one({"tweet_id": tweet_id})
        print(doc_record["full_text"])