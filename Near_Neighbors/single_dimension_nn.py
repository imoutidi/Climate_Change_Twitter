from Tool_Pack import tools
from pymongo import MongoClient, ASCENDING, errors
import tweepy
# import editdistance
# print(editdistance.eval('one banana', 'banana one'))
import os
import re
from collections import defaultdict
import pprint
import nltk
# nltk.download('stopwords')  # download stopwords corpus
# nltk.download('punkt')  # download punkt tokenizer
# nltk.download('wordnet')  # download WordNet lemmatizer

import numpy as np
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.metrics.pairwise import cosine_similarity

import torch
from transformers import BertTokenizer, BertModel


def create_data_set():
    out_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
    set_of_filler_sentences = set()
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection = db.tweet_documents
    cursor = collection.find({}, no_cursor_timeout=True)
    for count, document in enumerate(cursor):
        # remove urls
        text = re.sub(r'http\S+', '', document["full_text"])
        if 40 > len(text) > 20:
            set_of_filler_sentences.add(text)
        if len(set_of_filler_sentences) > 1000:
            break
    tools.save_pickle(out_path + "filler_sentences_20_40", set_of_filler_sentences)


if __name__ == "__main__":
    create_data_set()
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\filler_sentences")
    # for i in a:
    #     if "warming" not in i:
    #         if "change" not in i:
    #             print(i)
    print()

