import langdetect.lang_detect_exception

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

from langdetect import DetectorFactory    # consistent results
DetectorFactory.seed = 0
from langdetect import detect


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
        if len(text) > 100:
            set_of_filler_sentences.add(text)
        # if len(set_of_filler_sentences) > 1000:
        #     break
    tools.save_pickle(out_path + "filler_sentences_100", list(set_of_filler_sentences))

def combine_big_and_small_texts():
    small_texts = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
                                    r"english_filler_sentences")
    big_texts = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
                                  r"filler_sentences_100")
    dataset_list = list()
    text_counter = 0
    for small_t in small_texts:
        for i in range(10):
            print()


if __name__ == "__main__":
    create_data_set()
    # combine_big_and_small_texts()
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
    #                       r"filler_sentences_60")
    # b = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
    #                       r"english_filler_sentences")
    # b = list()
    # for i in a:
    #     if "warming" not in i:
    #         if "change" not in i:
    #             try:
    #                 if detect(i) == "en":
    #                     b.append(i)
    #             except langdetect.lang_detect_exception.LangDetectException as exLang:
    #                 print("----------------------------------------------------")
    #                 print(i)
    #                 print(exLang)
    #                 print("----------------------------------------------------")
    # tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
    #                   r"english_filler_sentences", b)
    print()

