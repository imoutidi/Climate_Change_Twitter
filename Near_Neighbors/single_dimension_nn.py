import langdetect.lang_detect_exception

from Tool_Pack import tools
from Graphs import process_tweets
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

from itertools import cycle



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
            try:
                if detect(text) == "en":
                    set_of_filler_sentences.add(text)
            except langdetect.lang_detect_exception.LangDetectException as exLang:
                print("----------------------------------------------------")
                print(text)
                print(exLang)
                print("----------------------------------------------------")
        if len(set_of_filler_sentences) > 100000:
            print()
            break
    tools.save_pickle(out_path + "filler_sentences_100", list(set_of_filler_sentences))


def combine_big_and_small_texts():
    small_texts = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
                                    r"test_tweets\english_filler_sentences")
    big_texts = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
                                  r"test_tweets\filler_sentences_100")
    dataset_list = list()
    small_texts_cycle = cycle(small_texts)
    for i in range(len(small_texts)):
        big_t = big_texts.pop(0)
        for idx in range(30):
            small_t = next(small_texts_cycle)
            dataset_list.append(big_t + " " + small_t)
    print()
    # tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\\"
    #                   r"test_tweets\merged_sentences_10440", dataset_list)


def calculate_vectors():
    tweet_dataset = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\\"
                                      r"I_O\test_tweets\merged_sentences_3480")
    bert_vectors = list()
    w2v_vectors = list()
    archiver = process_tweets.TweetArchiver()
    for tweet in tweet_dataset:
        bert_vectors.append(archiver.doc_vectorizer_bert(tweet))
        w2v_vectors.append(archiver.doc_vectorizer_word2vec(tweet))

    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\Vectors\bert_3480",
                      bert_vectors)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\Vectors\w2v_3480",
                      w2v_vectors)


if __name__ == "__main__":
    # create_data_set()
    # combine_big_and_small_texts()
    calculate_vectors()

    print()

