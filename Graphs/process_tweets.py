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


class TweetArchiver:
    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"
        self.output_path = self.path + r"I_O\\"
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.Climate_Change_Tweets
        self.collection = self.db.tweet_documents
        self.superdocs = self.db.super_documents
        self.stop_words = set(stopwords.words('english'))
        self.enrich_stopwords()
        self.model = BertModel.from_pretrained('bert-base-uncased')
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

    def enrich_stopwords(self):
        list_of_additional_words = ["it's", "we're", "...", "there's", "you're", "he's",
                                    "she's", "they're", "I'm", "rt"]
        for s_word in list_of_additional_words:
            self.stop_words.add(s_word)

    def remove_stopwords(self, text):
        words = word_tokenize(text)
        filtered_words = [word.lower() for word in words if word.lower() not in self.stop_words]
        filtered_text = ' '.join(filtered_words)
        return filtered_text

    # TODO recheck for currency in the text, now it gets removed.
    def stop_words_and_lemmatize(self, text):
        # remove urls
        text = re.sub(r'http\S+', '', text)
        # Tokenize the text
        words = word_tokenize(text)

        # Remove stopwords
        self.stop_words = set(stopwords.words('english'))
        filtered_words = [word for word in words if word.lower() not in self.stop_words]

        # Lemmatize the words
        lemmatizer = WordNetLemmatizer()
        lemmatized_words = [lemmatizer.lemmatize(word.lower()) for word in filtered_words]

        # Join the words back into a string
        preprocessed_text = ' '.join(lemmatized_words)
        preprocessed_text = re.sub(r'[\W\s]', ' ', preprocessed_text)
        words2 = word_tokenize(preprocessed_text)
        filtered_words = [word for word in words2 if len(word) > 1]
        preprocessed_text = ' '.join(filtered_words)
        return preprocessed_text

    def parse_tweets(self):
        set_of_tweets = set()
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    if tweet_obj.id not in set_of_tweets:
                        set_of_tweets.add(tweet_obj.id)
                        self.mongo_insert(tweet_obj)

    def mongo_insert(self, tweet_object):
        author_username = tweet_object.author.name
        author_id = tweet_object.author.id
        full_text = tweet_object.full_text
        tweet_id = tweet_object.id
        tweet_date = tweet_object.created_at
        preprocessed_text = self.stop_words_and_lemmatize(full_text)
        # tweet_id will be an index in the database.
        try:
            self.collection.insert_one({"author_id": author_id, "author_username": author_username,
                                        "full_text": full_text, "preprocessed_text": preprocessed_text,
                                        "tweet_id": tweet_id, "tweet_date": tweet_date})
        except errors.DuplicateKeyError as key_error:
            print(key_error)
            print(tweet_id)

    def indexing_users(self):
        # need to parse all tweets again to create user index of tweet ids
        set_of_tweets = set()
        user_to_tweets_posted_index = defaultdict(list)
        user_id_to_username_index = dict()
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    # if tweet_obj.id not in set_of_tweets:
                    #     user_to_tweets_posted_index[tweet_obj.author.id].append(tweet_obj.id)
                    #     if tweet_obj.author.id not in user_id_to_username_index:
                    #         user_id_to_username_index[tweet_obj.author.id] = tweet_obj.author.name
                    if tweet_obj.author.id not in user_id_to_username_index:
                        user_id_to_username_index[tweet_obj.author.id] = tweet_obj.author.screen_name

        # save indexes
        # tools.save_pickle(self.output_path + r"Indexes\user_id_to_tweets_ids_posted", user_to_tweets_posted_index)
        # tools.save_pickle(self.output_path + r"Indexes\user_id_to_username", user_id_to_username_index)
        tools.save_pickle(self.output_path + r"Indexes\user_id_to_screen_name", user_id_to_username_index)

    def create_super_documents(self):
        user_to_tweets_posted_index = tools.load_pickle(self.output_path + r"Indexes\user_id_to_tweets_ids_posted")
        # retrieve documents from the mongodb
        counter = 0
        print(len(user_to_tweets_posted_index))
        for user_id, tweet_id_list in user_to_tweets_posted_index.items():
            print(counter)
            counter += 1
            user_doc_string = ""
            for idx, tweet_id in enumerate(tweet_id_list):
                tweet_record = self.collection.find_one({"tweet_id": tweet_id})
                user_doc_string += tweet_record["preprocessed_text"].replace("rt", "") + " "
            # user_id_to_super_documents[user_id] = user_doc_string
            self.superdocs.insert_one({"author_id": user_id, "super_document": user_doc_string,
                                       "number_of_tweets": len(tweet_id_list)})

    # Calculates bert vectors for the super-documents and stores them in mongodb converted as text.
    def calculate_text_bert_vectors(self):
        # We need no_cursor_timeout=True because otherwise the cursor times out after ten minutes
        # Always remember to add also cursor.close() after the job is done.
        # The cursor can still close after 30 minutes because FML and this:
        # https://www.mongodb.com/docs/v4.4/reference/method/cursor.noCursorTimeout/#session-idle-timeout-overrides-nocursortimeout
        cursor = self.superdocs.find({}, no_cursor_timeout=True)
        all_author_ids = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Coordination\I_O\Datasets\Climate_Changed\\"
                                           r"I_O\bin\all_authors_ids")
        # all_author_ids = list()
        # for count, document in enumerate(cursor):
        #     all_author_ids.append(document["author_id"])
        # tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Coordination\I_O\Datasets\Climate_Changed\\"
        #                   r"I_O\bin\all_authors_ids", all_author_ids)
        cursor.close()
        counter = 0
        for author_id in all_author_ids:
            print(counter)
            counter += 1
            super_doc_record = self.superdocs.find_one({"author_id": author_id})
            vector = self.doc_vectorizer(super_doc_record["super_document"])
            string_vector = self.vector_to_string(vector)
            self.superdocs.update_one({"author_id": author_id}, {"$set": {"bert_vector": string_vector}})

        # similarity = cosine_similarity(vector1, vector2)
        # print(similarity[0][0])

    @staticmethod
    def vector_to_string(nd_vector):
        vector_string = ""
        for value in nd_vector[:-1]:
            vector_string += str(value) + ","
        vector_string += str(nd_vector[-1])
        return vector_string

    @staticmethod
    def string_to_vector(str_vector):
        vector_nums = str_vector.split(",")
        list_of_nums = [float(x) for x in vector_nums]
        np_vector = np.asarray(list_of_nums, dtype=np.float64)
        return np_vector

    def doc_vectorizer(self, document):
        max_chunk_length = 512
        chunks = [document[i:i + max_chunk_length] for i in range(0, len(document), max_chunk_length)]
        embeddings = np.zeros((len(chunks), max_chunk_length, 768))

        with torch.no_grad():
            for i, chunk in enumerate(chunks):
                # Tokenize the chunk and add special [CLS] and [SEP] tokens
                tokens = self.tokenizer.encode_plus(chunk, add_special_tokens=True, return_tensors='pt')
                # Get the BERT embeddings for the tokens
                outputs = self.model(**tokens)
                chunk_embeddings = outputs.last_hidden_state
                # Copy the embeddings into the embeddings array
                embeddings[i, :chunk_embeddings.shape[1], :] = chunk_embeddings.squeeze().numpy()

        # Average the embeddings across chunks to obtain a single vector representation for the entire document
        document_vector = np.mean(embeddings, axis=(0, 1))
        return document_vector

    def manual_adding_records(self):
        all_author_ids = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\\"
                                           r"Datasets\Climate_Changed\I_O\bin\all_authors_ids")
        target_id = all_author_ids[2627414]
        print()

    # Pruning users with less than X amount of tweets.
    def gather_user_ids_with_many_tweets(self, number_of_tweets):
        list_of_user_ids_with_many_tweets = list()
        all_author_ids = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\\"
                                           r"Datasets\Climate_Changed\I_O\bin\all_authors_ids")
        for idx, author_id in enumerate(all_author_ids):
            super_doc_record = self.superdocs.find_one({"author_id": author_id})
            try:
                if super_doc_record["number_of_tweets"] > number_of_tweets:
                    list_of_user_ids_with_many_tweets.append(author_id)
            except Exception as e:
                print(e)
                print(idx)
                print(super_doc_record)
        tools.save_pickle(self.output_path + r"bin\authors_with_more_than_"
                          + str(number_of_tweets) + "_tweets", list_of_user_ids_with_many_tweets)
        print(len(list_of_user_ids_with_many_tweets))


if __name__ == "__main__":
    climate_change_archiver = TweetArchiver()
    # climate_change_archiver.parse_tweets()
    # climate_change_archiver.working_on_users()
    # climate_change_archiver.create_super_documents()
    # climate_change_archiver.doc_vectorizer("tt")
    # climate_change_archiver.calculate_text_bert_vectors()
    # climate_change_archiver.manual_adding_records()
    # climate_change_archiver.gather_user_ids_with_many_tweets(10)
    climate_change_archiver.indexing_users()
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\I_O\\"
    #                       r"Indexes\user_id_to_username")
    print()
