import os
import re
import math
import numpy as np
from operator import itemgetter
from nltk.corpus import stopwords
from Tool_Pack import tools
from collections import defaultdict


class CorpusMaster:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"
        self.twitter_stopwords = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                                                   r"Text_Based\I_O\Pivot\mega_stop_words")
        self.user_to_post_count = defaultdict(int)
        # Default dict of a default dict need the lambda hack to make it work.
        # https://stackoverflow.com/questions/5029934/defaultdict-of-defaultdict
        # one issue here is that it is not pickleble
        # self.word_to_user_inverted_index = defaultdict(lambda: defaultdict(int))
        # A pickleble version is this
        # import functools
        # dd_int = functools.partial(defaultdict, int)
        # defaultdict(dd_int)
        self.word_to_user_inverted_index = dict()
        self.user_index = dict()

    def parse_tweets(self):
        date_dictionary = defaultdict(int)
        bancars_tweets = list()
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    if "ban" in tweet_obj.full_text and "cars" in tweet_obj.full_text:
                    # if "#bancars" in tweet_obj.full_text or "ban the cars" in tweet_obj.full_text:
                        bancars_tweets.append(tweet_obj)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Tests\Bin\bancars_keyword_list.pickle",
                          bancars_tweets)
                    # date_dictionary[tweet_obj.created_at.year] += 1

    def calculate_term_df(self):
        keyword_dictionary = defaultdict(int)
        for folder_index in range(16):
            print(folder_index)
            for filename in os.listdir(self.input_path + str(folder_index)):
                tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                for tweet_obj in tweet_records:
                    tweet_text = tweet_obj.full_text
                    word_list = tweet_text.split()
                    # Remove URLs
                    no_urls = [word.lower() for word in word_list if not re.match(r'https?://\S+', word)]
                    no_specials = [word.strip(":;'\"?.,<>*(){}[]!-_+=") for word in no_urls]
                    no_apostrophes = [word[:-2] if word.endswith("'s") else word for word in no_specials]
                    for keyword in no_apostrophes:
                        keyword_dictionary[keyword] += 1
        list_of_keyword_tuples = list()
        for keyword, frequency in keyword_dictionary.items():
            list_of_keyword_tuples.append((keyword, frequency))
        list_of_keyword_tuples = sorted(list_of_keyword_tuples, key=itemgetter(1), reverse=True)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\Tests\User_Based_Tests\\"
                          r"keyword_DF", list_of_keyword_tuples)

    def create_climate_stopwords(self):
        stop_words = set(stopwords.words('english'))
        climate_dfs = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\\"
                                        r"I_O\Tests\User_Based_Tests\keyword_DF")
        test_counter = 0
        for idx, df_tuple in enumerate(climate_dfs):
            if idx < 100:
                self.twitter_stopwords.add(df_tuple[0])
            if df_tuple[1] < 4:
                self.twitter_stopwords.add(df_tuple[0])
            if len(df_tuple[0]) < 2:
                self.twitter_stopwords.add(df_tuple[0])
        for s_word in stop_words:
            self.twitter_stopwords.add(s_word)
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\mega_stop_words",
                          self.twitter_stopwords)

    # TODO here convert the code to process tweets per year.
    def count_users_posts(self):
        for year in range(2006, 2020):
            self.user_to_post_count = defaultdict(int)
            print(year)
            if not os.path.exists(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                                  r"Text_Based\I_O\Pivot\Per_Year\\" + str(year)):
                os.makedirs(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                            r"Text_Based\I_O\Pivot\Per_Year\\" + str(year))

            for folder_index in range(16):
                print(folder_index)
                for filename in os.listdir(self.input_path + str(folder_index)):
                    tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                    # iterate on the corpus tweets
                    for tweet_obj in tweet_records:
                        if tweet_obj.created_at.year == year:
                            self.user_to_post_count[tweet_obj.author.id] += 1
            tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\Per_Year\\"
                              + str(year) + r"\user_to_post_count_dict_" + str(year), self.user_to_post_count)

    def create_inverted_index(self):
        for year in range(2006, 2020):
            self.word_to_user_inverted_index = dict()
            for folder_index in range(16):
                print(folder_index)
                for filename in os.listdir(self.input_path + str(folder_index)):
                    tweet_records = tools.load_pickle(self.input_path + str(folder_index) + r"\\" + filename)
                    # iterate on the corpus tweets
                    for tweet_obj in tweet_records:
                        if tweet_obj.created_at.year == year:
                            tweet_text = tweet_obj.full_text
                            word_list = tweet_text.split()
                            # Remove URLs
                            clean_urls = [word.lower() for word in word_list if not re.match(r'https?://\S+', word)]
                            no_specials = [word.strip(":;'\"?.,<>*(){}[]!-_+=") for word in clean_urls]
                            clean_apostrophes = [word[:-2] if word.endswith("'s") else word for word in no_specials]
                            for clean_word in clean_apostrophes:
                                if clean_word not in self.twitter_stopwords:
                                    if clean_word not in self.word_to_user_inverted_index:
                                        self.word_to_user_inverted_index[clean_word] = dict()
                                        self.word_to_user_inverted_index[clean_word][tweet_obj.author.id] = 1
                                    else:
                                        if tweet_obj.author.id not in self.word_to_user_inverted_index[clean_word]:
                                            self.word_to_user_inverted_index[clean_word][tweet_obj.author.id] = 1
                                        else:
                                            self.word_to_user_inverted_index[clean_word][tweet_obj.author.id] += 1
            # tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\\"
            #                   r"keyword_to_userid_to frequency_inverted_index", self.word_to_user_inverted_index)
            tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\Per_Year\\"
                              + str(year) + r"\keyword_to_userid_to_frequency_inverted_index_" + str(year),
                              self.word_to_user_inverted_index)

    def create_user_index(self):
        for year in range(2006, 2020):
            self.user_index = dict()
            inverted_index = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
                                               r"Pivot\Per_Year\\" + str(year) +
                                               r"\keyword_to_userid_to_frequency_inverted_index_" + str(year))
            user_to_post_count = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
                                                   r"I_O\Pivot\Per_Year\\" + str(year) +
                                                   r"\user_to_post_count_dict_" + str(year))
            for idx, (keyword, users_dict) in enumerate(inverted_index.items()):
                print(idx)
                for user_id, keyword_frequency in users_dict.items():
                    # Users with more than X number of tweets
                    if user_to_post_count[user_id] > 4:
                        if user_id not in self.user_index:
                            self.user_index[user_id] = [(keyword, keyword_frequency)]
                        else:
                            self.user_index[user_id].append((keyword, keyword_frequency))
            for user_id, frequency_list in self.user_index.items():
                self.user_index[user_id] = sorted(self.user_index[user_id], key=itemgetter(1), reverse=True)
            tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
                              r"I_O\Pivot\Per_Year\\" + str(year) +
                              r"\user_to_keywords_list_more_than_four_tweets_" + str(year), self.user_index)

    @staticmethod
    def normalize_user_index():
        for year in range(2006, 2010):
            user_index = tools.load_pickle(
                r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\Per_Year\\" + str(year) +
                r"\user_to_keywords_list_more_than_four_tweets_" + str(year))
            counter = 0
            for user_id, k_list in user_index.items():
                counter += len(k_list)
            mean_keywords_per_user = 0
            if len(user_index) > 0:
                mean_keywords_per_user = math.floor(counter/len(user_index))

            for user_id, keyword_list in user_index.items():
                sum_of_frequencies = 0
                if len(keyword_list) >= mean_keywords_per_user:
                    for word_tuple in keyword_list[:mean_keywords_per_user]:
                        sum_of_frequencies += word_tuple[1]
                    normalized_keywords = \
                        [(k_word[0], k_word[1] / sum_of_frequencies) for k_word in keyword_list[:mean_keywords_per_user]
                         if k_word[1] / sum_of_frequencies > 0.01]
                else:
                    for word_tuple in keyword_list:
                        sum_of_frequencies += word_tuple[1]
                    normalized_keywords = \
                        [(k_word[0], k_word[1] / sum_of_frequencies) for k_word in keyword_list
                         if k_word[1] / sum_of_frequencies > 0.01]
                user_index[user_id] = normalized_keywords
            tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\Per_Year\\"
                              + str(year) + r"\normalized_user_to_keywords_list_more_than_four_tweets_" + str(year),
                              user_index)

    @staticmethod
    def metrics_of_user_similarities():
        sims = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Indexes\\"
                                 r"finalized_indexes\Partitioned_Distances\user_similarities")
        values = list(sims.values())

        sims_array = np.array(values)
        mean = np.mean(sims_array)
        std_dev = np.std(sims_array)
        print(mean, std_dev)
        # mean = 0.3661729998829109
        # std_dev = 0.22546568640670767


if __name__ == "__main__":
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\Per_Year\2019\\"
    #                       r"normalized_user_to_keywords_list_more_than_four_tweets_2019")
    # b = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\"
    #                       r"Tweet_Documents_Distance\2017\Distances_Parts\1_2017")
    # print()
    c_corpus = CorpusMaster()
    # c_corpus.count_users_posts()
    # c_corpus.parse_tweets()
    # c_corpus.calculate_term_df()
    # c_corpus.create_climate_stopwords()
    # c_corpus.create_inverted_index()
    # c_corpus.create_user_index()
    c_corpus.normalize_user_index()
    # c_corpus.metrics_of_user_similarities()
