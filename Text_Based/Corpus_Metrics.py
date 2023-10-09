import os
import re
from operator import itemgetter
from nltk.corpus import stopwords
from Tool_Pack import tools
from collections import defaultdict


class CorpusMaster:

    def __init__(self):
        self.path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\Climate_Changed\\"
        self.input_path = self.path + r"Downloaded_Tweets\\"
        self.twitter_stopwords = set()

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
        for df_tuple in climate_dfs:
            if len(df_tuple[0]) < 2:
                print(df_tuple)
                test_counter += 1
        print(test_counter)


if __name__ == "__main__":
    c_corpus = CorpusMaster()
    # c_corpus.parse_tweets()
    # c_corpus.calculate_term_df()
    c_corpus.create_climate_stopwords()
