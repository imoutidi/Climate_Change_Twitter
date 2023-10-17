import os
import math
from operator import itemgetter
from Tool_Pack import tools
from collections import defaultdict


class GraphCreator:
    def __init__(self):
        self.main_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
        self.dict_of_user_similarity = defaultdict(list)
        # An index with words as keys, and lists of users with the frequency that they used each word
        self.word_to_users_inverted_index = defaultdict(list)

    def create_inverted_index(self):
        user_keyword_dict = \
            tools.load_pickle(self.main_path + r"I_O\Indexes\normalized_user_to_keywords_list_more_than_one_tweet")
        for user_id, keyword_list in user_keyword_dict.items():
            for keyword_tuple in keyword_list:
                self.word_to_users_inverted_index[keyword_tuple[0]].append((user_id, keyword_tuple[1]))
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
                          r"I_O\Indexes\word_to_user_more_than_one_tweet", self.word_to_users_inverted_index)


    def create_relations(self):
        user_keyword_vectors = \
            tools.load_pickle(self.main_path + r"I_O\Indexes\normalized_user_to_keywords_list_more_than_one_tweet")
        self.word_to_users_inverted_index = \
            tools.load_pickle(r"C:\Users\irmo\PycharmProjects\\"
                              r"Climate_Change_Twitter\Text_Based\I_O\Indexes\word_to_user_more_than_one_tweet")

        for user_id, list_of_word_freqs in user_keyword_vectors.items():
            for word_tuple in list_of_word_freqs:
                for idx, user_freq_tuple in enumerate(self.word_to_users_inverted_index[word_tuple[0]]):
                    # the users ids are always sorted in the tuple that are key in
                    # self.dict_of_user_similarity

                    # self.dict_of_user_similarity[()]
                    print()


if __name__ == "__main__":
    g_creator = GraphCreator()
    # g_creator.create_inverted_index()
    g_creator.create_relations()
