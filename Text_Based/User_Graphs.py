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
                print()


    def create_relations(self):
        user_keyword_vectors = \
            tools.load_pickle(self.main_path + r"I_O\Indexes\normalized_user_to_keywords_list_more_than_one_tweet")

        print()



if __name__ == "__main__":
    g_creator = GraphCreator()
    g_creator.create_inverted_index()
    g_creator.create_relations()