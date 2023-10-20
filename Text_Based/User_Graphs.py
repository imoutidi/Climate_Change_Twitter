import os
import math
from operator import itemgetter
from Tool_Pack import tools
from collections import defaultdict


class GraphCreator:
    def __init__(self):
        self.main_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
        self.dict_of_user_distance = defaultdict(int)
        # An index with words as keys, and lists of users with the frequency that they used each word
        self.word_to_users_inverted_index = defaultdict(list)
        self.common_words_users_index = defaultdict(set)

    def create_inverted_index(self):
        user_keyword_dict = \
            tools.load_pickle(self.main_path + r"I_O\Indexes\normalized_user_to_keywords_list_more_than_four_tweets")
        for user_id, keyword_list in user_keyword_dict.items():
            for keyword_tuple in keyword_list:
                self.word_to_users_inverted_index[keyword_tuple[0]].append((user_id, keyword_tuple[1]))
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
                          r"I_O\Indexes\word_to_user_more_than_four_tweets", self.word_to_users_inverted_index)

    def create_relations(self):
        a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Indexes\\"
                              r"normalized_user_to_keywords_list_more_than_four_tweets")

        self.word_to_users_inverted_index = \
            tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
                              r"Indexes\word_to_user_more_than_four_tweets")
        print()

        all_distances = 0
        couples_counter = 0
        for index, (word, list_of_user_freq) in enumerate(self.word_to_users_inverted_index.items()):
            print(index)
            # We iterate and calculate for couples of user ids, for each iteration we don't look back
            # and the last element has already been calculated

            # print(len(list_of_user_freq))
            for idx, user_freq_tuple in enumerate(list_of_user_freq[:-1]):
                # print("idx:", idx)
                for inner_user_tuple in list_of_user_freq[idx+1:]:
                    # the users ids are always sorted in the tuple that are key in
                    # self.dict_of_user_similarity that way we will not have reversed duplicates
                    current_distance = abs(user_freq_tuple[1] - inner_user_tuple[1])
                    if current_distance == 0.0:
                        print(user_freq_tuple[1], inner_user_tuple[1])

                    min_id = min(user_freq_tuple[0], inner_user_tuple[0])
                    max_id = max(user_freq_tuple[0], inner_user_tuple[0])
                    self.dict_of_user_distance[(min_id, max_id)] += current_distance
            print(len(self.dict_of_user_distance))
                    # all_distances += abs(user_freq_tuple[1] - inner_user_tuple[1])
                    # couples_counter += 1

        # print("Final distances mean:", all_distances / couples_counter)
        # print("Final number of couples:", couples_counter)
        # tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
        #                   r"Results\user_distances", self.dict_of_user_distance)

    def create_common_words_users_index(self):
        user_to_words_index = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
                                                r"I_O\Indexes\finalized_indexes\user_to_word_dict")
        self.word_to_users_inverted_index = \
            tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
                              r"Indexes\word_to_user_more_than_four_tweets")

        for idx, (user_id, keywords) in enumerate(user_to_words_index.items()):
            print(idx)
            current_user_commons = set()
            for word_tuple in keywords:
                users_of_the_word = self.word_to_users_inverted_index[word_tuple[0]]
                for user_tuple in users_of_the_word:
                    current_user_commons.add(user_tuple[0])
            for inner_user_id in current_user_commons:
                self.calculate_similarity(keywords, user_to_words_index[inner_user_id])


            print()
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
                          r"Pivot\common_words_users_index", self.common_words_users_index)

    @staticmethod
    def calculate_similarity(user_vector1, user_vector2):
        u1_set = set()
        u2_set = set()
        for keyword_tuple1, keyword_tuple2 in zip(user_vector1, user_vector2):
            u1_set.add(keyword_tuple1[0])
            u2_set.add(keyword_tuple2[0])
        common_words = u1_set.union(u2_set)
        print()
        if len(common_words) > 10:
            # Do the calculations
            
            print()

    @staticmethod
    def transform_user_to_keyword_index():
        user_to_word_dict = dict()
        user_to_word = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
                                         r"I_O\Indexes\normalized_user_to_keywords_list_more_than_four_tweets")
        for user_id, keyword_list in user_to_word.items():
            temp_dict = dict()
            for keyword_tuple in keyword_list:
                temp_dict[keyword_tuple[0]] = keyword_tuple[1]
            user_to_word_dict[user_id] = temp_dict
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Indexes\\"
                          r"finalized_indexes\user_to_word_dict", user_to_word_dict)


if __name__ == "__main__":
    g_creator = GraphCreator()
    # g_creator.create_inverted_index()
    # g_creator.create_relations()
    g_creator.transform_user_to_keyword_index()
    # g_creator.create_common_words_users_index()
