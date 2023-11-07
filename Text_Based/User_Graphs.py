import os
import math
import networkx as nx
from operator import itemgetter
from Tool_Pack import tools
from collections import defaultdict
import time


class GraphCreator:
    def __init__(self):
        self.main_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
        self.dict_of_user_distance = defaultdict(int)
        self.overall_network = nx.Graph()
        # An index with words as keys, and lists of users with the frequency that they used each word
        self.word_to_users_inverted_index = defaultdict(list)
        self.common_words_users_index = defaultdict(set)

    def create_inverted_index(self):
        for year in range(2006, 2020):
            print(year)
            self.word_to_users_inverted_index = defaultdict(list)
            user_keyword_dict = \
                tools.load_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                                  r"\normalized_user_to_keywords_list_more_than_four_tweets_" + str(year))
            print()
            for user_id, keyword_list in user_keyword_dict.items():
                for keyword_tuple in keyword_list:
                    self.word_to_users_inverted_index[keyword_tuple[0]].append((user_id, keyword_tuple[1]))
            tools.save_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                              r"\word_to_user_more_than_four_tweets_" + str(year),
                              self.word_to_users_inverted_index)

    # This method is deprecated
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
        for year in range(2006, 2020):
            print(year)
            user_to_words_index = tools.load_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                                                    r"\user_to_word_dict_" + str(year))
            self.word_to_users_inverted_index = tools.load_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                                                                  r"\word_to_user_more_than_four_tweets_" + str(year))
            user_similarities = dict()

            # start_time = time.perf_counter()
            for idx, (user_id, keywords) in enumerate(user_to_words_index.items()):
                current_user_commons = set()
                for current_keyword, current_frequency in keywords.items():
                    users_of_the_word = self.word_to_users_inverted_index[current_keyword]
                    for user_tuple in users_of_the_word:
                        current_user_commons.add(user_tuple[0])
                for inner_user_id in current_user_commons:
                    min_id = min(user_id, inner_user_id)
                    max_id = max(user_id, inner_user_id)

                    if (min_id, max_id) not in user_similarities and user_id != inner_user_id:
                        user_similarity = self.calculate_similarity(keywords, user_to_words_index[inner_user_id])
                        if user_similarity > 0.1:
                            user_similarities[(min_id, max_id)] = user_similarity
                #     print("Processing time: " + str(time.perf_counter() - start_time) + " seconds")
                if idx % 10000 == 0:
                    tools.save_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                                      r"\user_similarities_" + str(year), user_similarities)
                    tools.save_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                                      r"\last_user_id_and_idx_" + str(year), (user_id, idx))
            tools.save_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                              r"\user_similarities_" + str(year), user_similarities)

    def create_graph(self):
        user_similarities = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
                                              r"Indexes\finalized_indexes\Partitioned_Distances\user_similarities")
        for user_tuple, similarity in user_similarities.items():
            print()
            self.overall_network.add_edge(user_tuple[0], user_tuple[1], weight=similarity)

    @staticmethod
    def calculate_similarity(user_set1, user_set2):
        user_similarity = 2
        u1_set = set()
        u2_set = set()
        for k_word in user_set1:
            u1_set.add(k_word)
        for k_word in user_set2:
            u2_set.add(k_word)
        common_set = u1_set.intersection(u2_set)
        uncommon_set1 = u1_set.difference(common_set)
        uncommon_set2 = u2_set.difference(common_set)
        # We get only the very connected user couples.
        # The number 6 is calculated based on the average
        # number of keyword each user has.
        if len(common_set) > 6:
            # Do the calculations
            for common in common_set:
                user_similarity -= abs(user_set1[common] - user_set2[common])
            for uncommon in uncommon_set1:
                user_similarity -= user_set1[uncommon]
            for uncommon in uncommon_set2:
                user_similarity -= user_set2[uncommon]
        else:
            user_similarity = 0
        return user_similarity

    def transform_user_to_keyword_index(self):
        for year in range(2006, 2020):
            user_to_word_dict = dict()
            user_to_word = tools.load_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                                             r"\normalized_user_to_keywords_list_more_than_four_tweets_" + str(year))
            for user_id, keyword_list in user_to_word.items():
                temp_dict = dict()
                for keyword_tuple in keyword_list:
                    temp_dict[keyword_tuple[0]] = keyword_tuple[1]
                user_to_word_dict[user_id] = temp_dict
            tools.save_pickle(self.main_path + r"I_O\Pivot\Per_Year\\" + str(year) +
                              r"\user_to_word_dict_" + str(year), user_to_word_dict)

    # useful users per year.
    # 2006 0
    # 2007 89
    # 2008 608
    # 2009 14391
    # 2010 9750
    # 2011 4698
    # 2012 2102
    # 2013 3798
    # 2014 8563
    # 2015 34233
    # 2016 32351
    # 2017 74142
    # 2018 176547
    # 2019 51707


if __name__ == "__main__":
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\Pivot\\"
    #                       r"Per_Year\2008\user_to_post_count_dict_2008")
    # print()
    g_creator = GraphCreator()
    # g_creator.create_inverted_index()
    # g_creator.transform_user_to_keyword_index()
    g_creator.create_common_words_users_index()
