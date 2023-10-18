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

    def create_inverted_index(self):
        user_keyword_dict = \
            tools.load_pickle(self.main_path + r"I_O\Indexes\normalized_user_to_keywords_list_more_than_four_tweets")
        for user_id, keyword_list in user_keyword_dict.items():
            for keyword_tuple in keyword_list:
                self.word_to_users_inverted_index[keyword_tuple[0]].append((user_id, keyword_tuple[1]))
        tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\\"
                          r"I_O\Indexes\word_to_user_more_than_four_tweets", self.word_to_users_inverted_index)

    def create_relations(self):
        self.word_to_users_inverted_index = \
            tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
                              r"Indexes\word_to_user_more_than_four_tweets")

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
                    # The 0.007 number came out after calculating the mean of all the distances
                    # of all the users.
                    if current_distance > 0.007:
                        min_id = min(user_freq_tuple[0], inner_user_tuple[0])
                        max_id = max(user_freq_tuple[0], inner_user_tuple[0])
                        self.dict_of_user_distance[(min_id, max_id)] += current_distance
                    # all_distances += abs(user_freq_tuple[1] - inner_user_tuple[1])
                    # couples_counter += 1

        # print("Final distances mean:", all_distances / couples_counter)
        # print("Final number of couples:", couples_counter)
        # tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Text_Based\I_O\\"
        #                   r"Results\user_distances", self.dict_of_user_distance)


if __name__ == "__main__":
    g_creator = GraphCreator()
    # g_creator.create_inverted_index()
    g_creator.create_relations()
