import networkx as nx
# switch to system intepreter to use igraph
# from igraph import Graph
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from Tool_Pack import tools


def compare_tags(self):
    for idx, c_date in enumerate(self.all_dates[:-1]):
        # report_file.write(c_date + " to " + self.all_dates[idx+1] + "\n")
        prev_com_tags = tools.load_pickle(self.communities_path + c_date + "/com_tags")
        prev_annotated_coms = tools.load_pickle(self.communities_path + c_date + "/annotated_coms")
        next_com_tags = tools.load_pickle(self.communities_path + self.all_dates[idx + 1] + "/com_tags")
        next_annotated_coms = tools.load_pickle(self.communities_path + self.all_dates[idx + 1]
                                                + "/annotated_coms")
        x_com_list = list()
        for annotation in next_annotated_coms:
            x_com_list.append(annotation)

        y_com_list = list()
        common_tags_lists = list()
        similarity_lists = list()
        for annotation, com_tags in zip(prev_annotated_coms, prev_com_tags):
            common_list = list()
            sim_list = list()

            y_com_list.append(annotation)
            only_tags = self.re_list_tags(com_tags)

            for index, anno in enumerate(next_annotated_coms):
                next_only_tags = self.re_list_tags(next_com_tags[index])
                common_tags = set(only_tags[:50]).intersection(next_only_tags[:50])
                common_list.append(len(common_tags))

                community_rank_similarity = 0
                for tag in common_tags:
                    prev_tag_index = only_tags.index(tag)
                    next_tag_index = next_only_tags.index(tag)
                    tag_rank_similarity = 49 - abs(prev_tag_index - next_tag_index)

                    community_rank_similarity += tag_rank_similarity
                sim_list.append(community_rank_similarity)
            common_tags_lists.append(common_list)
            similarity_lists.append(sim_list)
        tag_array = np.array(common_tags_lists)
        similarity_array = np.array(similarity_lists)
        self.heat_map(tag_array, idx, x_com_list, y_com_list, "Common Tags")
        self.heat_map(similarity_array, idx, x_com_list, y_com_list, "Rank Similarity")


def heat_map(self, data_array, index, x_coms, y_coms, title):
    sns.set(font_scale=1)
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.heatmap(data_array, annot=True, fmt="d", xticklabels=x_coms, yticklabels=y_coms, cmap="YlOrBr")
    plt.xticks(rotation=65)
    # plt.title(title + " of communities from " + self.all_dates[index] + " to " + self.all_dates[index + 1])
    plt.title("Average percentage of users remaining in the same community")
    # plt.xlabel("Communities of " + self.all_dates[index + 1])
    # plt.ylabel("Communities of " + self.all_dates[index])

    plt.subplots_adjust(left=0.2, wspace=0.8, top=0.8)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    print("/home/iraklis/PycharmProjects/SO_New/IO_Files/Year_Communities/Heat_Maps/" + title + "/avg" +
          self.all_dates[index] + "_" + self.all_dates[index])

    plt.savefig("/home/iraklis/PycharmProjects/SO_New/IO_Files/Year_Communities/Heat_Maps/" + title + "/avg" +
                self.all_dates[index] + "_" + self.all_dates[index + 1], bbox_inches='tight', format="eps", dpi=300)