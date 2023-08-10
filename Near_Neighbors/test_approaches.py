from Tool_Pack import tools
import networkx as nx


def create_vectors_graph():
    tweet_dataset = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\\"
                                      r"I_O\test_tweets\merged_sentences_3480")
    bert_all_to_all = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\\"
                                        r"I_O\Similarities\bert_all_to_all")
    w2v_all_to_all = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\\"
                                       r"I_O\Similarities\w2v_all_to_all")
    bert_graph = nx.Graph()
    for idx, outer_sims in enumerate(bert_all_to_all[:-1]):
        for index, inner_sim in enumerate(outer_sims[idx + 1:]):
            bert_graph.add_edge(idx, index+idx+1, weight=inner_sim)
    communities = nx.community.louvain_communities(bert_graph, weight="weight", resolution=1.1,
                                                   seed=100)
    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\\"
                      r"I_O\Communities\bert_idx_coms", communities)
    ids_to_tweet = list()
    for com in communities:
        temp_com = list()
        for idx in com:
            temp_com.append(tweet_dataset[idx])
        ids_to_tweet.append(sorted(temp_com))
    print()


if __name__ == "__main__":
    create_vectors_graph()
