from Tool_Pack import tools
from LocalitySensitiveHashing import *
from pymongo import MongoClient
import time
# in case you need this: pip install BitVector


# TODO retrieve bert vectors from the mongo database
def get_bert_vectors():
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection_tweets = db.tweet_documents
    collection_superdocs = db.super_documents
    # author_record = collection_superdocs.find_one({"author_id": author_id})
    cursor = collection_superdocs.find({})
    with open(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\LSH_files\\"
              r"doc_vectors.csv", "w") as lsh_vectors:
        doc_counter = 0
        for document in cursor:
            lsh_vectors.write("sample_" + str(doc_counter) + "," + document["bert_vector"] + "\n")
            print(doc_counter)
            doc_counter += 1


#  LSH
def LSHing():
    lsh = LocalitySensitiveHashing(
        # datafile=r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\LSH_files\doc_vectors.csv",
        datafile=r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\Near_Neighbors\I_O\Vectors\\"
                 r"lsh_vectors_3480.csv",
        dim=768,  # bert vector dimension
        r=50,  # number of rows in each band for r-wise AND in each band
        b=100,  # number of bands for b-wise OR over all b bands
        expected_num_of_clusters=10,
        debug=0,
    )
    lsh.get_data_from_csv()
    lsh.show_data_for_lsh()
    lsh.initialize_hash_store()
    lsh.hash_all_data()
    lsh.display_contents_of_all_hash_bins_pre_lsh()

    similarity_groups = lsh.lsh_basic_for_neighborhood_clusters()
    coalesced_similarity_groups = lsh.merge_similarity_groups_with_coalescence(similarity_groups)

    merged_similarity_groups = lsh.merge_similarity_groups_with_l2norm_set_based(coalesced_similarity_groups)

    # lsh.evaluate_quality_of_similarity_groups(merged_similarity_groups)

    print("\n\nWriting the clusters to file 'clusters.txt'")
    lsh.write_clusters_to_file(merged_similarity_groups, "clusters.txt")


if __name__ == "__main__":
    # get_bert_vectors()
    start_time = time.perf_counter()
    LSHing()
    print("processing time: " + str(time.perf_counter() - start_time) + " seconds")


