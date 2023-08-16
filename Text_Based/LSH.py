from Tool_Pack import tools
from LocalitySensitiveHashing import *
from pymongo import MongoClient
import time
import numpy as np
import hnswlib
import pickle
# in case you need this: pip install BitVector

import shutil
import urllib.request as request
from contextlib import closing
import tarfile
import faiss


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


def lsh_random_projection():
    nbits = 4
    dimensions = 768
    # Those are the hyper planes
    plane_norms = np.random.rand(nbits, dimensions) - 0.5  # -0.5 is to center near the origin zero axis
    print()

    # Those are the vectors to hash.
    bert_3480 = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                                  r"Near_Neighbors\I_O\Vectors\bert_3480")
    # a = np.asarray([1, 2])
    # b = np.asarray([2, 1])
    # c = np.asarray([3, 1])

    # Checking if the vector is on the right of left sie of the hyper plane
    # and creating binary vectors for the hash table.
    binary_vectors = list()
    for b_vector in bert_3480:
        binary_vectors.append((np.dot(b_vector, plane_norms.T) > 0).astype(int))
    # a_dot = np.dot(a, plane_norms.T)
    # b_dot = np.dot(b, plane_norms.T)
    # c_dot = np.dot(c, plane_norms.T)


    # converting into binary vectors

    # a_dot = (a_dot > 0).astype(int)
    # b_dot = (b_dot > 0).astype(int)
    # c_dot = (c_dot > 0).astype(int)
    # print(b_dot)

    # vectors = [a_dot, b_dot, c_dot]
    buckets = {}

    for i in range(len(binary_vectors)):
        hash_str = "".join(binary_vectors[i].astype(str))
        if hash_str not in buckets:
            buckets[hash_str] = []
        buckets[hash_str].append(i)

    # for doc_list
    print(buckets)


def hnswlib_test():
    # Those are the vectors to hash.
    bert_3480 = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\\"
                                  r"Near_Neighbors\I_O\Vectors\bert_3480")
    bert1 = bert_3480[:1700]
    bert2 = bert_3480[1700:]
    b_d1 = np.vstack(bert1)
    b_d2 = np.vstack(bert2)
    bert_data = np.vstack(bert_3480)
    print()

    dim = 768
    num_elements = 3480

    # Generating sample data
    # data = np.float32(np.random.random((num_elements, dim)))
    ids1 = np.arange(1700)
    ids2 = np.arange(1700, 3480)

    print()

    # Declaring index
    nn_index = hnswlib.Index(space='l2', dim=dim)  # possible options are l2, cosine or ip

    # Initializing index - the maximum number of elements should be known beforehand
    nn_index.init_index(max_elements=num_elements, ef_construction=200, M=16)

    # Element insertion (can be called several times):
    nn_index.add_items(b_d1, ids1)
    print()
    # nn_index.add_items(b_d2, ids2)
    print()

    # Controlling the recall by setting ef:
    nn_index.set_ef(50)  # ef should always be > k

    # Query dataset, k - number of the closest elements (returns 2 numpy arrays)
    labels, distances = nn_index.knn_query(bert_data, k=10)

    print(labels)
    print(distances)

    tools.save_pickle(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\LSH_files\\"
                      r"indexes\test_index", nn_index)

    # Index objects support pickling
    # WARNING: serialization via pickle.dumps(p) or p.__getstate__() is NOT thread-safe with p.add_items method!
    # Note: ef parameter is included in serialization; random number generator is initialized with random_seed on Index load
    # p_copy = pickle.loads(pickle.dumps(p))  # creates a copy of index p using pickle round-trip

    ### Index parameters are exposed as class properties:
    # print(f"Parameters passed to constructor:  space={p_copy.space}, dim={p_copy.dim}")
    # print(f"Index construction: M={p_copy.M}, ef_construction={p_copy.ef_construction}")
    # print(f"Index size is {p_copy.element_count} and index capacity is {p_copy.max_elements}")
    # print(f"Search speed/quality trade-off parameter: ef={p_copy.ef}")


def increment_the_index():
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection_tweets = db.tweet_documents
    cursor = collection_tweets.find({})
    with open(r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\I_O\Datasets\LSH_files\\"
              r"doc_vectors.csv", "w") as lsh_vectors:
        doc_counter = 0
        for document in cursor:
            lsh_vectors.write("sample_" + str(doc_counter) + "," + document["bert_vector"] + "\n")
            print(doc_counter)
            doc_counter += 1

    print()



if __name__ == "__main__":
    # get_bert_vectors()
    # start_time = time.perf_counter()
    # LSHing()
    # print("processing time: " + str(time.perf_counter() - start_time) + " seconds")
    # lsh_random_projection()
    hnswlib_test()


