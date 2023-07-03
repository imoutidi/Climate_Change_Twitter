from Tool_Pack import tools
from LocalitySensitiveHashing import *
from pymongo import MongoClient

# TODO retrieve bert vectors from the mongo database


def get_bert_vectors():
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection_tweets = db.tweet_documents
    collection_superdocs = db.super_documents
    # author_record = collection_superdocs.find_one({"author_id": author_id})
    cursor = collection_superdocs.find({})
    for document in cursor:
        print(document["bert_vector"])
        keep_printing = input("Keep printing")
        if keep_printing != "Y":
            break


#  LSH


if __name__ == "__main__":
    get_bert_vectors()


