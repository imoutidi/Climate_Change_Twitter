import re
import csv
from pymongo import MongoClient
from Tool_Pack import tools


def csv_line_counter(csv_path):
    with open(csv_path) as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            pass
        return csv_reader.line_num - 1


def mongo_tests():
    dumy_list = [1, 2, 3, 4, 5, 6]
    client = MongoClient('localhost', 27017)
    db = client.Climate_Change_Tweets
    collection = db.test_base
    collection.insert_one({"author_id": 1, "author_username": "gg", "id_list": dumy_list})


# This will be deprecated (it replaced author ids with selected username which contain a lot of
# non utf-8 characters and duplicate names.)
# WARNING to run as a tool we need to replace the self variables.
def create_csv_file(self):

    # list_of_pairs = tools.load_pickle(self.output_path + r"bin\list_of_rt_edges_with_duplicates")
    list_of_top_pairs = tools.load_pickle(self.output_path + r"bin\list_of_top_pairs_10_percent_no_self_rt")
    # We will keep only alphanumerics
    regex = re.compile('[^a-zA-Z0-9]')
    with open(self.output_path + r"Graph_files\retweet_network_top_10_with_author_names_no_retweets.csv",
              "w", encoding='utf-8') as csv_file:
        csv_file.write("source, target, weight\n")
        counter = 0
        for rt_pair, num_of_rts in list_of_top_pairs.items():
            print(counter)
            counter += 1
            if self.collection_tweets.find_one({"author_id": rt_pair[0]}):
                name_pair0 = self.collection_tweets.find_one({"author_id": rt_pair[0]})["author_username"]
                name_pair0 = regex.sub('', name_pair0)
                if len(name_pair0) == 0:
                    name_pair0 = str(rt_pair[0])
                if self.collection_tweets.find_one({"author_id": rt_pair[1]}):
                    name_pair1 = self.collection_tweets.find_one({"author_id": rt_pair[1]})["author_username"]
                    name_pair1 = regex.sub('', name_pair1)
                    # if the regex cleared all character and the username is now blank we
                    # insert the author_id as label.
                    if len(name_pair1) == 0:
                        name_pair1 = str(rt_pair[1])
                    csv_file.write(name_pair0 + ", " + name_pair1 + ", " + str(num_of_rts) + "\n")
