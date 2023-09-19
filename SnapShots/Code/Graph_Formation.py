from Tool_Pack import tools
from collections import defaultdict
import os
from pymongo import MongoClient
import time


class ContentGraph:

    def __init__(self):
        self.distance_path = r"C:\Users\irmo\PycharmProjects\Climate_Change_Twitter\SnapShots\I_O\\" \
                             r"Tweet_Documents_Distance"
