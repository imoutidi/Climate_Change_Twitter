import numpy as np
import random

class LSH:
    def __init__(self, k, L, n, r):
        self.k = k  # Number of hash functions
        self.L = L  # Number of hash tables
        self.n = n  # Number of data points
        self.r = r  # Radius for nearby neighbors

        self.hash_tables = []

    def initialize_hash_tables(self):
        for _ in range(self.L):
            hash_table = {}
            self.hash_tables.append(hash_table)

    def hash(self, vector):
        hash_values = []
        for _ in range(self.L):
            hash_value = 0
            for _ in range(self.k):
                random_vector = np.random.randn(self.n)
                dot_product = np.dot(vector, random_vector)
                if dot_product >= 0:
                    hash_value = (hash_value << 1) + 1
                else:
                    hash_value = hash_value << 1
            hash_values.append(hash_value)
        return hash_values

    def index(self, data):
        self.initialize_hash_tables()
        for i, vector in enumerate(data):
            hash_values = self.hash(vector)
            for j, hash_value in enumerate(hash_values):
                if hash_value not in self.hash_tables[j]:
                    self.hash_tables[j][hash_value] = []
                self.hash_tables[j][hash_value].append(i)

    def query(self, query_vector):
        hash_values = self.hash(query_vector)
        candidate_set = set()
        for j, hash_value in enumerate(hash_values):
            if hash_value in self.hash_tables[j]:
                candidate_set.update(self.hash_tables[j][hash_value])

        result = []
        for i in candidate_set:
            distance = np.linalg.norm(query_vector - data[i])
            if distance <= self.r:
                result.append(i)

        return result


# Example usage
data = np.random.randn(1000, 10)  # Random data points
query = np.random.randn(10)  # Random query vector

lsh = LSH(k=4, L=5, n=10, r=0.5)
lsh.index(data)
result = lsh.query(query)

print("Nearest neighbors:")
for index in result:
    print(f"Index: {index}, Distance: {np.linalg.norm(query - data[index])}")