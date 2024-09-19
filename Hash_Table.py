"""
Implementação da Hash Table usada em Hash Externo Estático.
"""

import math

class Hash_Table:
    def __init__(self, buckets, bucket_size, hash_function):
        self.table = [None for i in range(0, bucket_size)]
        self.bucket_size = bucket_size
        self.hash_function = hash_function
    
    def insert(self, primary_key, record):
        hash_key = self.hash_function(primary_key)
        if self.table[hash_key] == None:
            self.table[hash_key] = [record]
            return True
        else:
            if record in self.table[hash_key]:
                return False
            else:
                self.table[hash_key].append(record)
                return True
    
    def search(self, x):
        hash_key = self.hash_function(x)
        if self.table[hash_key] == None:
            return False
        else:
            overflow = self.table[hash_key]
            for i in range(len(overflow)):
                if overflow[i] == x:
                    return True
                return False
    
    def delete(self, x):
        hash_key = self.hash_function(x)
        if self.table[hash_key] == None:
            return False
        else:
            self.table[hash_key].pop(x)
            return True