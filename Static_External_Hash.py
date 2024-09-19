"""
Implementação de Hash Externo Estático.
"""

import math

from Util import infer_types_from_record, check_interval, generate_range
from Hash_Table import Hash_Table

class Static_External_Hash:
    def __init__(self, block_size, field_sizes, filename):
        self.block_size = block_size
        self.field_sizes = field_sizes
        self._set_record_size()
        self.buckets = []
        self._read_file(filename)
    
    def _set_field_names(self, line):
        self.field_names = line.split(',')

    def _set_record_size(self):
        self.record_size = sum(self.field_sizes) + len(self.field_sizes)
    
    def _set_field_types(self):
        record = self.blocks[0][:self.record_size]
        self.field_types = infer_types_from_record(record, len(self.field_names))
    
    def _padding(self, field, field_id):
        diff = self.field_sizes[field_id] - len(field)
        padded_field = field + (' ' * diff)
        return padded_field

    def _format_record(self, record):
        formatted_record = ''
        fields = record.strip().split(',')
        for i in range(len(fields)):
            if len(fields[i]) < self.field_sizes[i]:
                padded_field = self._padding(fields[i], i)
                formatted_record += padded_field + ','
            else:
                formatted_record += fields[i] + ','
        return formatted_record

    def _write_record(self, record):
        fields = record.strip().strip(',')
        record_primary_key = int(fields[0])
        formatted_record = self._format_record(record)
        self.buckets.insert(primary_key=record_primary_key, record=record)

    def _is_prime(self, n):
        for i in range(2, math.sqrt(n)+1):
            if n % i == 0:
                return False
        return True

    def _compute_hash_table_size(self, file_length):
        hash_table_size = 2
        for i in range(2, file_length):
            if self._is_prime(i) and i > hash_table_size:
                hash_table_size = i
        return hash_table_size

    def _read_file_check_primary_key_constraint(self, records):
        primary_keys = []
        for record in records:
            fields = record.strip().split(',')
            record_primary_key = int(fields[0].strip())
            if record_primary_key not in primary_keys:
                primary_keys.append(record_primary_key)
            else:
                raise Exception("PrimaryKeyConstraint: Duplicated Primary Key.")

    def _read_file(self, filename):
        with open(filename, 'r') as file:
            # Checking primary key constraint
            file.readline()
            self._read_file_check_primary_key_constraint(records=file)
        with open(filename, 'r') as file:
            # Creating buckets (Hash Table)
            file_length = len(file.readlines())-1
            hash_table_size = self._compute_hash_table_size(file_length=file_length)
            hash_function = lambda key : key % hash_table_size
            self.buckets = Hash_Table(size=hash_table_size, hash_function=hash_function)
        with open(filename, 'r') as file:
            # Filling buckets
            self._set_field_names(file.readline())
            self._read_file_check_primary_key_constraint(records=file)
            for record in file:
                self._write_record(record)
            self._set_field_types()