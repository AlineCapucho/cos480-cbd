"""
Implementação de Hash Externo Estático.
"""

import math

from Util import infer_types_from_record, check_interval, generate_range
from Hash_Table import Hash_Table

class Static_External_Hash:
    def __init__(self, block_size):
        self.block_size = block_size

    def _set_field_names(self, fields_names):
        self.field_names = fields_names.strip().split(',')

    def _set_field_sizes(self, field_sizes):
        self.field_sizes = field_sizes
    
    def _set_field_types(self, record):
        self.field_types = infer_types_from_record(record, len(self.field_names))

    def _set_record_size(self):
        self.record_size = sum(self.field_sizes) + len(self.field_sizes)
    
    def _set_blocking_factor(self):
        self.blocking_factor = math.floor(self.block_size / self.record_size)

    def _read_line(self, file):
        char = file.read(1)
        line = ''

        while char and char != '\n':
            line += char
            char = file.read(1)
        return line

    def _read_csv_header(self, file):
        csv_header = self._read_line(file=file)
        self._set_field_names(fields_names=csv_header)

    def _calculate_csv_field_sizes(self, file):
        field_sizes = dict()
        for field in self.field_names:
            field_sizes[field] = 0
        record = self._read_line(file=file)
        self.number_of_records = 0
        self._set_field_types(record=record)
        while record != '':
            self.number_of_records += 1
            record_fields = record.strip().split(',')
            for i in range(len(self.field_names)):
                if len(record_fields[i]) > field_sizes[self.field_names[i]]:
                    field_sizes[self.field_names[i]] = int(len(record_fields[i]))
            record = self._read_line(file=file)
        self._set_field_sizes(field_sizes=list(field_sizes.values()))
    
    def _is_prime(self, n):
        for i in range(2, math.sqrt(n)+1):
            if n % i == 0:
                return False
        return True

    def _closest_prime(self, n):
        closest_prime = 2
        for i in range(2, n+1):
            if self._is_prime(n=i):
                closest_prime = i
        return closest_prime

    def _set_hash_function(self):
        # Computes the number of buckets and overflow buckets that will be created
        M_approx = int(self.number_of_records / 0.7)
        M = self._closest_prime(self, n=M_approx)
        M_over = int(0.3 * M)
        self.number_of_buckets = M
        self.number_of_overflow_buckets = M_over
        # Creates the hash function
        self.hash_function = lambda key : key % self.number_of_buckets

    def _write_txt_header(self, txt_file, txt_filepath):
        # Sets txt_file cursor at the beginning of the file
        txt_file.seek(0, 0)
        # Write table name
        table_name = txt_filepath.split('/')[2].split('.')[0]
        txt_file.write(table_name + '\n')
        # Write field names, sizes, types
        field_names = ','.join(str(field_name) for field_name in self.field_names) + '\n'
        field_sizes = ','.join(str(field_size) for field_size in self.field_sizes) + '\n'
        field_types = ','.join(str(field_type) for field_type in self.field_types) + '\n'
        txt_file.write(field_names)
        txt_file.write(field_sizes)
        txt_file.write(field_types)
        # Write number of records
        txt_file.write(str(self.number_of_records) + '\n')
        # Write blocking factor
        txt_file.write(str(self.blocking_factor) + '\n')
        # Write number of buckets
        txt_file.write(str(self.number_of_buckets) + '\n')
        # Write number of overflow buckets
        txt_file.write(str(self.number_of_overflow_buckets) + '\n')
        # Write creation timestamp
        if self.creation_date == None:
            creation_timestamp = datetime.now()
            creation_timestamp = creation_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            txt_file.write(creation_timestamp + '\n')
        else:
            txt_file.write(self.creation_date + '\n')
        # Write alteration timestamp
        alteration_timestamp = datetime.now()
        alteration_timestamp = alteration_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        txt_file.write(alteration_timestamp + '\n')
    
    def _padding(self, record_field, field_id):
        diff = self.field_sizes[field_id] - len(record_field)
        padded_record_field = record_field + (' ' * diff)
        return padded_record_field

    def _format_record(self, record):
        formatted_record = ''
        record_fields = record.strip().split(',')
        for i in range(len(self.field_names)):
            if len(record_fields[i]) < self.field_sizes[i]:
                padded_record_field = self._padding(record_field=record_fields[i], field_id=i)
                formatted_record += padded_record_field + ','
            else:
                formatted_record += record_fields[i] + ','
        return formatted_record
    
    def _header_length(self, file):
        header_length = 0
        for i in range(0, 10):
            file.seek(header_length, 0)
            line = self._read_line(file)
            header_length += len(line)+1
        return header_length

    def _create_filled_buckets(self, file):
        # Sets cursor to after header
        header_length = self._header_length(file=file)
        file.seek(header_length, 0)
        # Fills the space after header
        filled_bucket = ("#" * self.block_size) + '\n'
        for i in range(0, self.number_of_buckets + self.number_of_overflow_buckets):
            file.write(filled_bucket)
            offset = header_length + ((i + 1) * len(filled_bucket))

    def _insert_record_in_bucket(self, record, file):
        # Gets record primary key
        record_fields = record.strip().split(',')
        record_primary_key = record_fields[0]
        # Evaluates record primary key in hash function to get expected bucket id
        bucket_id = self.hash_function(int(record_primary_key))
        # Read bucket
        header_length = self._header_length(file=file)
        offset = header_length + bucket_id * self.block_size + bucket_id
        file.seek(offset, 0)
        bucket = file.read(self.block_size)
        # Checks if there is available space for insertion
        available_space = self.block_size - len(bucket.strip('#'))
        # If there is available space inserts record in bucket
        if available_space >= self.record_size:
            # Gets used part of the bucket
            body = bucket[:-available_space]
            # Formats record
            formatted_record = self._format_record(record=record)
            # Filling new empty space in bucket
            empty_space_size = available_space - self.record_size
            empty_space = "#" * empty_space_size
            # Writes new bucket to file
            bucket = body + record + empty_space
            file.seek(offset, 0)
            file.write(bucket)
        else:
            # If there is not available space, tries to insert in overflow bucket
            success = False
            for i in range(0, self.number_of_overflow_buckets):
                # If insertion was sucessful, goes out of for loop
                if success:
                    break
                # Checks each overflow bucket for available space
                offset = header_length + self.block_size * (self.number_of_buckets + i)
                file.seek(offset, 0)
                bucket = file.read(self.block_size)
                # Checks if there is available space for insertion
                available_space = self.block_size - len(bucket.strip('#'))
                if available_space >= self.record_size:
                    # Gets used part of the bucket
                    body = bucket[:-available_space]
                    # Formats record
                    formatted_record = self._format_record(record=record)
                    # Filling new empty space in bucket
                    empty_space_size = available_space - self.record_size
                    empty_space = "#" * empty_space_size
                    # Writes new bucket to file
                    bucket = body + record + empty_space
                    file.seek(offset, 0)
                    file.write(bucket)
                    success = True
                    break

    def _write_txt_records(self, txt_file, csv_file):
        # Creates filled buckets
        self._create_filled_buckets(file=txt_file)
        # Sets csv_file cursor to beginning of the file
        csv_file.seek(0, 0)
        # Skips csv_file header
        self._read_line(file=csv_file)
        # Writes each record in a bucket
        for i in range(0, self.number_of_records):
            record = self._read_line(file=csv_file)
            self._insert_record_in_bucket(record=record, file=txt_file)

    def _write_from_csv_to_txt(self, csv_file, csv_filepath):
        # Create txt file
        txt_filepath = '.' + csv_filepath.split('.')[1] + '.txt'
        txt_file = open(txt_filepath, 'w')
        # Write txt file header
        self._write_txt_header(txt_file=txt_file, txt_filepath=txt_filepath)
        # Return to begin of csv_file
        csv_file.seek(0, 0)
        # Skip csv_file header
        self._read_line(csv_file)
        # Write txt file records
        self._write_txt_records(txt_file=txt_file, csv_file=csv_file)
        txt_file.close()

    def from_csv_to_txt(self, csv_filepath):
        file = open(csv_filepath, 'r+')
        self._read_csv_header(file=file)
        self._calculate_csv_field_sizes(file=file)
        self._set_record_size()
        self._set_blocking_factor()
        self.deleted_records = []
        self.creation_date = None
        self._set_hash_function()
        self._write_from_csv_to_txt(csv_file=file, csv_filepath=csv_filepath)
        file.close()