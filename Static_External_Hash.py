"""
Implementação de Hash Externo Estático.
"""

import math
from datetime import datetime

import tarfile
import os

from Util import infer_types_from_record, check_interval, generate_range

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
        for i in range(2, int(math.sqrt(n)+1)):
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
        M = self._closest_prime(n=M_approx)
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
            bucket = body + formatted_record + empty_space
            file.seek(offset, 0)
            file.write(bucket)
            # Checks if bucket was written correctly
            file.seek(offset, 0)
            bucket_read = file.read(self.block_size)
            if bucket != bucket_read:
                raise Exception("WriteError: Bucket Not Written Correctly.")
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
                    bucket = body + formatted_record + empty_space
                    file.seek(offset, 0)
                    file.write(bucket)
                    # Checks if bucket was written correctly
                    file.seek(offset, 0)
                    bucket_read = file.read(self.block_size)
                    if bucket != bucket_read:
                        raise Exception("WriteError: Bucket Not Written Correctly.")
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
        # Creates txt file if does not already exists
        txt_filepath = '.' + csv_filepath.split('.')[1] + '.txt'
        try:
            file = open(txt_filepath, 'r')
            file.close()
        except:
            file = open(txt_filepath, 'w')
            file.close()
        # Opens txt file
        txt_file = open(txt_filepath, 'r+')
        # Write txt file header
        self._write_txt_header(txt_file=txt_file, txt_filepath=txt_filepath)
        # Return to begin of csv_file
        csv_file.seek(0, 0)
        # Skip csv_file header
        self._read_line(csv_file)
        # Write txt file records
        self._write_txt_records(txt_file=txt_file, csv_file=csv_file)
        txt_file.close()

    def _compress_txt_file(self, txt_filepath):
        tar_filepath = txt_filepath[:-3] + 'tar.gz'
        with tarfile.open(tar_filepath, "w:gz") as tar:
            tar.add(txt_filepath, arcname=txt_filepath.split('/')[-1])

        # with tarfile.open(tar_filepath, "w") as tar:
        #     tar.add(txt_filepath, arcname=txt_filepath)
    
    def _decompress_txt_file(self, tar_filepath, output_dir='./dataset/'):
        with tarfile.open(tar_filepath, "r:gz") as tar:
            tar.extractall(path=output_dir)

        # with tarfile.open(tar_filepath, "r") as tar:
        #     tar.extractall(path=output_dir)

    def _delete_txt_file(self, txt_filepath):
        if os.path.exists(txt_filepath):
            try:
                os.remove(txt_filepath)
            except:
                raise Exception(f"DeleteFileError: Could Not Delete {txt_filepath} File.")
        else:
            raise Exception(f"DeleteFileError: The File {txt_filepath} Does Not Exists.")

    def _compress_delete_txt_file(self, txt_filepath):
        # Compress txt file to a gzip file
        self._compress_txt_file(txt_filepath=txt_filepath)
        # Deletes txt file
        self._delete_txt_file(txt_filepath=txt_filepath)

    def from_csv_to_txt(self, csv_filepath):
        # Open csv file
        file = open(csv_filepath, 'r+')
        # Read csv file header
        self._read_csv_header(file=file)
        # Calculates field sizes and some other fields
        self._calculate_csv_field_sizes(file=file)
        # Calculates record size and blocking factor
        self._set_record_size()
        self._set_blocking_factor()
        # Sets creation date and hash function
        self.creation_date = None
        self._set_hash_function()
        # Writes csv file to a txt file
        self._write_from_csv_to_txt(csv_file=file, csv_filepath=csv_filepath)
        # Compresses txt file to gzip file and deletes txt file
        txt_filepath = '.' + csv_filepath.split('.')[1] + '.txt'
        self._compress_delete_txt_file(txt_filepath=txt_filepath)
        file.close()

    def _read_txt_header(self, file):
        self.table_name = self._read_line(file)
        self.field_names = self._read_line(file).strip().split(',')
        self.field_sizes = self._read_line(file).strip().split(',')
        self.field_sizes = [int(field_size) for field_size in self.field_sizes]
        self.field_types = self._read_line(file).strip().split(',')
        self._set_record_size()
        self._set_blocking_factor()
        self.number_of_records = int(self._read_line(file).strip())
        self.blocking_factor = int(self._read_line(file).strip())
        self.number_of_buckets = int(self._read_line(file).strip())
        self.hash_function = lambda key : key % self.number_of_buckets
        self.number_of_overflow_buckets = int(self._read_line(file).strip())
        self.creation_date = self._read_line(file).strip()
        self.alteration_date = self._read_line(file).strip()

    def _read_txt_file(self, txt_filepath):
        tar_filepath = txt_filepath[:-3] + 'tar.gz'
        self._decompress_txt_file(tar_filepath=tar_filepath)
        file = open(txt_filepath, 'r+')
        self._read_txt_header(file=file)
        return file

    def _search_by_primary_key(self, key, file):
        field_size = self.field_sizes[0]
        success = False
        header_length = self._header_length(file)
        # Checks if record is in bucket[hash(key)]
        hash_key = self.hash_function(int(key))
        offset = header_length + self.block_size * hash_key + hash_key
        file.seek(offset, 0)
        bucket = file.read(self.block_size)
        number_of_records_in_bucket = len(bucket.strip('#')) // self.record_size
        for i in range(0, number_of_records_in_bucket):
            if success == True:
                break
            record = bucket[i * self.record_size: (i + 1) * self.record_size]
            record_fields = record.strip().split(',')
            record_primary_key = record_fields[0].strip()
            if key == record_primary_key:
                yield [hash_key, i]
                success = True
                break
        # If record is not in bucket[hash(key)], checks if record is in some overflow bucket
        if not success:
            for i in range(0, self.number_of_overflow_buckets):
                offset = header_length + self.block_size * (self.number_of_buckets + i) + self.number_of_buckets + i
                file.seek(offset, 0)
                overflow_bucket = file.read(self.block_size)
                number_of_records_in_bucket = len(bucket.strip('#')) // self.record_size
                for j in range(0, number_of_records_in_bucket):
                    if success == True:
                        break
                    record = bucket[j * self.record_size: (j + 1) * self.record_size]
                    record_fields = record.strip().split(',')
                    record_primary_key = record_fields[0].strip()
                    if key == record_primary_key:
                        yield [self.number_of_buckets + i, j]
                        success = True
                        break
        # If failed to find record
        if not success:
            yield [-1, -1]

    def _search_by_field_value(self, field_id, value, file):
        field_size = self.field_sizes[field_id]
        success = False
        number_of_blocks = self.number_of_buckets + self.number_of_overflow_buckets
        header_length = self._header_length(file)
        for i in range(0, number_of_blocks):
            offset = header_length + self.block_size * i + i
            file.seek(offset, 0)
            bucket = file.read(self.block_size)
            number_of_records_in_bucket = len(bucket.strip('#')) // self.record_size
            for j in range(0, number_of_records_in_bucket):
                record = bucket[j * self.record_size: (j + 1) * self.record_size]
                record_fields = record.strip().split(',')
                record_field_value = record_fields[field_id].strip()
                if value == record_field_value:
                    yield [i, j]
                    success = True
        if not success:
            yield [-1, -1]
    
    def _search(self, field_id, value, file):
        if field_id == 0:
            for (i, j) in self._search_by_primary_key(key=value, file=file):
                yield [i, j]
        else:
            for (i, j) in self._search_by_field_value(field_id=field_id, value=value, file=file):
                yield [i, j]
    
    def _check_record_type_constraint(self, record):
        record_fields = record.strip().split(',')
        record_field_types = infer_types_from_record(record, len(record_fields))
        for i in range(len(self.field_names)):
            if record_field_types[i] != self.field_types[i]:
                return -1
        return 0
    
    def _check_record_size_constraint(self, record):
        record_fields = record.strip().split(',')
        record_field_sizes = [len(field) for field in record_fields]
        for i in range(len(self.field_names)):
            if record_field_sizes[i] > self.field_sizes[i]:
                return -1
        return 0

    def _check_record_primary_key_constraint(self, record, file):
        record_primary_key = record.strip().split(',')[0].strip()
        search_result = list(self._search(field_id=0, value=record_primary_key, file=file))[0]
        if search_result[0] != -1 and search_result[1] != -1:
            return -1
        return 0

    def _check_record_integrity(self, record, file):
        record_type_constraint = self._check_record_type_constraint(record=record)
        if record_type_constraint == -1:
            return -1
        record_size_constraint = self._check_record_size_constraint(record=record)
        if record_size_constraint == -1:
            return -1
        record_primary_key_constraint = self._check_record_primary_key_constraint(record=record, file=file)
        if record_primary_key_constraint == -1:
            return -1
        return 0

    def _insert(self, record, file):
        # Checks if record to insert is of correct size
        if len(record) != self.record_size:
            raise Exception("InsertionError: Incorrect Record Format.")

        # Gets record hash(key)
        record_fields = record.strip().split(',')
        record_primary_key = record_fields[0].strip()
        hash_key = self.hash_function(int(record_primary_key))
        
        # Gets bucket[hash(key)]
        header_length = self._header_length(file)
        offset = header_length + self.block_size * hash_key + hash_key
        file.seek(offset, 0)
        bucket = file.read(self.block_size)

        # Checks if there is available space in bucket bucket[hash(key)]
        success = False
        number_of_records_in_bucket = len(bucket.strip('#')) // self.record_size
        if number_of_records_in_bucket < self.blocking_factor:
            # If there is, then write record to bucket
            body = bucket[:self.record_size * number_of_records_in_bucket]
            padding = "#" * (self.block_size - self.record_size * (number_of_records_in_bucket + 1))
            bucket = body + record + padding
            file.seek(offset, 0)
            file.write(bucket)
            self.number_of_records += 1
            success = True
        else:
            # If there is not, then search for space available in some overflow bucket
            for i in range(0, self.number_of_overflow_buckets):
                if success:
                    break
                offset = header_length + self.block_size * (self.number_of_buckets + i) + self.number_of_buckets + i
                file.seek(offset, 0)
                bucket = file.read(self.block_size)
                number_of_records_in_bucket = len(bucket.strip('#')) // self.record_size
                if number_of_records_in_bucket < self.blocking_factor:
                    # If there is, then write record to overflow bucket
                    body = bucket[:self.record_size * number_of_records_in_bucket]
                    padding = "#" * (self.block_size - self.record_size * (number_of_records_in_bucket + 1))
                    bucket = body + record + padding
                    file.seek(offset, 0)
                    file.write(bucket)
                    self.number_of_records += 1
                    success = True
                    break
        
        # If there was not available space, then tell user
        if not success:
            raise Exception("InsertError: Buckets and overflow buckets full.")

    def insert_single_record(self, txt_filepath, record):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        # Check if the record respects the database integrity restriction
        record_integrity = self._check_record_integrity(record=record, file=file)
        if record_integrity == -1:
            file.close()
            # Deletes txt file
            self._delete_txt_file(txt_filepath=txt_filepath)
            raise Exception('InsertError: Invalid Record.')
        # Formats and inserts record
        formatted_record = self._format_record(record[:-1])
        self._insert(formatted_record, file)
        # Updates txt file header
        self._write_txt_header(txt_file=file, txt_filepath=txt_filepath)
        # Compresses txt file to gzip file and deletes txt file
        self._compress_delete_txt_file(txt_filepath=txt_filepath)
        file.close()

    def insert_multiple_records(self, txt_filepath, records):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        # Check if the record respects the database integrity restriction
        for record in records:
            record_integrity = self._check_record_integrity(record=record, file=file)
            if record_integrity == -1:
                file.close()
                # Deletes txt file
                self._delete_txt_file(txt_filepath=txt_filepath)
                raise Exception('InsertError: Invalid Record.')
        # Formats and inserts records
        for record in records:
            formatted_record = self._format_record(record[:-1])
            self._insert(formatted_record, file)
        # Updates txt file header
        self._write_txt_header(txt_file=file, txt_filepath=txt_filepath)
        # Compresses txt file to gzip file and deletes txt file
        self._compress_delete_txt_file(txt_filepath=txt_filepath)
        file.close()

    def _select(self, select_container, block_id, record_id, file):
        offset = self._header_length(file)
        offset += self.block_size * block_id + block_id + self.record_size * record_id
        file.seek(offset, 0)
        record = file.read(self.record_size)
        select_container.append(record)

    def select_by_single_primary_key(self, txt_filepath, key):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        select_container = []
        for (i, j) in self._search(field_id=0, value=key, file=file):
            if i == -1 and j == -1:
                file.close()
                # Deletes txt file
                self._delete_txt_file(txt_filepath=txt_filepath)
                raise Exception('SelectionError: Primary Key nonexistent.')
            else:
                self._select(select_container=select_container, block_id=i, record_id=j, file=file)
        file.close()
        # Deletes txt file
        self._delete_txt_file(txt_filepath=txt_filepath)
        return select_container

    def select_by_multiple_primary_key(self, txt_filepath, keys):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        select_container = []
        exception_counter = 0
        for key in keys:
            for (i, j) in self._search(field_id=0, value=key, file=file):
                if i == -1 and j == -1:
                    exception_counter += 1
                else:
                    self._select(select_container=select_container, block_id=i, record_id=j, file=file)
        file.close()
        # Deletes txt file
        self._delete_txt_file(txt_filepath=txt_filepath)
        if exception_counter == len(keys):
            raise Exception('SelectionError: Primary Keys nonexistent.')
        return select_container

    def select_by_field_interval(self, txt_filepath, field, start, end):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        field_id = self.field_names.index(field)
        field_type = self.field_types[field_id]
        possible_field_interval = check_interval(interval_type=field_type, start=start, end=end)
        if possible_field_interval == -1:
            file.close()
            # Deletes txt file
            self._delete_txt_file(txt_filepath=txt_filepath)
            raise Exception('SelectionError: Field Interval incomputable.')
        value_range = generate_range(range_type=field_type, start=start, end=end)
        if value_range == -1:
            file.close()
            # Deletes txt file
            self._delete_txt_file(txt_filepath=txt_filepath)
            raise Exception('SelectionError: Field Interval incomputable.')
        select_container = []
        exception_counter = 0
        for value in value_range:
            for (i, j) in self._search(field_id=field_id, value=str(value), file=file):
                if i == -1 and j == -1:
                    exception_counter += 1
                else:
                    self._select(select_container=select_container, block_id=i, record_id=j, file=file)
        file.close()
        # Deletes txt file
        self._delete_txt_file(txt_filepath=txt_filepath)
        if exception_counter == len(value_range):
            raise Exception('SelectionError: Requested Records nonexistent.')
        return select_container
    
    def _delete_record(self, bucket_id, record_id, file):
        # Read bucket
        offset = self._header_length(file)
        offset += self.block_size * bucket_id + bucket_id
        file.seek(offset, 0)
        bucket = file.read(self.block_size)
        # Brings records closer to begin of bucket
        number_of_records_in_bucket = len(bucket.strip('#')) // self.record_size
        body = ""
        # Starts by bringing records that are before the record to be deleted
        body += bucket[:self.record_size * record_id]
        # If there are records after the record to be deleted
        if number_of_records_in_bucket > record_id:
            body += bucket[self.record_size * (record_id + 1):self.record_size * number_of_records_in_bucket]
        # Creates padding for new empty space in bucket
        padding = "#" * (self.block_size - self.record_size * (number_of_records_in_bucket - 1))
        # Writes new bucket to file
        bucket = body + padding
        file.seek(offset, 0)
        file.write(bucket)

    def delete_record_by_primary_key(self, txt_filepath, key):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        # Searchs for position of record to be deleted
        for (i, j) in self._search(field_id=0, value=key, file=file):
            if i == -1 and j == -1:
                file.close()
                # Deletes txt file
                self._delete_txt_file(txt_filepath=txt_filepath)
                raise Exception('DeleteError: Primary Key nonexistent.')
            else:
                # Deletes the record
                self._delete_record(bucket_id=i, record_id=j, file=file)
                # Updates txt file header
                self._write_txt_header(txt_file=file, txt_filepath=txt_filepath)
                # Compresses txt file to gzip file and deletes txt file
                self._compress_delete_txt_file(txt_filepath=txt_filepath)
                file.close()
    
    def delete_record_by_criterion(self, txt_filepath, field, value):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        field_id = self.field_names.index(field)
        # Searchs for positions of records to be deleted
        for (i, j) in self._search(field_id=field_id, value=value, file=file):
            if i == -1 and j == -1:
                file.close()
                # Deletes txt file
                self._delete_txt_file(txt_filepath=txt_filepath)
                raise Exception('DeleteError: Field Value nonexistent.')
            else:
                # Deletes the record
                self._delete_record(bucket_id=i, record_id=j, file=file)
        # Updates txt file header
        self._write_txt_header(txt_file=file, txt_filepath=txt_filepath)
        # Compresses txt file to gzip file and deletes txt file
        self._compress_delete_txt_file(txt_filepath=txt_filepath)
        file.close()