"""
Implementação da Heap com registros de tamanho variável.
"""

import math
from datetime import datetime

import tarfile
import os

from Util import infer_types_from_record, check_interval, generate_range

class Variable_Size_Heap:
    def __init__(self, block_size):
        self.block_size = block_size
    
    def _set_field_names(self, fields_names):
        self.field_names = fields_names.strip().split(',')

    def _set_field_types(self, record):
        self.field_types = infer_types_from_record(record, len(self.field_names))

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

    def _calculate_some_header_fields(self, file):
        record = self._read_line(file=file)
        self.number_of_records = 0
        total_length = 0
        self._set_field_types(record=record)
        while record != '':
            self.number_of_records += 1
            total_length += len(record) + 1
            record_fields = record.strip().split(',')
            record = self._read_line(file=file)
        self.number_of_blocks = total_length // self.block_size

    def _write_txt_header(self, txt_file, txt_filepath):
        # Sets txt_file cursor at the beginning of the file
        txt_file.seek(0, 0)
        # Write table name
        table_name = txt_filepath.split('/')[2].split('.')[0]
        txt_file.write(table_name + '\n')
        # Write field names, sizes, types
        field_names = ','.join(str(field_name) for field_name in self.field_names) + '\n'
        field_types = ','.join(str(field_type) for field_type in self.field_types) + '\n'
        txt_file.write(field_names)
        txt_file.write(field_types)
        # Write number of records
        txt_file.write(str(self.number_of_records) + '\n')
        # Write number of blocks
        txt_file.write(str(self.number_of_blocks) + '\n')
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

    def _write_txt_records(self, txt_file, csv_file):
        # Sets csv_file cursor to beginning of the file
        csv_file.seek(0, 0)
        # Skips csv_file header
        self._read_line(file=csv_file)
        # Writes each record in a block
        block = ""
        record_count = 0
        for i in range(0, self.number_of_records):
            # Formats record
            record = self._read_line(file=csv_file) + "$"
            if record == "$":
                raise Exception("OutOfBorder: End Of File.")
            # Checks if we are in the case where the block is not full and we are not at the last record
            if len(block) + len(record) < self.block_size and i != self.number_of_records - 1:
                block += record
                record_count += 1
            elif len(block) + len(record) >= self.block_size and i != self.number_of_records - 1:
                # If block is full and we are not at the last record
                padding = "#" * (self.block_size - len(block))
                block += padding + '\n'
                txt_file.write(block)
                block = record
                record_count += 1
            else:
                # If we are at the last record
                padding = "#" * (self.block_size - len(block) - len(record))
                block += record + padding + '\n'
                txt_file.write(block)
                record_count += 1
        if record_count != self.number_of_records:
            print("Number of records expected to be written: ", self.number_of_records)
            print("Number of records actually written: ", record_count)
            raise Exception("WriteError: Wrong Number of Records Written.")

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
    
    def _decompress_txt_file(self, tar_filepath, output_dir='./dataset/'):
        with tarfile.open(tar_filepath, "r:gz") as tar:
            tar.extractall(path=output_dir)

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
        # Calculates field types and some other fields
        self._calculate_some_header_fields(file=file)
        # Sets creation date
        self.creation_date = None
        # Writes csv file to a txt file
        self._write_from_csv_to_txt(csv_file=file, csv_filepath=csv_filepath)
        # Compresses txt file to gzip file and deletes txt file
        txt_filepath = '.' + csv_filepath.split('.')[1] + '.txt'
        self._compress_delete_txt_file(txt_filepath=txt_filepath)
        file.close()

    def _read_txt_header(self, file):
        self.table_name = self._read_line(file)
        self.field_names = self._read_line(file).strip().split(',')
        self.field_types = self._read_line(file).strip().split(',')
        self.number_of_records = int(self._read_line(file).strip())
        self.number_of_blocks = int(self._read_line(file).strip())
        self.creation_date = self._read_line(file).strip()
        self.alteration_date = self._read_line(file).strip()

    def _read_txt_blocks(self, file):
        header_length = self._header_length(file=file)
        self.blocks = []
        for i in range(0, self.number_of_blocks):
            offset = header_length + self.block_size * i + i
            file.seek(offset, 0)
            block = file.read(self.block_size)
            self.blocks.append(block)

    def _read_txt_file(self, txt_filepath):
        tar_filepath = txt_filepath[:-3] + 'tar.gz'
        self._decompress_txt_file(tar_filepath=tar_filepath)
        file = open(txt_filepath, 'r+')
        self._read_txt_header(file=file)
        return file

    def _header_length(self, file):
        header_length = 0
        for i in range(0, 7):
            file.seek(header_length, 0)
            line = self._read_line(file)
            header_length += len(line)+1
        return header_length

    def _get_records_from_block(self, block):
        records = block.strip("#").split("$")[:-1]
        return records

    def _search(self, field_id, value, file):
        success = False
        header_length = self._header_length(file)
        # Check if there is a record with specified field value
        for i in range(0, self.number_of_blocks + 1):
            if success and field_id == 0:
                break
            # Gets a block
            offset = header_length + self.block_size * i + i
            file.seek(offset, 0)
            block = file.read(self.block_size)
            if i == self.number_of_blocks:
                print(block)
            # Gets records from block
            records = self._get_records_from_block(block=block)
            # Checks specified field from each record
            for j in range(len(records)):
                record_fields = records[j].split(',')
                record_field_value = record_fields[field_id].strip()
                if value == record_field_value:
                    yield [i, j]
                    success = True
        if not success:
            yield [-1, -1]
    
    def _check_record_type_constraint(self, record):
        record_fields = record.strip().split(',')
        record_field_types = infer_types_from_record(record, len(record_fields))
        for i in range(len(self.field_names)):
            if record_field_types[i] != self.field_types[i]:
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
        record_primary_key_constraint = self._check_record_primary_key_constraint(record=record, file=file)
        if record_primary_key_constraint == -1:
            return -1
        return 0

    def _insert(self, record, file):
        # Gets the last block
        offset = self._header_length(file)
        offset += self.block_size * (self.number_of_blocks - 1) + (self.number_of_blocks - 1)
        file.seek(offset, 0)
        block = file.read(self.block_size)

        # Check if there is space available in the last block
        if block.count("#") >= len(record) + 1:
            print("Case 1")
            # If there is, then write record to last block
            body = block.strip("#")
            padding = "#" * (block.count("#") - (len(record) + 1))
            block = body + record + "$" + padding + '\n'
            file.seek(offset, 0)
            file.write(block)
            self.number_of_records += 1
        else:
            print("Case 2")
            # If there is not, then create a new block
            padding = "#" * (self.block_size - len(record) - 1)
            block = record + "$" + padding + '\n'
            # Writes new block to the end of the file
            offset = self._header_length(file)
            offset += self.block_size * self.number_of_blocks + self.number_of_blocks
            file.seek(offset, 0)
            file.write(block)
            self.number_of_blocks += 1
            self.number_of_records += 1

    def insert_single_record(self, txt_filepath, record):
        file = self._read_txt_file(txt_filepath=txt_filepath)
        # Check if the record respects the database integrity restriction
        record_integrity = self._check_record_integrity(record=record, file=file)
        if record_integrity == -1:
            file.close()
            # Deletes txt file
            self._delete_txt_file(txt_filepath=txt_filepath)
            raise Exception('InsertError: Invalid Record.')
        # Inserts record
        self._insert(record, file)
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
        # Inserts records
        for record in records:
            self._insert(record, file)
        # Updates txt file header
        self._write_txt_header(txt_file=file, txt_filepath=txt_filepath)
        # Compresses txt file to gzip file and deletes txt file
        self._compress_delete_txt_file(txt_filepath=txt_filepath)
        file.close()

    def _select(self, select_container, block_id, record_id, file):
        # Reads specified block
        offset = self._header_length(file=file)
        offset += self.block_size * block_id + block_id
        file.seek(offset, 0)
        block = file.read(self.block_size)
        # Gets records from block
        records = self._get_records_from_block(block=block)
        # Gets specified record
        record = records[record_id]
        # Adds specified record to select container
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
    
    def select_by_single_field_value(self, txt_filepath, field, value):
        # Checks if field exists
        if field not in self.field_names:
            raise Exception('SelectionError: Field nonexistent.')
        # If field exists, search for record
        file = self._read_txt_file(txt_filepath=txt_filepath)
        field_id = self.field_names.index(field)
        select_container = []
        for (i, j) in self._search(field_id=field_id, value=value, file=file):
            if i == -1 and j == -1:
                file.close()
                # Deletes txt file
                self._delete_txt_file(txt_filepath=txt_filepath)
                raise Exception('SelectionError: Field Value nonexistent.')
            else:
                self._select(select_container=select_container, block_id=i, record_id=j, file=file)
        file.close()
        # Deletes txt file
        self._delete_txt_file(txt_filepath=txt_filepath)
        return select_container