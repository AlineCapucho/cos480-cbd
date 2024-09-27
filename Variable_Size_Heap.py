"""
Implementação da Heap com registros de tamanho variável.
"""

import math
from datetime import datetime
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

    def _get_csv_records(self, csv_file):
        # Sets cursor to beginning of file
        csv_file.seek(0, 0)
        # Skips csv header
        self._read_line(file=csv_file)
        # Read and store records
        records = []
        record = self._read_line(file=csv_file)
        # Sets record field types
        self._set_field_types(record=record)
        # Sets number of deleted records
        self.number_of_deleted_records = 0
        while record != "":
            records.append(record)
            record = self._read_line(file=csv_file)
        self.number_of_records = len(records)
        return records
    
    def _create_blocks_from_records(self, records):
        self.blocks = []
        block = ""
        for i in range(0, len(records)):
            formatted_record = records[i] + "$"
            if len(block) + len(formatted_record) <= self.block_size:
                block += formatted_record
            elif len(block) + len(formatted_record) > self.block_size and i != (len(records) - 1):
                padding = "#" * (self.block_size - len(block))
                block += padding
                self.blocks.append(block)
                block = formatted_record
            else:
                padding = "#" * (self.block_size - len(block))
                block += padding
                self.blocks.append(block)

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
        # Write number of deleted records
        txt_file.write(str(self.number_of_deleted_records) + '\n')
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

    def _write_txt_records(self, txt_file):
        for block in self.blocks:
            txt_file.write(block + '\n')

    def _write_from_csv_to_txt(self, csv_file, csv_filepath):
        # # Creates txt file if does not already exists
        # txt_filepath = '.' + csv_filepath.split('.')[1] + '.txt'
        # try:
        #     file = open(txt_filepath, 'r')
        #     file.close()
        # except:
        #     file = open(txt_filepath, 'w')
        #     file.close()
        # Creates new clean txt file
        txt_filepath = '.' + csv_filepath.split('.')[1] + '.txt'
        txt_file = open(txt_filepath, 'w')
        txt_file.close()
        # Opens txt file
        txt_file = open(txt_filepath, 'r+')
        # Write txt file header
        self._write_txt_header(txt_file=txt_file, txt_filepath=txt_filepath)
        # Write txt file records
        self._write_txt_records(txt_file=txt_file)
        txt_file.close()

    def from_csv_to_txt(self, csv_filepath):
        # Open csv file
        file = open(csv_filepath, 'r+')
        # Read csv file header
        self._read_csv_header(file=file)
        # Get csv records
        records = self._get_csv_records(csv_file=file)
        # Create blocks from records
        self._create_blocks_from_records(records=records)
        # Sets creation date
        self.creation_date = None
        # Writes csv file to a txt file
        self._write_from_csv_to_txt(csv_file=file, csv_filepath=csv_filepath)
        file.close()
    
    def _read_txt_header(self, file):
        self.table_name = self._read_line(file)
        self.field_names = self._read_line(file).strip().split(',')
        self.field_types = self._read_line(file).strip().split(',')
        self.number_of_records = int(self._read_line(file).strip())
        self.number_of_deleted_records = int(self._read_line(file).strip())
        self.creation_date = self._read_line(file).strip()
        self.alteration_date = self._read_line(file).strip()
    
    def _read_txt_blocks(self, file):
        header_length = self._header_length(file=file)
        offset = header_length
        file.seek(offset, 0)
        block = file.read(self.block_size)
        self.blocks = []
        while block != "":
            self.blocks.append(block)
            offset = header_length + self.block_size * len(self.blocks) + len(self.blocks)
            file.seek(offset, 0)
            block = file.read(self.block_size)
        self.number_of_blocks = len(self.blocks)

    def _read_txt_file(self, txt_filepath):
        file = open(txt_filepath, 'r+')
        self._read_txt_header(file=file)
        self._read_txt_blocks(file=file)
        return file

    def _header_length(self, file):
        header_length = 0
        for i in range(0, 7):
            file.seek(header_length, 0)
            line = self._read_line(file)
            header_length += len(line)+1
        return header_length

    def _get_records_from_block(self, block):
        # records = block.strip("#").split("$")
        # records = [record for record in records if record != ""]
        records = block.split("$")
        return records

    def _search(self, field_id, value):
        success = False
        # Search for record in blocks from txt file
        for i in range(0, len(self.blocks)):
            # If found the record and we are doing a select by primary key, end search
            if success and field_id == 0:
                break
            # Gets block[i]
            block = self.blocks[i]
            self.accessed_blocks += 1
            # Gets records from block[i]
            records = self._get_records_from_block(block=block)
            # Checks specified field from each record
            for j in range(len(records)):
                if records[j].strip("#") != "":
                    try:
                        record_fields = records[j].split(',')
                        record_field_value = record_fields[field_id].strip()
                        if value == record_field_value:
                            yield [i, j]
                            success = True
                    except:
                        print("Record: ", records[i])
                        raise Exception("SearchError: Could Not Analyse Record.")
        # If failed to find record
        if not success:
            yield [-1, -1]
    
    def _check_record_type_constraint(self, record):
        record_fields = record.strip().split(',')
        record_field_types = infer_types_from_record(record, len(record_fields))
        for i in range(len(self.field_names)):
            if record_field_types[i] != self.field_types[i]:
                return -1
        return 0

    def _check_record_primary_key_constraint(self, record):
        record_primary_key = record.strip().split(',')[0]
        search_result = list(self._search(field_id=0, value=record_primary_key))[0]
        if search_result[0] != -1 and search_result[1] != -1:
            return -1
        return 0

    def _check_record_integrity(self, record):
        record_type_constraint = self._check_record_type_constraint(record=record)
        if record_type_constraint == -1:
            return -1
        record_primary_key_constraint = self._check_record_primary_key_constraint(record=record)
        if record_primary_key_constraint == -1:
            return -1
        return 0

    def _get_txt_header(self):
        header = ""
        header += self.table_name + '\n'
        field_names = ','.join(str(field_name) for field_name in self.field_names)
        field_types = ','.join(str(field_type) for field_type in self.field_types)
        header += field_names + '\n'
        header += field_types + '\n'
        header += str(self.number_of_records) + '\n'
        header += str(self.number_of_deleted_records) + '\n'
        header += self.creation_date + '\n'
        alteration_timestamp = datetime.now()
        alteration_timestamp = alteration_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        header += alteration_timestamp + '\n'
        return header

    def _write_txt_file(self, txt_filepath):
        once = False
        with open(txt_filepath, 'r+') as txt_file:
            if not once:
                txt_header = self._get_txt_header()
                txt_file.seek(0, 0)
                txt_file.write(txt_header)
                once = True
            for block in self.blocks:
                txt_file.write(block + '\n')

    def _insert(self, record):
        # Get the last block
        block = ""
        exists_block = len(self.blocks) >= 1
        if exists_block:
            block = self.blocks[-1]
            self.accessed_blocks += 1

        # Check if there is space available in the last block
        if block.count("#") >= len(record):
            # If there is, then write record to the end of last block
            block_records = self._get_records_from_block(block=block)
            body = ""
            for block_record in block_records:
                if block_record.strip("#") != "":
                    body += block_record + "$"
            padding = "#" * (self.block_size - len(body) - len(record) - 1)
            block = body + record + "$" + padding
            self.blocks[-1] = block
            self.number_of_records += 1
        else:
            # If there is not, then create a new block
            padding = "#" * (self.block_size - len(record) - 1)
            block = record + "$" + padding
            # Writes new block to end the the file
            self.blocks.append(block)
            self.number_of_records += 1
        self.number_of_blocks = len(self.blocks)

    def insert_single_record(self, txt_filepath, record):
        self.accessed_blocks = 0
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        txt_file.close()
        # Check if the record respects the database integrity restriction
        record_integrity = self._check_record_integrity(record=record)
        if record_integrity == -1:
            raise Exception('InsertError: Invalid Record.')
        # Inserts record
        self._insert(record)
        # Writes txt file
        self._write_txt_file(txt_filepath=txt_filepath)

    def insert_multiple_records(self, txt_filepath, records):
        self.accessed_blocks = 0
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        txt_file.close()
        # Check if the record respects the database integrity restriction
        for record in records:
            record_integrity = self._check_record_integrity(record=record)
            if record_integrity == -1:
                raise Exception('InsertError: Invalid Record.')
        # Inserts records
        for record in records:
            self._insert(record)
        # Writes txt file
        self._write_txt_file(txt_filepath=txt_filepath)

    def _select(self, select_container, block_id, record_id):
        # Reads specified block
        block = self.blocks[block_id]
        self.accessed_blocks += 1
        # Gets records from block
        records = self._get_records_from_block(block=block)
        # Gets specified record
        record = records[record_id]
        # Adds specified record to select container
        select_container.append(record)

    def select_by_single_primary_key(self, txt_filepath, key):
        self.accessed_blocks = 0
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        select_container = []
        for (i, j) in self._search(field_id=0, value=key):
            if i == -1 and j == -1:
                txt_file.close()
                raise Exception('SelectionError: Primary Key nonexistent.')
            else:
                self._select(select_container=select_container, block_id=i, record_id=j)
        txt_file.close()
        return select_container

    def select_by_multiple_primary_key(self, txt_filepath, keys):
        self.accessed_blocks = 0
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        select_container = []
        exception_counter = 0
        for key in keys:
            for (i, j) in self._search(field_id=0, value=key):
                if i == -1 and j == -1:
                    exception_counter += 1
                else:
                    self._select(select_container=select_container, block_id=i, record_id=j)
        txt_file.close()
        if exception_counter == len(keys):
            raise Exception('SelectionError: Primary Keys nonexistent.')
        return select_container
    
    def select_by_field_interval(self, txt_filepath, field, start, end):
        self.accessed_blocks = 0
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        field_id = self.field_names.index(field)
        field_type = self.field_types[field_id]
        possible_field_interval = check_interval(interval_type=field_type, start=start, end=end)
        if possible_field_interval == -1:
            txt_file.close()
            raise Exception('SelectionError: Field Interval incomputable.')
        value_range = generate_range(range_type=field_type, start=start, end=end)
        if value_range == -1:
            txt_file.close()
            raise Exception('SelectionError: Field Interval incomputable.')
        select_container = []
        exception_counter = 0
        for value in value_range:
            for (i, j) in self._search(field_id=field_id, value=str(value)):
                if i == -1 and j == -1:
                    exception_counter += 1
                else:
                    self._select(select_container=select_container, block_id=i, record_id=j)
        txt_file.close()
        if exception_counter == len(value_range):
            raise Exception('SelectionError: Requested Records nonexistent.')
        return select_container
    
    def select_by_single_field_value(self, txt_filepath, field, value):
        self.accessed_blocks = 0
        # Checks if field exists
        if field not in self.field_names:
            raise Exception('SelectionError: Field nonexistent.')
        # If field exists, search for record
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        field_id = self.field_names.index(field)
        select_container = []
        for (i, j) in self._search(field_id=field_id, value=value):
            if i == -1 and j == -1:
                txt_file.close()
                raise Exception('SelectionError: Field Value nonexistent.')
            else:
                self._select(select_container=select_container, block_id=i, record_id=j)
        txt_file.close()
        return select_container

    def _recreating_txt_file(self, txt_filepath):
        once = False
        # Creates new clean txt file
        txt_filepath = txt_filepath[:-4] + "_2.txt"
        txt_file = open(txt_filepath, 'w')
        txt_file.close()
        # Writes header and compressed records to new txt file
        with open(txt_filepath, 'r+') as txt_file:
            if not once:
                txt_header = self._get_txt_header()
                txt_file.seek(0, 0)
                txt_file.write(txt_header)
                once = True
            for compressed_block in self.compressed_blocks:
                txt_file.write(compressed_block + '\n')

    def _delete_file(self, filepath):
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except:
                raise Exception(f"DeleteFileError: Could Not Delete {filepath} File.")
        else:
            raise Exception(f"DeleteFileError: The File {filepath} Does Not Exists.")

    def _rename_txt_file(self, txt_filepath):
        txt2_filepath = txt_filepath[:-4] + "_2.txt"
        os.rename(txt2_filepath, txt_filepath)

    def _compress_records(self, txt_filepath):
        records = []
        for i in range(0, len(self.blocks)):
            block = self.blocks[i]
            self.accessed_blocks += 1
            blocks_records = self._get_records_from_block(block=block)
            for blocks_record in blocks_records:
                if blocks_record.strip("#") != "":
                    records.append(blocks_record)
        self.compressed_blocks = []
        compressed_block = ""
        for i in range(0, len(records)):
            formatted_record = records[i] + "$"
            if len(compressed_block) + len(formatted_record) <= self.block_size:
                compressed_block += formatted_record
            elif len(block) + len(formatted_record) > self.block_size and i != (len(records) - 1):
                padding = "#" * (self.block_size - len(compressed_block))
                compressed_block += padding
                self.compressed_blocks.append(compressed_block)
                compressed_block = formatted_record
            else:
                padding = "#" * (self.block_size - len(compressed_block))
                compressed_block += padding
                self.compressed_blocks.append(compressed_block)

        self.number_of_deleted_records = 0
        self._recreating_txt_file(txt_filepath=txt_filepath)
        self._delete_file(filepath=txt_filepath)
        # self._rename_txt_file(txt_filepath=txt_filepath)

    def _delete_record(self, block_id, record_id):
        # Gets block
        block = self.blocks[block_id]
        self.accessed_blocks += 1
        # Deletes record from block
        block_records = self._get_records_from_block(block=block)
        head = ""
        tail = ""
        for j in range(0, len(block_records)):
            if j < record_id:
                head += block_records[j] + "$"
            elif j > record_id:
                tail += block_records[j] + "$"
        body = "#" * (self.block_size - len(head) - len(tail) - 1)
        block = head + body + '$' + tail
        self.blocks[block_id] = block
        self.number_of_records -= 1
        self.number_of_deleted_records += 1


        # block_records = self._get_records_from_block(block=block)
        # head = ""
        # body = ""
        # for j in range(0, len(block_records)):
        #     if j < record_id:
        #         head += block_records[j] + "$"
        #     elif j > record_id:
        #         body += block_records[j] + "$"
        # tail = "#" * (self.block_size - len(head) - len(body))
        # block = head + body + tail
        # self.blocks[block_id] = block
        # self.number_of_records -= 1
        # self.number_of_deleted_records += 1

    def delete_record_by_primary_key(self, txt_filepath, key):
        self.accessed_blocks = 0
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        # Searchs for position of record to be deleted
        for (i, j) in self._search(field_id=0, value=key):
            if i == -1 and j == -1:
                txt_file.close()
                raise Exception('DeleteError: Primary Key nonexistent.')
            else:
                # Deletes the record
                self._delete_record(block_id=i, record_id=j)
                # Updates txt file header
                self._write_txt_header(txt_file=txt_file, txt_filepath=txt_filepath)
                txt_file.close()
                # Checks if reordering is to be done
                if self.number_of_deleted_records >= 50:
                    self._compress_records(txt_filepath=txt_filepath)
                else:
                    # Writes txt file
                    self._write_txt_file(txt_filepath=txt_filepath)
    
    def delete_record_by_criterion(self, txt_filepath, field, value):
        self.accessed_blocks = 0
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        field_id = self.field_names.index(field)
        # Searchs for positions of records to be deleted
        for (i, j) in self._search(field_id=field_id, value=value):
            if i == -1 and j == -1:
                txt_file.close()
                raise Exception('DeleteError: Field Value nonexistent.')
            else:
                # Deletes the record
                self._delete_record(block_id=i, record_id=j)
        # Updates txt file header
        self._write_txt_header(txt_file=txt_file, txt_filepath=txt_filepath)
        txt_file.close()
        # Checks if reordering is to be done
        if self.number_of_deleted_records >= 50:
            self._compress_records(txt_filepath=txt_filepath)
        else:
            # Writes txt file
            self._write_txt_file(txt_filepath=txt_filepath)