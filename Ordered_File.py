"""
Implementação do arquivo ordenado.
"""

import math
from datetime import datetime
import os

from Util import infer_types_from_record, check_interval, generate_range

class Ordered_File:
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
        # Write number of blocks
        txt_file.write(str(self.number_of_blocks) + '\n')
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

    def _get_csv_records(self, csv_file):
        # Sets cursor to beginning of file
        csv_file.seek(0, 0)
        # Skips csv header
        self._read_line(file=csv_file)
        # Read and store records
        records = []
        record = self._read_line(file=csv_file)
        while record != "":
            records.append(record)
            record = self._read_line(file=csv_file)
        return records
    
    def _merge_sort(self, records):
        # Base case. A list of zero or one elements is sorted, by definition.
        if len(records) <= 1:
            return records

        # Recursive case. First, divide the list into equal-sized sublists
        # consisting of the first half and second half of the list.
        left = []
        right = []
        for i in range(len(records)):
            if i < len(records) / 2:
                left.append(records[i])
            else:
                right.append(records[i])

        # Recursively sort both sublists.
        left = self._merge_sort(records=left)
        right = self._merge_sort(records=right)

        # Then merge the now-sorted sublists.
        return self._merge(left=left, right=right)
    
    def _primary_key(self, record):
        record_fields = record.strip().split(",")
        record_primary_key = int(record_fields[0].strip())
        return record_primary_key

    def _merge(self, left, right):
        result = []

        while left != [] and right != []:
            # if left[0] <= right[0]:
            if self._primary_key(record=left[0]) <= self._primary_key(record=right[0]):
                result.append(left[0])
                left.pop(0)
            else:
                result.append(right[0])
                right.pop(0)

        # Either left or right may have elements left; consume them.
        # (Only one of the following loops will actually be entered.)
        while left != []:
            result.append(left[0])
            left.pop(0)
        while right != []:
            result.append(right[0])
            right.pop(0)
        return result

    def _write_txt_records(self, txt_file, csv_file):
        block_remaining = self.block_size - self.blocking_factor * self.record_size
        block_padding = "#" * block_remaining
        block = ''
        remaining_records = self.number_of_records - (self.number_of_records // self.blocking_factor)
        records = self._get_csv_records(csv_file=csv_file)
        ordered_records = self._merge_sort(records=records)
        for i in range(0, len(ordered_records)):
            record = ordered_records[i]
            try:
                formatted_record = self._format_record(record=record)
                block += formatted_record
                if (i + 1) % self.blocking_factor == 0:
                    block += block_padding + '\n'
                    txt_file.write(block)
                    block = ''
                elif i == self.number_of_records:
                    additional_padding = "#" * (self.block_size - self.record_size * remaining_records)
                    block += additional_padding + '\n'
                    txt_file.write(block)
            except:
                raise Exception(f"WriteError: Could Not Write Record: {record}")

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
        self.number_of_blocks = self.number_of_records // self.blocking_factor
        self.number_of_deleted_records = 0
        self.creation_date = None
        self._write_from_csv_to_txt(csv_file=file, csv_filepath=csv_filepath)
        file.close()
    
    def _header_length(self, file):
        header_length = 0
        for i in range(0, 10):
            file.seek(header_length, 0)
            line = self._read_line(file)
            header_length += len(line)+1
        return header_length

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
        self.number_of_blocks = int(self._read_line(file).strip())
        self.number_of_deleted_records = int(self._read_line(file).strip())
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

    def _read_ext_blocks(self, file):
        self.ext_blocks = []
        offset = 0
        file.seek(offset, 0)
        block = file.read(self.block_size)
        while block != "":
            self.ext_blocks.append(block)
            offset += self.block_size + 1
            file.seek(offset, 0)
            block = file.read(self.block_size)

    def _read_extension_file(self, txt_filepath):
        # Creates ext file if does not already exists
        ext_filepath = txt_filepath[:-4] + "_Ext.txt"
        try:
            file = open(ext_filepath, 'r')
            file.close()
        except:
            file = open(ext_filepath, 'w')
            file.close()
        file = open(ext_filepath, 'r+')
        self._read_ext_blocks(file=file)
        return ext_filepath

    def _read_txt_file(self, txt_filepath):
        file = open(txt_filepath, 'r+')
        self._read_txt_header(file=file)
        self._read_txt_blocks(file=file)
        return file

    def _search(self, field_id, value):
        field_size = self.field_sizes[field_id]
        success = False
        # Search for record in blocks from txt file
        for i in range(0, len(self.blocks)):
            # If found the record and we are doing a select by primary key, end search
            if success and field_id == 0:
                break
            # Checks if record is in block[i]
            block = self.blocks[i]
            number_of_records_in_block = len(block.strip('#')) // self.record_size
            for j in range(0, number_of_records_in_block):
                if success and field_id == 0:
                    break
                record = block[j * self.record_size: (j + 1) * self.record_size]
                record_fields = record.strip().split(',')
                record_field_value = record_fields[field_id].strip()
                if value == record_field_value:
                    yield [i, j]
                    success = True
        # Search for record in blocks from ext file
        for i in range(0, len(self.ext_blocks)):
            # If found the record and we are doing a select by primary key, end search
            if success and field_id == 0:
                break
            # Checks if record is in block[i]
            block = self.ext_blocks[i]
            number_of_records_in_block = len(block.strip('#')) // self.record_size
            for j in range(0, number_of_records_in_block):
                if success and field_id == 0:
                    break
                record = block[j * self.record_size: (j + 1) * self.record_size]
                record_fields = record.strip().split(',')
                record_field_value = record_fields[field_id].strip()
                if value == record_field_value:
                    yield [len(self.blocks) + i, j]
                    success = True
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
    
    def _check_record_size_constraint(self, record):
        record_fields = record.strip().split(',')
        record_field_sizes = [len(field) for field in record_fields]
        for i in range(len(self.field_names)):
            if record_field_sizes[i] > self.field_sizes[i]:
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
        record_size_constraint = self._check_record_size_constraint(record=record)
        if record_size_constraint == -1:
            return -1
        record_primary_key_constraint = self._check_record_primary_key_constraint(record=record)
        if record_primary_key_constraint == -1:
            return -1
        return 0

    def _write_ext_file(self, ext_filepath):
        with open(ext_filepath, 'w') as ext_file:
            for block in self.ext_blocks:
                ext_file.write(block + '\n')

    def _get_txt_header(self, txt_filepath):
        header = ""
        txt_file = open(txt_filepath, 'r')
        txt_file.seek(0, 0)
        for i in range(0, 10):
            header_line = self._read_line(file=txt_file)
            header += header_line + '\n'
        txt_file.close()
        return header

    def _write_txt_file(self, txt_filepath):
        once = False
        with open(txt_filepath, 'w') as txt_file:
            if not once:
                txt_header = self._get_txt_header(txt_filepath=txt_filepath)
                txt_file.write(txt_header)
                once = True
            for block in self.blocks:
                txt_file.write(block + '\n')

    def _insert(self, record):
        if len(record) != self.record_size:
            raise Exception("InsertionError: Incorrect Record Format.")

        # Get the last ext block
        block = ""
        exists_block = len(self.ext_blocks) >= 1
        if exists_block:
            block = self.ext_blocks[-1]

        # Check if there is space available in the last ext block
        number_of_records_in_block = len(block.strip('#')) // self.record_size
        if exists_block and number_of_records_in_block < self.blocking_factor:
            # If there is, then write record to last ext block
            body = block[:self.record_size * number_of_records_in_block]
            padding = "#" * (self.block_size - self.record_size * (number_of_records_in_block + 1))
            block = body + record + padding
            self.ext_blocks[-1] = block
            self.number_of_records += 1
        else:
            # If there is not, then create a new ext block
            padding = "#" * (self.block_size - self.record_size)
            block = record + padding
            # Writes new ext block to the end of the ext file
            self.ext_blocks.append(block)
            self.number_of_blocks += 1
            self.number_of_records += 1

    def insert_single_record(self, txt_filepath, record):
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
        # Check if the record respects the database integrity restriction
        record_integrity = self._check_record_integrity(record=record)
        if record_integrity == -1:
            txt_file.close()
            raise Exception('InsertError: Invalid Record.')
        # Formats and inserts record
        formatted_record = self._format_record(record[:-1])
        self._insert(formatted_record)
        # Updates txt file header
        self._write_txt_header(txt_file=txt_file, txt_filepath=txt_filepath)
        # Writes ext file
        self._write_ext_file(ext_filepath=ext_filepath)
        txt_file.close()

    def insert_multiple_records(self, txt_filepath, records):
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
        # Check if the record respects the database integrity restriction
        for record in records:
            record_integrity = self._check_record_integrity(record=record)
            if record_integrity == -1:
                txt_file.close()
                raise Exception('InsertError: Invalid Record.')
        # Formats and inserts records
        for record in records:
            formatted_record = self._format_record(record[:-1])
            self._insert(formatted_record)
        # Updates txt file header
        self._write_txt_header(txt_file=txt_file, txt_filepath=txt_filepath)
        # Writes ext file
        self._write_ext_file(ext_filepath=ext_filepath)
        txt_file.close()
    
    def _select(self, select_container, block_id, record_id):
        if block_id < len(self.blocks):
            block = self.blocks[block_id]
            record = block[self.record_size * record_id: self.record_size * (record_id + 1)]
            select_container.append(record)
        else:
            ext_block_id = len(self.blocks) - block_id
            block = self.ext_blocks[ext_block_id]
            record = block[self.record_size * record_id: self.record_size * (record_id + 1)]
            select_container.append(record)

    def select_by_single_primary_key(self, txt_filepath, key):
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
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
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
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
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
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
        # Checks if field exists
        if field not in self.field_names:
            raise Exception('SelectionError: Field nonexistent.')
        # If field exists, search for record
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
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

    def _recreating_txt_file(self, ordered_records, txt_filepath):
        self.number_of_deleted_records = 0
        txt_header = self._get_txt_header(txt_filepath=txt_filepath)
        txt_filepath = txt_filepath[:-4] + "_2.txt"
        with open(txt_filepath, 'w') as txt_file:
            block_remaining = self.block_size - self.blocking_factor * self.record_size
            block_padding = "#" * block_remaining
            block = ''
            remaining_records = self.number_of_records - (self.number_of_records // self.blocking_factor)
            txt_file.write(txt_header)
            for i in range(0, len(ordered_records)):
                record = ordered_records[i]
                try:
                    block += record
                    if (i + 1) % self.blocking_factor == 0:
                        block += block_padding + '\n'
                        txt_file.write(block)
                        block = ''
                    elif i == self.number_of_records:
                        additional_padding = "#" * (self.block_size - self.record_size * remaining_records)
                        block += additional_padding + '\n'
                        txt_file.write(block)
                except:
                    raise Exception(f"WriteError: Could Not Write Record: {record}")

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

    def _reordering(self, txt_filepath, ext_filepath):
        records = []
        for i in range(0, len(self.blocks)):
            block = self.blocks[i]
            for j in range(0, self.blocking_factor):
                record = block[self.record_size * j:self.record_size * (j + 1)]
                if record.strip("#") != "":
                    records.append(record)
        for i in range(0, len(self.ext_blocks)):
            block = self.ext_blocks[i]
            for j in range(0, self.blocking_factor):
                record = block[self.record_size * j:self.record_size * (j + 1)]
                if record.strip("#") != "":
                    records.append(record)
        ordered_records = self._merge_sort(records=records)
        self._recreating_txt_file(ordered_records=ordered_records, txt_filepath=txt_filepath)
        self._delete_file(filepath=txt_filepath)
        self._delete_file(filepath=ext_filepath)
        # self._rename_txt_file(txt_filepath=txt_filepath)

    def _delete_record(self, block_id, record_id):
        # If block is in txt file
        if block_id < len(self.blocks):
            block = self.blocks[block_id]
            # Deletes record from block
            head = block[:self.record_size * record_id]
            body = "#" * self.record_size
            tail = block[self.record_size * (record_id + 1):]
            block = head + body + tail
            self.blocks[block_id] = block
            self.number_of_deleted_records += 1
        else:
            # If block is in ext file
            ext_block_id = len(self.blocks) - block_id
            block = self.ext_blocks[ext_block_id]
            # Deletes record from block
            head = block[:self.record_size * record_id]
            body = "#" * self.record_size
            tail = block[self.record_size * (record_id + 1):]
            block = head + body + tail
            self.ext_blocks[ext_block_id] = block
            self.number_of_deleted_records += 1

    def delete_record_by_primary_key(self, txt_filepath, key):
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
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
                    self._reordering(txt_filepath=txt_filepath, ext_filepath=ext_filepath)
                # Writes txt file
                self._write_txt_file(txt_filepath=txt_filepath)
                # Writes ext file
                self._write_ext_file(ext_filepath=ext_filepath)
    
    def delete_record_by_criterion(self, txt_filepath, field, value):
        txt_file = self._read_txt_file(txt_filepath=txt_filepath)
        ext_filepath = self._read_extension_file(txt_filepath=txt_filepath)
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
            self._reordering(txt_filepath=txt_filepath, ext_filepath=ext_filepath)
        # Writes txt file
        self._write_txt_file(txt_filepath=txt_filepath)
        # Writes ext file
        self._write_ext_file(ext_filepath=ext_filepath)