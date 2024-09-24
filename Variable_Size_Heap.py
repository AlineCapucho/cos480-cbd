"""
Implementação da Heap com registros de tamanho variável.
"""

import math
from datetime import datetime

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
            else:
                padding = "#" * (self.block_size - len(block))
                block += padding
                self.blocks.append(block)
                block = ""

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

    def _read_txt_file(self, txt_filepath):
        file = open(txt_filepath, 'r+')
        self._read_txt_header(file=file)
        self._read_txt_blocks(file=file)
        return file

    def _header_length(self, file):
        header_length = 0
        for i in range(0, 6):
            file.seek(header_length, 0)
            line = self._read_line(file)
            header_length += len(line)+1
        return header_length

    def _get_records_from_block(self, block):
        records = block.strip("#").split("$")[:-1]
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
            # Gets records from block[i]
            records = self._get_records_from_block(block=block)
            # Checks specified field from each record
            for j in range(len(records)):
                record_fields = records[j].split(',')
                record_field_value = record_fields[field_id].strip()
                if value == record_field_value:
                    yield [i, j]
                    success = True
        # If failed to find record
        if not success:
            yield [-1, -1]
    
    def _select(self, select_container, block_id, record_id):
        # Reads specified block
        block = self.blocks[block_id]
        # Gets records from block
        records = self._get_records_from_block(block=block)
        # Gets specified record
        record = records[record_id]
        # Adds specified record to select container
        select_container.append(record)

    def select_by_single_primary_key(self, txt_filepath, key):
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