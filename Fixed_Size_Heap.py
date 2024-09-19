"""
Implementação da Heap com registros de tamanho fixo.
"""

import math
from datetime import datetime

from Util import infer_types_from_record, check_interval, generate_range

class Fixed_Size_Heap:
    def __init__(self, block_size, field_sizes, filename):
        self.block_size = block_size
        self.field_sizes = field_sizes
        self._set_record_size()
        self._set_blocking_factor()
        self.filename = filename
        self._set_txt_filename()

    def _set_field_names(self, line):
        self.field_names = line.split(',')

    def _set_record_size(self):
        self.record_size = sum(self.field_sizes) + len(self.field_sizes)
    
    def _set_blocking_factor(self):
        self.blocking_factor = math.floor(self.block_size / self.record_size)
    
    def _set_field_types(self, record):
        self.field_types = infer_types_from_record(record, len(self.field_names))

    def _set_deleted_records(self, header_deleted_records):
        if header_deleted_records != '':
            deleted_records = header_deleted_records.strip().split(',')
            for deleted_record in deleted_records:
                i = deleted_record[1]
                j = deleted_record[3]
                self.deleted_records.append([i, j])
        else:
            self.deleted_records = []
    
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

    def _read_file_check_primary_key_constraint(self, records):
        primary_keys = []
        for record in records:
            fields = record.strip().split(',')
            record_primary_key = int(fields[0].strip())
            if record_primary_key not in primary_keys:
                primary_keys.append(record_primary_key)
            else:
                raise Exception("PrimaryKeyConstraint: Duplicated Primary Key.")

    def _set_txt_filename(self):
        self.txt_filename = '.' + self.filename.split('.')[1] + '.txt'

    def _write_header_to_disk_from_csv(self, file, number_of_records):
        # Table name
        table_name = self.txt_filename.split('/')[2].split('.')[0]
        file.write(table_name + '\n')
        # Fields names, sizes, types
        field_names = ','.join(str(field_name) for field_name in self.field_names) + '\n'
        field_sizes = ','.join(str(field_size) for field_size in self.field_sizes) + '\n'
        field_types = ','.join(str(field_type) for field_type in self.field_types) + '\n'
        file.write(field_names)
        file.write(field_sizes)
        file.write(field_types)
        # Number of records
        file.write(str(number_of_records) + '\n')
        # Blocking factor
        file.write(str(self.blocking_factor) + '\n')
        # List of deleted records
        file.write('\n')
        # Creation timestamp
        timestamp = datetime.now()
        current_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        file.write(current_timestamp + '\n')
        # Alteration timestamp
        file.write(current_timestamp + '\n')

    def _write_to_disk_from_csv(self, records):
        with open(self.txt_filename, 'w') as file:
            number_of_records = len(records)
            self._write_header_to_disk_from_csv(file=file, number_of_records=number_of_records)
            remaining = self.block_size - self.blocking_factor * self.record_size
            # Inserting records by block
            for i in range(len(records)):
                file.write(records[i])
                if i % self.blocking_factor == self.blocking_factor - 1:
                    padding = "#" * remaining
                    file.write(padding)

    def _read_line(self, file):
        char = file.read(1)
        line = ''

        while char and char != '\n':
            line += char
            char = file.read(1)
        return line

    def from_csv_to_txt(self):
        with open(self.filename, 'r') as file:
            self._read_line(file)
            self._read_file_check_primary_key_constraint(records=file)
        with open(self.filename, 'r') as file:
            header = self._read_line(file)
            self._set_field_names(header)
            record = self._read_line(file)
            self._set_field_types(record=record)
            formatted_records = []
            while record != '':
                formatted_record = self._format_record(record)
                formatted_records.append(formatted_record)
                record = self._read_line(file)
        self._write_to_disk_from_csv(records=formatted_records)

    def _set_field_types_by_read_header(self, field_types):
        self.field_types = field_types.split(',')

    def _read_header(self, file):
        self.table_name = self._read_line(file)
        field_names = self._read_line(file)
        self._set_field_names(field_names)
        field_sizes = self._read_line(file)
        field_types = self._read_line(file)
        self._set_field_types_by_read_header(field_types)
        self.number_of_records = int(self._read_line(file).strip())
        self.blocking_factor = int(self._read_line(file).strip())
        self._set_deleted_records(self._read_line(file))
        self.creation_file = self._read_line(file)
        self.alteration_file = self._read_line(file)    

    def _read_from_disk(self):
        try:
            file = open(self.txt_filename, 'r+')
            self._read_header(file)
            return file
        except:
            raise Exception("FileNotFound: Txt File nonexistent.")
    
    def _header_length(self, file):
        header_length = 0
        for i in range(0, 9):
            file.seek(header_length, 0)
            line = self._read_line(file)
            header_length += len(line)+1
        return header_length

    def _search(self, field_id, value, file):
        field_size = self.field_sizes[field_id]
        success = False
        number_of_blocks = math.ceil(self.number_of_records / self.blocking_factor)
        header_length = self._header_length(file)
        for i in range(0, number_of_blocks):
            if success and field_id == 0:
                break
            for j in range(0, self.blocking_factor):
                offset = header_length + self.block_size * i + self.record_size * j
                if self.field_sizes[:field_id] != []:
                    offset += sum(self.field_sizes[:field_id]) + field_id
                file.seek(offset, 0)
                field_value = file.read(field_size).strip()
                if field_value == '':
                    continue
                if field_value == value:
                    yield [i, j]
                    success = True
                    if field_id == 0:
                        break
        if not success:
            yield [-1, -1]

    def _check_unique_primary_key_constraint(self, record):
        file = self._read_from_disk()
        record_fields = record.strip().split(',')
        record_primary_key = record_fields[0]
        search_result = list(self._search(field_id=0, value=record_primary_key, file=file))[0]
        i, j = search_result[0], search_result[1]
        file.close()
        if i != -1 and j != -1:
            return -1
        return 0

    def _check_record_integrity(self, record):
        record_fields = record.strip().split(',')
        record_field_sizes = [len(field) for field in record_fields]
        record_field_types = infer_types_from_record(record, len(record_fields))
        if len(record_fields)-1 != len(self.field_names):
            return -1
        for i in range(len(self.field_names)):
            if record_field_sizes[i] > self.field_sizes[i]:
                return -1
            if record_field_types[i] != self.field_types[i]:
                return -1
        return 0

    def _insert(self, record, file):
        if self.deleted_records != []:
            block_id, record_id = self.deleted_records[0]
            offset = self._header_length(file)
            offset += self.block_size * block_id + self.record_size * record_id
            file.seek(offset, 0)
            file.write(record)
            self.deleted_records.pop(0)
        elif self.number_of_records % self.blocking_factor != 0:
            offset = self._header_length(file)
            offset += self.number_of_records - (self.number_of_records % self.blocking_factor)
            file.seek(offset, 0)
            file.write(record)
        else:
            file.seek(0, 2)
            file.write(record)
            padding = ' ' * (file.block_size - file.record_size)
            file.write(padding)

    def insert_single_record(self, record):
        file = self._read_from_disk()
        record_integrity = self._check_record_integrity(record)
        unique_primary_key_constraint = self._check_unique_primary_key_constraint(record=record)
        if record_integrity == -1:
            file.close()
            raise Exception('InsertError: Invalid Record.')
        if unique_primary_key_constraint == -1:
            file.close()
            raise Exception('InsertError: Duplicated Primary Key.')
        formatted_record = self._format_record(record[:-1])
        self._insert(formatted_record, file)
        file.close()

    def insert_multiple_records(self, records):
        file = self._read_from_disk()
        records_integrity = []
        for record in records:
            record_integrity = self._check_record_integrity(record)
            unique_primary_key_constraint = self._check_unique_primary_key_constraint(record=record)
            if record_integrity == -1:
                file.close()
                raise Exception('InsertError: Invalid Record.')
            if unique_primary_key_constraint == -1:
                file.close()
                raise Exception('InsertError: Duplicated Primary Key.')
        for record in records:
            formatted_record = self._format_record(record[:-1])
            self._insert(formatted_record, file)
        file.close()

    def _select(self, select_container, block_id, record_id, file):
        offset = self._header_length(file)
        offset += self.block_size * block_id + self.record_size * record_id
        file.seek(offset, 0)
        record = file.read(self.record_size)
        select_container.append(record)

    def select_by_single_primary_key(self, key):
        file = self._read_from_disk()
        select_container = []
        for (i, j) in self._search(field_id=0, value=key, file=file):
            if i == -1 and j == -1:
                file.close()
                raise Exception('SelectionError: Primary Key nonexistent.')
            else:
                self._select(select_container=select_container, block_id=i, record_id=j, file=file)
        file.close()
        return select_container
    
    def select_by_multiple_primary_key(self, keys):
        file = self._read_from_disk()
        select_container = []
        exception_counter = 0
        for key in keys:
            for (i, j) in self._search(field_id=0, value=key, file=file):
                if i == -1 and j == -1:
                    exception_counter += 1
                else:
                    self._select(select_container=select_container, block_id=i, record_id=j, file=file)
        file.close()
        if exception_counter == len(keys):
            raise Exception('SelectionError: Primary Keys nonexistent.')
        return select_container
    
    def select_by_field_interval(self, field, start, end):
        file = self._read_from_disk()
        field_id = self.field_names.index(field)
        field_type = self.field_types[field_id]
        possible_field_interval = check_interval(interval_type=field_type, start=start, end=end)
        if possible_field_interval == -1:
            file.close()
            raise Exception('SelectionError: Field Interval incomputable.')
        value_range = generate_range(range_type=field_type, start=start, end=end)
        if value_range == -1:
            file.close()
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
        if exception_counter == len(value_range):
            raise Exception('SelectionError: Requested Records nonexistent.')
        return select_container

    def _delete_record(self, block_id, record_id, file):
        offset = self._header_length(file)
        offset += self.block_size * block_id + self.record_size * record_id
        file.seek(offset, 0)
        body = ' ' * self.record_size
        file.write(body)
        self.deleted_records.append([block_id, record_id])

    def delete_record_by_primary_key(self, key):
        file = self._read_from_disk()
        for (i, j) in self._search(field_id=0, value=key, file=file):
            if i == -1 and j == -1:
                file.close()
                raise Exception('DeleteError: Primary Key nonexistent.')
            else:
                self._delete_record(block_id=i, record_id=j, file=file)
                file.close()
    
    def delete_record_by_criterion(self, field, value):
        file = self._read_from_disk()
        field_id = self.field_names.index(field)
        for (i, j) in self._search(field_id=field_id, value=value, file=file):
            if i == -1 and j == -1:
                file.close()
                raise Exception('DeleteError: Field Value nonexistent.')
            else:
                self._delete_record(block_id=i, record_id=j, file=file)
                file.close()