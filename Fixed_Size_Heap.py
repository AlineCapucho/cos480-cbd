"""
Implementação da Heap com registros de tamanho fixo.
"""

import math

from Util import infer_types_from_record, check_interval, generate_range

class Fixed_Size_Heap:
    def __init__(self, block_size, field_sizes, filename):
        self.block_size = block_size
        self.field_sizes = field_sizes
        self._set_record_size()
        self.blocks = []
        self._read_file(filename)
        self.deleted_records = []

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
        if self.blocks == []:
            self.blocks.append(self._format_record(record))
        elif len(self.blocks[-1]) + self.record_size < self.block_size:
            self.blocks[-1] += self._format_record(record)
        else:
            self.blocks.append(self._format_record(record))

    def _read_file(self, filename):
        with open(filename, 'r') as file:
            self._set_field_names(file.readline())
            for record in file:
                self._write_record(record)
            self._set_field_types()
    
    def _search(self, field_id, value):
        field_size = self.field_sizes[field_id]
        number_of_records = math.floor(self.block_size / self.record_size)
        success = False
        for i in range(len(self.blocks)):
            if success and field_id == 0:
                break
            for j in range(0, number_of_records):
                offset = self.record_size * j
                if self.field_sizes[:field_id] != []:
                    offset += sum(self.field_sizes[:field_id]) + field_id
                field_value = self.blocks[i][offset:offset + field_size].strip()
                if field_value == '':
                    continue
                if field_value == value:
                    yield [i, j]
                    success = True
                    if field_id == 0:
                        break
        return [-1, -1]

    def _select(self, select_container, block_id, record_id):
        offset = self.record_size * record_id
        record = self.blocks[block_id][offset:offset + self.record_size]
        select_container.append(record)

    def select_by_single_primary_key(self, key):
        select_container = []
        for (i, j) in self._search(field_id=0, value=key):
            if i == -1 and j == -1:
                raise Exception('SelectionError: Primary Key nonexistent.')
            else:
                self._select(select_container=select_container, block_id=i, record_id=j)
        return select_container
    
    def select_by_multiple_primary_key(self, keys):
        select_container = []
        exception_counter = 0
        for key in keys:
            for (i, j) in self._search(field_id=0, value=key):
                if i == -1 and j == -1:
                    exception_counter += 1
                else:
                    self._select(select_container=select_container, block_id=i, record_id=j)
        if exception_counter == len(keys):
            raise Exception('SelectionError: Primary Keys nonexistent.')
        return select_container
    
    def select_by_field_interval(self, field, start, end):
        field_id = self.field_names.index(field)
        field_type = self.field_types[field_id]
        possible_field_interval = check_interval(interval_type=field_type, start=start, end=end)
        if possible_field_interval == -1:
            raise Exception('SelectionError: Field Interval incomputable.')
        value_range = generate_range(range_type=field_type, start=start, end=end)
        if value_range == -1:
            raise Exception('SelectionError: Field Interval incomputable.')
        select_container = []
        exception_counter = 0
        for value in value_range:
            for (i, j) in self._search(field_id=field_id, value=str(value)):
                if i == -1 and j == -1:
                    exception_counter += 1
                else:
                    self._select(select_container=select_container, block_id=i, record_id=j)
        if exception_counter == len(value_range):
            raise Exception('SelectionError: Requested Records nonexistent.')
        return select_container

    def _delete_record(self, block_id, record_id):
        offset = self.record_size * record_id
        head = self.blocks[block_id][:offset]
        body = ' ' * self.record_size
        tail = self.blocks[block_id][offset + self.record_size:]
        self.blocks[block_id] = head + body + tail
        self.deleted_records.append([block_id, record_id])

    def delete_record_by_primary_key(self, key):
        for (i, j) in self._search(field_id=0, value=key):
            if i == -1 and j == -1:
                raise Exception('DeleteError: Primary Key nonexistent.')
            else:
                self._delete_record(block_id=i, record_id=j)
    
    def delete_record_by_criterion(self, field, value):
        field_id = self.field_names.index(field)
        for (i, j) in self._search(field_id=field_id, value=value):
            if i == -1 and j == -1:
                raise Exception('DeleteError: Field Value nonexistent.')
            else:
                self._delete_record(block_id=i, record_id=j)