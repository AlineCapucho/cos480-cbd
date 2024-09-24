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