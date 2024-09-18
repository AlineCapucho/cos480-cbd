"""
Funções úteis para os demais programas.
"""

import re
from datetime import datetime

import pandas as pd

def infer_type(value):
    # Check for integer
    if re.match(r'^[+-]?\d+$', value):
        return 'int'
    
    # Check for float
    elif re.match(r'^[+-]?\d*\.\d+$', value):
        return 'float'
    
    # Check for date (format: YYYY-MM-DD)
    elif re.match(r'^\d{4}-\d{2}-\d{2}$', value):
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return 'date'
        except ValueError:
            return 'string'  # Caso não seja uma data válida
    
    # Check for time (format: HH:MM:SS)
    elif re.match(r'^\d{2}:\d{2}:\d{2}$', value):
        try:
            datetime.strptime(value, '%H:%M:%S')
            return 'time'
        except ValueError:
            return 'string'  # Caso não seja um horário válido
    
    # If none of the above, treat it as a string
    else:
        return 'string'

def infer_types_from_record(record, record_length):
    fields = record.strip().split(',')
    return [infer_type(field.strip()) for field in fields][:record_length]

def range_between_integers(start, end):
    ints = [i for i in range(int(start), int(end))]
    return ints

def range_between_dates(start, end):
    dates_in_datetime = pd.date_range(start=start,end=end).to_pydatetime().tolist()
    dates = [datetime.strftime(elem, '%Y-%m-%d') for elem in dates_in_datetime]
    return dates

def range_between_times(start, end):
    times_in_datetime = pd.date_range(start, end, freq="1s")
    times = [datetime.strftime(elem, '%H:%M:%S') for elem in times_in_datetime]
    return times

def generate_range(range_type, start, end):
    if range_type == 'int':
        return range_between_integers(start, end)
    elif range_type == 'date':
        return range_between_dates(start, end)
    elif range_type == 'time':
        return range_between_times(start, end)
    else:
        return -1

def check_interval(interval_type, start, end):
    if interval_type == 'int':
        if start == end or start > end:
            return -1
        return 0
    elif interval_type == 'date':
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')

        if start_date == end_date:
            return -1
        elif start_date > end_date:
            return -1
        else:
            return 0
    elif interval_type == 'time':
        start_time = datetime.strptime(start, '%H:%M:%S')
        end_time = datetime.strptime(end, '%H:%M:%S')

        if start_time == end_time:
            return -1
        elif start_time > end_time:
            return -1
        else:
            return 0
    else:
        return -1