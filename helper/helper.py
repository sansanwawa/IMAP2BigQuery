import re


#Field Mapping
DEFAULT_DATA_TYPE = 'STRING'
BINARY_TYPES = [253,254]
MAPPING_DATA_TYPES = {
    0: 'FLOAT',
    1: 'INTEGER',
    2: 'INTEGER',
    3: 'INTEGER',
    4: 'FLOAT',
    5: 'FLOAT',
    7: 'TIMESTAMP',
    8: 'INTEGER',
    9: 'INTEGER',
    10: 'DATE',
    11: 'TIME',
    12: 'DATETIME',
    15: 'STRING',
    249: 'STRING',
    250: 'STRING',
    251: 'STRING',
    253: 'STRING',
    254: 'STRING'
}

invalid_escape = re.compile(r'\\[0-7]{1,3}')  # up to 3 digits for byte values up to FF

def get_type(sql_type):
    if sql_type in MAPPING_DATA_TYPES:
       return MAPPING_DATA_TYPES[sql_type]
    else:
       return DEFAULT_DATA_TYPE
