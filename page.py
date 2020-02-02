from config import *
import struct

class Page:

    def __init__(self, page_id):
        self.num_records = 0
        self.pg_id = page_id
        self.data = bytearray(4096)

    def get_id(self):
        return self.pg_id

    def has_capacity(self):
        return self.num_records*8 == 4096
        pass
    
    # use only to write record data, rid and timestamp
    def write(self, value):
        self.num_records += 1
        write_pos = self.num_records*8
        self.data[(write_pos-8):(write_pos)] = struct.pack('>Q', value)
        pass
    # must be used to write schema
    def write_schema(self, schema_encoding):
        self.num_records += 1
        write_pos = self.num_records*8
        self.data[(write_pos-8):(write_pos)] = struct.pack('>Q', int(schema_encoding,2))
        
    def read(self, offset):
        return struct.unpack('>Q', self.data[(offset+1)*8 - 8:8*(offset+1)])[0]

    # must be used to read schema
    def read_schema(self, offset, num_columns):
        schema_encoding = self.data[(offset+1)*8 - 8:8*(offset+1)]
        schema_in_bits = ''.join(format(byte, '08b') for byte in schema_encoding)
        return schema_in_bits[64-num_columns:]

    def print_data(self):
        print('page id: ' + str(self.pg_id))
        for record in range(self.num_records):
            print(struct.unpack('>Q', self.data[(record+1)*8- 8:8*(record+1)])[0])


