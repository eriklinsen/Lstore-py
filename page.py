# from config import *
import struct

class Page:

    def __init__(self, page_id):
        self.num_records = 2
        self.pg_id = page_id
        self.data = bytearray(4096)

    def get_id(self):
        return self.pg_id

    def has_capacity(self):
        return (self.num_records - 2) != 510
        pass

    def get_capacity(self):
        return 510 - (self.num_records-2)
    
    # use only to write record data, rid and timestamp
    def write(self, value):
        if not self.has_capacity():
            print('page write error: page has no more available space.')
            return
        self.num_records += 1
        write_pos = self.num_records*8
        self.data[(write_pos-8):(write_pos)] = struct.pack('>q', value)
        self.update(self.num_records, 1)

    # must be used to write schema
    def write_schema(self, schema_encoding):
        if not self.has_capacity():
            print('page write error: page has no more available space.')
            return
        self.num_records += 1
        write_pos = self.num_records*8
        self.data[(write_pos-8):(write_pos)] = struct.pack('>q', int(schema_encoding,2))
        self.update(self.num_records, 1)
    
    # used to update pre-existing values
    def update(self, value, offset):
        if offset > 511 or offset < 0:
            print('page update error: offset out of range.')
            return
        self.data[(offset+1)*8 - 8:8*(offset+1)] = struct.pack('>q', value)

    def update_schema(self, schema_encoding, offset):
        if offset > 511 or offset < 0:
            print('page update error: offset out of range.')
            return
        self.data[(offset+1)*8 - 8:8*(offset+1)] = struct.pack('>q',
                int(schema_encoding,2))

    # used to read data from page:
    def read(self, offset):
        if offset > 511 or offset < 0:
            print('page read error: offset out of range.')
            return
        return struct.unpack('>q', self.data[(offset+1)*8 - 8:8*(offset+1)])[0]

    # must be used to read schema
    def read_schema(self, num_columns, offset):
        if offset > 511 or offset < 0:
            print('page read error: offset out of range.')
            return
        schema_encoding = self.data[(offset+1)*8 - 8:8*(offset+1)]
        schema_in_bits = ''.join(format(byte, '08b') for byte in schema_encoding)
        return schema_in_bits[64-num_columns:]

    def load_data(self, read_data):
        for i in range(len(read_data)):
            self.data[i] = read_data[i]
        self.num_records = struct.unpack('>q', self.data[8:16])[0]
