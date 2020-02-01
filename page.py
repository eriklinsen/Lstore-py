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

    def write(self, value):
        self.num_records += 1
        write_pos = self.num_records*8
        self.data[(write_pos-8):(write_pos)] = struct.pack('>Q', value)
        pass

    def read(self, offset):
        return struct.unpack('>Q', self.data[(offset+1)*8 - 8:8*(offset+1)])[0]

    def print_data(self):
        print('page id: ' + str(self.pg_id))
        for record in range(self.num_records):
            print(struct.unpack('>Q', self.data[(record+1)*8- 8:8*(record+1)])[0])


