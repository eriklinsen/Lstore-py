# from config import *
import struct
import threading

class Page:

    def __init__(self, page_id):
        self.num_records = 2
        self.pg_id = page_id
        self.dirty = False
        self.pin_lock = threading.Lock()
        self.pinned = 0
        self.data = bytearray(4096)

    def get_id(self):
        return self.pg_id

    def pin(self):
        self.pin_lock.acquire()
        if self.pinned < threading.activeCount():
            self.pinned += 1
        self.pin_lock.release()

    def unpin(self):
        self.pin_lock.acquire()
        if self.pinned != 0:
            self.pinned -= 1
        self.pin_lock.release()

    def has_capacity(self):
        return (self.num_records - 2) != 510
        pass

    def get_capacity(self):
        return 510 - (self.num_records-2)
    
    # use only to write record data, rid and timestamp
    def write(self, value):
        self.pin()
        if not self.has_capacity():
            print('page write error: page has no more available space.')
            return
        self.num_records += 1
        write_pos = self.num_records*8
        self.data[(write_pos-8):(write_pos)] = struct.pack('>q', value)
        self.update(self.num_records, 1)
        self.dirty = True
        self.unpin()

    # must be used to write schema
    def write_schema(self, schema_encoding):
        self.pin()
        if not self.has_capacity():
            print('page write error: page has no more available space.')
            return
        self.num_records += 1
        write_pos = self.num_records*8
        self.data[(write_pos-8):(write_pos)] = struct.pack('>q', int(schema_encoding,2))
        self.update(self.num_records, 1)
        self.dirty = True
        self.unpin()
    
    # used to update pre-existing values
    def update(self, value, offset):
        self.pin()
        if offset > 511 or offset < 0:
            print('page update error: offset out of range.')
            return
        self.data[(offset+1)*8 - 8:8*(offset+1)] = struct.pack('>q', value)
        self.dirty = True
        self.unpin()

    def update_schema(self, schema_encoding, offset):
        self.pin()
        if offset > 511 or offset < 0:
            print('page update error: offset out of range.')
            return
        self.data[(offset+1)*8 - 8:8*(offset+1)] = struct.pack('>q',
                int(schema_encoding,2))
        self.dirty = True
        self.unpin()

    # used to read data from page:
    def read(self, offset):
        self.pin()
        if offset > 511 or offset < 0:
            print('page read error: offset out of range.')
            return
        self.unpin()
        return struct.unpack('>q', self.data[(offset+1)*8 - 8:8*(offset+1)])[0]

    # must be used to read schema
    def read_schema(self, num_columns, offset):
        self.pin()
        if offset > 511 or offset < 0:
            print('page read error: offset out of range.')
            return
        schema_encoding = self.data[(offset+1)*8 - 8:8*(offset+1)]
        schema_in_bits = ''.join(format(byte, '08b') for byte in schema_encoding)
        self.unpin()
        return schema_in_bits[64-num_columns:]

    def load_data(self, read_data):
        for i in range(len(read_data)):
            self.data[i] = read_data[i]
        recorded_num = struct.unpack('>q', self.data[8:16])[0]
        if recorded_num != 0:
            self.num_records = recorded_num
        else:
            self.data[8:16] = struct.pack('>q', self.num_records)

