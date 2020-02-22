from table import Table

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.table = table
        self.idx = {}
        self.rid_map = {}

    """
    Returns the location of all records with the given value
    """
    def locate(self, value): 
        try:
            return self.idx[value]
        except KeyError:
            return []

    """
    Create index on specific column
    """
    def create_index(self, column_number):
        self.table.directory_lock.acquire()
        if column_number >= self.table.num_columns:
            print('error: cannot create index on column that does not exist')
            return
        for rid in self.table.page_directory:
            if rid > self.table.rid_block[1]:
                continue
            # obtain page-related information for given rid:
            rid_tuple = self.table.page_directory[rid]
            # obtain id of page containing key value
            page_id = rid_tuple[0] + column_number
            # obtain page containing key value
            page = self.table.pages[self.table.page_index[page_id]]
            # read key value from page
            key_value = page.read(rid_tuple[2])
            self.rid_map[rid] = key_value
            if key_value in self.idx.keys() and rid not in self.idx[key_value]:
                self.idx[key_value].append(rid)
            else:
                self.idx[key_value] = [rid]
        self.table.directory_lock.release()

    """
    Add a new key, rid pair to index
    """
    def add_key(self, rid, column_number):
        rid_tuple = self.table.page_directory[rid]
        page_id = rid_tuple[0] + column_number
        page = self.table.pages[self.table.page_index[page_id]]
        key_value = page.read(rid_tuple[2])
        try:
            if rid not in self.idx[key_value]:
                self.idx[key_value].append(rid)
                self.rid_map[rid] = key_value
        except KeyError:
            self.idx[key_value] = [rid]
            self.rid_map[rid] = key_value

    """
    Update prexisting key, rid pair. From old_key->rid to new_key->rid
    """
    def update_index(self, rid, new_key):
        old_key = self.rid_map[rid]
        if old_key != new_key:
            self.rid_map[rid] = new_key
            self.idx[old_key].remove(rid)
            if self.idx[old_key] == []:
                del self.idx[old_key]
            try:
                if rid not in self.idx[new_key]:
                    self.idx[new_key].append(rid)
            except KeyError:
                self.idx[new_key] = [rid]


    def drop_index(self, table, column_number):
        pass

    def delete(self, rid, key):
        self.idx[key].remove(rid)
        del self.rid_map[rid]
        if self.idx[key] == []:
            del self.idx[key]

    def print_index(self):
        print(self.idx)
