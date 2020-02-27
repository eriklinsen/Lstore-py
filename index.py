
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.indices = {}
        self.table = table
        self.primary_key = self.table.key
        self.rid_maps = {}
        self.create_index(self.primary_key)

    """
    Returns the location of all records with the given value
    """
    def locate(self, column_number, value):
        idx = self.indices[column_number]
        try:
            return idx[value]
        except KeyError:
            return []

    """
    Create index on specific column
    """
    def create_index(self, column_number):
        if column_number >= self.table.num_columns:
            print('error: cannot create index on column that does not exist')
            return

        self.indices[column_number] = {}
        self.rid_maps[column_number] = {}
        idx = self.indices[column_number]
        rid_map = self.rid_maps[column_number]

        query_columns = [0]*self.table.num_columns
        query_columns[column_number] = 1

        for rid in self.table.page_directory:
            if rid > self.table.rid_block[1]:
                continue
            record = self.table.get_records([rid], query_columns, 0)[0]
            key_value = record.columns[column_number]
            rid_map[rid] = key_value
            if key_value in idx.keys() and rid not in idx[key_value]:
                idx[key_value].append(rid)
            else:
                idx[key_value] = [rid]

    """
    Add a new key, rid pair to index
    """
    def add_key(self, rid, column_number, bp):
        self.table.directory_lock.acquire()
        idx = self.indices[column_number]
        rid_map = self.indices[column_number]
        rid_tuple = self.table.page_directory[rid]
        page_id = rid_tuple[0] + column_number
        # page = self.table.pages[self.table.page_index[page_id]]
        page = bp.get_page(self.table.name, page_id)
        key_value = page.read(rid_tuple[2])
        try:
            if rid not in idx[key_value]:
                idx[key_value].append(rid)
                rid_map[rid] = key_value
        except KeyError:
            idx[key_value] = [rid]
            rid_map[rid] = key_value

        self.table.directory_lock.release()

    """
    Update prexisting key, rid pair. From old_key->rid to new_key->rid
    """
    def update_index(self, rid, column_number, new_key):
        idx = self.indices[column_number]
        rid_map = self.rid_maps[column_number]

        old_key = rid_map[rid]
        if old_key != new_key:
            rid_map[rid] = new_key
            idx[old_key].remove(rid)
            if idx[old_key] == []:
                del idx[old_key]
            try:
                if rid not in idx[new_key]:
                    idx[new_key].append(rid)
            except KeyError:
                idx[new_key] = [rid]
    
    
    
    def locate_range(self, begin, end, column_number):
        rids = []
        idx = self.indices[column_number]
        sorted_keys = sorted(idx.keys())
        for key in sorted_keys:
            if key > end:
                break
            if key in range(begin, end+1):
                rids.extend(idx[key])
        return rids

    def drop_index(self, column_number):
        del self.indices[column_number]
        del self.rid_maps[column_number]

    def delete(self, rid, column_number, key):
        idx = self.indices[column_number]
        rid_map = self.rid_maps[column_number]
        idx[key].remove(rid)
        del rid_map[rid]
        if idx[key] == []:
            del idx[key]

    def print_index(self):
        print(self.idx)
