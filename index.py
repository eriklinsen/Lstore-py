import threading

class Index:

    def __init__(self, table): 
        self.index_lock = threading.Lock()
        self.indices = {}
        self.table = table
        self.primary_key = self.table.key
        self.rid_maps = {}
        self.init_indices(self.table.num_columns)
        self.create_index(self.primary_key)
        """
        Used for concurrent transaction support:
        """
        self.lock_map = {}
        self.owned_xlocks = {}
        self.owned_rlocks = {} 
        self.index_log = {}


    def init_indices(self, num_columns):
        for i in range(num_columns):
            self.indices[i] = None

    """
    Returns the location of all records with the given value
    """
    def locate(self, column_number, value):
        with self.index_lock:
            idx = self.indices[column_number]
            try:
                return idx[value]
            except KeyError:
                return []

    def obtain_xlock(self, rid):
        with self.index_lock:
            thread_id = threading.current_thread().ident
            if thread_id not in self.owned_xlocks:
                self.owned_xlocks[thread_id] = set()
            else:
                if rid in self.owned_xlocks[thread_id]:
                    return True
            if self.lock_map[rid][1] != 1 and (self.lock_map[rid][0] == 0 or rid in self.owned_rlocks[thread_id]):
                self.lock_map[rid][1] = 1
                self.owned_xlocks[thread_id].add(rid)
            else:
                return False
            return True
    
    def obtain_rlock(self, rids):
        with self.index_lock:
            thread_id = threading.current_thread().ident
            if thread_id not in self.owned_rlocks:
                self.owned_rlocks[thread_id] = set()
            for rid in rids:
                if rid in self.owned_rlocks[thread_id]:
                    continue
                if self.lock_map[rid][1] != 1:
                    self.lock_map[rid][0] += 1
                    self.owned_rlocks[thread_id].add(rid)
                else:
                    return False 
            return True

    def release_xlock(self, rids):
        with self.index_lock:
            for rid in rids:
                self.lock_map[rid][1] = 0
            
    def release_rlock(self, rids):
        with self.index_lock:
            for rid in rids:
                if self.lock_map[rid][0] != 0:
                    self.lock_map[rid][0] -= 1

    """
    Create index on specific column
    """
    def create_index(self, column_number):
        if column_number >= self.table.num_columns:
            print('error: cannot create index on column that does not exist')
            return
        with self.index_lock:
            self.indices[column_number] = {}
            self.rid_maps[column_number] = {}
            idx = self.indices[column_number]
            rid_map = self.rid_maps[column_number]

            query_columns = [0]*self.table.num_columns
            query_columns[column_number] = 1

            for rid in self.table.base_rids:
                record = self.table.get_records([rid], query_columns, 0)[0]
                key_value = record.columns[column_number]
                rid_map[rid] = key_value
                if key_value in idx.keys() and rid not in idx[key_value]:
                    idx[key_value].append(rid)
                else:
                    idx[key_value] = [rid]
                if rid not in self.lock_map:
                    self.lock_map[rid] = [0,0]

    """
    Add a new key, rid pair to index
    """
    def add_key(self, rid, column_number, bp):
        with self.index_lock:
            self.table.directory_lock.acquire()
            idx = self.indices[column_number]
            rid_map = self.rid_maps[column_number]
            rid_tuple = self.table.page_directory[rid]
            page_id = rid_tuple[0] + column_number
            page = bp.get_page(self.table.name, page_id)
            key_value = page.read(rid_tuple[2])
            try:
                if rid not in idx[key_value]:
                    idx[key_value].append(rid)
                    rid_map[rid] = key_value
            except KeyError:
                idx[key_value] = [rid]
                rid_map[rid] = key_value
            if rid not in self.lock_map:
                self.lock_map[rid] = [0,0]
            if threading.current_thread().ident not in self.index_log:
                self.index_log[threading.current_thread().ident] = []
            self.index_log[threading.current_thread().ident].append(('add', rid, column_number, key_value)) 
            self.table.directory_lock.release()

    """
    Update prexisting key, rid pair. From old_key->rid to new_key->rid
    """
    def update_index(self, rid, column_number, new_key):
        with self.index_lock:
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
            if threading.current_thread().ident not in self.index_log:
                self.index_log[threading.current_thread().ident] = []
            self.index_log[threading.current_thread().ident].append(('update', rid, column_number, old_key, new_key)) 
 
    def locate_range(self, begin, end, column_number):
        with self.index_lock:
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
        with self.index_lock:
            del self.indices[column_number]
            del self.rid_maps[column_number]

    def delete(self, rid, column_number, key):
        with self.index_lock:
            idx = self.indices[column_number]
            rid_map = self.rid_maps[column_number]
            idx[key].remove(rid)
            del rid_map[rid]
            if idx[key] == []:
                del idx[key]
            if threading.current_thread().ident not in self.index_log:
                self.index_log[threading.current_thread().ident] = []
            self.index_log[threading.current_thread().ident].append(('delete', rid, column_number, key))

    def release_locks(self, thread_id):
        locks_owned = self.ownership_map[thread_id]
        for lock in locks_owned:
            if lock[0] == 'r':
                self.release_rlock(lock[1])
            else:
                self.release_xlock(lock[1])

    def undo_add(self, rid, column_number, key_value):
        with self.index_lock:
            idx = self.indices[column_number]
            rid_map = self.rid_map[column_number]
            idx[key_value].remove(rid)
            del rid_map[rid]
            if idx[key] == []:
                del idx[key]

    def undo_update(self, rid, column_number, old_key, new_key):
        with self.index_lock:
            idx = self.indices[column_number]
            rid_map = self.rid_maps[column_number]
            if old_key != new_key:
                rid_map[rid] = old_key
                idx[new_key].remove(rid)
                if idx[new_key] == []:
                    del idx[new_key]
                try:
                    if rid not in idx[old_key]:
                        idx[old_key].append(rid)
                except KeyError:
                    idx[old_key] = [rid]

    def undo_delete(self, rid, column_number, key):
        with self.index_lock:
            idx = self.indices[column_number]
            rid_map = self.rid_maps[column_number]
            idx[key].append(rid)
            rid_map[rid] = key_value

    def rollback_index(self, thread_id):
        if thread_id in self.index_log:
            archive = self.index_log[thread_id]
            while len(archive) != 0:
                chage = archive.pop()
                if change[0] == 'add':
                    self.undo_add(*change[1:])
                    continue
                elif change[0] == 'update':
                    self.undo_update(*change[1:])
                    continue
                elif change[0] == 'delete':
                    self.undo_delete(*change[1:])
                else:
                    raise Exception('rollback index error: archive error')
            
    def print_index(self):
        print(self.idx)
