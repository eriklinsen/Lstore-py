from table import Table, Record
from index import Index
import threading


class Query:
    """
    Creates a Query object that can perform different queries on the specified
    table. 
    """
    def __init__(self, table): 
        if table == None:
            raise AttributeError('query init error: cannot query on a non-existent table')
        self.table = table
        self.key = self.table.key
        self.bp = self.table.bp

    """
    Delete a record with specified key.
    """
    def delete(self, key):
        rid = self.table.index.locate(self.key, key)[0]
        got_lock = self.table.index.obtain_xlock([rid])
        if got_lock:
            self.table.invalidate_record(rid)
            for i in self.table.index.indices.keys():
                if self.table.index.indices[i] != None:
                    self.table.index.delete(rid, i, key)
            return True
        else:
            print(threading.current_thread().ident, 'aborted on delete')
            return False
    """
    Insert a record with specified columns.
    """
    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            print(threading.current_thread().ident, 'aborted on insert')
            return False
        rid = self.table.insert_base_record(*columns)
        self.table.index.add_key(rid, self.key, self.bp)
        return True

    """
    Read a record with specified key. Will return an empty list if query
    cannot be completed or if no records are found.
    """
    def select(self, key, column_number, query_columns):
        if len(query_columns) is not self.table.num_columns:
            #print('select error: number of queried columns must match number of columns in table')
            return []
        
        if self.table.index.indices[column_number] == None:
            self.table.index.create_index(column_number)

        rids = self.table.index.locate(column_number, key)
        got_lock = self.table.index.obtain_rlock(rids)
        if len(rids) != 0 and got_lock:
            records = self.table.get_records(rids, query_columns, key)      
            return records
        elif len(rids) == 0:
            print('select error: table does not contain specified key')
            return []
        else:
            # print(threading.current_thread().ident, 'aborted on select')
            return False
    """
    Update a record with specified key and columns.
    Currently assumes uniqueness of primary key.
    """
    def update(self, key, *columns):
        if len(columns) != self.table.num_columns:
            print('update error: query only specifies updates on ', end='')
            print(len(columns), end=' columns, even though table has following number of columns: ')
            print(self.table.num_columns)
        try:
            rid = self.table.index.locate(self.key, key)[0]
        except IndexError:
            #print('update error: specified key not found. cannot perform update on non-existent record')
            return
        
        nulled_list = [None]*self.table.num_columns
        if nulled_list == list(columns):
            return

        got_lock = self.table.index.obtain_xlock(rid)
        if got_lock:
            self.table.update_record(rid, columns)
            for i in range(len(columns)):
                # if we've updated column value and updated value is different
                # from old value:
                if columns[i] != None:
                    try:
                        self.table.index.update_index(rid, i, columns[i])
                    except KeyError:
                        pass
            return True
        else:
            # print(threading.current_thread().ident, 'aborted on update')
            return False
    """
    Aggregate values stored in specfied column over specified range of key
    values:
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        column_sum = 0
        key = 0
        query_columns = [0]*self.table.num_columns
        query_columns[aggregate_column_index] = 1
        rids = self.table.index.locate_range(start_range, end_range,
                self.key)

        if len(rids) is not 0:
            records = self.table.get_records(rids, query_columns, key)
            for record in records:
                column_sum += record.columns[aggregate_column_index]
        return column_sum

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            #if u == False:
                #print(threading.current_thread().ident, 'aborted on increment')
            return u

        return False


