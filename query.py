from table import Table, Record
from index import Index


class Query:
    """
    Creates a Query object that can perform different queries on the specified
    table. 
    """
    def __init__(self, table):
        self.table = table
        self.key = self.table.key
        self.bp = self.table.bp

    """
    Delete a record with specified key.
    """
    def delete(self, key):
        rid = self.table.index.locate(self.key, key)[0]
        self.table.invalidate_record(rid)
        for i in self.table.index.indices.keys():
            self.table.index.delete(rid, i, key)
    """
    Insert a record with specified columns.
    """
    def insert(self, *columns):
        rid = self.table.insert_base_record(*columns)
        self.table.index.add_key(rid, self.key, self.bp)

    """
    Read a record with specified key. Will return an empty list if query
    cannot be completed or if no records are found.
    """
    def select(self, key, column_number, query_columns):
        if len(query_columns) is not self.table.num_columns:
            print('select error: number of queried columns must match number of columns in table')
            return []

        rids = self.table.index.locate(column_number, key)
        if len(rids) is not 0:
            records = self.table.get_records(rids, query_columns, key)      
            return records
        else:
            print('select error: table does not contain specified key')
            return []
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
            print('update error: specified key not found. cannot perform update on non-existent record')
            return
        
        nulled_list = [None]*self.table.num_columns
        if nulled_list == list(columns):
            return

        self.table.update_record(rid, columns)
        for i in range(len(columns)):
            # if we've updated column value and updated value is different
            # from old value:
            if columns[i] != None:
                try:
                    self.table.index.update_index(rid, i, columns[i])
                except KeyError:
                    pass
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
