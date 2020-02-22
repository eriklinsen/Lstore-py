from table import Table, Record
from index import Index


class Query:
    """
    Creates a Query object that can perform different queries on the specified
    table. 
    """
    def __init__(self, table):
        self.table = table
        self.primary_key = table.key
        self.indexes = {}
        self.indexes[self.primary_key] = Index(table)
        if self.table.num_records > 0:
            self.indexes[self.primary_key].create_index(self.primary_key)

    """
    Delete a record with specified key.
    """
    def delete(self, key):
        rid = self.indexes[self.primary_key].locate(key)[0]
        self.table.invalidate_record(rid)
        for index in self.indexes.values():
            index.delete(rid, key)
    """
    Insert a record with specified columns.
    """
    def insert(self, *columns):
        rid = self.table.insert_base_record(*columns)
        self.indexes[self.primary_key].add_key(rid,self.primary_key)

    """
    Read a record with specified key. Will return an empty list if query
    cannot be completed or if no records are found.
    """
    def select(self, key, column_index, query_columns):
        # *** CURRENTLY CREATING INDEX IF USER SELECTS ON A COLUMN THAT IS NOT
        # INDEXED. I'M NOT SURE IF THIS HOW WE SHOULD DO IT
        if len(query_columns) is not self.table.num_columns:
            print('select error: number of queried columns must match number of columns in table')
            return []
        try:
            index = self.indexes[column_index]
        except KeyError:
            self.indexes[column_index] = Index(self.table)
            self.indexes[column_index].create_index(column_index)
            index = self.indexes[column_index]

        rids = index.locate(key)
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
            rid = self.indexes[self.primary_key].locate(key)[0]
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
                    self.indexes[i].update_index(rid, columns[i])
                except KeyError:
                    pass
    """
    Aggregate values stored in specfied column over specified range of key
    values:
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        column_sum = 0
        query_columns = [0]*self.table.num_columns
        query_columns[aggregate_column_index] = 1
        for key in range(start_range, end_range+1):
            rids = self.indexes[self.primary_key].locate(key)
            if len(rids) is not 0:
                records = self.table.get_records(rids, query_columns, key)
                for record in records:
                    column_sum += record.columns[aggregate_column_index]
        return column_sum
