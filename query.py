from table import Table, Record
from index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.idx = Index(table)
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        rid = self.table.insert_record(*columns)
        self.idx.update_index(rid,self.table.key)
        pass

    """
    # Read a record with specified key. will return an empty list if query
    # cannot be completed or if no records are found
    """

    def select(self, key, query_columns):
        if len(query_columns) is not self.table.num_columns:
            print('error: number of queried columns must match number of columns in table')
            return []
        rids = self.idx.locate(key)
        if len(rids) is not 0:
            records = self.table.get_records(rids, query_columns)      
            return records
        else:
            return []
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        column_sum = 0
        query_columns = [0]*self.table.num_columns
        query_columns[aggregate_column_index] = 1
        for key in range(start_range, end_range+1):
            rids = self.idx.locate(key)
            if len(rids) is not 0:
                records = self.table.get_records(rids, query_columns)
                for record in records:
                    column_sum += record.columns[0]
        return column_sum
        pass
