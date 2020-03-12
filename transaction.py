from table import Table, Record
from index import Index
import threading

class Transaction:

    """
    Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.tables = []
        pass

    """
    Adds the given query to this transaction
    Example:
    q = Query(grades_table)
    t = Transaction()
    t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            query_object = query.__self__
            table = query_object.table
            if table not in self.tables:
                self.tables.append(table)
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort() 
        return self.commit()

    def abort(self):
        thread_id = threading.current_thread().ident
        for table in self.tables:
            table.rollback(thread_id)
        return False

    def commit(self):
        thread_id = threading.current_thread().ident
        for table in self.tables:
            table.commit(thread_id)
        return True
