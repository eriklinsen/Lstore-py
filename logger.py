import threading
"""
Not a conventional logger. More of a session logger. That is, this will store
all operations performed by a set of transactions on a table within the scope
of a single session. If the database is closed, then all changes recorded in
the logger will be lost.
"""
class Logger():

    def __init__(self):
        """
        The log is a nested dictionary that will contain data related to
        transactions performed on a table by a given transaction thread.
        For a transaction being executed on a thread with id thread_id, the
        logger records an operation, op, in the following manner:
        log[thread_id][op] = (operation tuple)
        Of course, because the information in the logger is thread id specific,
        and because thread id's can be re-used, transaction-specific archives
        will have to be cleared after the transaction commits and terminates.
        """
        self.log = {}
        """
        The 
        """
        self.dir_log = {}

    def archive_delete(self, rid, rid_tuple):
        thread_id = threading.current_thread().ident
        if thread_id not in self.log:
            self.log[thread_id] = {}
        if 'deletes' not in self.log[thread_id]:
            self.log[thread_id]['deletes'] = []
        """
        Record the delete. This is done by recording the rid of the deleted
        record. The page directory entry for the record is also recorded in the
        directory log (dir_log), so that the entry can be restored in the event
        of a rollback.
        """
        self.log[thread_id]['deletes'].append(rid)
        self.dir_log[rid] = rid_tuple

    def archive_update(self, tail_rid, base_rid, old_pointer, base_indir_id, base_schema_id, base_offset, tail_schema):
        thread_id = threading.current_thread().ident
        if thread_id not in self.log:
            self.log[thread_id] = {}
        if 'updates' not in self.log[thread_id]:
            self.log[thread_id]['updates'] = []
        """
        Record the updated. This is done by specifying the tail record that
        encodes the update (via the tail_rid) and the schema encoding for that
        tail record. The base_indir_rid and indir_offset is used to specify the
        exact indirection pointer of the updated base record that will point to
        the update upon the update's creation. This data is also used to
        restore the update in the event of a rollback.
        """
        self.log[thread_id]['updates'].append((tail_rid, base_rid, old_pointer,
            base_indir_id, base_schema_id, base_offset, tail_schema))

    def archive_insert(self, rid):
        thread_id = threading.current_thread().ident
        if thread_id not in self.log:
            self.log[thread_id] = {}
        if 'inserts' not in self.log[thread_id]:
            self.log[thread_id]['inserts'] = []
        """
        Record the insertion of a record. This is done by recording the rid of
        the inserted record.
        """
        self.log[thread_id]['inserts'].append(rid)


    def clear_archive(self, thread_id):
        del self.log[thread_id]
