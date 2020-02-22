from page import Page
from time import time
import pickle
import pathlib
import threading
import os

INDIRECTION_COLUMN = 3
RID_COLUMN = 2
TIMESTAMP_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 0


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    def __init__(self, name, num_columns, key, rid_space):
        self.name = name
        
        """
        key is the index of the primary key column
        """
        self.key = key
        
        """
        The rid_block will specify a range/interval of possible values that can
        be used as rids for the table's records. When all the values in this
        range have been assigned, the table requets a new rid_block.
        """
        self.rid_block = rid_space.assign_space()
        
        """
        global_rid_space is the table's reference to the global rid space
        allocater. The table will make requests to this global rid space
        allocater for another rid_block when the current rid_block is
        'depleted'
        """
        self.global_rid_space = rid_space
        
        """
        The rid_block_offset keeps track of our current position in the current
        rid space that has been allocated for the table.
        """
        self.rid_block_offset = 0

        """
        num_columns simply represents the number of columns in the table.
        """
        self.num_columns = num_columns
        
        """
        Keep track of the number of records assoicated with this table. for
        book-keeping purposes
        """
        self.num_records = 0
        
        """
        Keep track of number updates made to records in table. Merge is
        initiated upon every 512 updates.
        """
        self.num_updates = 0
        
        """
        Record offset is used to determine the position of the record data
        within a given page. For example, if the column values for a record are
        located in the 8th - 15th bytes of the base pages associated with that
        record, then the record offset would be 1. If the column values for
        a record are located in the 16th - 23rd bytes of the base pages
        associated with that record, the record offset is 2, and so on.   
        """
        self.record_offset = 0

        """
        The directory_lock is used to regulate concurrent access to the page
        directory. Currently, there are only two threads that will have to
        compete for the lock/acquire and release the lock: The background merge
        thread and the main forefround thread.
        """
        self.directory_lock = threading.Lock()

        """
        Simple flag that indicates if a merge is currently occuring.
        """
        self.merging = False

        """
        page_ranges contains a list of page ranges. A page range is purely
        logical. It only contains the list of page ids for each physical page
        in a page range.
        Each page range conists of 512 records. This way, every time a set of
        base pages gets filled up, we allocate a new set of base pages and
        write the data to those base pages and place the id's for these base
        pages into a new page range. We only create a new page range when the
        base pages in the most recently created page range are full. When this
        happens, we allocate a new set of base pages, place their id's in a page
        range and insert this into page_ranges.

        For example, we create a new table of four columns and insert 512
        records into this table. The data for these records will span the first
        set of 4 base pages and 4 metadata base pages allocated for these
        records (say these pages have ids 1,2,3, and 4). Then page_ranges would
        look like: 
        [ [1,2,3,4,5,6,7,8] ].
        
        Now suppose we immediately insert another 100 records. The 'current'
        page range is  full. So, we allocate a new set of base pages (and
        4 pages to hold the metadata), write the data to these pages and
        page_ranges will now look like:
        [ [1,2,3,4,5,6,7,8] [9,10,11,12,13,14,15,16] ]

        The purpose of the page range is to establish an associative
        relationship between base pages (which contain base records) and tail
        pages (which contain tail records).
        Suppose we make an unpdate (see update_record) to a record whose base
        record spans pages 1 - 8 (i.e. a record contained in the first page
        range). We allocate a set of tail pages, place the id's for these tail
        pages into the appropriate page range, and then write the tail record
        data to the tail pages. Now page_ranges would look like:
        [ [1,2,3,4,5,6,7,8,19,20,21,22,23,24,25,26] [9,10,11,12,13,14,15,16] ]
        """
        self.page_ranges = []
        
        """
        The page directory maps each rid for each record to a python tuple.
        That python tuple contains the range of page id's for all base pages that
        contain the record data. It also contains the record offset.
        
        For example, suppose we have a table of 4 columns and there's a record
        within this table that has an rid of 1. Also suppose that this record
        spans base pages with id's 0,1,2,3. Moreover, suppose that each of
        the column values for the record are located in the first 8 bytes of
        each of the physical base pages. Then the entry for this rid would look
        like: 1 -> (0,7,0,0) since base pages would have ids 0-3, metadata
        pages would have ids 4-7 and these pages would be located in page range
        0.
        """
        self.page_directory = {}
        
        """
        page_index maps page id's to specific locations within the pages list
        (where all pages associated with this table are stored). Suppose
        a physical page with an id of 3 is located in the 5th position of the
        pages list, then the entry for that page id would look like:
        3 -> 5
        """
        self.page_index = {}

        """
        Keeps track of the page ranges that are ready for merging. Only page
        ranges with full base pages are merged.
        """
        self.merge_queue = []
        
        """
        The pages list simply contains all physical pages associated with this
        table. the pages in this list are not necessarily stored in any
        particular order.
        """
        self.pages = []

        pass


# ==================== DISK-RELATED FUNCTIONS ====================

    """
    Each table has a directory of the form:
    table_name
    |___________metadata
    |
    |___________page_file

    the metadata file contains the pickled data structures used by the table.
    the page_file (will) contain the bytes for all files that have been
    allocated for the table.
    """
    
    def init_table_dir(self):
        page_path = self.name+'/page_file'
        metadata_path = self.name+'/metadata'
        pathlib.Path(self.name+'/').mkdir(parents=True, exist_ok=True)
        open(page_path, mode='wb').close()
        open(metadata_path, mode = 'wb').close()
    
    def close_table(self):
        metadata = [self.rid_block, self.rid_block_offset, self.num_updates,
                self.record_offset, self.page_ranges, self.page_directory,
                self.page_index, self.merge_queue, self.pages, self.num_records]
        f = open(self.name+'/metadata', mode='wb')
        pickle.dump(metadata, f)
        f.close()

    def open_table(self):
        try:
            f = open(self.name+'/metadata', mode='rb')
            metadata = pickle.load(f)
            self.rid_block = metadata[0]
            self.rid_block_offset = metadata[1]
            self.num_updates = metadata[2]
            self.record_offset = metadata[3]
            self.page_ranges = metadata[4]
            self.page_directory = metadata[5]
            self.page_index = metadata[6]
            self.merge_queue = metadata[7]
            self.pages = metadata[8]
            self.num_records = metadata[9]
            f.close()
        except FileNotFoundError:
            self.init_table_dir()

    def delete_files(self):
        os.unlink(self.name+'/page_file')
        os.unlink(self.name+'/metadata')
        os.rmdir(self.name)
        
# ==================== HELPER FUNCTIONS ====================

    """
    Return a new unique rid for record.
    Will retrieve a new rid space from the rid space allocator. This will be
    done whenever the current rid space is 'depleted':
    """
    def get_rid(self):
        if self.rid_block_offset == 512:
            self.rid_block = self.global_rid_space.assign_space()
            self.rid_block_offset = 0
        rid = self.rid_block[0] + self.rid_block_offset
        self.rid_block_offset += 1
        return rid

    """
    Take the set of base pages, which are clearly located in the most recently
    created page range, and determine if they can hold any more records:
    """
    def current_base_page_is_full(self):
        if len(self.page_ranges) is 0:
            return False
        else:
            page_id = self.page_ranges[-1][0]
            page = self.pages[self.page_index[page_id]]
            if page.has_capacity() is True:
                return False
            else:
                return True
    """
    Determines if the base pages for a given page range are full
    """
    def base_page_is_full(self, page_range):
        page_id = page_range[0]
        page = self.pages[self.page_index[page_id]]
        return not page.has_capacity()

    """
    Determines if the most recently allocated set of tail pages for a given
    page range are full
    """
    def tail_page_is_full(self, page_range):
        page_id = page_range[-1]
        page = self.pages[self.page_index[page_id]]
        return not page.has_capacity()

    """
    Allocates base pages that will contain metadata
    """
    def allocate_metadata_pages(self, page_range):
        for i in range(4):
            metadata_column = Page(len(self.pages))
            self.page_index[metadata_column.get_id()] = len(self.pages)
            self.pages.append(metadata_column)
            page_range.append(metadata_column.get_id())

    """
    Creates metadata pages given metadata list (indirection pointer, rid, time
    stamp, schema encoding)
    """
    def write_metadata(self, metadata, page_range):
        base_splice = page_range[:self.num_columns+4]
        for i in range(len(metadata)):
            page_id = base_splice[self.num_columns+i]
            page = self.pages[self.page_index[page_id]]
            if i == 3:
                page.write_schema(metadata[i])
            else:
                page.write(metadata[i])

    """
    Update page directory and related book-keeping fields.
    """
    def update_directory(self, rid, page_range, page_ranges_idx):
        self.page_directory[rid] = (page_range[0],
                page_range[self.num_columns+3],
                self.record_offset, page_ranges_idx)
        self.record_offset += 1
        self.num_records += 1
            

    """
    For given update range, loads a snapshot of this update range into the
    merge queue. If changes have been made to this update range since it was
    first placed into the merge queue, then the old range will be swapped out
    with the new one. Must only be called while merge is not ongoing, so as to
    avoid conflicts.
    """
    def load_merge_queue(self, page_range, old_range):
        if old_range in self.merge_queue:
            i = self.merge_queue.index(old_range)
            self.merge_queue[i] = page_range[:]
        else:
            self.merge_queue.append(page_range[:])
 
    """
    Allocates a set of tail pages that are used to contain tail records.
    """
    def allocate_tail_pages(self, page_range):
        for i in range(self.num_columns + 5):
            page = Page(len(self.pages))
            self.page_index[page.get_id()] = len(self.pages)
            self.pages.append(page)
            page_range.append(page.get_id())

    """
    Determines if a new set of tail pages need to be allocated.
    """
    def need_tail_page_allocation(self, page_range):
        
        """
        If no tail pages have been allocated for this page range, then
        allocate some. Clearly, if the page range containing the record to be
        updated is only large enough to accomodate the base pages and meta
        data pages, then no tail pages have been allocated
        """
        if len(page_range) == self.num_columns + 4:
            return True
        else:

            """ 
            Page ids for tail pages are always inserted into page range after
            ids for base pages (simply due to order in which they are created).
            get a physical tail page via last page id in page range.
            """
            tail_page = self.pages[self.page_index[page_range[-1]]]

            """
            A given set of tail records will span multiple physical pages, but
            since alll values associated with a record will occupy the same
            amount of space (64 bits), each physical tail page will have the
            same capacity. Hence, if one tail page is full, all tail pages for
            a tail record are full.
            """
            return not tail_page.has_capacity()

# ==================== RECORD RETRIEVAL ====================

    """
    Used by select function in Query class.
    Get all records corresponding to the record ids in rids and pull out the
    column data coressponding to values in query_columns. So if rids = [1] and
    query_columns = [1,0,0,1] we pull the column data contained in columns
    0 and 3 for record 1.
    To obtain a page we do:
    rid -> (page_directory) -> page_id and offset -> (page_index) -> page
    Will return a list of record objects containing key and requested column
    data.
    """

    def get_most_recent_update(self, rid, indirection_pointer, query_columns, tps):
        rid_tuple = self.page_directory[rid]
        schema_encoding_id = rid_tuple[1]
        schema_encoding_col = self.pages[self.page_index[schema_encoding_id]]
        base_schema = schema_encoding_col.read_schema(self.num_columns,
                rid_tuple[2])
        tail_encodings = '0'*self.num_columns
        columns = [0]*self.num_columns
        for idx in range(len(base_schema)):
            if base_schema[idx] == '0':
                page_id = rid_tuple[0] + idx
                page = self.pages[self.page_index[page_id]]
                columns[idx] = page.read(rid_tuple[2])
        while indirection_pointer != 0:
            tail_tuple = self.page_directory[indirection_pointer]
            tail_schema_id = tail_tuple[1] - 1
            tail_schema_col = self.pages[self.page_index[tail_schema_id]]
            tail_schema = tail_schema_col.read_schema(self.num_columns,
                    tail_tuple[2])
            for idx in range(len(tail_schema)):
                if tail_schema[idx] == '1' and tail_encodings[idx] == '0':
                    page = self.pages[self.page_index[tail_tuple[0] + idx]]
                    columns[idx] = page.read(tail_tuple[2])
                    tail_encodings = list(tail_encodings)
                    tail_encodings[idx] = '1'
                    tail_encodings = ''.join(tail_encodings)
            if int(tail_encodings,2) & int(base_schema,2) == int(base_schema,2):
                break
            indir_id = tail_tuple[0] + self.num_columns
            indir_col = self.pages[self.page_index[indir_id]]
            indirection_pointer = indir_col.read(tail_tuple[2])
            if indirection_pointer > tps and tps !=0:
                break
        return columns

    def get_records(self, rids, query_columns, key):
        records = []
        self.directory_lock.acquire()
        for rid in rids:
            rid_tuple = self.page_directory[rid]
            first_page_id = rid_tuple[0]
            first_page = self.pages[self.page_index[first_page_id]]
            tps = first_page.read(0)
            indir_id = rid_tuple[1] - 3
            indir_col = self.pages[self.page_index[indir_id]]
            indirection_pointer = indir_col.read(rid_tuple[2])
            columns = []
            if indirection_pointer == 0 or (indirection_pointer >= tps and tps != 0):
                for idx in range(len(query_columns)):
                    if query_columns[idx] is 1:
                        page_id = rid_tuple[0] + idx
                        page = self.pages[self.page_index[page_id]]
                        column_val = page.read(rid_tuple[2])
                        columns.append(column_val)
                    else:
                        columns.append(None)
            else:
                updated_record_columns = self.get_most_recent_update(rid,
                        indirection_pointer, query_columns, tps)
                key = updated_record_columns[self.key]
                for idx in range(len(query_columns)):
                    if query_columns[idx] == 1:
                        columns.append(updated_record_columns[idx])
                    else:
                        columns.append(None)
            
            record = Record(rid, key, columns)
            records.append(record)
        self.directory_lock.release()
        return records

    """
    Get complete record with metadata included. Used for testing purposes.
    """
    def get_full_record(self, rids, key):
        records = []
        for rid in rids:
            rid_tuple = self.page_directory[rid]
            columns = []
            for i in range(self.num_columns):
                page_id = rid_tuple[0] + i
                page = self.pages[self.page_index[page_id]]
                column_val = page.read(rid_tuple[2])
                columns.append(column_val)
            """
            Starting from the back, get TPS, then schema and work back until we get the
            indirection column.
            """
            for i in range(4):
                page_id = rid_tuple[1] - i
                page = self.pages[self.page_index[page_id]] 
                if i != SCHEMA_ENCODING_COLUMN:
                    column_val = page.read(rid_tuple[2])
                else:
                    column_val = page.read_schema(self.num_columns,
                            rid_tuple[2])
                columns.append(column_val)
            record = Record(rid, key, columns)
            records.append(record)
        return records

# ==================== INSERTING NEW RECORDS ====================

    """
    Inserts a new record into the table. As records are inserted we create
    and insert the appropriate entries for the page_directory and page_index
    structures. 
    """
    def insert_base_record(self, *columns):
        self.directory_lock.acquire()
        schema_encoding = '0' * self.num_columns
        curr_time = int(time())
        """
        If 'current' base pages is full, or if no records have been inserted,
        then create new page range containing a fresh set of base pages and
        insert record data into these base pages.
        """
        if self.current_base_page_is_full() or self.num_records is 0:
            self.record_offset = 2
            page_range = []
            # data storage is column-oriented:
            for column in columns:
                page = Page(len(self.pages))
                page.write(column)
                # create page_index entry
                self.page_index[page.get_id()] = len(self.pages)
                self.pages.append(page)
                page_range.append(page.get_id())
            # obtain new rid from currently assigned rid space
            rid = self.get_rid()
            # create metadata pages and write metadata to those pages
            self.allocate_metadata_pages(page_range)
            # metadata list containing indirection pointer, rid of base record,
            # time-stamp, schema encoding, and TPS:
            metadata = [0, rid, curr_time, schema_encoding]
            self.write_metadata(metadata, page_range)
            self.update_directory(rid, page_range, len(self.page_ranges))
            self.page_ranges.append(page_range)
            self.directory_lock.release()
            # return rid of newly inserted record
            return rid
        else:
            range_idx = 0
            page_range = self.page_ranges[-1]
            for column in columns:
                # pull out page id from current page range
                page_id = self.page_ranges[-1][range_idx]
                # get page corresponding to this id
                page = self.pages[self.page_index[page_id]]
                # write data to page
                page.write(column)
                range_idx += 1
            # obtain a new rid from currently assigned rid space
            rid = self.get_rid()
            # write metadata to metadata pages
            metadata = [0, rid, curr_time, schema_encoding]
            self.write_metadata(metadata, page_range)
            self.update_directory(rid, page_range, len(self.page_ranges)-1)
            # return rid of newly created record
            self.directory_lock.release()
            return rid

# ==================== UPDATING AND TAIL RECORD CREATION ====================

    """
    Inserts a single tail record into tail page (i.e. set of physical tail
    pages).
    """
    def insert_tail_record(self, tail_page_range, column_update, indir_rid, base_rid):
        rid_tuple = self.page_directory[base_rid]
        schema_encoding = ['0']*self.num_columns
        tail_record_offset = 0
        # write tail record data
        for i in range(self.num_columns):
            page_id = tail_page_range[i]
            page = self.pages[self.page_index[page_id]]
            update_value = column_update[i]
            tail_record_offset = 512 - page.get_capacity()
            if update_value is not None:
                schema_encoding[i] = '1'
                page.write(update_value)
            else:
                page.write(0)
        tail_rid = self.global_rid_space.assign_tail_rid()
        # place metadata into list for easy access in upcoming for loop
        metadata = [indir_rid]
        metadata.append(tail_rid)
        metadata.append(int(time()))
        metadata.append(schema_encoding)
        metadata.append(base_rid)
        # write tail record metadata
        for k in range(5):
            page_id = tail_page_range[self.num_columns:][k]
            page = self.pages[self.page_index[page_id]]
            if k != 3:
                page.write(metadata[k])
            else:
                schema_encoding = ''.join(schema_encoding)
                page.write_schema(schema_encoding)
        # update indirection column of base record
        # get page containing indirection pointers and update
        indir_page_id = rid_tuple[1] - 3
        page = self.pages[self.page_index[indir_page_id]]
        offset = rid_tuple[2]
        page.update(tail_rid, offset)
        # update schema encoding column of base record
        # get page containing schema encodings and update
        schema_page_id = indir_page_id + 3
        page = self.pages[self.page_index[schema_page_id]]
        base_schema = page.read_schema(self.num_columns,offset) 
        base_schema = bin(int(base_schema,2) | int(schema_encoding,2))
        page.update_schema(base_schema[2:], offset)
        # create the following page directory entry:
        # tail_rid -> (id0,idn,offset,page_range)
        self.page_directory[tail_rid] = (tail_page_range[0],tail_page_range[-1],
                tail_record_offset, rid_tuple[3])
    """
    Updates a record. This is achieved by simply creating a tail record that
    represents the update, and inserting this tail record into the set of tail 
    pages that belong to the same page range as the base record that's getting
    updated.
    For instance, suppose we have a two column table with one record, R1, and
    suppose this 0th and 1st columns of this record have values 2 and
    4 respectively. Initially, this record will look like:
    RID:    COL0:   COL1:  SCHEMA:  TIMESTAMP:  INDIRECTION:
    R1      2       4      00       time1       0

    where time1 simply represents the time of the base record's creation.
    Now suppose we update the 1st column value to 5. the corresponding tail
    record would like this:
    RID:    COL0:   COL1:   SCHEMA:  TIMESTAMP:  INDIRECTION:
    T1      X       5       01       time2       0
    
    And the base record will be:
    RID:    COL0:   COL1:  SCHEMA:  TIMESTAMP:  INDIRECTION:
    R1      2       4      01       time1       T1

    If the base record is contained in the page range located at
    page_ranges[1], then the tail record will be written to the tail pages
    located at page_ranges[1]. Of course, if page_ranges[1] has only base
    pages, then a set of tail pages will be allocated, and their id's will be
    placed in page_ranges[1].
    
    """
    def update_record(self, rid, column_update):
        self.directory_lock.acquire()
        rid_tuple = self.page_directory[rid]
        page_range_idx = rid_tuple[3]
        page_range = self.page_ranges[page_range_idx]
        old_range = page_range[:]
        
        if self.need_tail_page_allocation(page_range):
            self.allocate_tail_pages(page_range)

        indir_page = self.pages[self.page_index[rid_tuple[1]-3]]
        indirection_pointer = indir_page.read(rid_tuple[2])
        tail_page_range = page_range[-(self.num_columns + 5):]
        self.insert_tail_record(tail_page_range, column_update,
                indirection_pointer, rid)

        if self.base_page_is_full(page_range) and self.tail_page_is_full(page_range) and not self.merging:
            self.load_merge_queue(page_range, old_range)

        
        self.directory_lock.release()
        
        self.num_updates += 1
        if self.num_updates >= 510 and not self.merging and not len(self.merge_queue) == 0:
            merge_thread = threading.Thread(target=self.__merge, args=())
            self.num_updates = 0
            merge_thread.start()

# ==================== RECORD INVALIDATION ====================

    def invalidate_record(self, rid):
        self.directory_lock.acquire()
        try:
            rid_tuple = self.page_directory[rid]
        except KeyError:
            print('error: cannot delete nonexistent record')
            return

        rid_page_id = rid_tuple[1] - RID_COLUMN
        rid_page = self.pages[self.page_index[rid_page_id]]
        # invalidate base record:
        rid_page.update(0,rid_tuple[2])
        # obtain indirection pointer to perform tail record invalidation
        indir_page = self.pages[self.page_index[rid_tuple[1]-3]]
        indirection_pointer = indir_page.read(rid_tuple[2])
        del self.page_directory[rid]
        self.directory_lock.release()
    
# ==================== MERGE ====================

    def get_tail_record(self, id_segment, offset):
        tail_record_data = []
        for i in range(self.num_columns):
            page_id = id_segment[i]
            tail_page = self.pages[self.page_index[page_id]]
            data = tail_page.read(offset)
            tail_record_data.append(data)
        for k in range(5):
            page_id = id_segment[self.num_columns + k]
            tail_page = self.pages[self.page_index[page_id]]
            if k != 3:
                data = tail_page.read(offset)
                tail_record_data.append(data)
            else:
                data = tail_page.read_schema(self.num_columns, offset)
                tail_record_data.append(data)
        return tail_record_data
            
    def process_tail_page(self, id_segment):
        tail_records = []
        tail_id = id_segment[-1]
        tail_page = self.pages[self.page_index[tail_id]]
        offset = tail_page.num_records - 1
        while offset >= 1:
            tail_records.append(self.get_tail_record(id_segment, offset))
            offset -= 1
        return tail_records

    def write_tps_to_base(self, base_page_ids, tail_records):
        most_recent_update = tail_records[0]
        tail_id = most_recent_update[-4]
        for page_id in base_page_ids:
            page = self.pages[self.page_index[page_id]]
            page.update(tail_id, 0)

    def write_updates_to_base_pages(self, tail_records, base_page_ids, updated_mappings):
        tail_encodings_map = {}
        for tail_record in tail_records:
            tail_schema  = tail_record[-2]
            base_rid = tail_record[-1]  
            if base_rid not in tail_encodings_map:
                tail_encodings_map[base_rid] = '0'*self.num_columns
            tail_encoding = tail_encodings_map[base_rid]
            # determine if tail record contains a yet to be encountered update:
            if int(tail_encoding,2) | int(tail_schema,2) != int(tail_encoding,2):
                # If it does, write update and update tail_encodings
                try:
                    base_tuple = self.page_directory[base_rid]
                except KeyError:
                    continue
                base_offset = base_tuple[2]
                for i in range(len(base_page_ids)):
                    if tail_schema[i] == '1' and tail_encoding[i] == '0':
                        data = tail_record[i]
                        base_page_id = base_page_ids[i]
                        base_page = self.pages[self.page_index[base_page_id]]
                        base_page.update(data, base_offset)
                # following list comprehension was taken from stack overflow.
                # this is used to update the tail encodings.
                ones = [i for i, num in enumerate(tail_schema) if num == '1']
                for j in ones:
                    tail_encoding = list(tail_encoding)
                    tail_encoding[j] = '1'
                    tail_encoding = ''.join(tail_encoding)
                tail_encodings_map[base_rid] = tail_encoding

                if base_rid not in updated_mappings:
                    updated_mappings[base_rid] = (base_page_ids, base_tuple[3])


                        
    def process_tail_records(self, id_segments, base_page_ids, updated_mappings):
        tps_updated = False
        while id_segments != []:
            latest_segment = id_segments[-1]
            tail_records = self.process_tail_page(latest_segment)
            
            if not tps_updated:
                self.write_tps_to_base(base_page_ids, tail_records)
                tps_updated = True

            self.write_updates_to_base_pages(tail_records, base_page_ids,
                    updated_mappings)
            id_segments.pop(-1)

    def copy_base_pages(self, base_page_ids, update_range):
        for i in range(len(base_page_ids)):
            new_page_id = base_page_ids[i]
            old_page_id = update_range[i]
            old_page = self.pages[self.page_index[old_page_id]]
            new_page = self.pages[self.page_index[new_page_id]]
            for k in range(0,512):
                old_data = old_page.read(k)
                new_page.update(old_data,k)
    
    def modify_page_directory(self, updated_mappings):
        self.directory_lock.acquire()
        for rid in updated_mappings.keys():
            base_page_ids = updated_mappings[rid][0]
            old_tuple = self.page_directory[rid]
            page_range = self.page_ranges[old_tuple[3]]
            new_tuple = list(old_tuple)
            new_tuple[0] = base_page_ids[0]
            new_tuple = tuple(new_tuple)
            self.page_directory[rid] = new_tuple
            for i in range(len(base_page_ids)):
                page_range[i] = base_page_ids[i]
        self.directory_lock.release()

    """
    Used for testing purposes:
    """
    def output_pages(self, base_page_ids):
        b = []
        for i in range(512):
            rec = []
            for bid in base_page_ids:
                page = self.pages[self.page_index[bid]]
                data = page.read(i)
                rec.append(data)
            b.append(rec)
        for l in range(512):
            print('record:', end='')
            print(l, end=' ')
            print(b[l])

    def __merge(self):
        self.merging = True
        print('Merge begin!')
        updated_mappings = {}
        for update_range in self.merge_queue:
            tail_page_ids = update_range[self.num_columns+4:]
            id_segments = []
            while tail_page_ids != []:
                id_segment = []
                for i in range(self.num_columns+5):
                    id_segment.append(tail_page_ids.pop(0))
                id_segments.append(id_segment)

            base_page_ids = []

            """
            Allocate new set of base pages for merging. Metadata pages are not
            touched, so we only allocate enought to write metadata.
            """
            for i in range(self.num_columns):
                page = Page(len(self.pages))
                self.page_index[page.get_id()] = len(self.pages)
                base_page_ids.append(page.get_id())
                self.pages.append(page)

            self.copy_base_pages(base_page_ids, update_range)
            self.process_tail_records(id_segments, base_page_ids,
                    updated_mappings)

        self.modify_page_directory(updated_mappings)
        # print('outputing merged pages for range: ', sep='')
        # print(update_range)
        #self.output_pages(base_page_ids)
        self.merging = False
        self.merge_queue = []
        print('Merge terminated!')
        pass

