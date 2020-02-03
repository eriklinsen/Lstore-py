from lstore.page import Page
from time import time

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

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, rid_space):
        self.name = name
        self.key = key
        """
        the rid_block will specify a range/interval of possible values that can
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
        self.rid_block_offset = 0
        self.num_columns = num_columns
        """
        keep track of the number of records assoicated with this table. for
        book-keeping purposes
        """
        self.num_records = 0
        """
        record offset is used to determine the position of the record data
        within a given page. For example, if the column values for a record are
        located in the 8th - 15th bytes of the base pages associated with that
        record, then the record offset would be 1. If the column values for
        a record are located in the 16th - 23rd bytes of the base pages
        associated with that record, the record offset is 2, and so on.   
        """
        self.record_offset = 0
        """
        page_ranges contains a list of page ranges. A page range is purely
        logical. It only contains the list of page ids for each physical page
        in a page range.
        Each page range conists of 512 records. this way, each time a set of
        base pages are filled up, we allocated a new set of base pages and
        write the data to the base pages and place the id's for these base
        pages into a new page range. We only create a new page range when the
        current base pages in the most recently created page range are full.
        When this happens, we allocate a new set of base pages, place their
        id's in a page range and insert this into page_ranges.

        For example, we create a new table of four columns and insert 512
        records into this table. The data for these records will span the first
        set of 4 base pages and 4 metadata base pages allocated for these
        records (say thay have ids 1,2,3, and 4). Then page_ranges would look
        like: [[1,2,3,4,5,6,7,8]].
        Now suppose we immediately insert another 100 records. The 'current'
        page range is  full. So, we allocated a new set of base pages, write
        the data to these pages and page_ranges looks like this:
        [[1,2,3,4,5,6,7,8][9,10,11,12,13,14,17,18]]
        """
        self.page_ranges = []
        """
        the page directory maps each rid for each record to a python tuple.
        that python tuple contains the range of page id's for all pages that
        contain the record data. It also contains the record offset.
        For example, suppose we have a table of 4 columns and there's a record
        within this table that has an rid of 1. Also suppose that this record
        spans base pages with id's 0,1,2,3. Moreover, suppose that each of
        the column values for the record are located in the first 8 bytes of
        each of the physical base pages. Then the entry for this rid would look
        like: 1 -> (0,7,0,0) since base pages would have ids 0-3, metadata
        pages would have ids 4-7 and these pages would be located in page range
        0
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
        the pages list simply contains all physical pages associated with this
        table. the pages in this list are not necessarily stored in any
        particular order.
        """
        self.pages = []
        pass

    def get_num_records(self):
        return self.num_records

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
    take the set of base pages, which are clearly located in the most recently
    created page range, and determine if they can hold any more records:
    """
    def base_page_is_full(self):
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
    Used by select function in Query class.
    get all records corresponding to the record ids in rids and pull out the
    column data coressponding to values in query_columns. So if rids = [1] and
    query_columns = [1,0,0,1] we pull the column data contained in columns
    0 and 3 for record 1.
    To obtain a page we do:
    rid -> (page_directory) -> page_id and offset -> (page_index) -> page
    Will return a list of record objects containing key and requested column
    data.

    :param rids: list   # rids for all records to be retrieved
    :param query_columns: list  # list specifying values to be retrieved
    """

    def get_most_recent_update(self, rid, indirection_pointer, query_columns):
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
            tail_schema_id = tail_tuple[1]
            tail_schema_col = self.pages[self.page_index[tail_schema_id]]
            tail_schema = tail_schema_col.read_schema(self.num_columns,
                    tail_tuple[2])
            updated_idx = tail_schema.find('1')
            if tail_encodings[updated_idx] == '0':
                page = self.pages[self.page_index[tail_tuple[0] + updated_idx]]
                columns[updated_idx] = page.read(tail_tuple[2])
                tail_encodings = list(tail_encodings)
                tail_encodings[updated_idx] = '1'
                tail_encodings = ''.join(tail_encodings)
            if int(tail_encodings,2) & int(base_schema,2) == int(base_schema,2):
                break
            indir_id = tail_tuple[0] + self.num_columns
            indir_col = self.pages[self.page_index[indir_id]]
            indirection_pointer = indir_col.read(tail_tuple[2])
        return columns

    def get_records(self, rids, query_columns):
        records = []
        for rid in rids:
            rid_tuple = self.page_directory[rid]
            indir_id = rid_tuple[0] + self.num_columns
            indir_col = self.pages[self.page_index[indir_id]]
            indirection_pointer = indir_col.read(rid_tuple[2])
            columns = []
            if indirection_pointer == 0:
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
                        indirection_pointer, query_columns)
                for idx in range(len(query_columns)):
                    if query_columns[idx] == 1:
                        columns.append(updated_record_columns[idx])
                    else:
                        columns.append(None)
            record = Record(rid, self.key, columns)
            records.append(record)
        return records

    """
    get complete record with metadata included.used for testing purposes

    :param rids: list   # rids of all records to be retrieved
    """
    def get_full_record(self, rids):
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
            starting from the back, get schema and work back until we get the
            indirection column
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
            record = Record(rid, self.key, columns)
            records.append(record)
        return records

    """
    Used by insert function in Query class:
    As records are inserted we create and insert the appropriate entries for
    the page_directory and page_index structures.
    
    :param *columns: tuple  # all column values for record to be inserted
    """
    def insert_base_record(self, *columns):
        schema_encoding = '0' * self.num_columns
        curr_time = int(time())
        """
        if 'current' base pages is full, or if no records have been inserted,
        then create new page range containing a fresh set of base pages and
        insert record data into these base pages
        """
        if self.base_page_is_full() or self.num_records is 0:
            self.record_offset = 0
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
            # create indirection column
            indir_col = Page(len(self.pages))
            indir_col.write(0) # if val in indir_col is 0, no tail pages exist
            self.page_index[indir_col.get_id()] = len(self.pages)
            self.pages.append(indir_col)
            page_range.append(indir_col.get_id())
            # create rid column page
            rid_col = Page(len(self.pages))
            rid_col.write(rid)
            self.page_index[rid_col.get_id()] = len(self.pages)
            self.pages.append(rid_col)
            page_range.append(rid_col.get_id())
            # create time stamp column page
            time_col = Page(len(self.pages))
            time_col.write(curr_time)
            self.page_index[time_col.get_id()] = len(self.pages)
            self.pages.append(time_col)
            page_range.append(time_col.get_id())
            # create schema encoding column page
            schema_col = Page(len(self.pages))
            schema_col.write_schema(schema_encoding) 
            self.page_index[schema_col.get_id()] = len(self.pages)
            self.pages.append(schema_col)
            page_range.append(schema_col.get_id())

            self.page_directory[rid] = (page_range[0], page_range[-1],
                    self.record_offset, len(self.page_ranges))
            self.page_ranges.append(page_range)
            self.record_offset += 1
            self.num_records += 1
            # return rid of newly inserted record
            return rid
        else:
            range_idx = 0
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
            # insert metadata columns:
            range_span = len(self.page_ranges[-1]) - 1
            # insert to indirection column
            page_id = self.page_ranges[-1][range_span - INDIRECTION_COLUMN]
            page = self.pages[self.page_index[page_id]]
            page.write(0)
            # insert to rid column
            page_id = self.page_ranges[-1][range_span - RID_COLUMN]
            page = self.pages[self.page_index[page_id]]
            page.write(rid)
            # insert to timestamp column
            page_id = self.page_ranges[-1][range_span - TIMESTAMP_COLUMN]
            page = self.pages[self.page_index[page_id]]
            page.write(curr_time)
            # insert to schema encoding column
            page_id = self.page_ranges[-1][range_span - SCHEMA_ENCODING_COLUMN]
            page = self.pages[self.page_index[page_id]]
            page.write_schema(schema_encoding)
            # create a new entry in the page directory
            self.page_directory[rid] = (self.page_ranges[-1][0],
                    self.page_ranges[-1][-1], self.record_offset,
                    len(self.page_ranges)-1)
            self.num_records += 1
            self.record_offset += 1
            # return rid of newly created record
            return rid

    """
    :param page_range: list     # page range to be updated with tail page
                                      id's
    """
    def allocate_tail_pages(self, page_range):
        for i in range(self.num_columns + 4):
            page = Page(len(self.pages))
            self.page_index[page.get_id()] = len(self.pages)
            self.pages.append(page)
            page_range.append(page.get_id())

    """
    :param tail_page_range: list    # range of page id's for tail pages
    :param column_update: list  # update values
    :param indir_rid    # rid of most recent tail record
    :param rid_tuple     # tuple for record that's going to be updated
    """
    def insert_tail_record(self, tail_page_range, column_update, indir_rid, rid_tuple):
        schema_encoding = ['0']*self.num_columns
        metadata = [indir_rid]
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
        metadata.append(tail_rid)
        metadata.append(int(time()))
        metadata.append(schema_encoding)
        # write tail record metadata
        for k in range(4):
            page_id = tail_page_range[self.num_columns:][k]
            page = self.pages[self.page_index[page_id]]
            if k != 3:
                page.write(metadata[k])
            else:
                schema_encoding = ''.join(schema_encoding)
                page.write_schema(schema_encoding)
        # update indirection column of base record
        # get page containing indirection pointers and update
        indir_page_id = rid_tuple[0]+self.num_columns
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

    def update_record(self, rid, column_update):
        rid_tuple = self.page_directory[rid]
        page_range_idx = rid_tuple[3]
        page_range = self.page_ranges[page_range_idx]
        
        """
        if no tail pages have been allocated for this page range, then
        allocate some. Clearly, if page range containing the record to be
        updated is only large enough to accomodate the base pages and meta
        data pages, then no tail pages have been allocated
        """
        if len(page_range) == self.num_columns + 4:
            self.allocate_tail_pages(page_range)
        else:
            """ 
            page ids for tail pages are always inserted into page range after
            ids for base pages (simply due to order in which they are created).
            get a physical tail page via last page id in page range
            """
            tail_page = self.pages[self.page_index[page_range[-1]]]
            """
            A given set of tail records will span multiple physical pages, but
            since alll values associated with a record will occupy the same
            amount of space (64 bits), each physical tail page will have the
            same capacity. Hence, if one tail page is full, all tail pages for
            a tail record are full
            """
            if tail_page.has_capacity() == False:
                self.allocate_tail_pages(page_range)

        indir_col = self.pages[self.page_index[rid_tuple[0]+self.num_columns]]
        indir_rid = indir_col.read(rid_tuple[2])
        tail_page_range = page_range[-(self.num_columns + 4):]
        self.insert_tail_record(tail_page_range, column_update, indir_rid, rid_tuple)

    def __merge(self):
        pass

