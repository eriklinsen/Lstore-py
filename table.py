from page import Page
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
        set of 4 base pages allocated for these records (say thay have ids
        1,2,3, and 4). Then page_ranges would look like: [[1,2,3,4]].
        Now suppose we immediately insert another 100 records. The 'current'
        page range is  full. So, we allocated a new set of base pages, write
        the data to these pages and page_ranges looks like this:
        [[1,2,3,4][5,6,7,8]]
        """
        self.page_ranges = []
        """
        the page directory maps each rid for each record to a python tuple.
        that python tuple contains the range of page id's for all pages that
        contain the record data. It also contains the record offset.
        For example, suppose we have a table of 4 columns and there's a record
        within this table that has an rid of 1. Also suppose that this record
        spans base pages with id's 1,2,3,4. Moreover, suppose that each of
        the column values for the record are located in the first 8 bytes of
        each of the physical base pages. Then the entry for this rid would look
        like: 1 -> (1,4,0)
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
            if page.has_capacity() is False:
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
    """
    def get_records(self, rids, query_columns):
        records = []
        for rid in rids:
            rid_tuple = self.page_directory[rid]
            columns = []
            for idx in range(len(query_columns)):
                if query_columns[idx] is 1:
                    page_id = rid_tuple[0] + idx
                    page = self.pages[self.page_index[page_id]]
                    column_val = page.read(rid_tuple[2])
                    columns.append(column_val)
            record = Record(rid, self.key, columns)
            records.append(record)
        return records
    """
    get complete record with metadata included:
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
            for i in range(4):
                page_id = rid_tuple[1] - i
                page = self.pages[self.page_index[page_id]] 
                if i != SCHEMA_ENCODING_COLUMN:
                    column_val = page.read(rid_tuple[2])
                else:
                    column_val = page.read_schema(rid_tuple[2],
                            self.num_columns)
                columns.append(column_val)
            record = Record(rid, self.key, columns)
            records.append(record)
        return records

    """
    Used by insert function in Query class:
    As records are inserted we create and insert the appropriate entries for
    the page_directory and page_index structures.
    """
    def insert_record(self, *columns):
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
                    self.record_offset)
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
                    self.page_ranges[-1][-1], self.record_offset)
            self.num_records += 1
            self.record_offset += 1
            # return rid of newly created record
            return rid

    def __merge(self):
        pass

