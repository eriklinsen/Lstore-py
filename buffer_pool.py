from page import Page
import pathlib

class BufferPool:
    
    def __init__(self, size):
        self.limit = size
        self.pages = []
        self.table_index = {}
        # self.dirty_map = {}
        # self.pinned_map = {}

    def get_page(self, table_name, page_id):
        try:
            sub_directory = self.table_index[table_name]
        except KeyError:
            self.table_index[table_name] = {}
            sub_directory = self.table_index[table_name]

        try:
            location = sub_directory[page_id]
            return self.pages[location]
        except KeyError:
            if len(self.pages) < self.limit:
                sub_directory[page_id] = len(self.pages)
                location = sub_directory[page_id]
                self.fetch(table_name, page_id, None)
                return self.pages[location]
            # else:
            #   location = self.evict()
        pass

    def allocate(self, table_name, num_pages):
        path = table_name+'/page_file'
        f = open(path, mode='ab')
        page_bytes = bytearray(4096*num_pages)
        f.write(page_bytes)
        f.close()

    def fetch(self, table_name, page_id, location):
        path = table_name+'/page_file'
        f = open(path, mode='rb+')
        f.seek(4096*page_id)
        read_data = f.read(4096)
        page = Page(page_id)
        page.load_data(read_data)
        if location == None:
            self.pages.append(page)
        else:
            self.pages[location] = page
        f.close()

    def evict(self):
        pass

    def write_page(self, page, page_id, table_name):
        path = table_name+'/page_file'
        f = open(path, mode='rb+')
        f.seek(4096*page_id)
        f.write(page.data)
        f.close

    def flush(self):
        for table in self.table_index:
            sub_directory = self.table_index[table]
            for page_id in sub_directory:
                location = sub_directory[page_id]
                page = self.pages[location]
                if page.dirty:
                    self.write_page(page, page_id, table) 
