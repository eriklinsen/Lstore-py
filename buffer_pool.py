from page import Page
import pathlib

class BufferPool:
    
    def __init__(self, size):
        self.limit = size
        self.pages = []
        self.table_index = {}
        self.eviction_queue = []

    def get_page(self, table_name, page_id):
        try:
            sub_directory = self.table_index[table_name]
        except KeyError:
            self.table_index[table_name] = {}
            sub_directory = self.table_index[table_name]

        try:
            location = sub_directory[page_id]
            self.eviction_queue.append((table_name, page_id))
            return self.pages[location]
        except KeyError:
            if len(self.pages) < self.limit:
                location = self.fetch(table_name, page_id, None)
                self.eviction_queue.append((table_name, page_id))
                sub_directory[page_id] = location
                return self.pages[location]
            else:
               location = self.evict()
               self.fetch(table_name, page_id, location)
               sub_directory[page_id] = location
               self.eviction_queue.append((table_name, page_id))
               return self.pages[location]

    def allocate(self, table_name, page_id):
        if len(self.pages) < self.limit:
            page = Page(page_id)
            self.pages.append(page)
            location = self.pages.index(page)
        else:
            location = self.evict()
            page = Page(page_id)
            self.pages[location] = page

        try:
            sub_directory = self.table_index[table_name]
        except KeyError:
            self.table_index[table_name] = {}
            sub_directory = self.table_index[table_name]
        sub_directory[page_id] = location
         
        # else:
        #   evict()
        #   page = Page(page_id)
        #   self.pages.append(page)

        #path = table_name+'/page_file'
        #f = open(path, mode='ab')
        #page_bytes = bytearray(4096*num_pages)
        #f.write(page_bytes)
        #f.close()

    def fetch(self, table_name, page_id, location):
        path = table_name+'/page_file'
        f = open(path, mode='rb+')
        f.seek(4096*page_id)
        read_data = f.read(4096)
        page = Page(page_id)
        page.load_data(read_data)
        if location == None:
            self.pages.append(page)
            location = self.pages.index(page)
        else:
            self.pages[location] = page
        f.close()
        return location

    def evict(self):
        for e in self.eviction_queue:
            page = self.pages[self.table_index[e[0]][e[1]]]
            if page.pinned == 0:
                self.eviction_queue.pop(self.eviction_queue.index(e))
                if page.dirty:
                    self.write_page(page, e[1], e[0])
                del self.table_index[e[0]][e[1]]
                location = self.pages.index(page)
                return location

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
