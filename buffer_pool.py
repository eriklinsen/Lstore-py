from page import Page
import threading
import collections
import os

class BufferPool:
    
    def __init__(self, size):
        self.limit = size
        self.pages = []
        self.table_index = {}
        self.eviction_queue = collections.OrderedDict()
        self.buffer_lock = threading.Lock()

    def get_page(self, table_name, page_id):
        self.buffer_lock.acquire()
        if table_name in self.table_index:
            sub_directory = self.table_index[table_name]
        else:
            self.table_index[table_name] = {}
            sub_directory = self.table_index[table_name]

        if page_id in sub_directory:
            location = sub_directory[page_id]
            if (table_name, page_id) in self.eviction_queue:
                self.eviction_queue.pop((table_name, page_id))
            self.eviction_queue[(table_name, page_id)] = None
            self.buffer_lock.release()
            return self.pages[location]
        else:
            if len(self.pages) < self.limit:
                location = self.fetch(table_name, page_id, None) 
                if (table_name, page_id) in self.eviction_queue:
                    self.eviction_queue.pop((table_name, page_id))
                self.eviction_queue[(table_name, page_id)] = None
                sub_directory[page_id] = location
                self.buffer_lock.release()
                return self.pages[location]
            else:
               location = self.evict()
               self.fetch(table_name, page_id, location)
               sub_directory[page_id] = location
               if (table_name, page_id) in self.eviction_queue:
                   self.eviction_queue.pop((table_name, page_id))
               self.eviction_queue[(table_name, page_id)] = None
               self.buffer_lock.release()
               return self.pages[location]

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
        location = None
        for e in self.eviction_queue:
            page = self.pages[self.table_index[e[0]][e[1]]]
            if page.pinned == 0:
                self.eviction_queue.pop(e)
                if page.dirty:
                    self.write_page(page, e[1], e[0])
                del self.table_index[e[0]][e[1]]
                location = self.pages.index(page)
                break
            else:
                continue
        if location == None:
            print('fatal buffer pool error: all pages are pinned')
            os._exit(1)
        return location

    def write_page(self, page, page_id, table_name):
        path = table_name+'/page_file'
        try:
            f = open(path, mode='rb+')
            f.seek(4096*page_id)
            f.write(page.data)
            f.close
        except FileNotFoundError:
            pass

    def flush(self):
        for table in self.table_index:
            sub_directory = self.table_index[table]
            for page_id in sub_directory:
                location = sub_directory[page_id]
                page = self.pages[location]
                if page.dirty:
                    self.write_page(page, page_id, table) 
