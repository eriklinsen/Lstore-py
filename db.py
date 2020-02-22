from table import Table
import pathlib
import pickle

class RIDspace():

    def __init__(self):
        self.rid_block = (1,512)
        self.tail_rid = pow(2,60)

    def assign_space(self):
        assigned_block = self.rid_block
        new_block = ((self.rid_block[0]+512),(self.rid_block[1]+512))
        self.rid_block = new_block
        return assigned_block

    def assign_tail_rid(self):
        assigned_rid = self.tail_rid
        self.tail_rid = self.tail_rid - 1
        return assigned_rid


class Database():

    def __init__(self):
        self.tables = []
        self.table_data = []
        self.table_map = {}
        self.rid_space = RIDspace()
        self.root_path = None

    """
    If open() is invoked, then the following directory will be constructed:
    
    root_path
    |___________tables
    |
    |___________rid_space

    where tables contains the pickled table_data list, and rid_space contains
    the pickled rid space allocator.
    """
    
    def init_dir(self, root_path):
        pathlib.Path(root_path+'/').mkdir(parents=True, exist_ok=True)
        f = open(root_path+'/tables', mode='wb')
        f.close
        f = open(root_path+'/rid_space', mode='wb')
        f.close
        self.root_path = root_path
        
    def open(self, root_path):
        try:
            f1 = open(root_path+'/tables', mode='rb')
            f2 = open(root_path+'/rid_space', mode='rb')
            self.table_data = pickle.load(f1)
            self.rid_space = pickle.load(f2)
            self.root_path = root_path
            f1.close()
            f2.close()
            table_data = self.table_data[:]
            print(table_data)
            for bundle in table_data:
                table = self.create_table(bundle[0], bundle[1], bundle[2])

        except FileNotFoundError:
            self.init_dir(root_path)

    def close(self):
        try:
            open(self.root_path + '/tables', mode='rb').close()
            open(self.root_path + '/rid_space', mode='rb').close()

            f = open(self.root_path + '/tables', mode='wb')
            pickle.dump(self.table_data, f)
            f.close()
            f = open(self.root_path + '/rid_space', mode='wb')
            pickle.dump(self.rid_space, f)
            f.close()
            for table in self.tables:
                table.close_table()
        except FileNotFoundError:
            print('db close error: cannot close without ever having opened')


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.rid_space)
        self.tables.append(table)
        self.table_data.append((name, num_columns, key))
        table.open_table()
        self.table_map[name] = self.tables.index(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        try:
            i = self.table_map[name]
            self.tables.pop(i)
        except KeyError:
            pass
    
    def get_table(self, name):
        try:
            i = self.table_map[name]
            return self.tables[i]
        except KeyError:
            print('get table error: requested table does not exist')

