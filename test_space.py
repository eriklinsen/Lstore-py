from table import *
from query import Query
from time import process_time

class RIDspace():

    def __init__(self):
        self.rid_block = (1,512)
    def assign_space(self):
        assigned_block = self.rid_block
        new_block = ((self.rid_block[0]+512),(self.rid_block[1]+512))
        self.rid_block = new_block
        return assigned_block

def dump_pages(table):
    for page in table.pages:
        page.print_data()

def show_records(records):
    if records is [] or records is None:
        print('no records found')
    else:
        for record in records:
            print('RID: KEY: COLUMNS:')
            print(record.rid, end='    ')
            print(record.key, end='    ')
            print(record.columns)


rid_alloc = RIDspace()

# simple insert and select test:

table1 = Table('T1',4,0,rid_alloc)
query1 = Query(table1)

query1.insert(5,  9,        73456, 987)
query1.insert(10, 23,       872,   1)
query1.insert(1,  887,      23,    872)
query1.insert(15, 92928783, 4,     8)

print('page directory for table1:')
print(table1.page_directory)
print('page index for for table1:')
print(table1.page_index)
print('page ranges for table1:')
print(table1.page_ranges)
print('index over key for table1:')
print(query1.idx.idx)

print('===simple select test===')
print('should output a record with column values: [9,73456]')
records_null = query1.select(5,[0,1,1,0])
show_records(records_null)
print('===simple sum test. should print 30, 31 and 988===')
# should print 30:
print(query1.sum(5,15,0))
#should print 31:
print(query1.sum(0,15,0))
# should print 988:
print(query1.sum(3,10,3))

print('===output a single record with metadata===')
# full record test:
r1 = query1.idx.locate(15)
full_r1 = table1.get_full_record(r1)
show_records(full_r1)

print('===perform single update of record===')
query1.update(15,[None,2,None,None])
query1.update(15,[None,None,13,None])
full_r1 = table1.get_full_record(r1)
show_records(full_r1)

print('===perform 10k inserts on table2===')
# bigger insert and select test:
table2 = Table('T2', 5, 0, rid_alloc)
query2 = Query(table2)
insert_time_0 = process_time()
for i in range(0, 10000):
    query2.insert(906659671+i, 93, i, i, i)
    #if i%512 == 1:
     #   print(rid_alloc.rid_block)
insert_time_1 = process_time()
print('Inserting 10k records took: \t\t\t', insert_time_1 - insert_time_0)
#print('table2 info:')
#print(table2.page_directory)
print('===performing select on table2===')
# should return record with rid = 1113, key = 906660271, columns = [key,
# 93, 600, 600, 600]
records_one = query2.select(906659671+600, [1,1,1,1,1])
records_two = query2.select(906659671+5000, [1,1,1,1,1])
show_records(records_one)
show_records(records_two)

print('===performing sum on table2===')
# sums up 0 + 1 + 2 + 3 + .... + 10. Should be 55
print(query2.sum(906659671, 906659671+10, 2))
# sums up 0 + 1 + 2 + ..... 2500. Should be 3126250
print(query2.sum(906659671, 906659671+2500, 2))


