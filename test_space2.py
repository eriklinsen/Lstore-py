from db import Database
from query import Query

db = Database()
db.open('LDB')


# SIMPLE TEST:
table_alpha = db.create_table('alpha', 3, 0)
query_alpha = Query(table_alpha)

query_alpha.insert(5, 11, 12)
query_alpha.insert(10, 13, 14)
query_alpha.insert(15, 16, 17)

for index in query_alpha.indexes.values():
    print('printing index via test space:')
    index.print_index()
records = query_alpha.select(15, 0, [1, 0, 1])
for record in records:
    print(record.columns)

query_alpha.update(15, None, 6, 7)
records = query_alpha.select(15, 0, [1,1,1])
for record in records:
    print(record.columns)

query_alpha.update(15, 2, None, 9)
print('attempt select on 15, should not return')
records = query_alpha.select(15, 0, [1,1,1])
records = query_alpha.select(2, 0, [1,1,1])

print('result of select after 2nd update:')
print('Should output [2,6,9]')
for record in records:
    print(record.columns)

db.drop_table('alpha')

# TEST
table_beta = db.create_table('beta', 4, 0)
query_beta = Query(table_beta)

print('now inserting into beta')
for i in range(1,10001):
    query_beta.insert(i*5, i, i, i)

records = query_beta.select(500, 0, [1,1,1,1])
print('should be: [500, 100, 100, 100]')
for record in records:
    print(record.columns)

for i in range(1000):
    query_beta.update(25, None, None, None, i)
    query_beta.update(500, None, i, None, None)

print('active threads: ',end='')
print(threading.activeCount())

records = query_beta.select(25, 0, [1,1,1,1])
print('result of select on 25 with key 0')
for record in records:
    print(record.columns)

print('active threads: ',end='')
print(threading.activeCount())

records = query_beta.select(999, 1, [1,1,1,1])
print('result of select on 999 with key 1')
for record in records:
    print(record.columns)

records = query_beta.select(500, 0, [1,1,1,1])
for record in records:
    print(record.columns)

db.close()
