from db import Database
from query import Query
import threading

db = Database()
db.open('ALPHA')


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
records = query_alpha.select(15, 0, [1,1,1])
records = query_alpha.select(2, 0, [1,1,1])

print('result of select after 2nd update:')
for record in records:
    print(record.columns)

db.drop_table(table_alpha)

# TEST
table_beta = db.create_table('beta', 4, 0)
query_beta = Query(table_beta)

for i in range(511):
    query_beta.insert(i*5, i, i, i)

records = query_beta.select(500, 0, [1,1,1,1])
print('should be: [500, 100, 100, 100]')
for record in records:
    print(record.columns)

for i in range(1000):
    query_beta.update(25, None, None, None, i)

for i in range(200):
    records = query_beta.select(25, 0, [1,1,1,1])
    for record in records:
        print('result of select on 25')
        print(record.columns)


