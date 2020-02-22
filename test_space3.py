from db import Database
from query import Query
from time import process_time

db = Database()
db.open('LDB')

table = db.get_table('beta')
q_beta = Query(table)

records = []
select_time_0 = process_time()
for key in range(1,10001):
    records = records + q_beta.select(key*5, 0, [1,1,1,1])
select_time_1 = process_time()
print('Time: \t\t\t', select_time_1 - select_time_0)

for record in records:
    print(record.columns)

time_0 = process_time()
print(q_beta.sum(0,50000,0))
time_1 = process_time()
print('Time: \t\t\t', time_1 - time_0)

db.close()



