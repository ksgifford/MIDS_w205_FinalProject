from pandas import *
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres@localhost:5432/final_project')

df = read_sql_query('select * from "bus_stop_seattle"',con=engine)

# show top 5 entries
print(df.head())
print("--------------------")
# grab individual columns
print(df.stop_id.ix[2])
print("--------------------")
# filtering
print(df[df.stop_id == 10350])
