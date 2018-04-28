# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# How to set up postgres server on RHEL
# to use it with airflow 

"""
1/ install postgres



import psycopg2
#%% 
conn_string = "dbname=postgres user=airflow password=airflow"
conn = psycopg2.connect(conn_string)

#%%

import sqlalchemy

conn_string2 = "postgresql+psycopg2://airflow:airflow@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string2)
print(engine.table_names())
# %%
cur = conn.cursor()
res = cur.execute("Select count(*) cnt_1 from airflow.test;").fetchall()

print(res)
# %% 