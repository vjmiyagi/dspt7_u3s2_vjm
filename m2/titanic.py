import os
import psycopg2
from psycopg2.extras import execute_values
import sqlite3
import pandas as pd 
import numpy as np
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")


"""

Connect to ElephantSQL-hosted PostgreSQL

"""
conn = psycopg2.connect(dbname=DB_NAME,
                        user=DB_USER,
                        password=DB_PASS,
                        host=DB_HOST)
### A "cursor", a structure to iterate over db records to perform queries
cur = conn.cursor()


# Get Titanic dataset into pandas DataFrame

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "titanic.csv")

df = pd.read_csv('titanic.csv')
print(df.shape)
print(df.head())
print(df.columns)
df.rename(columns={"Siblings/Spouses Aboard": "SiblingSpousesAboard", "Parents/Children Aboard": "ParentsChildrenAboard"}, inplace=True)
print(df.columns)

df['Survived'] = df['Survived'].values.astype(bool)
df = df.astype("object") 


print(df.head())


# Convert DataFrame to list

pax_list = list(df.to_records(index=False))


"""

Create Titanic Table in PostGRES

"""

q = "DROP TABLE IF EXISTS titanic;"
cur.execute(q)
conn.commit()

query = '''
CREATE TABLE IF NOT EXISTS titanic (
    Survived BOOLEAN,
    Pclass INT,
    Name VARCHAR(81),
    Sex VARCHAR(6),
    Age INT,
    MateSibs INT,
    FolksKids INT,
    Fare DECIMAL
)
'''

cur.execute(query)
conn.commit()
pax = 0

insrt_sql = f'''INSERT INTO titanic
    (Survived, Pclass, Name, Sex, Age, MateSibs, FolksKids, Fare) VALUES %s
'''
execute_values(cur, insrt_sql, pax_list)
conn.commit()

# Tidy up
cur.close()
conn.close()