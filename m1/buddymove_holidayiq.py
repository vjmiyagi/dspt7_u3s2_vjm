import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine


# Read csv into pandas DataFrame
df = pd.read_csv('buddymove_holidayiq.csv')
print(df.head())
print()
df.rename(columns={"User Id": "UserID"}, inplace=True)
print(df.head())
print()


# Create a connection to sqlite DB
conn = sqlite3.connect("buddy.db")
curs = conn.cursor()
q = """
    CREATE TABLE IF NOT EXISTS users (UserID text, Sports number,
    Religious number, Nature number,
    Theatre number, Shoppping number,
    Picnic number)
    """
curs.execute(q)
conn.commit()


# Fill sqlite3 database with pandas df contents
df.to_sql('users', conn, if_exists='replace', index=False)


# Define function for queries
def askme(query):
    r = curs.execute(query).fetchall()
    r = r[0]
    return r


# Get row count from new sqlite3 table
t = "Table rows number: "
q = """
    SELECT
        count(distinct UserID)
    FROM
        users;"""
r = askme(q)
conn.commit()
print(t, r[0])


"""
    How many users who reviewed at least 100 Nature
    in the category also reviewed at least 100 in the
    Shopping category?
"""
t = "Users that reviewed at least 100 Nature and Shopping: "
q = """
    SELECT
        count(distinct UserID)
    FROM
        users
    WHERE Nature > 99
    AND Shopping > 99;"""
r = askme(q)
print(t, r[0])


"""
    Get the averages for each genre
"""
t = "Genre averages are: "
q = """
    SELECT
        round(AVG(Sports),2) as Sports
        ,round(AVG(Religious),2) as Religious
        ,round(AVG(Nature),2) as Nature
        ,round(AVG(Theatre),2) as Theatre
        ,round(AVG(Shopping),2) as Shopping
        ,round(AVG(Picnic),2) as Picnic
    FROM
        users;"""
r = curs.execute(q).fetchall()
r = r[0]
print()


print("Average review per category:")
print(f"Sports:                {r[0]:0.2f}")
print(f"Religious:            {r[1]:0.2f}")
print(f"Nature:               {r[2]:0.2f}")
print(f"Theatre:              {r[3]:0.2f}")
print(f"Shopping:             {r[4]:0.2f}")
print(f"Picnic:               {r[5]:0.2f}")
print()


# Tidy up
curs.close()
conn.close()
