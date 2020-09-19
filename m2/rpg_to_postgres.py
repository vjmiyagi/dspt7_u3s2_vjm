import os
import os.path
import psycopg2
import sqlite3
import pdb
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())


"""
SETUP ACCESS TO ELEPHANT SQL"""
rpg_Name = os.getenv("rpg_Name")
rpg_User = os.getenv("rpg_User")
rpg_PW = os.getenv("rpg_PW")
rpg_host = os.getenv("rpg_host")


"""
Connect to ElephantSQL-hosted PostgreSQL"""
conn = psycopg2.connect(dbname=rpg_Name,
                        user=rpg_User,
                        password=rpg_PW,
                        host=rpg_host)


### A "cursor", a structure to iterate over db records to perform queries
cur = conn.cursor()

"""
CONNECT to SQLITE3 DB"""
sl_conn = sqlite3.connect("rpg_db.sqlite3")
sl_cursor = sl_conn.cursor()
characters = sl_cursor.execute('SELECT * FROM charactercreator_character').fetchall()
print(characters)


"""
Create Character Table in PostGRES"""
q = '''
CREATE TABLE IF NOT EXISTS rpg_characters (
    character_id SERIAL PRIMARY KEY,
	name VARCHAR(30),
	level INT,
	exp INT,
	hp INT,
	strength INT, 
	intelligence INT,
	dexterity INT,
	wisdom INT
)
'''
cur.execute(q)
conn.commit()

"""
Insert Character Data in POSTGRES"""
for character in characters:
    q = f''' INSERT INTO rpg_characters
        (character_id, name, level, exp, hp, strength, intelligence, dexterity, wisdom) VALUES
        {character}
    '''
    cur.execute(q)
conn.commit()


"""
Quick hop over to ElephantSQL, ran a query and viola!  Take a peak at 
ElephantSQL_rpg_characters.png
"""


# Tidy up
cur.close()
conn.close()
sl_conn.close()
sl_cursor.close()