"""Use sqlite3 to load and write queries to
explore the data, and answer questions"""


# IMPORTS
import os
import sqlite3
import pandas as pd

"""
Connect to the sqlite database"""
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")
conn = sqlite3.connect(DB_FILEPATH)
curs = conn.cursor()


# This method runs a query and returns the results
def askme(query):
    r = curs.execute(query).fetchall()
    r = r[0]
    return r


print()


"""
How many total Characters are there?"""

q = """
    SELECT
        COUNT(DISTINCT character_id)
    FROM
        charactercreator_character;"""
r = askme(q)
t = "Total number of characters are: "
print(t, r[0])
print()
"""
How many of each specific subclass?"""
# CLERIC:
q = """
    SELECT
        COUNT(DISTINCT character_ptr_id)
    FROM
        charactercreator_cleric;"""
r1 = askme(q)
t1 = "Cleric"


# Fighter
q = """
    SELECT
        COUNT(DISTINCT character_ptr_id)
    FROM
        charactercreator_fighter;"""
r2 = askme(q)
t2 = "Fighter"


# Thief
q = """
    SELECT
        COUNT(DISTINCT character_ptr_id)
    FROM
        charactercreator_thief;"""
r3 = askme(q)
t3 = "Thief"


# Mage
q = """
    SELECT
        COUNT(DISTINCT character_ptr_id)
    FROM
        charactercreator_mage;"""
r4 = askme(q)
t4 = "Mage"


# Necromancer
q = """
    SELECT
        COUNT(DISTINCT mage_ptr_id)
    FROM
        charactercreator_necromancer;"""
r5 = askme(q)
t5 = "Necromancer"


print(f"{t1:^10} {t2:^10} {t3:^10} {t4:^10} {t5:^10}")
print(f"{r1[0]:^10} {r2[0]:^10} {r3[0]:^10} {r4[0]:^10} {r5[0]:^10}")
print("NOTE: Necromancers are a sub-specialty of Mage")
print()

"""
How many total Items?"""
q = """
    SELECT
        COUNT(DISTINCT item_id)
    FROM
        armory_item;"""
r = askme(q)
t = "Total number of items are: "
print(t, r[0])
print()


"""
How many of the Items are weapons?
How many are not?"""

q = """
    SELECT
        COUNT(DISTINCT item_id)
    FROM
        armory_item
    WHERE
        item_id > 137;"""
r1 = askme(q)
t1 = "Weapons"
q = """
    SELECT
        COUNT(DISTINCT item_id)
    FROM
        armory_item
    WHERE
        item_id < 138;"""
r2 = askme(q)
t2 = "Non-Weapons"
print(f"{t1:^15}{t2:^15}")
print(f"{r1[0]:^15}{r2[0]:^15}")

"""
How many Items does each character have?
(Return first 20 rows)"""
q = """
    SELECT
        name as Name,
        COUNT(item_id) as Items
    FROM
        charactercreator_character c,
        charactercreator_character_inventory i
    WHERE
        c.character_id = i.character_id
    GROUP BY
        c.character_id;"""
r = curs.execute(q).fetchmany(20)
df = pd.DataFrame(r, columns=["Character", "Items"])
print(df)
print()


"""
How many Weapons does each character have?
(Return first 20 rows)"""
q = """
    SELECT
        c.name as character_name,
        count(distinct w.item_ptr_id) as weapon_count
    FROM
        charactercreator_character c
        LEFT JOIN charactercreator_character_inventory inv ON c.character_id = inv.character_id
        LEFT JOIN armory_weapon w ON w.item_ptr_id = inv.item_id
    GROUP BY
        c.character_id;"""
r = curs.execute(q).fetchmany(20)
df = pd.DataFrame(r, columns=["Character", "Weapons"])
print(df)
print()


"""
On average, how many Items does each
Character have?"""
q = """
    SELECT
        AVG(Items)
    FROM(
            SELECT
                count(DISTINCT item_id) Items
            FROM
                charactercreator_character_inventory
            Group BY
                character_id
        );"""
r = askme(q)
print(f"The average number of items each character carries is {r[0]:0.2f}.")


"""
On average, how many Weapons does each
character have?"""
q = """
    SELECT
        AVG(weapons) as average
    FROM(
            SELECT
                cci.character_id,
                COUNT(DISTINCT(aw.item_ptr_id)) as weapons
            FROM
                charactercreator_character_inventory as cci
                LEFT JOIN armory_weapon as aw on cci.item_id = aw.item_ptr_id
            GROUP BY
                character_id
    );"""
r = askme(q)
print(f"Each character carries an average of {r[0]:0.2f} weapons.")


# Tidy up
curs.close()
conn.close()
