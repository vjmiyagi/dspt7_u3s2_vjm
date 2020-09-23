import os
import json
from pdb import set_trace as breakpoint
from dotenv import load_dotenv
import pymongo
import pandas as pd 


load_dotenv()


MONGO_USER = os.getenv("MONGO_USER", default="OOPS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOOPS")
MONGO_CLUSTER = os.getenv("MONGO_CLUSTER", default="OOOPS")

uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTER}?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)
# print('URI:', uri)
print()


# Set the DB to Analytics
analytics_db = client.sample_analytics
print(analytics_db.list_collection_names())


# Access a specific collection
transactions = analytics_db.transactions
t = transactions.count_documents({})
print(f"Transactions # : {t}")
# print(transactions.count_documents({}))

# How many customers have more than 50 transactions
x = transactions.count_documents({'transaction_count': {'$gt': 50}})
print(f"Customers with more than 50 transactions: {x}") 
# print(transactions.count_documents({'transaction_count': {'$gt': 50}}))


# Get all the customers into a Pandas DataFrame
customers = analytics_db.customers
all_customers = customers.find()
df = pd.DataFrame(all_customers)
print(df.shape)
print(df.head())


# Illustrating problem with Mongo
customers.insert_one({'full_name': 'VJ Miyagi'})
all_customers = customers.find()
df = pd.DataFrame(all_customers)
print(df.shape)
print(df.tail())
print()


"""
Write JSON Data from RPG DB to MongoDB
"""
with open('m3/test_data_json.txt') as json_file:
    rpg_data = json.load(json_file)


# Create an rpg_data database
my_db = client.rpg_data

# Create a characters collection in the rpg_data DB
character_table = my_db.characters

# Insert the JSON data into characters collection
character_table.insert_many(rpg_data)
x = character_table.count_documents({})
print(f"Characters in rpg DB: {x}")
