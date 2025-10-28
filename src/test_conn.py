from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("HOSTNAME", "127.0.0.1")
db_name = os.getenv("DATABASE", "mongofilm")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

if username and password:
    # connect with authentication
    c = MongoClient(host, 27017, username=username, password=password)
else:
    # connect without authentication
    c = MongoClient(host, 27017)

print("✅ Connected! Databases:", c.list_database_names())
