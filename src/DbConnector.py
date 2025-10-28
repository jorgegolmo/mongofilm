# DbConnector.py
from pymongo import MongoClient
from pathlib import Path
from dotenv import load_dotenv
from os import getenv

# load .env from project root (parent of this file)
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

class DbConnector:
    def __init__(self,
                 HOST=getenv("HOSTNAME") or "127.0.0.1",
                 DATABASE=getenv("DATABASE") or "mongofilm",
                 USER=getenv("USERNAME") or None,
                 PASSWORD=getenv("PASSWORD") or None,
                 PORT=getenv("PORT") or "27017"):
        # sanitize empty strings to None
        if USER == "":
            USER = None
        if PASSWORD == "":
            PASSWORD = None

        # Ensure database name is a string
        if not isinstance(DATABASE, str) or DATABASE.strip() == "":
            raise RuntimeError("DATABASE environment variable is missing or empty. Set DATABASE in .env to the name of your database (e.g. mongofilm).")

        self.host = HOST
        self.database_name = DATABASE
        self.port = PORT

        if USER and PASSWORD:
            uri = f"mongodb://{USER}:{PASSWORD}@{self.host}:{self.port}/{self.database_name}"
        else:
            uri = f"mongodb://{self.host}:{self.port}/"

        try:
            self.client = MongoClient(uri)
            # access database object
            self.db = self.client[self.database_name]
        except Exception as e:
            print("❌ ERROR: Failed to connect to db:", e)
            self.client = None
            self.db = None
            raise RuntimeError("Could not connect to MongoDB: " + str(e))

        print("✅ Connected to database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        if self.client:
            self.client.close()
            print("\n-----------------------------------------------")
            print("Connection to %s-db is closed" % self.db.name)
