from tools.mongo.config import MONGO_DB_NAME
from tools.mongo.mongo_client import MongoConnection


class MongoDB:
    def __init__(self):
        try:
            self.client = MongoConnection.get_client()
            self.db = self.client[MONGO_DB_NAME]
        except Exception as e:
            print("🚫 Failed to initialize MongoDB:", e)
            raise

    def get_collection(self, name: str):
        if self.db is None:
            raise ConnectionError("MongoDB database is not initialized.")
        return self.db[name]
