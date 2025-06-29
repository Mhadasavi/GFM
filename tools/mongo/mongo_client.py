from pymongo import MongoClient, errors
from tenacity import retry, stop_after_attempt, wait_fixed

from tools.mongo.config import MONGO_URI


class MongoConnection:
    _client = None

    @staticmethod
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def get_client():
        if MongoConnection._client is None:
            try:
                client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
                client.server_info()
                MongoConnection._client = client
            except errors.ServerSelectionTimeoutError as e:
                print("🔁 Retrying MongoDB connection...")
                raise e
            except Exception as e:
                print("❌ Unexpected error while connecting to MongoDB:", str(e))
                raise e
            return MongoConnection._client
