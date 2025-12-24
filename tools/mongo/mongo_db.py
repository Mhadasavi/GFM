import logging

from pymongo.errors import ServerSelectionTimeoutError

from tools.mongo.config import MONGO_DB_NAME
from tools.mongo.mongo_client import MongoConnection

logger = logging.getLogger(__name__)


class MongoDB:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        try:
            self.client = MongoConnection.get_client()
            self.db = self.client[MONGO_DB_NAME]
        except ServerSelectionTimeoutError as e:
            logger.error("Failed to initialize MongoDB")
            logger.error("Root cause: %s", e)
            raise

    def get_collection(self, name: str):
        if self.db is None:
            raise ConnectionError("MongoDB database is not initialized.")
        return self.db[name]
