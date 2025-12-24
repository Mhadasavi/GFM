import logging

from pymongo import MongoClient, errors
from tenacity import retry, stop_after_attempt, wait_fixed

from tools.mongo.config import MONGO_URI

logger = logging.getLogger(__name__)
MAX_RETRIES = 5
WAIT_SECONDS = 2


class MongoConnection:
    _client = None

    @staticmethod
    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_fixed(WAIT_SECONDS),
        reraise=True,
        before=lambda retry_state: logger.info(
            "Connecting to MongoDB (attempt %s/%s)",
            retry_state.attempt_number,
            MAX_RETRIES,
        ),
    )
    def get_client():
        if MongoConnection._client is None:
            try:
                client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
                client.server_info()
                MongoConnection._client = client
                logger.info("Established MongoDB connection...")
            except errors.ServerSelectionTimeoutError:
                logger.warning("MongoDB unavailable, retrying...")
                raise
            except Exception:
                logger.exception("Unexpected error while connecting to MongoDB")
                raise
            return MongoConnection._client
