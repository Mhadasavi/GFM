import logging

import pandas as pd
from numpy.ma.extras import unique

from tools.mongo.mongo_db import MongoDB


class MongoDbWriter:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def write(self, input_path: str, collection_name: str):
        df = pd.read_csv(input_path)
        mongo = MongoDB()
        collection = mongo.get_collection(collection_name)
        collection.create_index([("meta_row_id", 1)], unique=True)

        # Loop through and insert
        for record in df.to_dict(orient="records"):
            collection.update_one(
                {"meta_row_id": record["meta_row_id"]}, {"$set": record}, upsert=True
            )
        self.logger.info("Data successfully written to DB.")
