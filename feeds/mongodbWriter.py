import pandas as pd

from tools.mongo.mongo_db import MongoDB


class MongoDbWriter:
    def write(self, input_path):
        df = pd.read_csv(input_path)
        mongo = MongoDB()
        collection = mongo.get_collection("metaDataCollection")

        # Loop through and insert
        for record in df.to_dict(orient="records"):
            collection.insert_one(record)
