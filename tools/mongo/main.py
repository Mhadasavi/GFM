from tools.mongo.mongo_db import MongoDB


def main():
    try:
        mongo = MongoDB()
        users = mongo.get_collection("metaDataCollection")

        # Insert example
        # result = users.insert_one({"name": "Utkarsh", "role": "Data Engineer"})
        # print(f"Inserted ID: {result.inserted_id}")

        # Read example
        print("Users in DB:")
        count = 0
        for doc in users.find():
            count = count + 1
            print(f"{count}:{doc}")

    except Exception as e:
        print("❌ Mongo operation failed:", str(e))


if __name__ == "__main__":
    main()
