from tools.mongo.mongo_db import MongoDB


def main():
    try:
        mongo = MongoDB()
        users = mongo.get_collection("users")

        # Insert example
        result = users.insert_one({"name": "Utkarsh", "role": "Data Engineer"})
        print(f"Inserted ID: {result.inserted_id}")

        # Read example
        print("Users in DB:")
        for doc in users.find():
            print(doc)

    except Exception as e:
        print("❌ Mongo operation failed:", str(e))


if __name__ == "__main__":
    main()
