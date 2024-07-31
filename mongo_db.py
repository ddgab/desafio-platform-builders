from pymongo import MongoClient
from pymongo.errors import PyMongoError


connect = "mongodb+srv://teste_dados_leitura:o7c4Cc8NDeXYbAMH@mongodbtestebuilders.vuzqjs5.mongodb.net/?retryWrites=true&w=majority"

class MongoDB:
    def __init__(self):
        try:
            self.client = MongoClient(connect)
            print(f"Sucess Connection MongoDB")
        except PyMongoError as e:
            print(f"Erro Connection MongoDB: {e}")

    def __del__(self):
        if self.client:
            self.client.close()
            print("Connection MongoDB Closed")

    def define_db(self, collection_name, database):
        try:
            self.db = self.client[database]
            collection = self.db[collection_name]
            return collection
        except PyMongoError as e:
            error = f"{type(e).__module__}:{type(e).__name__}: {str(e).rstrip()}"
            print(error)
            return False