from pymongo import *  # Import everything from the original pymongo module
from pymongo import MongoClient as RealMongoClient  # Import the real MongoClient

# Custom MongoClient wrapper
class MongoClient(RealMongoClient):
    def __init__(self, *args, **kwargs):
        """
        Extend the original MongoClient initialization.
        Pass all arguments to the real MongoClient.
        """
        super().__init__(*args, **kwargs)

    def get_database_names(self):
        """Custom method: Get a list of all database names."""
        return self.list_database_names()

    def get_collection_names(self, database_name):
        """Custom method: Get a list of all collection names in a database."""
        database = self[database_name]
        return database.list_collection_names()

    # Add any other truly custom functionality here as needed


# Re-export everything from pymongo and replace MongoClient
__all__ = [name for name in dir() if not name.startswith("_")]
