from src.cloud_io import MongoIo
from src.constants import MONGO_DATABASE_NAME
from src.exception import customException
import os, sys



def fetch_product_names_from_cloud():
    try:
        mongo = MongoIo()
        collection_names = mongo.mongo_ins._mongo_operation__connect_database.list_collection_names()
        return [collection_name.replace('_', ' ')
                for collection_name in collection_names]


    except Exception as e:
        raise customException(e, sys)