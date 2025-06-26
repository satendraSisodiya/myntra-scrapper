import sys
import pandas as pd
from src.constants import *
from src.exception import customException
from database_connect import mongo_operation as mongo

class MongoIo:
    mongo_ins = None

    def __init__(self):
        if MongoIo.mongo_ins is None:
            mongo_db_url = "mongodb+srv://sahil:q1w2e3r4t5y6@cluster0.ooty7p8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
            if mongo_db_url is None:
                raise Exception(f"Environment key: {MONGODB_URL_KEY} is not set")
            MongoIo.mongo_ins = mongo(client_url = mongo_db_url,
                                      database_name = MONGO_DATABASE_NAME)
        self.mongo_ins = MongoIo.mongo_ins      

    def store_reviews(self, product_name:str, reviews:pd.DataFrame):
        try: 
            collection_name = product_name.replace(" ", "_")
            self.mongo_ins.bulk_insert(reviews, collection_name)
        except Exception as e:
            raise customException(e, sys)

    def get_reviews(self, product_name:str):
        try:
            data = self.mongo_ins.find(
                collection_name = product_name.replace(" ", "_")
                )
        except Exception as e:
            raise customException(e, sys)         
