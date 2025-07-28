import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import certifi
ca=certifi.where()

import pandas as pd
import numpy as np 
import pymongo
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def csv_to_json_convertor(self,file_path):
        try:
            data=pd.read_csv(file_path)
            data.reset_index(drop=True,inplace=True)
            #creating every record --> json, inevitably making a list of key value pairs out of all the records
            records=list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def clear_collection(self, database, collection):
        try:
            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[database]
            self.collection = self.database[collection]
            result = self.collection.delete_many({})
            print(f"Deleted {result.deleted_count} documents from collection")
            return result.deleted_count
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    #initializing object with constructor
    def insert_data_mongodb(self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]

            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return(len(self.records))
        except Exception as e:
            raise NetworkSecurityException(e,sys)

if __name__ == '__main__':
    FILE_PATH = "Network_Data/phisingData.csv"
    DATABASE = "sghosh"
    Collection = "NetworkData"
    networkobj = NetworkDataExtract()
    
    # Clear the collection first
    print("Clearing existing data from MongoDB...")
    deleted_count = networkobj.clear_collection(DATABASE, Collection)
    print(f"Deleted {deleted_count} records from MongoDB")
    
    # Re-push the data with correct column names
    print("Re-pushing data with correct column names...")
    records = networkobj.csv_to_json_convertor(file_path = FILE_PATH)
    print(f"Converted {len(records)} records from CSV")
    no_of_records= networkobj.insert_data_mongodb(records,DATABASE,Collection)
    print(f"Successfully inserted {no_of_records} records into MongoDB") 