import os
import pymongo
from pymongo import MongoClient, errors
import csv
import time
import re
import datetime

years = [2019, 2020]
client = MongoClient()

class Connect(object):
    @staticmethod    
    def get_connection():
        return MongoClient("mongodb://$[username]:$[password]@$[hostlist]/$[database]?authSource=$[authSource]")   

db = client.lab5

def insert():
    batch_size = 1000 
    file_names = [r"Odata2019File.csv", r"Odata2020File.csv"]
    for j in range(2):
        file_name, year = file_names[j], years[j]
        with open(file_name, "r", encoding="cp1251") as csv_file:
            print(f"{file_name} -- на опрацюванні...\n")
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            i = 0 
            batches_num = 0 
            document_bundle = []
            num_inserted = db.inserted_docs.find_one({"year": year})
            if num_inserted == None:
                num_inserted = 0
            else:
                num_inserted = num_inserted["num_docs"]
                print(f"Пропускаємо {num_inserted} документів...")
            for row in csv_reader:
                if batches_num * batch_size + i < num_inserted:
                    i += 1
                    if i == batch_size:
                        i = 0
                        batches_num += 1
                    continue
                document = row
                document['year'] = year
                document_bundle.append(document)
                i += 1
                if i == batch_size:
                    i = 0
                    batches_num += 1
                    db.collection_zno_data.insert_many(document_bundle)
                    document_bundle = []
                    if batches_num == 1:
                        db.inserted_docs.insert_one({"num_docs": batch_size, "year": year})
                    else:
                        db.inserted_docs.update_one({
                            "year": year, "num_docs": (batches_num - 1) * batch_size}, 
                            {"$inc": {
                                "num_docs": batch_size
                            }  })
            if i != 0 and document_bundle:
                db.inserted_docs.update_one({
                    "year": year, "num_docs": batches_num * batch_size}, 
                    {"$inc": {
                        "num_docs": i
                    }  })
                db.collection_zno_data.insert_many(document_bundle)


if __name__ == "__main__":
    time.sleep(10)
    time1 = time.time()
    insert()
    logs = time.time()-time1
    with open('logs_time.txt', 'w') as file:
        file.write(f'Тривалість записування даних {logs}')



