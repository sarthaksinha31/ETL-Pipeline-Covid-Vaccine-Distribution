from pymongo import MongoClient
import requests

def upload_vaccine_distribution(connection_string):
    print("Uploading Vaccine Distributions to Mongo DB")
    client = MongoClient(connection_string)
    db = client['dap_project']
    collection = db['vaccine_distribution']
    data = requests.get('https://data.cdc.gov/resource/unsk-b7fc.json?$limit=36952').json()
    x = collection.insert_many(data)
    print(db.list_collection_names())
    print("MongoDB updated with vaccine distribution data where collection name is - vaccine_distribution")
