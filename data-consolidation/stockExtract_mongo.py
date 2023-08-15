import yfinance as yf
import json
from pymongo import MongoClient

def upload_stock_data(connection_string):
    print("Uploading Stock Market data of Vaccine Manufacturers")
    client = MongoClient(connection_string)
    db = client['dap_project']
    updated_data = []
    pfizer = yf.Ticker('PFE')
    pfizer_data = json.loads(pfizer.history(start="2019-01-01",  end="2022-11-25").reset_index().to_json(orient='records',date_format='iso'))
    for obj in pfizer_data:
        obj = {key.lower(): value for key, value in obj.items()}
        obj['company'] = 'Pfizer'
        updated_data+=[obj]
    moderna = yf.Ticker('MRNA')
    moderna_data = json.loads(moderna.history(start="2019-01-01",  end="2022-11-25").reset_index().to_json(orient='records',date_format='iso'))
    for obj in moderna_data:
        obj = {key.lower(): value for key, value in obj.items()}
        obj['company'] = 'Moderna'
        updated_data+=[obj]

    jannsen = yf.Ticker('JNJ')
    jannsen_data = json.loads(jannsen.history(start="2019-01-01",  end="2022-11-25").reset_index().to_json(orient='records',date_format='iso'))
    for obj in jannsen_data:
        obj = {key.lower(): value for key, value in obj.items()}
        obj['company'] = 'Johnson & Johnson'
        updated_data+=[obj]

    novavax = yf.Ticker('NVAX')
    novavax_data = json.loads(novavax.history(start="2019-01-01",  end="2022-11-25").reset_index().to_json(orient='records',date_format='iso'))
    for obj in novavax_data:
        obj = {key.lower(): value for key, value in obj.items()}
        obj['company'] = 'Novavax'
        updated_data+=[obj]

    collection = db['vaccine_stock_price']
    x = collection.insert_many(updated_data)
    print(db.list_collection_names())
    print('MongoDB updated with Stock Data where collection name is - vaccine_stock_price')