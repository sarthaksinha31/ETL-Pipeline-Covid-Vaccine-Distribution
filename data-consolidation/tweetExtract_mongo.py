import snscrape.modules.twitter as sntwitter
import pandas as pd
import itertools, json
from datetime import datetime
from pymongo import MongoClient

def upload_twitter_data(connection_string):
    print("Uploading Twitter Tweets by Vaccine Manufacturers")
    client = MongoClient(connection_string)
    db = client['dap_project']
    vaccine_comp = ['pfizer','moderna_tx','JNJNews','Novavax']
    vacc_comp_map = {
        'pfizer':'Pfizer',
        'moderna_tx':'Moderna',
        'JNJNews':'Johnson & Johnson',
        'Novavax':'Novavax'
    }
    vacc_tweet_data = []
    for vac in vaccine_comp:
        start_time = datetime.now()
        df = pd.DataFrame(itertools.islice(sntwitter.TwitterSearchScraper(
        f'"(from:{vac}) since:2019-01-01 until:2022-11-25"').get_items(), 500000))
        end_time = datetime.now()
        df.columns = df.columns.str.lower()
        data = json.loads(df.to_json(orient='records'))
        updated_data = [{**obj, 'company':vacc_comp_map.get(vac) } for obj in data]
        vacc_tweet_data+=updated_data
        print(f'{vac} Duration: {end_time - start_time}')

    collection = db['vaccine_manufacture_tweets']
    x = collection.insert_many(vacc_tweet_data)
    print(db.list_collection_names())
    print("MongoDB updated with Twitter Data where collection name is - vaccine_manufacture_tweets")