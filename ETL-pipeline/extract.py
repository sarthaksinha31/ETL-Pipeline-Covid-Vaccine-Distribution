from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
import math
from constants import mongodb_connection_string

mongo_conn_str = mongodb_connection_string
DistributionDataFrame = create_dagster_pandas_dataframe_type(
    name = "DistributionDataFrame",
    columns = [
        PandasColumn.string_column("date", non_nullable = True),
        PandasColumn.string_column("distributed_janssen", non_nullable = True),
        PandasColumn.string_column("distributed_moderna", non_nullable = True),
        PandasColumn.string_column("distributed_pfizer", non_nullable = True),
        PandasColumn.string_column("distributed_novavax", non_nullable = False),
    ]
)

StockPriceDataFrame = create_dagster_pandas_dataframe_type(
    name = "StockPriceDataFrame",
    columns = [
        PandasColumn.string_column("date", non_nullable = True),
        PandasColumn.string_column("company", non_nullable = True),
        PandasColumn.float_column("open", non_nullable = False),
        PandasColumn.float_column("high", non_nullable = False),
        PandasColumn.float_column("low", non_nullable = False),
        PandasColumn.float_column("close", non_nullable = False),
        PandasColumn.integer_column("volume", non_nullable = False)
        
    ]
)

TweetDataFrame = create_dagster_pandas_dataframe_type(
    name = "TweetDataFrame",
    columns = [
        PandasColumn.integer_column("date", non_nullable = True),
        PandasColumn.string_column("company", non_nullable = True),
        PandasColumn.integer_column("likecount", non_nullable = False),
        PandasColumn.integer_column("retweetcount", non_nullable = False)
    ]
)

@op(ins={'start': In(bool)}, out=Out(DistributionDataFrame))
def extract_distribution(start) -> DistributionDataFrame:
    conn = MongoClient(mongo_conn_str)
    db = conn["dap_project"]
    distribution = pd.DataFrame(db['vaccine_distribution'].find({}))
    distribution = distribution[["date", "distributed_janssen", "distributed_moderna",
    "distributed_pfizer", "distributed_novavax"]]
    distribution.to_csv("staging/distribution.csv",index=False) #save the distributiondataFrame to staging
    conn.close()
    return distribution

@op(ins={'start': In(bool)}, out=Out(StockPriceDataFrame))
def extract_stock(start) -> StockPriceDataFrame:
    conn = MongoClient(mongo_conn_str)
    db = conn["dap_project"]
    price = pd.DataFrame(db['vaccine_stock_price'].find({}))
    price = price[['date', 'high', 'open', 'close', 'low', 'volume', 'company']]
    price.to_csv("staging/stock_price.csv",index=False) #save the StockPriceDataFrame to staging
    return price

@op(ins={'start': In(bool)}, out=Out(TweetDataFrame))
def extract_tweet(start) -> TweetDataFrame:
    conn = MongoClient(mongo_conn_str)
    db = conn["dap_project"]
    tweet = pd.DataFrame(db['vaccine_manufacture_tweets'].find({}))
    tweet = tweet[['date', 'company', 'likecount', 'retweetcount']]
    tweet.to_csv("staging/tweet.csv",index=False) #save the TwitterDataFrame to staging
    return tweet