from dagster import op, Out, In, DagsterType
import pandas as pd
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import math


TransformedDistributionDataFrame = create_dagster_pandas_dataframe_type(
    name = "TransformedDistributionDataFrame",
    columns = [
        PandasColumn.string_column("date", non_nullable = True),
        PandasColumn.integer_column("distributed_janssen", non_nullable = True),
        PandasColumn.integer_column("distributed_moderna", non_nullable = True),
        PandasColumn.integer_column("distributed_pfizer", non_nullable = True),
        PandasColumn.float_column("distributed_novavax", non_nullable = False),

    ]
)


TransformedStockDataFrame = create_dagster_pandas_dataframe_type(
    name = "TransformedStockDataFrame",
    columns = [
        PandasColumn.string_column("date", non_nullable = True),
        PandasColumn.string_column("company", non_nullable = True),
        PandasColumn.float_column("open", non_nullable = False),
        PandasColumn.float_column("high", non_nullable = False),
        PandasColumn.float_column("low", non_nullable = False),
        PandasColumn.float_column("close", non_nullable = False),
        PandasColumn.float_column("volume", non_nullable = False)
        
    ]
)

TransformedTweetDataFrame = create_dagster_pandas_dataframe_type(
    name = "TransformedTweetDataFrame",
    columns = [
        PandasColumn.string_column("date", non_nullable = True),
        PandasColumn.string_column("company", non_nullable = True),
        PandasColumn.integer_column("likecount", non_nullable = True),
        PandasColumn.integer_column("retweetcount", non_nullable = True)

    ]
)

@op (ins = {'start':In(None)}, out = Out(TransformedDistributionDataFrame))
def transform_distribution(start) -> TransformedDistributionDataFrame:
    
    distribution = pd.read_csv("staging/distribution.csv")
    distribution["date"] = list(map(lambda x: str(datetime.strptime(x[0:-13], '%Y-%m-%d').date()), distribution["date"]))
    distribution["distributed_janssen"] = list(map(lambda x: int(x), distribution["distributed_janssen"]))
    distribution["distributed_moderna"] = list(map(lambda x: int(x), distribution["distributed_moderna"]))
    distribution["distributed_pfizer"] = list(map(lambda x: int(x), distribution["distributed_pfizer"]))
    distribution["distributed_novavax"] = list(map(lambda x:  float(0) if math.isnan(float(x)) == True else float(x), distribution["distributed_novavax"]))
    distribution_grouped = distribution.groupby("date").sum()
    distribution_grouped['date'] = distribution_grouped.index
    return distribution_grouped

@op (ins = {'distribution': In(TransformedDistributionDataFrame)}, out = Out(None))
def stage_transformed_distribution(distribution):
    distribution.to_csv("staging/transformed_distribution.csv",index=False)

@op (ins = {'start':In(None)}, out = Out(TransformedStockDataFrame))
def transform_stock(start) -> TransformedStockDataFrame:
    price = pd.read_csv("staging/stock_price.csv")
    price['date'] = list(map(lambda x: str(datetime.strptime(x[0:-14], '%Y-%m-%d').date()), price["date"]))
    price['high'] = list(map(lambda x: float(x), price['high']))
    price['open'] = list(map(lambda x: float(x), price['open']))
    price['close'] = list(map(lambda x: float(x), price['close']))
    price['low'] = list(map(lambda x: float(x), price['low']))
    price['volume'] = list(map(lambda x: float(x), price['volume']))
    return price

@op (ins = {'stock': In(TransformedStockDataFrame)}, out = Out(None))
def stage_transformed_stock(stock):
    stock.to_csv("staging/transformed_stock.csv",index=False)

@op (ins = {'start':In(None)}, out = Out(TransformedTweetDataFrame))
def transform_tweet(start) -> TransformedTweetDataFrame:
    tweet = pd.read_csv("staging/tweet.csv")
    tweet['date'] = list(map(lambda x: datetime.fromtimestamp(int(str(x)[0:-3])).date(), tweet["date"]))
    return tweet

@op (ins = {'tweet': In(TransformedTweetDataFrame)}, out = Out(None))
def stage_transformed_tweet(tweet):
    tweet.to_csv("staging/transformed_tweet.csv",index=False)
