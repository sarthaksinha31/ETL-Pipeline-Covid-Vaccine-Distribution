from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine
import pandas as pd
from extract import *
from queries import query
import psycopg2
from constants import postgres_connection_string,postgres_user,postgres_password

logger = get_dagster_logger()
postgres_conn_string = postgres_connection_string

try:
    dbConnection = psycopg2.connect(
        user = postgres_user,
        password = postgres_password,
        host = "127.0.0.1",
        port = "5432",
        database = "postgres")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
except (Exception , psycopg2.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if dbConnection in locals(): 
        dbConnection.close()

@op(ins={'start': In(None)}, out=Out(None))
def load_distribution(start):
    create_table_command = query.get('CREATE_VACCINE_DISTRIBUTION_TABLE')
    dbCursor.execute(create_table_command)
    dbCursor.close()
    engine = create_engine(postgres_conn_string)
    DistributionDataFrame = pd.read_csv("staging/transformed_distribution.csv")
    rowcount = DistributionDataFrame.to_sql("vaccine_distribution",
    schema = "public",
    con = engine,
    index = False,
    if_exists = "replace"
    )

    logger.info("%i vaccine distribution records loaded" % rowcount)
    engine.dispose(close=True)

@op(ins={'start': In(None)}, out=Out(None))
def load_stocks(start):
    create_table_command = query.get('CREATE_STOCK_TABLE')
    dbCursor.execute(create_table_command)
    dbCursor.close()
    engine = create_engine(postgres_conn_string)
    StockPriceDataFrame = pd.read_csv("staging/transformed_stock.csv")
    rowcount = StockPriceDataFrame.to_sql("vaccine_stock_price",
    schema = "public",
    con = engine,
    index = False,
    if_exists = "replace"
    )
    logger.info("%i stock market records loaded" % rowcount)
    engine.dispose(close=True)

@op(ins={'start': In(None)}, out=Out(None))
def load_tweets(start):
    create_table_command = query.get('CREATE_TWITTER_TABLE')
    dbCursor.execute(create_table_command)
    dbCursor.close()
    engine = create_engine(postgres_conn_string)
    StockPriceDataFrame = pd.read_csv("staging/transformed_tweet.csv")
    rowcount = StockPriceDataFrame.to_sql("vaccine_manuf_tweet",
    schema = "public",
    con = engine,
    index = False,
    if_exists = "replace"
    )
    logger.info("%i twitter records loaded" % rowcount)
    engine.dispose(close=True)