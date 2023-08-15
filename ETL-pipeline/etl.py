import webbrowser
from dagster import job
from extract import *
from transform import *
from load import *

@job
def etl():
    load_distribution(
            stage_transformed_distribution(
            transform_distribution(
                extract_distribution()
                )
                )
    )

    load_stocks(
        stage_transformed_stock(
            transform_stock(
                extract_stock()
            )
        )
    )

    load_tweets(
        stage_transformed_tweet(
            transform_tweet(
                extract_tweet()
            )
        )
    )



    
