from stockExtract_mongo import upload_stock_data
from tweetExtract_mongo import upload_twitter_data
from vaccine_distribution_mongo import upload_vaccine_distribution

def main():
    connection_string = input("Please enter the mongo db connection string for example -> 'mongodb+srv://{id}:{password}@cluster.mongodb.net' : ")
    upload_stock_data(connection_string)
    upload_twitter_data(connection_string)
    upload_vaccine_distribution(connection_string)

if __name__=="__main__":
    main()