# from sqlalchemy import create_engine
import pandas as pd
from visual_query import QUERIES
import psycopg2, os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from constants import postgres_connection_string,postgres_user,postgres_password``

postgres_conn_string = postgres_connection_string
company_list = ['Pfizer','Moderna','Johnson & Johnson','Novavax']

try:
    dbConnection = psycopg2.connect(
        user = postgres_user,
        password = postgres_password,
        host = "127.0.0.1",
        port = "5432",
        database = "postgres")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    print('connection established successfully')
except (Exception , psycopg2.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if dbConnection in locals(): 
        dbConnection.close()


def get_stock_vs_distribution_data(company):
    map_distribution = {
        'Pfizer':'distributed_pfizer',
        'Moderna':'distributed_moderna',
        'Johnson & Johnson':'distributed_janssen',
        'Novavax':'distributed_novavax'
    }
    sql_command = QUERIES.get('VACCINE_STOCK_COMBINED').format(company=company)
    dbCursor.execute(sql_command)
    data = dbCursor.fetchall()
    column_names = [desc[0] for desc in dbCursor.description]
    df = pd.DataFrame(data, columns = column_names)
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d')
    df_2021_filtered = df[(df['date'] >= "2020-01-01") & (df['date']<= "2022-12-31")]
    df_month = df_2021_filtered.resample('M', on='date').agg({'open':'mean','close': 'mean', 'distributed_janssen': 'sum','distributed_moderna':'sum','distributed_pfizer':'sum','distributed_novavax':'sum'})
    df_month.reset_index(inplace=True)
    vac_distrib_column = map_distribution.get(company)
    return df_month,vac_distrib_column

def get_stock_vs_tweeter_data(company):
    sql_command = QUERIES.get('TWEET_STOCK_COMBINED').format(company=company)
    dbCursor.execute(sql_command)
    data = dbCursor.fetchall()
    column_names = [desc[0] for desc in dbCursor.description]
    df = pd.DataFrame(data, columns = column_names)
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d')
    df_2021 = df[(df['date'] >= "2021-01-01") & (df['date']< "2022-01-01")]
    df_2021.sort_values(by='date',inplace=True)
    return df_2021

def get_stock_distribution_data():
    sql_command = QUERIES.get('VACCINE_DISTRIBUTION_AND_STOCK')
    dbCursor.execute(sql_command)
    data = dbCursor.fetchall()
    column_names = [desc[0] for desc in dbCursor.description]
    df = pd.DataFrame(data, columns = column_names)
    return df

def plot_stock_vs_distribution(company,df_month,vac_distrib_column):
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # Add traces
    fig.add_trace(
        go.Bar(x=df_month['date'], y=df_month[vac_distrib_column], name="Vaccine Distribution Data",marker = {'color' : '#3D65A5'}),
        secondary_y=False,
    )
    fig.add_trace(
        go.Line(x=df_month['date'], y=df_month['close'], name="Average Stock Price",mode='lines+markers',marker = {'color' : '#E57A77'}),
        secondary_y=True,
    )
    # Add figure title
    fig.update_layout(
        title_text=f"<b>{company}</b> - Vaccine Distribution and Average Stock Price by Year (2021-22)"
    )
    # Set x-axis title
    fig.update_xaxes(title_text="Month and Year")
    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Vaccine Distribution</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Average Stock Price</b>", secondary_y=True)
    # fig.show()
    return fig

def plot_stock_vs_tweeter(df_2021,company):
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # Add traces
    fig.add_trace(
        go.Bar(x=df_2021['date'], y=df_2021['retweetcount'], name="Twitter Like Count",marker = {'color' : '#36454A'}),
        secondary_y=False,
    )
    fig.add_trace(
        go.Line(x=df_2021['date'], y=df_2021['close'], name="Average Stock Price",marker = {'color' : '#75D3F2'}),
        secondary_y=True,
    )
    # Add figure title
    fig.update_layout(
        title_text=f"<b>{company}</b> - Twitter Like Count and Average Stock Price by Year (2021-22)"
    )
    # Set x-axis title
    fig.update_xaxes(title_text="Month and Year")
    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Twitter Like Count</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Average Stock Price</b>", secondary_y=True)
    # fig.show()
    return fig

def create_distribution_vs_stocks_plot():
    for company in company_list:
        df_month,col_name = get_stock_vs_distribution_data(company)
        fig = plot_stock_vs_distribution(company,df_month,col_name)
        if not os.path.exists('distribution-stock-plots'):
            os.makedirs('distribution-stock-plots')
        fig.write_image(f"distribution-stock-plots/{company}-distribution-vs-stocks.png")
    return True

def create_tweet_vs_stocks_plot():
    for company in company_list:
        df_2021= get_stock_vs_tweeter_data(company)
        fig = plot_stock_vs_tweeter(df_2021,company)
        if not os.path.exists('tweets-stock-plots'):
            os.makedirs('tweets-stock-plots')
        fig.write_image(f"tweets-stock-plots/{company}-distribution-vs-stocks.png")
    return True

def plot_roi(df):
    roi = []
    new_df = df[df['company'] == 'Pfizer']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['Close']
    close = new_df[new_df['date'] == '2022-03-31']['Close']
    roi.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)

    new_df = df[df['company'] == 'Moderna']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['Close']
    close = new_df[new_df['date'] == '2022-03-31']['Close']
    roi.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)

    new_df = df[df['company'] == 'Johnson & Johnson']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['Close']
    close = new_df[new_df['date'] == '2022-03-31']['Close']
    roi.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)

    new_df = df[df['company'] == 'Novavax']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['Close']
    close = new_df[new_df['date'] == '2022-03-31']['Close']
    roi.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)

    fig = px.bar(x = ['Pfizer','Moderna','Johnson & Johnson', 'Novavax'],y = roi, title = '<b>Return on Investment</b>')
    fig.update_xaxes(title_text="Vaccine Companies")
    fig.update_yaxes(title_text="ROI in Dollars")
    fig.update_layout(barmode='stack', xaxis={'categoryorder': 'total ascending'})
    # fig.show()
    return fig

def plot_distribution_change(df):
    dist_change = []
    new_df = df[df['company'] == 'Pfizer']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['distributed_pfizer']
    close = new_df[new_df['date'] == '2022-03-31']['distributed_pfizer']
    dist_change.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)
    
    new_df = df[df['company'] == 'Moderna']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['distributed_moderna']
    close = new_df[new_df['date'] == '2022-03-31']['distributed_moderna']
    dist_change.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)
    
    new_df = df[df['company'] == 'Johnson & Johnson']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['distributed_janssen']
    close = new_df[new_df['date'] == '2022-03-31']['distributed_janssen']
    dist_change.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)
    
    new_df = df[df['company'] == 'Novavax']
    #start = new_df['date'] == '2020-12-14'
    start = new_df[new_df['date'] == '2021-04-01']['distributed_novavax']
    close = new_df[new_df['date'] == '2022-03-31']['distributed_novavax']
    dist_change.append(close.iloc[0] - start.iloc[0])
    
    fig = px.bar(x = ['Pfizer','Moderna','Johnson & Johnson', 'Novavax'],y = dist_change, title = '<b>Percentage Change in Distribution</b>')
    fig.update_xaxes(title_text="Vaccine Companies")
    fig.update_yaxes(title_text="Percentage Change in Distribution")
    fig.update_layout(barmode='stack', xaxis={'categoryorder': 'total ascending'})
    return fig

def plot_volume_change(df):
    vol = []
    new_df = df[df['company'] == 'Pfizer']
    start = new_df[new_df['date'] == '2021-04-01']['Volume']
    close = new_df[new_df['date'] == '2022-03-31']['Volume']
    vol.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)
    
    new_df = df[df['company'] == 'Moderna']
    start = new_df[new_df['date'] == '2021-04-01']['Volume']
    close = new_df[new_df['date'] == '2022-03-31']['Volume']
    vol.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)
    
    new_df = df[df['company'] == 'Johnson & Johnson']
    start = new_df[new_df['date'] == '2021-04-01']['Volume']
    close = new_df[new_df['date'] == '2022-03-31']['Volume']
    vol.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)
    
    new_df = df[df['company'] == 'Novavax']
    start = new_df[new_df['date'] == '2021-04-01']['Volume']
    close = new_df[new_df['date'] == '2022-03-31']['Volume']
    vol.append(((close.iloc[0] - start.iloc[0])/start.iloc[0])*100)
    fig = px.bar(x = ['Pfizer','Moderna','Johnson & Johnson', 'Novavax'],y = vol, title = '<b>Percentage Change in Volume</b>')
    fig.update_xaxes(title_text="Vaccine Companies")
    fig.update_yaxes(title_text="Perecentage Change in Volume")
    fig.update_layout(barmode='stack', xaxis={'categoryorder': 'total ascending'})
    return fig

def plot_stock_volatility(df):
    day_change = []
    # Pfizer
    df = get_stock_distribution_data()
    new_df = df[df['company'] == 'Pfizer'][['date', 'Open', 'High', 'Low', 'Close']]
    new_df = new_df[(new_df['date']>= '2021-04-01') & (new_df['date']<= '2022-03-31')]
    change = ((new_df['High'] - new_df['Low']).sum())/len(new_df['High'])
    day_change.append((change/((new_df['Close'].sum())/len(new_df['High'])))*100)
    # Moderna
    new_df = df[df['company'] == 'Moderna'][['date', 'Open', 'High', 'Low', 'Close']]
    new_df = new_df[(new_df['date']>= '2021-04-01') & (new_df['date']<= '2022-03-31')]
    change = ((new_df['High'] - new_df['Low']).sum())/len(new_df['High'])
    day_change.append((change/((new_df['Close'].sum())/len(new_df['High'])))*100)
    # Johnson and Johnson
    new_df = df[df['company'] == 'Johnson & Johnson'][['date', 'Open', 'High', 'Low', 'Close']]
    new_df = new_df[(new_df['date']>= '2021-04-01') & (new_df['date']<= '2022-03-31')]
    change = ((new_df['High'] - new_df['Low']).sum())/len(new_df['High'])
    day_change.append((change/((new_df['Close'].sum())/len(new_df['High'])))*100)
    # Novavax
    new_df = df[df['company'] == 'Novavax'][['date', 'Open', 'High', 'Low', 'Close']]
    new_df = new_df[(new_df['date']>= '2021-04-01') & (new_df['date']<= '2022-03-31')]
    change = ((new_df['High'] - new_df['Low']).sum())/len(new_df['High'])
    day_change.append((change/((new_df['Close'].sum())/len(new_df['High'])))*100)
    fig = px.bar(x = ['Pfizer','Moderna','Johnson & Johnson', 'Novavax'],y = day_change, title = '<b>Volatitlity FY 21 22</b>')
    fig.show()
    fig.update_xaxes(title_text="Vaccine Companies")
    fig.update_yaxes(title_text="Volatility")
    fig.update_layout(barmode='stack', xaxis={'categoryorder': 'total ascending'})
    return fig