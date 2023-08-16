# Investigating Vaccine Stock Price and Vaccine Distribution Correlation with a Dagster-based ETL Pipeline

## Project Overview

In this project we have developed an ETL pipeline using Dagster where the datasets of different vaccine companies are accumulated using REST APIs and stored the data into Mongo DB. 

For Dashboard plotly Dash has been used which is a python framework to develop data application.

## Steps to run the project
1. Create a virtual environment and install the dependencies using pip3 install -r requirements.txt and now activate the virtual environment.
2. Go to data-consolidation folder and run run.py file using python3 run.py, it will ask to enter the mongo db connection string please enter the connection string.
3. Go to ETL-pipeline folder and update the constants.py file with postgres and mongo db database configurations.
4. Run the pipeline using dagit -f etl.py
5. Go to dashboard folder update the constants.py file with postgres configurations.
6. Run the dashboard web app using the command - python3 app.py.


