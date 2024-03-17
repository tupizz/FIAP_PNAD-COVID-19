import os
import sys
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import helpers

credentials = service_account.Credentials.from_service_account_file(
    'credentials.json',
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)

client = bigquery.Client(credentials=credentials)

queries = [
    "SELECT * FROM `fiap-417300.PNAD_COVID_19.05_2020`;",
    "SELECT * FROM `fiap-417300.PNAD_COVID_19.06_2020`;",
    "SELECT * FROM `fiap-417300.PNAD_COVID_19.07_2020`;",
]

# Execute each query and store the results in a list of DataFrames
dataframes = [
    client.query(query).to_dataframe() for query in queries
]

# Concatenate all DataFrames into one large DataFrame
big_dataframe = pd.concat(dataframes, ignore_index=True)

# Save the large DataFrame to a CSV file
big_dataframe.to_csv('data.csv', index=False)

# Assuming big_dataframe is your main DataFrame
print("Data Overview:")
print(big_dataframe.info())

# Generate summary statistics
print("\nSummary Statistics:")
print(big_dataframe.describe())

