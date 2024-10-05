# Sales_Performance_Analysis

Sales Performance Analysis Using ETL and SQL

### Project Overview

Sales performance analysis is vital for businesses that want to understand how their products perform across different regions, customer segments, and time periods. By analyzing sales data, organizations can identify trends, optimize pricing strategies, and make data-driven
decisions to boost revenue.

### Project Objective

In this project, you will design an end-to-end ETL process to extract raw sales data from CSV files, transform it by performing cleaning and aggregation, and load the cleaned data into a PostgreSQL database. After the ETL process, you will use SQL to answer key business questions about sales performance. This project will enhance your skills in data wrangling, ETL, and SQL querying, giving you practical experience in analyzing data to support business decision-making.

### Data Sources

Global Electronics Retailers Dataset, which contains the following tables:
● Customers: Customer details and profiles.
● Exchange Rates: Currency exchange rates relevant to sales data.
● Products: Information on the products sold.
● Sales: Transaction records and sales data.
● Stores: Store locations and their attributes.

### Tools

-  Visual Studio Code - to write Python scripts
    -  [download_here](https://code.visualstudio.com/)
-  PostgreSQL  -  to analyse data
    -  [download_here](https://www.postgresql.org/)

 ### Data Preparation

In the initial preparation phase, I designed an end-to-end ETL process to extract raw sales data from CSV files, transform it by performing cleaning and aggregation, and load the cleaned data into a PostgreSQL database.

### Exploratory Data Analysis

Once the data is loaded into the database, I used SQL to answer the following challenging business questions:
1. Which stores consistently achieve the highest sales volume, and how does this vary by year and quarter?
○ Analyze store performance over time to identify trends in sales volume across different periods.
2. What are the most profitable products in each region, and how do exchange rate fluctuations impact profitability?
○ Evaluate product profitability by region, taking into account the effects of currency exchange rates.
3. Which customer segments exhibit the highest repeat purchase behavior, and what are the key factors driving loyalty?
○ Determine which customer demographics are most loyal and analyze the factors contributing to repeat purchases.
4. How do delivery delays impact sales performance, and what are the most common causes of delays across different stores and regions?
○ Investigate the relationship between delivery delays and sales, identifying patterns and causes of delays.
5. What is the relationship between store size (in square meters) and revenue generation? Are larger stores more effective in driving sales, or is there a diminishing return as store size increases?
○ Explore the correlation between store size and revenue to determine the most effective store size for sales performance.


### Data Analysis

```
import pandas as pd 
from sqlalchemy import create_engine 
from dotenv import load_dotenv
import os
import psycopg2

def extract(filepath):
    data = pd.read_csv(filepath, encoding='unicode_escape')
    print(f'Data has been successfully loaded from {filepath}')
    return data 

# extract('Customer.csv')

def transform(data):
    data['FirstName'] = data['Name'].str.split(' ', expand=True)[0]
    data['LastName'] = data['Name'].str.split(' ', expand=True)[1]
    data['Birthday'] = pd.to_datetime(data['Birthday'])
    data = data.drop(['Name'], axis=1)
    print('Data transformation successful')
    return data 

def load(data, tablename):
    #defining credentials for postgres
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    name = os.getenv('DB_NAME')
    
    #starting postgres engine
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}')
    
    #load data into postgres
    data.to_sql(tablename, engine, if_exists='replace', index=False)
    print(f'Data has been successfully loaded into {tablename} table')

def pipeline(filepath, tablename):
    data = extract(filepath)
    transformed_data = transform(data)
    load(transformed_data, tablename) 

filepath = 'Customers.csv'
tablename = 'Customers'

pipeline(filepath, tablename)
```


