import requests
from datetime import date
import pandas as pd
import io
from pandas import json_normalize 
import json
from ast import literal_eval
import pyodbc as odbc
import os
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

#Extracting the US covid19 data from API1
today = date.today()

url=os.getenv('url')
host1=os.getenv('host1')
key1=os.getenv('key1')
querystring = {"start_date":"2020-02-09","end_date":today}

headers = {
    'x-rapidapi-host': host1,
    'x-rapidapi-key': key1
    }
#Identification and alerting of when a data input source in the service is broken
try:
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
except requests.exceptions.HTTPError as errh:
    print('Bad Status Code',response.status_code)
except requests.exceptions.ConnectionError as errc:
    print ("Error Connecting")
r = response.text
#Data cleaning: transforming the US covid data from Json format to dataframe
test=json.loads(r)
test1=test['records']
df=pd.DataFrame(test1)
df = df.set_index('dateofrecord')
df1=pd.DataFrame(df['cases'].values.tolist(),index=df.index)
df1['country']='The US'
df1=df1[['country','totalconfirmed']]
df1.reset_index(level=0, inplace=True)
df1.rename(columns={'dateofrecord': 'dateymd'},inplace = True)
'''print(df1)'''

#Extracting Indian covid19 data from API2
url_india=os.getenv('url_india')
host2=os.getenv('host2')
key2=os.getenv('key2')

headers_india = {
    'x-rapidapi-host': host2,
    'x-rapidapi-key': key2
    }
try:
    response1 = requests.get(url_india, headers=headers_india)
    response1.raise_for_status()
except requests.exceptions.HTTPError as errh1:
    print('Bad Status Code',response.status_code)
except requests.exceptions.ConnectionError as errc1:
    print ("Error Connecting")
r_india=response1.text

#Data cleaning: transforming the Indian covid data in Json format to dataframe 
#Unifying the fields of both dataframes extracted with two different APIs
df_india=pd.read_json(r_india)
df_india['country']='India'
df_india=df_india[['dateymd','totalconfirmed','country']]
'''print(df_india)'''

#Appending both dataframes rowwise to generate a centralized table
centralized=pd.concat([df1,df_india],sort=False,ignore_index=True)
centralized.reset_index(level=0, inplace=True)
print(centralized)

#Importing the centralized table to SQL server(authentication setting)
records=centralized.values.tolist()
try:
    conn=odbc.connect("Driver={SQL Server Native Client 11.0};"
                    "Server=LAPTOP-FPH6QQ5H;"
                    "Database=Unity Assignment;"
                    "UID=huangyue;"
                    "PWD=BUlanNI000...;"
                    "Trusted_Connection=no;")
except odbc.DatabaseError as e:
    print('Database Error:')
except odbc.Error as e:
    print('Connection Error:')

#Avoiding dupicate rows in data appending to SQL server
sql_insert='''
INSERT INTO [Unity Assignment].[dbo].[Centralized] ([index],[dateymd],[country],[totalconfirmed])
VALUES(?,?,?,?)
'''
sql_delete_duplicate='''
WITH cte AS (
    SELECT [index],[dateymd],[country],[totalconfirmed],ROW_NUMBER() OVER(PARTITION BY [dateymd],[country],[totalconfirmed] ORDER BY [dateymd]) row_num
    FROM [Unity Assignment].[dbo].[Centralized])

    DELETE FROM cte WHERE row_num>1
'''
try: 
    cursor=conn.cursor() 
    cursor.executemany(sql_insert,records)
    cursor.commit();

    cursor.execute(sql_delete_duplicate)
    cursor.commit();
except Exception as e:
    cursor.rollback()
    print(str(e[1]))
finally:
    cursor.close()
    conn.close()

