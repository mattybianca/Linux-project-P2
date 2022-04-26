# Project Programming 2 -- Set up a Data Server

#################
# Mattia Biancaterra
#       mattia.bincaterra@usi.ch
# Pietro Bonazzi
#       pietro.bonazzi@usi.ch
# Luca Colzani
#       luca.colzani@usi.ch
# Giovanni Gizzi
#       giovanni.gizzi@usi.ch
#################

#################
# This script computes the daily volatilty of all layer-1 selected tokens.
# Then the value of the volatility is stored into a DBproject's table named "daily_vol".
# All the steps descriptions and formulas can be found in the report.


from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
import numpy as np




# Connection to DBproject database
cnx = create_engine( "mysql+mysqldb://{userid}:{password}@localhost/{my_database}".format(
                userid='p2', 
                password='****',
                my_database='DBproject') )
print(cnx)


# Layer 1 tokens
tokens = ['ADA', 'ALGO', 'ATOM', 'AVAX', 'DOT', 'ETH',
                'FTM', 'HBAR', 'LUNA', 'NEAR', 'SOL', 'TRX' ]

# Get the data from the database
# All the tables in the database have sincornyzed dates, so it's irrelevante the table from which I get the dates.
# we chose the first table in alphabetic order: ADA.
query_date = "SELECT date FROM ADA"

df_mkt = pd.DataFrame()
query = "SELECT date, mkt_cap FROM {}"
df_mkt['date'] = pd.read_sql_query(query_date, cnx)

for k in tokens:
    temp = pd.read_sql_query(query.format(k), cnx)
    temp.rename(columns={'mkt_cap':'{}'.format(k)}, inplace=True)
    df_mkt = df_mkt.merge(temp, on = 'date', how ='left')



# Step-1
square_of_sum = (df_mkt.drop('date', axis=1).sum(axis=1))**2
sum_of_squares = ((df_mkt.drop('date', axis=1))**2).sum(axis=1)

#print(square_of_sum)
#print(sum_of_squares)

# Step-2
df_mkt['Div'] = square_of_sum/sum_of_squares
#print(df_mkt['Div']. head(100))

# Step-3
df_mkt['Pcrix'] = (df_mkt.drop('date', axis=1).sum(axis=1))/df_mkt['Div']
#print(df_mkt['Pcrix'])

# Step-4
df_mkt['Rcrix'] = (df_mkt['Pcrix'] - df_mkt['Pcrix'].shift(1)) / df_mkt['Pcrix'].shift(1)
#print(df_mkt['Rcrix'])

# Step-5a
window = 30
df_mkt['mu_hat'] = df_mkt['Rcrix'].rolling(window).mean()
#print(df_mkt['mu_hat'][29:])

# Step-5b
# Add a column with the difference used in the next passage
df_mkt['diff'] = df_mkt['Rcrix'] - df_mkt['mu_hat']


# Step-6
# Shift(1) since the sum must start at t-1
df_mkt['RV'] = np.sqrt(1/window * (df_mkt['diff']**2).shift(1).rolling(30).sum())*np.sqrt(365)*100 


# Step-7
df_mkt['vol_idx'] =  df_mkt['RV']/df_mkt['Div']
#print(df_mkt['vol_idx'][df_mkt['vol_idx'].first_valid_index():100])

# Query used to create all the tables in DBproject
query_table = '''CREATE TABLE vol_idx
(
    date DATE NOT NULL,
    value  DECIMAL(14,2) NOT NULL,
    INDEX date (date),
    INDEX value (value)
)'''

try:
    cnx.execute(query_table)
    print('New table added successfully')
except:
    print('The table for vol_idx exists already...')
    print('Follow up with loading/updating data into the table')

if_exists = 'replace'

df = df_mkt[['date', 'vol_idx']]
df.to_sql('vol_idx', cnx, if_exists= if_exists, index = False)