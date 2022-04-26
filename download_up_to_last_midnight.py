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
# This script dowloads the marketcap, price and volume of selected layer-1 tokens.
# It should be run only once to fill the Database with all information up to last midnight
# Then another script will update the Database every day at midnight. 

from sqlalchemy import create_engine
import pandas as pd
import  time, requests
import datetime as dt





# Connection to DBproject database
cnx = create_engine( "mysql+mysqldb://{userid}:{password}@localhost/{my_database}".format(
                userid='p2', 
                password='****',
                my_database='DBproject') )
print(cnx)


# Layer 1 tokens
tokens = ['ADA', 'ALGO', 'ATOM', 'AVAX', 'DOT', 'ETH',
                'FTM', 'HBAR', 'LUNA', 'NEAR', 'SOL', 'TRX' ]


# Names of tokens for the requests to API coingecko
# In coingecko API the requests of data are made using the {id} which is the name and not the ticker        
namesCG = ['Cardano', 'Algorand', 'Cosmos', 'Avalanche-2', 'Polkadot',
        'Ethereum', 'Fantom', 'Hedera-hashgraph', 'Terra-luna', 'Near', 'Solana', 'Tron' ]

tokens_df = pd.DataFrame()
tokens_df['tokens'] = tokens
tokens_df['CG_names'] = namesCG



# Query used to create all the tables in DBproject

query_tables = '''CREATE TABLE {}
(   
    unix_date BIGINT(20) NOT NULL,
    date DATE NOT NULL,
    price  DECIMAL(14,4) NOT NULL,
    mkt_cap DECIMAL(25,2) NOT NULL,
	volume DECIMAL(25,2) NOT NULL,
    INDEX unix_date(unix_date),
    INDEX date (date),
    INDEX price (price),
    INDEX mkt_cap (mkt_cap),
    INDEX volume (volume)
);'''




# Get last midnigh in unix
#           Reference for last midninght in UNIX
#           https://stackoverflow.com/questions/19594747/what-was-midnight-yesterday-as-an-epoch-time
last_midnight = (int(time.time() // 86400)) * 86400 

# The starting date is chosen arbitrarily
# The timezone is set to UTC sicne both the Server and API service use this timezione
start_date_unix = int(dt.datetime(2018, 6, 1, tzinfo=dt.timezone.utc).timestamp())

for token, name in zip(tokens_df['tokens'], tokens_df['CG_names']):
    
    print('=====================')
    print('start download of -- {}'.format(token))
    
    res = requests.get('https://api.coingecko.com/api/v3/coins/{}/market_chart/range?vs_currency=usd&from={}&to={}'.format(
                                                name.lower(), start_date_unix, last_midnight))
    
    # The response is a json formatted as  {prices: [[date, price]..[]], 
    #                                            total_volume:[[date, total_volume]..[]],
    #                                            market_caps:  [[date, market_caps]..[]]}
    #
    # The problem is that each element is not a number but a list containing the date and the
    # value associated to that date.

    # Transform the response components into a DataFrame objects
    # column idx 0 contains the dates
    # column idx 1 contains the actual values

    prices = pd.DataFrame(res.json()['prices'])
    volumes = pd.DataFrame(res.json()['total_volumes'])
    mktcaps = pd.DataFrame(res.json()['market_caps'])
    
    df = pd.DataFrame()
    df['unix_date'] = prices.iloc[:,0]
    df['date'] = pd.to_datetime(df['unix_date'], unit='ms')
    df['price'] =  round(prices.iloc[:,1], 4)
    df['mkt_cap'] = round(mktcaps.iloc[:,1], 2)
    df['volume'] = round(volumes.iloc[:,1], 2)
    


    try: 
        cnx.execute(query_tables.format(token))
        print('New table added successfully')


    except:
        print('The table for {} exists already...'.format(token))
        print('Follow up with loading/updating data into the table')

    if_exists = 'replace'
    print('Writing to SQL --  {}, --- if_exists = {}'.format(token, if_exists))


    # Write to DBproject database
    df.to_sql(token, cnx, if_exists= if_exists, index = False)
    print('Finished -- {}'.format(token))
    print('=====================')



