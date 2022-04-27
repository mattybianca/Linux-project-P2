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
# The script runs via crontab every midnight and downloads the latest information available.
# Then everything is added to DBproject database.

import os, json, time, requests
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt




## MySQL connection -- SQLalchemy
cnx = create_engine( "mysql+mysqldb://{userid}:{password}@localhost/{my_database}".format(
                userid='p2', 
                password='verystrongpassword',
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



# Get last midnigh in unix
#           Reference for last midninght in UNIX
#           https://stackoverflow.com/questions/19594747/what-was-midnight-yesterday-as-an-epoch-time
last_midnight = (int(time.time() // 86400)) * 86400
# The timezone is set to UTC sicne both the Server and API service use this timezione


for token, name in zip(tokens_df['tokens'], tokens_df['CG_names']):
    
    print('=====================')
    print('start download of -- {}'.format(token))
    res = requests.get('https://api.coingecko.com/api/v3/simple/price?ids={}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true'.format(
                                                                                name.lower()))
    res = res.json()[name.lower()]

    print("Writing to SQL --  {} ".format(token))
    
    # Write to DBproject database
    query = "INSERT INTO {} VALUES".format(token)
    query = query + "('{}', '{}', '{}', '{}', '{}') ; ".format(last_midnight*1000,
                                             dt.datetime.fromtimestamp(last_midnight),
                                             round(res['usd'],4),
                                             round(res['usd_market_cap'],2),
                                             round(res['usd_24h_vol'],2))
                                             
    cnx.execute(query)
    print('Finished -- {}'.format(token))
    print('=====================')



