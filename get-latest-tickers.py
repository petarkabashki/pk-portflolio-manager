#!python

##############################################################
### Gets the latest prices for selected pairs from coinmarketcap
#########################
#%%
# import IPython
import pandas as pd
import numpy as np

from datetime import datetime, date
from math import *
import json
from decimal import *
getcontext().prec = 6
import re
from decimal import Decimal

from os import listdir
from os.path import isfile, join

#%%

import configparser
import json
import pytz
from datetime import datetime
from requests import Request, Session
from dateutil import parser

symbols = ['BTC,ETH,XRP,ALGO,ADA,LINK,BNB,DOT,POL,XLM,LTC,AVAX,AAVE,EGLD,ATOM,DOGE,GRT,ENJ,CAKE,TRX,BUSD,LUNA,USDC,USDT,SOL,MATIC,FIL,ICP,CRV,REN,RUNE,KSM,ZIL,THETA,LRC,MANA,SHIB,ILV,GALA,AXS,SAND,RNDR,RENDER,CHZ,APT,ARB,OP,RAY,INJ,BEAM,NEAR,ALU,KAS,TIA,PYR,SNX,MYRIA,IMX,COQ,QNT,TET,AMP,PT,SUSHI,UNI,FLOW,WAVES,COMP']
convert = ['USDT' ]

# def get_info():
    # Read the API key from the coinmarket.ini file
    # config = configparser.ConfigParser()
    # config.read('coinmarket.ini')
    # api_key = config['DEFAULT']['API_KEY']

    # Set up the request parameters and headers
# url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/ohlcv/historical'

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
parameters = { 
    # 'slug': 'bitcoin', 
    'symbol': symbols,
    'convert': convert,
    # 'time_start': '2020-01-01'
                }
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '733a53cb-19a4-4070-8291-a03aa07da1ad'
}

# Send the request and retrieve the response
session = Session()
session.headers.update(headers)
response = session.get(url, params=parameters)
data = json.loads(response.text)

output_file = 'cmc-quotes.csv'
pd.DataFrame.from_records([
    {'asset':c, **data['data'][c]['quote']['USDT']} 
    for  c in data['data'].keys()
    if data['data'][c]['quote']['USDT']
    ]).to_csv(output_file)


print(f'Successfully downloaded {len(data["data"])} tickers to "{output_file}"')

# cron 0 * * * * ~/miniconda3/envs/py310/bin/python ./get-latest-tickers.py
# cron */60 * * * * /home/grenada/miniconda3/envs/py310/bin/python ./get-latest-tickers.py

#%%
