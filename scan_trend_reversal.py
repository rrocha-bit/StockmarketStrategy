# -*- coding: utf-8 -*-
"""
Created on Sat Apr 10 15:55:42 2021

@author: Renan Rocha
"""

from pandas_datareader import data
import pandas as pd, datetime, pyodbc, numpy as np
import talib as ta
import time
 
def connection():
    conn = pyodbc.connect("Driver={SQL Server};Server='INSERT YOUR SERVER NAME HERE';Database=B3;Trusted_Connection=yes,autocommit=True")
    return conn


def cod_stocks():
    conn=connection()
    cur= conn.cursor()
    query_codigo = '''select id_stock from dimBrazilianStocks'''
    df = pd.read_sql(query_codigo, conn)
    df2 = []
    for a in df['id_stock']:
        df2.append(a)    
    cur.close()
    conn.close()
    return df2


def extrai_dados_insere_bd(stocks):
    hoje = datetime.date.today()
    inicio = datetime.date.today()-datetime.timedelta(days=60)
    conn=connection()
    cur= conn.cursor()
    query_insert = """INSERT INTO dbo.StockHistory VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cur.execute("Truncate table dbo.StockHistory")
    conn.commit()
    for stock in stocks:    
        panel_data = data.DataReader(stock + '.SA', 'yahoo', inicio, hoje)
        
        df = pd.DataFrame(panel_data)
        df['date'] = df.index
        df['date'] = pd.to_datetime(df['date'], errors='coerce', format=('%d/%m/%Y')).dt.strftime("%Y-%m-%d")
        df['stock'] = str(stock)
        df.rename(columns={'High': 'high', 'Low': 'low', 'Volume':'volume', 'Open':'open','Close':'close', 'Adj Close': 'adjclose'}, inplace=True)   
        df['open'] = df['open'].round(2)
        df['close'] = df['close'].round(2)
        df['high'] = df['high'].round(2)
        df['low'] = df['low'].round(2)
        df['adjclose'] = df['adjclose'].round(2)
        df['volume'] = df['volume'].astype(int)
        df['ema9'] = ta.MA(df['close'], 9).astype(float).round(2)
        df = df.replace({np.nan:None,"NaT":None,'nan':None, 'NaN':None})
        df.drop_duplicates(keep='first', inplace=True)
        df = df.dropna(subset=['ema9'])
        df = df.filter(items=['date', 'high', 'low', 'open', 'close', 'volume', 'adjclose', 'stock', 'ema9'])
        tuples = [tuple(x) for x in df.values]
        
        if df.shape[0] > 0:
            try:
                cur.executemany(query_insert,tuples)
                conn.commit()
                tuples.clear()
                print('{} rows of stock {} has been inserted succesfully'.format(df.shape[0],stock) )
                #cur.execute("Nome da Procedure") Se for necessário incluir a execução de uma Procedure
            except:
                print('Error to insert stock data: {}. Need to check'.format(stock))
        else:
            print('DataFrame of stock {} is empty, need attencion.'.format(stock))
            
            
    cur.execute("Exec B3.dbo.sp_TrendReversal")
    conn.commit()
    time.sleep(3)
            
    cur.close()
    conn.close()
    
    print('Check table StockReversals to see if new trend reversals have been detected')


extrai_dados_insere_bd(cod_stocks())


