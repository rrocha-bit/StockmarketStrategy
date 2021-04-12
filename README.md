# Stock Market Strategy

# Summary
  
This project will focus on importing data of a stock list from IBOVESPA index (brazilian stock market). The main feature is to detect trend reversals with a very simple trade strategy based on moving averages of 9 periods. This structure will enable to expand the features to analyze other objective trading systems.
The evolution of this project will be to acheive two more targets:
  1 - simulating whether the strategy would have been profitable if applied to a certain stock in a certain period.
  2 - testing hyphotesis comparing wheter a strategy A would have been more profitable than strategy B.
  
# Logical structure

SQL Server 14.0 database to store results.
Python 3.8
  Libraries:
      Pandas 
      Datetime
      Pyodbc
      Numpy
      Datareader
Datasource - Yahoo Finance API

# Running

1 - Make sure you do have Python and SQL Server installed.
2 - Download files database_setup.sql and scan_trend-reversals.py
3 - Run database_setup.sql first to get your database all set.
4 - Run scan_trend_reversals.py (Insert your server name in the 14th line of the code)
5 - Check tabble StockReversal to see if new trend reversals have been detected.










