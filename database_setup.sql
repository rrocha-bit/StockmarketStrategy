CREATE DATABASE [B3]
GO

USE [B3]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[dimBrazilianStocks]
CREATE TABLE [dbo].[dimBrazilianStocks](
	[id_stock] [varchar](20) PRIMARY KEY,
	[StockDescription] [varchar](100) NULL,
	[MarketType] [varchar](50) NULL
)
GO



DROP TABLE if exists StockHistory

CREATE TABLE [dbo].[StockHistory](
	[date] date,
	[high] float,
	[low] float,
	[open] float,
	[close] float,
	[volume] bigint,
	[adjclose] float,
	[stock] varchar(20),
	[ema9] float
	PRIMARY KEY ([date],[stock],[close])
	FOREIGN KEY (stock) REFERENCES dimBrazilianStocks(id_stock)
) 
GO



DROP TABLE IF EXISTS [StockReversals]
CREATE TABLE [dbo].[StockReversals](
	[date] [date],
	[stock] [varchar](20),
	[signal] [varchar](30) NULL
	PRIMARY KEY ([date], [stock])
	FOREIGN KEY (stock) REFERENCES dimBrazilianStocks(id_stock)
)
GO

INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('ABCB4','ABC BRASIL','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('ALPA4','ALPARGATAS','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('AZUL4','AZUL','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('BBDC4','BRADESCO','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('BIDI4','BANCO INTER','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('BMGB4','BANCO BMG','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('BPAN4','BANCO PAN','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('BRAP4','BRADESPAR','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('CMIG4','CEMIG','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('FESA4','FERBASA','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('GGBR4','GERDAU','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('GOAU4','GERDAU MET','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('GOLL4','GOL','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('ITSA4','ITAUSA','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('ITUB4','ITAUUNIBANCO','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('KLBN4','KLABIN S/A','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('LAME4','LOJAS AMERIC','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('PETR4','PETROBRAS','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('PINE4','PINE','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('PNVL4','DIMED','PN')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('POMO4','MARCOPOLO','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('RAPT4','RANDON PART','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('RCSL4','RECRUSUL','PN')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('SANB4','SANTANDER BR','PN')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('SAPR4','SANEPAR','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('SHUL4','SCHULZ','PN')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('TAEE4','TAESA','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('TASA4','TAURUS ARMAS','PN      N2')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('TELB4','TELEBRAS','PN')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('TRPL4','TRAN PAULIST','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('WHRL4','WHIRLPOOL','PN')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('MWET4','WETZEL S/A','PN')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('OIBR4','OI','PN      N1')
INSERT INTO [B3].[dbo].[dimBrazilianStocks] VALUES ('SLED4','SARAIVA LIVR','PN      N2')


USE B3
GO

CREATE PROCEDURE sp_TrendReversal AS


DECLARE @seq AS int
DECLARE @count as int
DECLARE @param as int
DECLARE @stock as varchar(20)
DECLARE @dateref as date
DECLARE @StockResults as Table ([date] date, [stock] varchar(20), [close] float, [open] float, [high] float, [low] float, direction int, position int, [signal] varchar(30))
DECLARE @PotencialReversals as Table ([date] date, [stock] varchar(20), [close] float, [open] float, [high] float, [low] float, direction int, position int, [signal] varchar(30))

DROP TABLE IF EXISTS #temp_StockDateReference
SELECT row_number() over (order by stock) AS seq, [stock], max([date]) as [date] INTO #temp_StockDateReference FROM StockHistory WITH (NOLOCK) group by stock

set @seq = 1
set @count = (SELECT max(seq) FROM #temp_StockDateReference)
set @param = 100


DROP TABLE IF EXISTS #StockResults
DROP TABLE IF EXISTS #temp_StockTrends




	WHILE @seq <= @count
	BEGIN
	set @stock = (SELECT [stock] FROM #temp_StockDateReference WHERE [seq] = @seq)
	set @dateref = (SELECT [date] FROM #temp_StockDateReference WHERE [seq] = @seq)


	--Create temporary table
	SELECT [date],[stock],[close],[open],[high],[low],[ema9]	
	,CASE WHEN [ema9] > [close] THEN -1 
		  WHEN [ema9] < [close] THEN  1 ELSE 0 END position

	,CASE WHEN LAG([ema9],1,0) OVER (ORDER BY [stock], [date]) > [ema9] THEN -1
		  WHEN LAG([ema9],1,0) OVER (ORDER BY [stock], [ema9]) < [ema9] THEN  1 ELSE 0 END direction

	INTO #temp_StockTrends

	FROM [dbo].[StockHistory] WITH (NOLOCK)
	WHERE [stock] = @stock
	-- ends


	INSERT INTO @StockResults
	SELECT  [date]
	       ,[stock]
		   ,[close]
		   ,[open]
		   ,[high]
		   ,[low]
		   ,direction
		   ,position
		   ,CASE WHEN direction = 1 AND LAG(direction,1,0) OVER(ORDER BY stock, [date]) = -1 THEN 'Potencial Uptrend Reversal'
		     	 WHEN direction = -1 AND LAG(direction,1,0) OVER(ORDER BY stock, [date]) = 1 THEN 'Potencial Downtrend Reversal' ELSE 'trending' END [signal]
	
	FROM #temp_StockTrends WITH (NOLOCK)
	WHERE [date] BETWEEN DATEADD(DAY, -4,@dateref) AND @dateref				
	--IF I keep this range thatr will give me a potencial trend reversal spot				 
						 
	DROP TABLE IF EXISTS #temp_StockTrends			
	
	
	set @seq = @seq +1
	
	END

	INSERT INTO [StockReversals]
	SELECT [date],[stock],[signal] from @StockResults WHERE [date] = @dateref AND [signal]<> 'trending'


	GO
