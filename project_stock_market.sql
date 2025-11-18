/*
 ISDS 570 Data Transformation for Business - Stock Market Case
 Dr. Kalczynski

*/

-------------------------------------------------------------
-- First, let us check the stock market data we have --------
-------------------------------------------------------------

-- What is the date range?
SELECT min(date),max(date) FROM eod_quotes;

-- Really? How many companies have full data in each year?
SELECT date_part('year',date), COUNT(*)/252 FROM eod_quotes GROUP BY date_part('year',date);

-- Let's decide on some practical time range (e.g. 2015-2020)
SELECT ticker, date, adj_close FROM eod_quotes WHERE date BETWEEN '2016-01-01' AND '2021-03-26';

-- And create a (simple version of) view v_eod_quotes_2016_2021
/*
-- LIFELINE
-- DROP VIEW public.v_eod_quotes_2016_2021;

CREATE OR REPLACE VIEW public.v_eod_quotes_2016_2021 AS
 SELECT eod_quotes.ticker,
    eod_quotes.date,
    eod_quotes.adj_close
   FROM eod_quotes
  WHERE eod_quotes.date >= '2016-01-01'::date AND eod_quotes.date <= '2021-03-26'::date;

ALTER TABLE public.v_eod_quotes_2016_2021
    OWNER TO postgres;

*/

-- Check
SELECT min(date),max(date) FROM v_eod_quotes_2016_2021;

-------------------------------------------------------------
-- Next, let's us explore the required packages in R --------
-------------------------------------------------------------

-- Install PerformanceAnalytics and PortfolioAnalytics using RStudio

-- Check help

-------------------------------------------------------------------------
-- We have stock quotes but we could also use daily index data ----------
-------------------------------------------------------------------------

-- Let's download 2016-2021 of SP500TR from Yahoo https://finance.yahoo.com/quote/%5ESP500TR/history?p=^SP500TR

-- An analysis of the CSV indicated that to make it compatible with eod
-- - all unusual formatting has to be removed
-- - a "ticker" column with the value SP500TR need to be added 
-- - the volume column has to be updated (zeros are fine)

-- Import the (modified) CSV to a (new) data table eod_indices which reflects the original file's structure

/*

LIFELINE:

-- DROP TABLE public.eod_indices;

CREATE TABLE public.eod_indices
(
    symbol character varying(16) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    open real,
    high real,
    low real,
    close real,
    adj_close real,
    volume double precision,
    CONSTRAINT eod_indices_pkey PRIMARY KEY (symbol, date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.eod_indices
    OWNER to postgres;

*/

-- Check
SELECT * FROM eod_indices LIMIT 10;

-- Create a view analogous to our quotes view: v_eod_indices_2016_2021

/*
--LIFELINE
-- DROP VIEW public.v_eod_indices_2016_2021;

CREATE OR REPLACE VIEW public.v_eod_indices_2016_2021 AS
 SELECT eod_indices.symbol,
    eod_indices.date,
    eod_indices.adj_close
   FROM eod_indices
   WHERE eod_indices.date >= '2016-01-01'::date AND eod_indices.date <= '2021-03-26'::date;

   
ALTER TABLE public.v_eod_indices_2016_2021
    OWNER TO postgres;
*/

-- CHECK
SELECT MIN(date),MAX(date) FROM v_eod_indices_2016_2021;

-- We can combine the two views using UNION which help us later (this will take a while)
SELECT * FROM v_eod_quotes_2016_2021 
UNION 
SELECT * FROM v_eod_indices_2016_2021;

-------------------------------------------------------------------------
-- Next, let's prepare a custom calendar (using a spreadsheet) --------
-------------------------------------------------------------------------
-- We will use NETWORKDAYS.INTL function

-- date, y,m,d,dow,trading (format date and dow!)

-- Save as custom_calendar.csv and import to a new table

/*
LIFELINE:
-- DROP TABLE public.custom_calendar;

CREATE TABLE public.custom_calendar
(
    date date NOT NULL,
    y integer,
    m integer,
    d integer,
    dow character varying(3) COLLATE pg_catalog."default",
    trading smallint,
    CONSTRAINT custom_calendar_pkey PRIMARY KEY (date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.custom_calendar
    OWNER to postgres;

*/

-- CHECK:
SELECT * FROM custom_calendar LIMIT 10;

-- Let's add some columns to be used later: eom (end-of-month) and prev_trading_day

/*
-- LIFELINE
ALTER TABLE public.custom_calendar
    ADD COLUMN eom smallint;

ALTER TABLE public.custom_calendar
    ADD COLUMN prev_trading_day date;
*/

-- CHECK:
SELECT * FROM custom_calendar LIMIT 10;

-- Now let's populate these columns

-- Identify trading days
SELECT * FROM custom_calendar WHERE trading=1;
-- Identify previous trading days via a nested query
SELECT date, (SELECT MAX(CC.date) FROM custom_calendar CC 
			  WHERE CC.trading=1 AND CC.date<custom_calendar.date) ptd 
			  FROM custom_calendar;
-- Update the table with new data 
UPDATE custom_calendar
SET prev_trading_day = PTD.ptd
FROM (SELECT date, (SELECT MAX(CC.date) FROM custom_calendar CC WHERE CC.trading=1 AND CC.date<custom_calendar.date) ptd FROM custom_calendar) PTD
WHERE custom_calendar.date = PTD.date;
-- CHECK
SELECT * FROM custom_calendar ORDER BY date;
-- We could really use the last trading day of 2014 (as the end of the month)
INSERT INTO custom_calendar VALUES('2016-1-1',2016,01,01,'Fri',1,1,NULL);
-- Re-run the update
-- CHECK again
SELECT * FROM custom_calendar ORDER BY date;

-- Identify the end of the month
SELECT CC.date,CASE WHEN EOM.y IS NULL THEN 0 ELSE 1 END endofm FROM custom_calendar CC LEFT JOIN
(SELECT y,m,MAX(d) lastd FROM custom_calendar WHERE trading=1 GROUP by y,m) EOM
ON CC.y=EOM.y AND CC.m=EOM.m AND CC.d=EOM.lastd;
-- Update the table with new data
UPDATE custom_calendar
SET eom = EOMI.endofm
FROM (SELECT CC.date,CASE WHEN EOM.y IS NULL THEN 0 ELSE 1 END endofm FROM custom_calendar CC LEFT JOIN
(SELECT y,m,MAX(d) lastd FROM custom_calendar WHERE trading=1 GROUP by y,m) EOM
ON CC.y=EOM.y AND CC.m=EOM.m AND CC.d=EOM.lastd) EOMI
WHERE custom_calendar.date = EOMI.date;
-- CHECK
SELECT * FROM custom_calendar ORDER BY date;
SELECT * FROM custom_calendar WHERE eom=1 ORDER BY date;

------------------------------------------------------------------
-- We can now use the calendar to query prices and indexes -------
------------------------------------------------------------------

-- Calculate stock price (or index value) statistics
-- min, average, max and volatility (st.dev) of prices/index values for each month
SELECT symbol, y,m,min(adj_close) as min_adj_close,avg(adj_close) as avg_adj_close, max(adj_close) as max_adj_close,stddev_samp(adj_close) as std_adj_close 
FROM custom_calendar CC INNER JOIN v_eod_indices_2016_2021 II ON CC.date=II.date
GROUP BY symbol,y,m
ORDER BY symbol,y,m;
-- YOUR TURN: Change v_eod_indices_2015_2020 TO (SELECT * FROM v_eod_indices_2016_2021 UNION SELECT * FROM v_eod_quotes_2016_2021 WHERE ticker='AAPL')

-- Identify end-of-month prices/values for stock or index
SELECT II.*
FROM custom_calendar CC INNER JOIN v_eod_indices_2016_2021 II ON CC.date=II.date
WHERE CC.eom=1
ORDER BY II.date;
-- YOUR TURN: Identify end-of-month prices for v_eod_quotes_2016_2021

------------------------------------------------------------------
-- Determine the completeness of price or index data -------------
------------------------------------------------------------------

-- Incompleteness may be due to when the stock was listed/delisted or due to errors

-- First, let's see how many trading days were there between 2016 and 2021
SELECT COUNT(*) 
FROM custom_calendar 
WHERE trading=1 AND date BETWEEN '2016-01-01' AND '2021-03-26';

-- Now, let us check how many price items we have for each stock in the same date range
SELECT ticker,min(date) as min_date, max(date) as max_date, count(*) as price_count
FROM v_eod_quotes_2016_2021
GROUP BY ticker
ORDER BY price_count DESC;

-- Let's calculate the percentage of complete trading day prices for each stock and identify 99%+ complete
SELECT ticker
, count(*)::real/(SELECT COUNT(*) FROM custom_calendar WHERE trading=1 AND date BETWEEN '2016-01-01' AND '2021-03-26')::real as pct_complete
FROM v_eod_quotes_2016_2021
GROUP BY ticker
HAVING count(*)::real/(SELECT COUNT(*) FROM custom_calendar WHERE trading=1 AND date BETWEEN '2016-01-01' AND '2021-03-26')::real>=0.99
ORDER BY pct_complete DESC;

-- YOUR TURN: try running the above query without casting (remove ::real) - do you know why it does not work?

-- Let's store the excluded tickers (less than 99% complete in a table)
SELECT ticker, 'More than 1% missing' as reason
INTO exclusions_2016_2021
FROM v_eod_quotes_2016_2021
GROUP BY ticker
HAVING count(*)::real/(SELECT COUNT(*) FROM custom_calendar WHERE trading=1 AND date BETWEEN '2016-01-01' AND '2021-03-26')::real<0.99;

-- Also define the PK constraint for exclusions_2016_2021
/*
-- LIFELINE:
ALTER TABLE public.exclusions_2016_2021
    ADD CONSTRAINT exclusions_2016_2021_pkey PRIMARY KEY (ticker);
*/

-- YOUR TURN: apply the same procedure for the indices and store exclusions (if any) in the same table: exclusions_2016_2021

-- CHECK
SELECT * FROM exclusions_2016_2021;

-- We will be adding rows to exclusions_2015_2020 table (for other reasons) later

-- Let combine everything we have (it will take some time to execute)
SELECT * FROM v_eod_indices_2016_2021 WHERE symbol NOT IN  (SELECT DISTINCT ticker FROM exclusions_2016_2021)
UNION
SELECT * FROM v_eod_quotes_2016_2021 WHERE ticker NOT IN  (SELECT DISTINCT ticker FROM exclusions_2016_2021);

-- And let's store it as a new view v_eod_2016_2021

/*
-- LIFELINE:
-- DROP VIEW public.v_eod_2016_2021;

CREATE OR REPLACE VIEW public.v_eod_2016_2021 AS
 SELECT v_eod_indices_2016_2021.symbol,
    v_eod_indices_2016_2021.date,
    v_eod_indices_2016_2021.adj_close
   FROM v_eod_indices_2016_2021
  WHERE NOT (v_eod_indices_2016_2021.symbol::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021))
UNION
 SELECT v_eod_quotes_2016_2021.ticker AS symbol,
    v_eod_quotes_2016_2021.date,
    v_eod_quotes_2016_2021.adj_close
   FROM v_eod_quotes_2016_2021
  WHERE NOT (v_eod_quotes_2016_2021.ticker::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021));

ALTER TABLE public.v_eod_2016_2021
    OWNER TO postgres;

*/

-- CHECK:
SELECT * FROM v_eod_2016_2021; -- slow
SELECT DISTINCT symbol FROM v_eod_2016_2021;

-- Let's create a materialized view mv_eod_2016_2021

/*
--LIFELINE

-- DROP MATERIALIZED VIEW public.mv_eod_2016_2021;

CREATE MATERIALIZED VIEW public.mv_eod_2016_2021
TABLESPACE pg_default
AS
 SELECT v_eod_indices_2016_2021.symbol,
    v_eod_indices_2016_2021.date,
    v_eod_indices_2016_2021.adj_close
   FROM v_eod_indices_2016_2021
  WHERE NOT (v_eod_indices_2016_2021.symbol::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021))
UNION
 SELECT v_eod_quotes_2016_2021.ticker AS symbol,
    v_eod_quotes_2016_2021.date,
    v_eod_quotes_2016_2021.adj_close
   FROM v_eod_quotes_2016_2021
  WHERE NOT (v_eod_quotes_2016_2021.ticker::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021))
WITH NO DATA;

ALTER TABLE public.mv_eod_2016_2021
    OWNER TO postgres;
*/

-- We must refresh it (it will take time but it is one-time or infrequent)
REFRESH MATERIALIZED VIEW mv_eod_2016_2021 WITH DATA;

-- CHECK
SELECT * FROM mv_eod_2016_2021; -- faster
SELECT DISTINCT symbol FROM mv_eod_2016_2021; -- fast
SELECT * FROM mv_eod_2016_2021 WHERE symbol='AAPL' ORDER BY date;
SELECT * FROM mv_eod_2016_2021 WHERE symbol='SP500TR' ORDER BY date;

-- We can even add a couple of indexes to our materialized view if we want to speed up access some more

--------------------------------------------------------
-- Calculate daily returns or changes ------------------
--------------------------------------------------------

-- We will assume the following definition R_1=(P_1-P_0)/P_0=P_1/P_0-1.0 (P:price, i.e., adj_close)

-- First let us join the calendar with the prices (and indices)

SELECT EOD.*, CC.* 
FROM mv_eod_2016_2021 EOD INNER JOIN custom_calendar CC ON EOD.date=CC.date;

-- Next, let us use the prev_trading_day in a join to determine prev_adj_close (this will take some time)
SELECT EOD.symbol,EOD.date,EOD.adj_close,PREV_EOD.date AS prev_date,PREV_EOD.adj_close AS prev_adj_close
FROM mv_eod_2016_2021 EOD INNER JOIN custom_calendar CC ON EOD.date=CC.date
INNER JOIN mv_eod_2016_2021 PREV_EOD ON PREV_EOD.symbol=EOD.symbol AND PREV_EOD.date=CC.prev_trading_day;

-- Change the columns in the select clause to return (ret) and create another materialized view mv_ret_2016_2021
SELECT EOD.symbol,EOD.date,EOD.adj_close/PREV_EOD.adj_close-1.0 AS ret
FROM mv_eod_2016_2021 EOD INNER JOIN custom_calendar CC ON EOD.date=CC.date
INNER JOIN mv_eod_2016_2021 PREV_EOD ON PREV_EOD.symbol=EOD.symbol AND PREV_EOD.date=CC.prev_trading_day;

-- Let's make another materialized view - this time with the returns

/*
-- LIFELINE:

-- DROP MATERIALIZED VIEW public.mv_ret_2016_2021;

CREATE MATERIALIZED VIEW public.mv_ret_2016_2021
TABLESPACE pg_default
AS
 SELECT eod.symbol,
    eod.date,
    eod.adj_close / prev_eod.adj_close - 1.0::double precision AS ret
   FROM mv_eod_2016_2021 eod
     JOIN custom_calendar cc ON eod.date = cc.date
     JOIN mv_eod_2016_2021 prev_eod ON prev_eod.symbol::text = eod.symbol::text AND prev_eod.date = cc.prev_trading_day
WITH NO DATA;

ALTER TABLE public.mv_ret_2016_2021
    OWNER TO postgres;
*/

-- We must refresh it (it will take time but it is one-time or infrequent)
REFRESH MATERIALIZED VIEW mv_ret_2016_2021 WITH DATA;

-- CHECK
SELECT * FROM mv_ret_2016_2021;
SELECT * FROM mv_ret_2016_2021 WHERE symbol='AAPL' ORDER BY date;
SELECT * FROM mv_ret_2016_2021 WHERE symbol='SP500TR' ORDER BY date;

------------------------------------------------------------------
-- Identify potential errors and expand the exlusions list --------
------------------------------------------------------------------

-- Let's explore first
SELECT min(ret),avg(ret),max(ret) from mv_ret_2016_2021;
SELECT * FROM mv_ret_2016_2021 ORDER BY ret DESC;

-- Make an arbitrary decision how much daily return is too much (e.g. 100%), identify such symbols
-- and add them to exclusions_2016_2021
INSERT INTO exclusions_2016_2021
SELECT DISTINCT symbol, 'Return higher than 100%' as reason FROM mv_ret_2016_2021 WHERE ret>1.0;

-- CHECK:
SELECT * FROM exclusions_2016_2021 WHERE reason LIKE 'Return%';
-- They should be excluded BUT THEY ARE NOT!
SELECT * FROM mv_eod_2016_2021 WHERE symbol='GWPH';
SELECT * FROM mv_ret_2016_2021 WHERE symbol='GWPH' ORDER BY ret DESC;
-- IMPORTANT: we have stored (materialized) views, we need to refresh them IN A SEQUENCE!
REFRESH MATERIALIZED VIEW mv_eod_2016_2021 WITH DATA;
-- CHECK:
SELECT * FROM mv_eod_2016_2021 WHERE symbol='GWPH'; -- excluded

REFRESH MATERIALIZED VIEW mv_ret_2016_2021 WITH DATA;
-- CHECK:
SELECT * FROM mv_ret_2016_2021 WHERE symbol='GWPH'; -- excluded


-- Monthly (eom) prices export
SELECT PR.* 
INTO export_monthly_prices_2016_2021
FROM custom_calendar CC LEFT JOIN mv_eod_2016_2021 PR ON CC.date=PR.date
WHERE CC.trading=1 AND CC.eom=1;


-------------------------------------------
-- Create a role for the database  --------
-------------------------------------------
-- rolename: stockmarketreader
-- password: read123

/*
-- LIFELINE:
-- REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM stockmarketreader;
-- DROP USER stockmarketreader;

CREATE USER stockmarketreader WITH
	LOGIN
	NOSUPERUSER
	NOCREATEDB
	NOCREATEROLE
	INHERIT
	NOREPLICATION
	CONNECTION LIMIT -1
	PASSWORD 'read123';
*/

-- Grant read rights (on existing tables and views)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO stockmarketreader;

-- Grant read rights (for future tables and views)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT ON TABLES TO stockmarketreader;
   

----------------------------------
-- For homework/projects  --------
----------------------------------

SELECT LEFT(symbol,1) letter,COUNT(*) as no_of_symbols
FROM
(SELECT DISTINCT symbol FROM mv_eod_2016_2021) S
GROUP BY LEFT(symbol,1)
ORDER BY letter;

