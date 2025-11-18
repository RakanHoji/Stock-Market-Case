# Project ISDS 570

# Stock Market Case in R
rm(list=ls(all=T)) # this just removes everything from memory

# Prepare a CSV file daily_prices_2015_2020.csv 
# in pgAdmin

# Load CSV Files ----------------------------------------------------------
#
# Load daily prices from CSV - no parameters needed
dp<-read.csv('/Users/rakanidrissi/Desktop/CSUF/ISDS 570/data/monthly_prices_2015_2020.csv') # no arguments

#Explore
head(dp) #first few rows
tail(dp) #last few rows
nrow(dp) #row count
ncol(dp) #column count

#remove the last row (because it was empty/errors)
dp<-head(dp,-1)

#This is an easy way (csv) but we are not going to use it here
rm(dp) # remove from memory
#We are going to perform most of the transformation tasks in R

# Connect to PostgreSQL ---------------------------------------------------

# Make sure you have created the reader role for our PostgreSQL database
# and granted that role SELECT rights to all tables
# Also, make sure that you have completed (or restored) Part 3b db

# ONLY IF YOU STILL AN AUTHENTICATION ERROR:
# Try changing the authentication method from scram-sha-256 to md5 or trust (note: trust is not a secure connection, use only for the purpose of completing the class)
# this is done by editing the last lines of the pg_hba.conf file,
# which is stored in C:\Program Files\PostgreSQL\14\data (for version 14)
# Restart the computer after the change


require(RPostgres) # did you install this package?
require(DBI)
conn <- dbConnect(RPostgres::Postgres()
                  ,user="stockmarketreader"
                  ,password="read123"
                  ,host="localhost"
                  ,port=5432
                  ,dbname="stockmarket"
)

#custom calendar
qry<-'SELECT * FROM custom_calendar ORDER by date' # Original Query

ccal<-dbGetQuery(conn,qry)
#eod prices and indices
qry1="SELECT symbol,eod_indices.date,adj_close FROM eod_indices INNER JOIN custom_calendar ON eod_indices.date = custom_calendar.date WHERE eod_indices.date BETWEEN '2014-12-31' AND '2020-12-31' and eom=1"
qry2="SELECT ticker,eod_quotes.date,adj_close FROM eod_quotes INNER JOIN custom_calendar ON eod_quotes.date = custom_calendar.date WHERE eod_quotes.date BETWEEN '2014-12-31' AND '2020-12-31' and eom=1"
eom<-dbGetQuery(conn,paste(qry1,'UNION',qry2))
dbDisconnect(conn)
rm(conn)

#Explore
head(ccal)
tail(ccal)
nrow(ccal)

head(eom)
tail(eom)
nrow(eom)

head(eom[which(eom$symbol=='SP500TR'),])

#For monthly we may need one more data item (for 2014-12-31)
#We can add it to the database (INSERT INTO) - but to practice:
eom_row<-data.frame(symbol='SP500TR',date=as.Date('2014-12-31'),adj_close=3769.44)
eom<-rbind(eom,eom_row)
tail(eom)

# Use Calendar --------------------------------------------------------

tmonthly<-ccal[which(ccal$trading==1 & ccal$eom==1),,drop=F]
head(tmonthly)
nrow(tmonthly)-1 #trading days between 2015 and 2020

# Completeness ----------------------------------------------------------
# Percentage of completeness
pct<-table(eom$symbol)/(nrow(tmonthly)-1)
selected_symbols_monthly<-names(pct)[which(pct>=0.99)]
eom_complete<-eom[which(eom$symbol %in% selected_symbols_monthly),,drop=F]

#check
head(eom_complete)
tail(eom_complete)
nrow(eom_complete)

#YOUR TURN: perform all these operations for monthly data
#Create eom and eom_complete
#Hint: which(ccal$trading==1 & ccal$eom==1)

# Transform (Pivot) -------------------------------------------------------

require(reshape2) #did you install this package?
eom_pvt<-dcast(eom_complete, date ~ symbol,value.var='adj_close',fun.aggregate = mean, fill=NULL)
#check
eom_pvt[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eom_pvt) # column count
nrow(eom_pvt)

# YOUR TURN: Perform the same set of tasks for monthly prices (create eom_pvt)

# Merge with Calendar -----------------------------------------------------
eom_pvt_complete<-merge.data.frame(x=tmonthly[,'date',drop=F],y=eom_pvt,by='date',all.x=T)

#check
eom_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eom_pvt_complete)
nrow(eom_pvt_complete)

#use dates as row names and remove the date column
rownames(eom_pvt_complete)<-eom_pvt_complete$date
eom_pvt_complete$date<-NULL #remove the "date" column

#re-check
eom_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eom_pvt_complete)
nrow(eom_pvt_complete)

# Missing Data Imputation -----------------------------------------------------
# We can replace a few missing (NA or NaN) data items with previous data
# Let's say no more than 3 in a row...
require(zoo)
eom_pvt_complete<-na.locf(eom_pvt_complete,na.rm=F,fromLast=F,maxgap=3)
#re-check
eom_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eom_pvt_complete)
nrow(eom_pvt_complete)

# Calculating Returns -----------------------------------------------------
require(PerformanceAnalytics)
eom_ret<-CalculateReturns(eom_pvt_complete)

#check
eom_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eom_ret)
nrow(eom_ret)

#remove the first row
eom_ret<-tail(eom_ret,-1) #use tail with a negative value
#check
eom_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eom_ret)
nrow(eom_ret)

# YOUR TURN: calculate eom_ret (monthly returns)

# Check for extreme returns -------------------------------------------
# There is colSums, colMeans but no colMax so we need to create it
colMax <- function(data) sapply(data, max, na.rm = TRUE)
# Apply it
max_monthly_ret<-colMax(eom_ret)
max_monthly_ret[1:10] #first 10 max returns
# And proceed just like we did with percentage (completeness)
selected_symbols_monthly<-names(max_monthly_ret)[which(max_monthly_ret<=1.00)]
length(selected_symbols_monthly)

#subset eod_ret
eom_ret<-eom_ret[,which(colnames(eom_ret) %in% selected_symbols_monthly),drop=F]
#check
eom_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eom_ret)
nrow(eom_ret)

#YOUR TURN: subset eom_ret data

# Export data from R to CSV -----------------------------------------------
write.csv(eom_ret,'/Users/rakanidrissi/Desktop/CSUF/ISDS 570/Dropped_Tables/eom.ret.cvs')

# You can actually open this file in Excel!


# Tabular Return Data Analytics -------------------------------------------

# We will select 'SP500TR' and 12/3 RANDOM TICKERS
set.seed(100) # seed can be any number, it will ensure repeatability
random12 <- sample(colnames(eom_ret)[grep("^A",colnames(eom_ret))],3) 
# We need to convert data frames to xts (extensible time series)
Ra<-as.xts(eom_ret[,random12,drop=F])
Rb<-as.xts(eom_ret[,'SP500TR',drop=F]) #benchmark

head(Ra)
head(Rb)

# And now we can use the analytical package...

# Stats
table.Stats(Ra)

# Distributions
table.Distributions(Ra)

# Returns
table.AnnualizedReturns(cbind(Rb,Ra),scale=12) # note for monthly use scale=12

# Accumulate Returns
acc_Ra<-Return.cumulative(Ra)
acc_Rb<-Return.cumulative(Rb)


# Capital Assets Pricing Model
table.CAPM(Ra,Rb)

# YOUR TURN: try other tabular analyses

# Graphical Return Data Analytics -----------------------------------------

# Cumulative returns chart
chart.CumReturns(Ra,legend.loc = 'topleft')
chart.CumReturns(Rb,legend.loc = 'topleft')
chart.CumReturns(cbind(Rb,Ra), legend.loc = 'topleft')

#Box plots
chart.Boxplot(cbind(Rb,Ra))

chart.Drawdown(Ra,legend.loc = 'bottomleft')

# YOUR TURN: try other charts

# MV Portfolio Optimization -----------------------------------------------

# withhold the last 253 trading days
Ra_training<-head(Ra,-12)
Rb_training<-head(Rb,-12)

# use the last 253 trading days for testing
Ra_testing<-tail(Ra,12)
Rb_testing<-tail(Rb,12)

#optimize the MV (Markowitz 1950s) portfolio weights based on training
table.AnnualizedReturns(Rb_training)
mar<-mean(Rb_training) #we need daily minimum acceptable return

require(PortfolioAnalytics)
require(ROI) # make sure to install it
require(ROI.plugin.quadprog)  # make sure to install it
pspec<-portfolio.spec(assets=colnames(Ra_training))
pspec<-add.objective(portfolio=pspec,type="risk",name='StdDev')
pspec<-add.constraint(portfolio=pspec,type="full_investment")
pspec<-add.constraint(portfolio=pspec,type="return",return_target=mar)

#optimize portfolio
opt_p<-optimize.portfolio(R=Ra_training,portfolio=pspec,optimize_method = 'ROI')

#extract weights (negative weights means shorting)
opt_w<-opt_p$weights

# YOUR TURN: try adding the long-only constraint and re-optimize the portfolio

#apply weights to test returns
Rp<-Rb_testing # easier to apply the existing structure
#define new column that is the dot product of the two vectors
Rp$ptf<-Ra_testing %*% opt_w

#check
head(Rp)
tail(Rp)

#Compare basic metrics
table.AnnualizedReturns(Rp)

# Chart Hypothetical Portfolio Returns ------------------------------------

chart.CumReturns(Rp,legend.loc = 'bottomright')

# End of Part 3c
# End of Stock Market Case Study 