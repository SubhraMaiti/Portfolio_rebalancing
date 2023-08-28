import pandas as pd
from pyotp import TOTP
from SmartApi import SmartConnect
import os
import json
import urllib
import datetime as dt
from dateutil.relativedelta import relativedelta
import time

def token_lookup(ticker, instrument_list, exchange="NSE"):
    for instrument in instrument_list:
        if instrument["name"] == ticker and instrument["exch_seg"] == exchange and instrument["symbol"].split("-")[-1] == "EQ":
            return instrument["token"]

def symbol_lookup(token, instrument_list, exchange="NSE"):
    for instrument in instrument_list:
        if instrument["token"] == token and instrument["exch_seg"] == exchange and instrument["symbol"].split("-")[-1] == "EQ":
            return instrument["name"]

def hist_data(tickers, st_date, end_date, interval, exchange="NSE"):
    key_path = r'D:\Personal\Learning\Python Algo Trading\Trading Robot'
    os.chdir(key_path)
    key_secret = open("Key.txt", "r").read().split()
    obj = SmartConnect(api_key=key_secret[0])
    obj.generateSession(key_secret[2],key_secret[3],TOTP(key_secret[4]).now())
    #authToken = data['data']['jwtToken']
    #refreshToken = data['data']['refreshToken']

    #check if todays json file is already in computer
    #file_name
    #Enter json file location
    file_location = ""
    ticker_file = file_location+"\\"+dt.datetime.now().strftime("%Y-%m-%d")+".txt"
    
    if os.path.isfile(ticker_file):
        with open(ticker_file, "r") as file:
            instrument_list = json.loads(file.read())
    else:
        instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = urllib.request.urlopen(instrument_url)
        instrument_list = json.loads(response.read())
        with open(ticker_file, "w") as file:
            file.write(json.dumps(instrument_list))
    
    df_data = {}
    
    for ticker in tickers:
        print(ticker)
        params = {"exchange":exchange,
                  "symboltoken": token_lookup(ticker, instrument_list),
                  "interval": interval,
                  "fromdate": st_date,
                  "todate": end_date
                  }
        stock_data = obj.getCandleData(params)
        df_data[ticker] = pd.DataFrame(stock_data["data"], columns=["Time", "Open", "High", "Low", "Close", "Volume"])
        time.sleep(0.4)
        
    return df_data

def calculate_monthly_return(df):
    all_ticker_return = {}
    monthly_ticker_return = {}
    for ticker in df.keys():
        print(ticker)
        df2 = df[ticker]
        df2['Time']=pd.to_datetime(df2['Time'])
        df2['Year-Month'] = df2['Time'].apply(lambda x: str(x.year) + ' ' + str(x.month))
        
        first_day = df2.groupby('Year-Month').nth(0)
        first_day.index = pd.RangeIndex(len(first_day.index))
        last_day = df2.groupby('Year-Month').nth(-1)
        last_day.index = pd.RangeIndex(len(last_day.index))
        
        return_matrix = pd.DataFrame()
        return_matrix['Year-Month'] = first_day['Year-Month']
        return_matrix['Open'] = first_day['Open']
        return_matrix['Monthly Return'] = last_day['Close'] - first_day['Open']
        return_matrix['Percent'] = (return_matrix['Monthly Return']/return_matrix['Open'])*100 
        all_ticker_return[ticker] = return_matrix
        
        if ticker == "BAJAJFINSV":
            for year_month in return_matrix['Year-Month']:
                monthly_ticker_return[year_month] = pd.DataFrame(columns=["Ticker", "Percent"])
            
        for index, year_month in enumerate(return_matrix['Year-Month']):
            insert = return_matrix.loc[index]['Percent']
            monthly_ticker_return[year_month].loc[len(monthly_ticker_return[year_month])] = {'Ticker': ticker, 'Percent': insert}
            
    return monthly_ticker_return
    #return all_ticker_return
    
def get_previous_year_month(year_month):
    cur_month = dt.datetime.strptime(year_month, '%Y %m')
    next_month = cur_month + relativedelta(months=-1)
    return next_month.strftime('%Y %#m')
    
        
def build_portfolio(cal_matrix, no_stocks, year_month):
    year_month = get_previous_year_month(year_month)
    df2 = cal_matrix[year_month].sort_values('Percent',ascending = False).head(no_stocks)['Ticker']
    df2.index = pd.RangeIndex(len(df2.index))
    return df2

def check_performance(portfolio, cal_matrix, keep_no_stocks, year_month):
    prev_year_month = get_previous_year_month(year_month)
    stocks = portfolio.tolist()
    df2 = cal_matrix[prev_year_month]
    df2 = df2[df2['Ticker'].isin(stocks)]
    df2 = df2.sort_values('Percent',ascending = False)
    percent = df2['Percent'].mean()
    print("Return of the Portfolio for " + prev_year_month + ": " + str(round(percent,2)))
    df2 = df2.head(keep_no_stocks)['Ticker']
    df2.index = pd.RangeIndex(len(df2.index))
    return df2, percent

def rebalance_portfolio(portfolio, cal_matrix, no_stocks, year_month):
    keep_no_stocks = len(portfolio) - no_stocks
    portfolio, a = check_performance(portfolio, cal_matrix, keep_no_stocks, year_month) 
    add_portfolio = build_portfolio(cal_matrix, no_stocks, year_month)
    portfolio = portfolio._append(add_portfolio)
    portfolio.index = pd.RangeIndex(len(portfolio.index))
    print(portfolio.tolist())
    return portfolio, a

def test_strategy():
    start_month = "2018 2"
    
    portfolio = build_portfolio(cal_matrix, 6, start_month)
    print(portfolio.tolist())
    
    a = []
    for start_month in pd.date_range('2018-03-01', '2022-12-31', freq='MS'):
        rebalance_month = start_month.strftime('%Y %#m')
        print(rebalance_month)
        portfolio, percent = rebalance_portfolio(portfolio, cal_matrix, 3, rebalance_month)
        a.append(percent)
        print("========")
    
    return a

tickers_list = ["BAJAJFINSV", "ASIANPAINT", "BAJFINANCE", "BHARTIARTL", "TITAN", 
              "AXISBANK", "ICICIBANK", "KOTAKBANK", "TCS", "INFY",
              "NESTLEIND", "RELIANCE", "HINDUNILVR", "MARUTI", "WIPRO",
              "TECHM", "TATAMOTORS", "SUNPHARMA", "HDFCBANK", "TATASTEEL", 
              "SBIN", "NTPC", "M&M", "HCLTECH", "ITC",
              "INDUSINDBK", "ULTRACEMCO", "POWERGRID", "LT", "JSWSTEEL", "LIQUIDBEES"]
df = hist_data(tickers_list, "2018-01-01 09:15", "2022-12-31 03:30", "ONE_DAY")
cal_matrix = calculate_monthly_return(df)
a = test_strategy()
