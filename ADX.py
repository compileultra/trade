#!/usr/bin/python
# coding: utf-8

import pandas as pd
import datetime



file1_name = "daily_trading_info_2014-2018.xlsx"
file2_name = "daily_trading_info_2019-2020.xlsx"

file1 = pd.read_excel(file1_name, sheet_name = None)
file2 = pd.read_excel(file2_name, sheet_name = None)

companies = []
file1_index = []
stock1_index = list(file1.keys())[:]
file2_index = []
stock2_index = list(file2.keys())[:]

import copy
companies = copy.deepcopy(stock2_index)


print('Number of stocks in 2014-2018 excel: ',len(stock1_index))
print('Number of stocks in 2019-2020 excel: ',len(stock2_index))
print('Number of stocks in current run: ',len(companies))

for i in range(0, len(stock1_index)):
    if(stock1_index[i] not in companies):
        print('Number of stocks in 2014-2018 but not in current run: ',stock1_index[i])


# Find companies after 2018
rest_comy = []
for i in range(0, len(stock2_index)):
    if(stock2_index[i] not in stock1_index):
        print(stock2_index[i])
        rest_comy.append(stock2_index[i])
print(len(rest_comy))

# Write out excels for companies after 2018
result_name = []
result_sheet = []

for i in range(0, len(rest_comy)):
    result_name.append(rest_comy[i])
    result_sheet.append(file2[rest_comy[i]].iloc[:,1:])

for w in range(0, len(result_sheet)):
    result_sheet[w].to_excel(result_name[w]+".xlsx", sheet_name = result_name[w], index=False)
    print(w, result_name[w])

# Concatenate company files if it has for 2014-2018
result_name = []
result_sheet = []
# column mismatch at sheet 0
result_name.append(stock1_index[0])
a = file1[stock1_index[0]].iloc[:,1:-1]
b = file2[stock1_index[0]].iloc[:,1:]
result_sheet.append( pd.concat([a, b]) )

for i in range(1, len(stock1_index)):
    result_name.append(stock1_index[i])
    a = file1[stock1_index[i]].iloc[:,1:]
    b = file2[stock1_index[i]].iloc[:,1:]
    #print('this is a',i)
    #print(a)
    #print('this is b',i)
    #print(b)
    result_sheet.append( pd.concat([a, b]) )
print(len(result_name))
print(len(result_sheet))

for w in range(0, len(result_sheet)):
    result_sheet[w].to_excel(result_name[w]+".xlsx", sheet_name = result_name[w], index=False)
    if(w%100==0):
        print(w,result_name[w])

# Set company index to post-2018 since it includes all 800 companies
company_index = []
company_index = list(file2.keys())[:]

from talib import abstract
for w in range(0, len(company_index)):
    df = pd.read_excel(company_index[w]+".xlsx")
    inputs = {
    'open': df["open"],
    'high': df["high"],
    'low':  df["low"],
    'close': df["close"],
    'volume': df["volume"]
    }
    # uses close prices (default)
    out5ma   = abstract.SMA(inputs, timeperiod=5)
    out10ma  = abstract.SMA(inputs, timeperiod=10)
    out20ma  = abstract.SMA(inputs, timeperiod=20)
    out60ma  = abstract.SMA(inputs, timeperiod=60)
    out120ma = abstract.SMA(inputs, timeperiod=120)
    outRSI   = abstract.RSI(inputs,timeperiod=14)
    outADX   = abstract.ADX(inputs, timperiod = 14)
    outema   = abstract.EMA(inputs, timeperiod=30)
    outMACD  = abstract.MACD(inputs, fastperiod=12, slowperiod=26, signalperiod=9)
    #MACD,MACDsignal,MACDhist
    outMACDFix=abstract.MACDFIX(inputs,signalperiod=9)
    #outKD=abstract.STOCH(inputs, 5, 3, 0, 3, 0,prices=['high', 'low', 'open'])
    outKD    = abstract.STOCH(inputs, 5, 3, 0, 3, 0,prices=['high', 'low', 'open'])
    a = pd.DataFrame(out5ma, columns=["5MA"])
    b = pd.DataFrame(out60ma, columns=["10MA"])
    c = pd.DataFrame(out120ma, columns=["20MA"])
    d = pd.DataFrame(out20ma, columns=["60MA"])
    e = pd.DataFrame(out20ma, columns=["120MA"])
    f = pd.DataFrame(outRSI, columns=["RSI"])
    g = pd.DataFrame(outADX, columns=["ADX"])
    h = pd.DataFrame(outema, columns=["EMA"])
    i = pd.DataFrame(map(list, zip(*outMACD)),columns=["MACD","MACDsignal","MACDhist"])
    j = pd.DataFrame(map(list, zip(*outKD)),columns=["slowK","slowD"])
    pd.concat([df,a,b,c,d,e,f,g,h,i,j], axis=1).to_excel(company_index[w]+".xlsx",sheet_name = company_index[w], index=False)
    if(w%100==0):
        print(w)

# Prepare train data
train_set = pd.read_excel("strategy_results_train.xlsx")
dataset = copy.deepcopy(train_set)
temp = []
for i in range(0, dataset.shape[0]):
#for i in range(0, 60):
# for i in range(0, 1):
    if(i%1000==0):
        print(i, datetime.datetime.now())
    trade_company = dataset.iloc[i][0].split("_")[0]
    trade_date    = dataset.iloc[i][0].split("_")[1]
    df = pd.read_excel(trade_company+".xlsx")
    trade_date_datetime = datetime.date(int(trade_date[:4]), int(trade_date[4:]), 1) - datetime.timedelta(days=1)
    lower_index = 0
    for index in range(0, df.shape[0]):
        if(df["date"].iloc[index]< trade_date_datetime):
            lower_index = index
        else:
            break
    temp.append(list(df.iloc[lower_index, :]))
pd.DataFrame(temp).to_excel("train_output.xlsx", index=False)


# Prepare test dataset
testset = pd.read_excel("sampleSubmission.xlsx")
temp = []
for i in range(1, testset.shape[0]):
# for i in range(0, 10):
    if(i%1000==0):
        print(i, datetime.datetime.now())
    trade_company = testset.iloc[i][0].split("_")[0]
    trade_date    = testset.iloc[i][0].split("_")[1]
    df = pd.read_excel(trade_company+".xlsx")
    trade_date_datetime = datetime.date(int(trade_date[:4]), int(trade_date[4:]), 1) - datetime.timedelta(days=1)
    lower_index = 0
    for index in range(0, df.shape[0]):
        if(df["date"].iloc[index]< trade_date_datetime):
            lower_index = index
        else:
            break
    if(lower_index==0):
        temp.append(dash = ["-","","","","","","","",""])
    else:
        temp.append(list(df.iloc[lower_index, :]))
pd.DataFrame(temp).to_excel("test_output.xlsx", index=False)
pd.DataFrame(temp).to_csv("test_output.csv", index=False)

