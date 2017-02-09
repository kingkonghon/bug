import pandas as pd
#import pandas.io.data as web
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import datetime
import math
'''
reader = pd.read_csv('F:\\testdata\\au_overnight_interest_rates.csv',header=None,index_col=0,parse_dates=True)
interest_rates_buffer = pd.DataFrame(reader)
interest_rates_buffer.columns = ['AUDIR']
#interest_rates_buffer.index += datetime.timedelta(hours=14,minutes=30)
interest_rates_buffer.index += datetime.timedelta(hours=2)

reader = pd.read_csv('F:\\AUDUSD_H4_UTC+0_00.csv',header=None)
temp_date = np.array(pd.to_datetime(reader[0] + ' ' + reader[1], format='%Y.%m.%d %H:%M'))
reader.index = temp_date
price = pd.DataFrame(reader[5])
price.columns = ['AUDUSD']

interest_rates_buffer['before'] = np.zeros(interest_rates_buffer['AUDIR'].size)
interest_rates_buffer['after'] = np.zeros(interest_rates_buffer['AUDIR'].size)
interest_rates_buffer['extreme'] = np.zeros(interest_rates_buffer['AUDIR'].size)
interest_rates_buffer['count'] = np.zeros(interest_rates_buffer['AUDIR'].size)
interest_rates_buffer['correct'] = np.zeros(interest_rates_buffer['AUDIR'].size)



for i in range(1,interest_rates_buffer['AUDIR'].size):
    start_time = interest_rates_buffer.index[i]
    end_time = start_time + datetime.timedelta(hours=8)
    folling_price = price['AUDUSD'].loc[start_time:end_time]
    if interest_rates_buffer['AUDIR'][i] != interest_rates_buffer['AUDIR'][i-1]:
        interest_rates_buffer['count'][i] = 1
        interest_rates_buffer['before'][i] = folling_price[0]
        interest_rates_buffer['after'][i] = folling_price[-1]
        if (interest_rates_buffer['AUDIR'][i] > interest_rates_buffer['AUDIR'][i-1]):
            interest_rates_buffer['extreme'][i] = folling_price.max()
            if (folling_price[-1] > folling_price[0]):
                interest_rates_buffer['correct'][i] = 1
        elif (interest_rates_buffer['AUDIR'][i] < interest_rates_buffer['AUDIR'][i-1]):
            interest_rates_buffer['extreme'][i] = folling_price.min()
            if (folling_price[-1] < folling_price[0]):
                interest_rates_buffer['correct'][i] = 1
    #interest_rates_buffer['extreme'][i] = extreme.max

print interest_rates_buffer

print interest_rates_buffer['correct'].sum(), interest_rates_buffer['count'].sum()
print interest_rates_buffer['correct'].sum()/interest_rates_buffer['count'].sum()
'''
''' ------------------------------------reaction------------------------------------------------------
'''
'''
reader = pd.read_csv('F:\\AUDUSD_H4_UTC+0_00.csv',header=None)
temp_date = np.array(pd.to_datetime(reader[0] + ' ' + reader[1], format='%Y.%m.%d %H:%M'))
reader.index = temp_date
price = pd.DataFrame(reader[5])
price.columns = ['AUDUSD']

interest_rates_buffer = pd.read_csv('F:\\us_overnight_interest_rates.csv',header=0,index_col=0,parse_dates=True)
price = price.join(interest_rates_buffer,how='outer')
price.columns = ['AUDIR','USDIR']
price['USDIR'] = price['USDIR'].ffill()
price = price.dropna(how='any')

price_buffer = pd.read_csv('F:\\AUDUSD_H4_UTC+0_00.csv',header=None)
temp_date = np.array(pd.to_datetime(price_buffer[0] + ' ' + price_buffer[1], format='%Y.%m.%d %H:%M'))
price_buffer.index = temp_date
price = price.join((price_buffer[5]))
price = price.ffill()
price.columns = ['AUDIR','USDIR','AUDUSD']

rets_aud = np.log(price['AUDUSD']/price['AUDUSD'].shift(1))
rets_nzd = np.log(price['NZDUSD']/price['NZDUSD'].shift(1))
rets_aud = rets_aud.dropna(how='any')
rets_nzd = rets_nzd.dropna(how='any')
x = np.column_stack(np.log(price['AUDUSD'],price['AUDIR'][:-1],price['USDIR'][:-1],price['NZDIR'][:-1]))
x = sm.add_constant(x)
model = sm.OLS(rets_aud,x)
result = model.fit()
print result.summary()
intercept = result.params[0]
beta = result.params[1]
coef_aud_ir = result.params[2]
coef_usd_ir = result.params[3]
coef_nzd_ir = result.params[4]

sample = price.loc['20030801':'20160701']
indicator = np.log(sample['AUDUSD']) - beta * np.log(sample['NZDUSD'])
len = indicator.size
indicator.name = 'indicator'
indicator = pd.DataFrame(indicator)
indicator['adjuster'] = coef_usd_ir *sample['USDIR'] + coef_nzd_ir * sample['NZDIR'] + coef_aud_ir * sample['AUDIR'] + intercept
indicator['adjuster'][0] = 0
indicator['adjuster'] = indicator['adjuster'].cumsum()
indicator['ad_indicator'] = indicator['indicator'] - indicator['adjuster']
indicator['mean'] = pd.rolling_mean(indicator['ad_indicator'],window=24)

plt.plot(indicator['ad_indicator'])
plt.plot(indicator['indicator'])
plt.plot(indicator['mean'])
plt.legend(loc=0)
plt.show()
'''
'''
          -------------------------download data--------------------------------
start_date = datetime.datetime(2003,8,1)
end_date = datetime.datetime(2016,7,10)
price = web.DataReader('^HGU16.CMX','yahoo',start_date,end_date)

print price
#price.to_csv('F:\\testdata\\sp500.csv',header=True,index=True)
'''
'''          -------------------------prediction first--------------------------------
reader = pd.read_csv('F:\\ad_au_overnight_interest_rates.csv',header=None,index_col=0,parse_dates=True)
interest_buffer = pd.DataFrame(reader)
interest_buffer.columns = ['AUDIR']

reader = pd.read_csv('F:\\testdata\\us_overnight_interest_rates.csv',header=0,index_col=0,parse_dates=True)
interest_buffer = interest_buffer.join(reader,how='outer')
interest_buffer.columns = ['AUDIR','USDIR']
interest_buffer = interest_buffer.ffill()
interest_buffer = interest_buffer.dropna(how='any')
interest_buffer['IRDIF'] = interest_buffer['USDIR'] - interest_buffer['AUDIR']
interest_buffer['IRC'] = interest_buffer['IRDIF'] - interest_buffer['IRDIF'].shift(1)

reader = pd.read_csv('F:\\testdata\\AUDUSD_H4_UTC+0_00.csv',header=None,index_col=None)
temp_date = np.array(pd.to_datetime(reader[0] + ' ' + reader[1], format='%Y.%m.%d %H:%M'))
price_buffer = pd.DataFrame(reader[5])
price_buffer.index = temp_date
price_buffer.columns = ['AUDUSD']

price_buffer = price_buffer.join(interest_buffer['IRDIF'],how='outer')

reader = pd.read_csv('F:\\testdata\\XAUUSD_H4_UTC+0_00_030101_160709.csv',header=None,index_col=None)
temp_date = np.array(pd.to_datetime(reader[0] + ' ' + reader[1], format='%Y.%m.%d %H:%M'))
reader.index = temp_date
price_buffer = price_buffer.join(reader[5],how='outer')
price_buffer.columns = ['AUDUSD','IRDIF','XAUUSD']

reader = pd.read_csv('F:\\testdata\\atx_index.csv',header=0,index_col=0,parse_dates=True)
price_buffer = price_buffer.join(reader['Adj Close'].shift(1),how='outer')
price_buffer.columns = ['AUDUSD','IRDIF','XAUUSD','ATX']

price_buffer = price_buffer.join(reader['Adj Close'].shift(2),how='outer')
price_buffer.columns = ['AUDUSD','IRDIF','XAUUSD','ATX','ATXL1']
#print price_audusd

reader = pd.read_csv('F:\\testdata\\sp500.csv',header=0,index_col=0,parse_dates=True)
price_buffer = price_buffer.join(reader['Adj Close'].shift(1),how='outer')
price_buffer.columns = ['AUDUSD','IRDIF','XAUUSD','ATX','ATXL1','SP500']

price_buffer = price_buffer.join(reader['Adj Close'].shift(2),how='outer')
price_buffer.columns = ['AUDUSD','IRDIF','XAUUSD','ATX','ATXL1','SP500','SP500L1']
price_buffer = price_buffer.ffill()
price_buffer = price_buffer.dropna(how='any')

y = price_buffer['AUDUSD'][1:]
x = np.column_stack((price_buffer['IRDIF'][1:],price_buffer['XAUUSD'].shift(1)[1:],price_buffer['ATX'][1:],price_buffer['ATXL1'][1:],price_buffer['SP500'][1:],price_buffer['SP500L1'][1:]))
x = sm.add_constant(x)
model = sm.OLS(y,x)
result = model.fit()
print result.summary()
'''





'''
coef = result.params

price_buffer['prediction'] = coef[0] + coef[1] * price_buffer['IRDIF'][1:] + coef[2] * price_buffer['XAUUSD'].shift(1)[1:] + coef[3] * price_buffer['ATX'][1:] + coef[4] * price_buffer['SP500']

price_buffer[['AUDUSD','prediction']].plot()
plt.show()
'''
'''
y = price_buffer['AUDUSD'][1:]
x = np.column_stack((price_buffer['IRDIF'],price_buffer['XAUUSD'].shift(1)[1:],price_buffer['ATX'],price_buffer['SP500']))
x = sm.add_constant(x)
model = sm.OLS(y,x)
result = model.fit()
print result.summary()

'''
'''
y = np.log(price_audusd['AUDUSD'][2:])
x = np.column_stack((price_audusd['AUDIR'][1:-1],price_audusd['AUDIR'].shift(1)[1:-1],price_audusd['USDIR'][1:-1],price_audusd['USDIR'].shift(1)[1:-1])) #price_audusd['IRDIF'][1:-1]

x = sm.add_constant(x)
model = sm.OLS(y,x)
result = model.fit()
print result.summary()

'''

reader = pd.read_csv('F:\\testdata\\EURUSD_D1.csv',header=None,index_col=None)
temp_date = np.array(pd.to_datetime(reader[0] + ' ' + reader[1], format='%Y.%m.%d %H:%M'))
price_audusd = pd.DataFrame(reader[5])
price_audusd.index = temp_date
price_audusd.columns = ['EURUSD']

reader = pd.read_csv('F:\\testdata\\eur_overnight.csv',header=0,index_col=0,parse_dates=True)
price_audusd = price_audusd.join(reader,how='outer')
price_audusd.columns = ['EURUSD','EURIR']

reader = pd.read_csv('F:\\testdata\\usd_overnight.csv',header=0,index_col=0,parse_dates=True)
price_audusd = price_audusd.join(reader,how='outer')
price_audusd.columns = ['EURUSD','EURIR','USDIR']

price_audusd = price_audusd.ffill()
price_audusd = price_audusd.dropna(how='any')

for i in range(price_audusd.index.size-1,-1,-1):
    if (price_audusd['EURIR'][i] == '.') or (price_audusd['USDIR'][i] == '.'):
        price_audusd = price_audusd.drop(price_audusd.index[i],0)

price_audusd['EURIR'] = pd.to_numeric(price_audusd['EURIR'])
price_audusd['USDIR'] = pd.to_numeric(price_audusd['USDIR'])

price_audusd['days'] = np.zeros(price_audusd.index.size)
for i in range(1,price_audusd.index.size):
    price_audusd['days'][i] = (price_audusd.index[i] - price_audusd.index[i-1]).days

x = (np.log((1+price_audusd['USDIR'].shift(1)*price_audusd['days']/365) / (1+price_audusd['EURIR'].shift(1)*price_audusd['days']/365)))[1:]
y = (np.log(price_audusd['EURUSD'] / price_audusd['EURUSD'].shift(1)))[1:]

x = sm.add_constant(x)
model = sm.OLS(y,x)
result = model.fit()
print result.llf
print result.summary()
alpha = result.params[0]
beta = result.params[1]

price_audusd['predict'] = math.exp(alpha) * (1+price_audusd['USDIR'].shift(1)*price_audusd['days']/365) / (1+price_audusd['EURIR'].shift(1)*price_audusd['days']/365) * price_audusd['EURUSD'].shift(1)
price_audusd[['EURUSD','predict']].plot()
plt.show()
'''
print price_audusd

y = (np.log(price_audusd['EURUSD']/price_audusd['EURUSD'].shift(1)))[1:]
x = (price_audusd['IRDIF'].shift(1))[1:]
x = sm.add_constant(x)
model = sm.OLS(y,x)
result = model.fit()
print result.summary()
'''
'''
reader = pd.read_csv('F:\\testdata\\atx_index.csv',header=0,index_col=0,parse_dates=True)
price_audusd = price_audusd.join(np.log(reader['Adj Close']),how='outer')
price_audusd.columns = ['AUDUSD','AUDIR','USDIR','ATX']
price_audusd = price_audusd.ffill()
price_audusd = price_audusd.dropna(how='any')
#print price_audusd

reader = pd.read_csv('F:\\testdata\\sp500.csv',header=0,index_col=0,parse_dates=True)
price_audusd = price_audusd.join(np.log(reader['Adj Close']),how='outer')
price_audusd.columns = ['AUDUSD','AUDIR','USDIR','ATX','SP500']
price_audusd = price_audusd.ffill()
price_audusd = price_audusd.dropna(how='any')


y = (price_audusd['AUDUSD'] - price_audusd['AUDUSD'] .shift(1))[1:]
x = np.column_stack((price_audusd['IRDIF'].shift(1)[1:],price_audusd.shift(1)['ATX'][1:],price_audusd.shift(1)['SP500'][1:] ))
x = sm.add_constant(x)
model = sm.OLS(y,x)
result = model.fit()
print result.summary()


#price_audusd[['AUDUSD','IRDIF','ATX','SP500']].plot(subplots=True)
#plt.show()

rets_aud = np.log(price_audusd['AUDUSD']/price_audusd['AUDUSD'].shift(1))
rets_atx = np.log(price_audusd['ATX']/price_audusd['ATX'].shift(1))
rets_sp500 = np.log(price_audusd['SP500']/price_audusd['SP500'].shift(1))

x = np.column_stack((rets_atx[1:-1],rets_sp500[1:-1])) #price_audusd['IRDIF'][1:-1]
x = sm.add_constant(x)
model = sm.OLS(rets_aud[2:],x)
result = model.fit()
print result.summary()

'''