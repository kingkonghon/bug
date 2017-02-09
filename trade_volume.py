import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

price_buffer = pd.read_csv('F:\\testdata\\EURUSD_M1_UTC+0_00_150101_160901.csv',header=None,index_col=None)
temp_date = np.array(pd.to_datetime(price_buffer[0] + ' ' + price_buffer[1], format='%Y.%m.%d %H:%M'))
price_buffer = price_buffer.drop(0,1)
price_buffer = price_buffer.drop(1,1)
#price_buffer['close'] = reader[5]
#price_buffer['high'] = reader[3]
#price_buffer['low'] = reader[4]
#price_buffer['volume'] = reader[6]
price_buffer.columns = ['open','high','low','close','volume']
price_buffer.index = temp_date
'''
price_buffer['high_gap'] = price_buffer['high'] - price_buffer['open']
price_buffer['low_gap'] = price_buffer['open'] - price_buffer['low']

criterion = price_buffer['low_gap'].map(lambda x:x >= 0.00013)
open_order = price_buffer[criterion]
criterion = open_order['high_gap'].map(lambda x:x < 0.00013)
losing_order = open_order[criterion]

losing_percent = losing_order.index.size / float(open_order.index.size)
average_loss = losing_order['low_gap'].sum() / losing_order.index.size
expectation = 0.00013 * (1 - losing_percent) - average_loss * losing_percent

print losing_percent, losing_order['low_gap'].max(), average_loss, expectation



num, bins, patches = plt.hist(price_buffer['low_gap'],bins=1000)
dis = pd.Series(num)
dis.index = bins[1:]
dis = dis / price_buffer['low_gap'].size
print dis
'''

'''
####minute returns for daily mean change
rets = price_buffer['close'] - price_buffer['close'].shift(1)

start_t = price_buffer.index[1]
time_index = [start_t] * 365
value = pd.Series(np.zeros(365))

for i in range(0,365):
    td = pd.Timedelta(days=1)
    end_t = start_t + td

    subperiod_data =  rets.loc[start_t:end_t]
    value[i] = subperiod_data.mean()

    start_t = end_t
    time_index[i] = end_t

value.index = time_index
value = value.dropna(how='any')

#mean_changes = value - value.shift(1)

num, bins, patches = plt.hist(value,bins=100)

d = {'bins':bins[1:], 'num':num/value.index.size}
dis = pd.DataFrame(data=d)

cum_pro = 0
i = 1
while cum_pro < 0.9:
    threshold = 0.00001 * i
    criterion = dis['bins'].map(lambda x:x >= -threshold and x <=threshold)
    normal = dis[criterion]

    cum_pro = normal['num'].sum()
    print cum_pro
    i +=1
#print pd.rolling_std(mean_changes[1:],100)
print threshold
criterion = value.map(lambda x:x < -threshold or x > threshold)
signal = value[criterion]
signal = signal.to_frame()
signal = signal.join(price_buffer['close'])
print signal



#value.plot()
plt.show()
'''
start_t = price_buffer.index[0]
time_index = [start_t] * 365
value = pd.Series(np.zeros(365))

for i in range(0,365):
    td = pd.Timedelta(hours=1)
    end_t = start_t + td

    subperiod_data =  price_buffer['close'].loc[start_t:end_t]
    value[i] = subperiod_data.mean()

    start_t = end_t
    time_index[i] = end_t

value.index = time_index
value = value.dropna(how='any')

