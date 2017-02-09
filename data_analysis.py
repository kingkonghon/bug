import pandas as pd
import matplotlib.pyplot as plt
'''
price_buffer = pd.read_csv('F:\\testdata\\trade_record.csv',header=None,index_col=None)

criterion = price_buffer[4].map(lambda x:x == 0.1)
first_order = price_buffer[criterion]
fir_order_num = first_order.index.size
fir_profit = first_order[5].sum()
criterion = first_order[5].map(lambda x:x > 0)
winning_fir_order = first_order[criterion]
win_fir_order_num = winning_fir_order.index.size
print fir_order_num, win_fir_order_num, fir_profit

criterion = price_buffer[4].map(lambda x:x == 0.2)
second_order = price_buffer[criterion]
sec_order_num = second_order.index.size
sec_profit = second_order[5].sum()
criterion = second_order[5].map(lambda x:x > 0)
winning_sec_order = second_order[criterion]
win_sec_order_num = winning_sec_order.index.size
print sec_order_num, win_sec_order_num, sec_profit

criterion = price_buffer[5].map(lambda x:x < 0)
losing_order = price_buffer[criterion]

plt.plot(losing_order[6],losing_order[5],'ro')
plt.show()

'''

'''
price_buffer = pd.read_csv('C:\\Program Files (x86)\\MetaTrader Nano 4\\tester\\files\\Trend_MeanAltering.csv',header=None,index_col=None)
price_buffer.columns = ['openl','closel','profit','op','htfs']

criterion = price_buffer['op'].map(lambda x:x == 1)
buy_orders = price_buffer[criterion]
criterion = buy_orders['htfs'].map(lambda x:x == 1)
buy_orders = buy_orders[criterion]


total_num = buy_orders.index.size

criterion = buy_orders['profit'].map(lambda x:x > 0)
buy_orders = buy_orders[criterion]
profit_num = buy_orders.index.size
print total_num,profit_num

criterion = price_buffer['op'].map(lambda x:x == -1)
sell_orders = price_buffer[criterion]
criterion = sell_orders['htfs'].map(lambda x:x == -1)
sell_orders = sell_orders[criterion]

total_num += sell_orders.index.size

criterion = sell_orders['profit'].map(lambda x:x>0)
sell_orders = sell_orders[criterion]
profit_num += sell_orders.index.size
print total_num,profit_num
print profit_num/float(total_num)


#plt.hist(price_buffer['openl'],bins=100)
#plt.hist(price_buffer['openl'],bins=100)
#plt.show()
'''
data_buffer = pd.read_csv('F:\\testdata\\TT_Analysis_test.csv',header=None,index_col=None)
data_buffer.columns = ['price','indicator','Equity']
sid = data_buffer['indicator']
data_len = len(data_buffer.index)
peak = 0
trough = 9999
p_index = 0
t_index = 0
point = 0.00001

pnt = pd.DataFrame(columns=['extreme'])
time_index = []
cur_num = 0

look_for_peak = True

for i in range(0,data_len):
    if peak - trough < point * 500:
        if data_buffer['price'][i] > peak:
            peak = data_buffer['price'][i]
            p_index = i
        elif data_buffer['price'][i] < trough:
            trough = data_buffer['price'][i]
            t_index = i
    else:
        if data_buffer['price'][i-1] == peak:
            if look_for_peak == False:
                pnt.loc[cur_num] = [trough]
                time_index.append(t_index)
            look_for_peak = True
            trough = peak
        else:
            if look_for_peak == True:
                pnt.loc[cur_num] = [peak]
                time_index.append(p_index)
            look_for_peak = False
            peak = trough
        cur_num += 1

pnt.index = time_index

print pnt

ideal_indicator =[]
ii_index = []

extreme_num = len(pnt.index)
for i in range(0,extreme_num-1):
    max_indicator = data_buffer['indicator'][pnt.index[i]:pnt.index[i+1]].max()
    idx_max = data_buffer['indicator'][pnt.index[i]:pnt.index[i+1]].idxmax()
    min_indicator = data_buffer['indicator'][pnt.index[i]:pnt.index[i + 1]].min()
    idx_min = data_buffer['indicator'][pnt.index[i]:pnt.index[i + 1]].idxmin()
    if (pnt['extreme'][pnt.index[i]] < pnt['extreme'][pnt.index[i+1]]) and (max_indicator > -min_indicator):
        ideal_indicator.append(-min_indicator)
        ii_index.append(idx_min)
    elif (pnt['extreme'][pnt.index[i]] > pnt['extreme'][pnt.index[i+1]]) and  (max_indicator < -min_indicator):
        ideal_indicator.append(max_indicator)
        ii_index.append(idx_max)

idt = pd.DataFrame(data=(ideal_indicator),columns=['ideal_idt'])
idt.index = ii_index



idt_num = len(idt.index)
idt['kalman'] = [0]*idt_num

m = idt['ideal_idt'][idt.index[0]]
idt['kalman'].loc[idt.index[0]] = m
P = 0
delta = 0.0001
Vw = delta / (1-delta)
Ve = 0.001
for i in range(1,idt_num):
    a = m
    R = P + Vw
    Q = R + Ve
    e = idt['ideal_idt'][idt.index[i]] - a
    K = R / Q
    m = a + K * e
    P = R - K * R
    idt['kalman'].loc[idt.index[i]] = m

print idt

data_buffer = data_buffer.join(idt,how='outer')
data_buffer['ideal_idt'][0] = 0
data_buffer['kalman'][0] = 0
data_buffer = data_buffer.ffill()
data_buffer['ideal_idt2'] = -data_buffer['ideal_idt']
data_buffer['kalman2'] = -data_buffer['kalman']

plt.subplot(311)
data_buffer['price'].plot()
pnt['extreme'].plot()
plt.subplot(312)
data_buffer['indicator'].plot()
data_buffer['ideal_idt'].plot()
data_buffer['ideal_idt2'].plot()
data_buffer['kalman'].plot()
data_buffer['kalman2'].plot()
plt.subplot(313)
plt.hist(data_buffer['indicator'],bins=50)
plt.show()
