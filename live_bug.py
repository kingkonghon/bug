import datetime as dt
import urllib
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv
import MySQLdb
from sqlalchemy import create_engine


def parsePage(url):

    page = urllib.urlopen(url)
    soup = BeautifulSoup(page)

    pre_id_str = ''

    time_current = dt.datetime.now()
    time_delta = dt.timedelta(days=time_current.weekday() + 1)
    year = (time_current - time_delta).year

    time_delta_for_timezone = dt.timedelta(hours=5)

    temp_buffer = {}

    event_num = 0

    df = pd.DataFrame(columns=['webid','time','event','currency','impact','actual','forecast','previous'])

    for event_id in soup.find_all('tr'):
        id_str = event_id.get('data-eventid')
        if id_str == pre_id_str:
            continue
        if id_str is not None:
            pre_id_str = id_str
            try:
                temp_buffer['webid'] = int(id_str)
            except ValueError:
                #print event_id
                continue
            #for string in soup.stripped_strings:
            #    print string
            for child in event_id.children:
                #print child
                try:
                    if child['class'][2] == 'detail':
                        pass
                    elif child['class'][2] == 'graph':
                        pass
                    elif child['class'][2] == 'date':
                        span = child.span
                        if span is not None:
                            temp_date_str = span.span.string
                            if temp_date_str is not None:
                                if temp_date_str == 'Jan 1':
                                    year += 1
                                date_str = temp_date_str
                            #temp_buffer[child['class'][2]] = '%s %d'%(date_current,year)
                    elif child['class'][2] == 'time':
                        if child.span is None:
                            temp_time_str = child.string
                        else:
                            temp_time_str = child.span.string
                        if temp_time_str is not None:
                            if temp_time_str.find(':') == -1:
                                temp_dt = dt.datetime.strptime('%s %d'%(date_str,year),'%b %d %Y')
                            else:
                                #print temp_str
                                temp_dt = dt.datetime.strptime('%s %d %s'%(date_str,year,temp_time_str),'%b %d %Y %I:%M%p')
                            temp_buffer[child['class'][2]] = dt.datetime.strftime(temp_dt + time_delta_for_timezone, '%Y.%m.%d %H:%M')
                    elif child['class'][2] == 'impact':
                        span = child.span
                        if span == None:
                            temp_buffer[child['class'][2]] = None
                        else:
                            temp_buffer[child['class'][2]] = span['class'][0]
                    elif child['class'][2] == 'event':
                        div = child.div
                        if div == None:
                            temp_buffer[child['class'][2]] = None
                        else:
                            temp_buffer[child['class'][2]] = div.span.string
                    else:
                        temp_buffer[child['class'][2]] = child.string

                except TypeError, KeyError:
                    continue
            #print temp_buffer
            df.loc[event_num] = temp_buffer
            event_num += 1

    #print df
    return df


def inspectEvent (df):
    time_delta_for_localtime = dt.timedelta(hours=8)

    time_current = dt.datetime.now() - time_delta_for_localtime

    time_current_str = dt.datetime.strftime(time_current, '%Y.%m.%d %H:%M')
    print time_current
    criterion = df['forecast'].map(lambda x:x is not None)
    upcoming_event = df[criterion]
    upcoming_event = upcoming_event.loc[lambda df:df.time > time_current_str]
    #print upcoming_event

    upcoming_time = upcoming_event.iloc[0]['time']
    upcoming_event = upcoming_event.loc[lambda df:df.time == upcoming_time]

    secs_left = (dt.datetime.strptime(upcoming_time, '%Y.%m.%d %H:%M') - time_current).seconds

    return  secs_left,upcoming_event

    '''
    time_current_uplimit = time_current + dt.timedelta(minutes=10)
    time_current_downlimit = time_current - dt.timedelta(minutes=10)
    time_current_uplimit_str = dt.datetime.strftime(time_current_uplimit, '%Y.%m.%d %H:%M')
    time_current_downlimit_str = dt.datetime.strftime(time_current_downlimit, '%Y.%m.%d %H:%M')

    print time_current_downlimit_str
    print time_current_uplimit_str

    criterion = df['time'].map(lambda x:(x < time_current_uplimit_str) and (x > time_current_downlimit_str))
    current_event = df[criterion]
    print current_event.empty
    '''

def findActual(df_events, df_result,datebase_ip, table_name):
    page = urllib.urlopen(url)
    soup = BeautifulSoup(page)
    time_delta_for_localtime = dt.timedelta(hours=8)

    pre_id_str = ''

    total_event_num = df_events.index.size
    found_event_num = 0
    is_found_event = False
    for event_id in soup.find_all('tr'):
        id_str = event_id.get('data-eventid')
        if id_str == pre_id_str:
            continue
        if id_str is not None:
            pre_id_str = id_str
            try:
                id_num = int(id_str)
            except ValueError:
                continue

            #search by webid
            search_df = df_events.loc[lambda df: df.webid == id_num]
            if not search_df.empty:
                #if search_df.iloc[0]['actual'] is not None:
                #    continue
                #found start
                is_found_event = True
                temp_buffer = search_df.iloc[0].to_dict()
                for child in event_id.children:
                    try:
                        if child['class'][2] == 'actual':
                            value = child.string
                            if value is None:
                                return  False
                            else:
                                temp_buffer['actual'] = value
                                updateAcutalValue(datebase_ip,table_name,id_num,value)
                                temp_buffer['record time'] = dt.datetime.strftime(
                                    dt.datetime.now() - time_delta_for_localtime, '%Y.%m.%d %H:%M')
                                df_result.loc[df_result['webid'].size] = temp_buffer
                                print temp_buffer
                                #write file
                                try:
                                    with open('E:\\forex_factory.csv', 'ab') as csvfile:
                                        writer = csv.DictWriter(csvfile, fieldnames=['webid', 'time', 'record time', 'event', 'currency', 'impact', 'actual', 'forecast', 'previous'])
                                        writer.writerow(temp_buffer)
                                except IOError:
                                    print 'write dict error'
                                found_event_num += 1
                                if found_event_num == total_event_num:
                                    return True
                    except TypeError, KeyError:
                        continue
            else:
                if is_found_event:
                    return False

def updateCurrentNewsTable(ip, table_name, df):
    engine = create_engine('mysql+mysqldb://root:@%s:3306/forex_news' % ip, pool_recycle = 300)
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=True, index_label='id')



def updateAcutalValue(ip,table_name, webid, actual_value):
    try:
        conn = MySQLdb.connect(host = ip, port=3306,user='root',passwd='',db='forex_news')
        cur = conn.cursor()

        sql_line = 'update %s set actual = \'%s\' where webid = %d;' % (table_name, actual_value,webid)
        #sql_line = 'select * from %s where webid = %d' %(table_name,webid)
        print sql_line

        num = cur.execute(sql_line)
        #'CREATE TABLE `current_news`(`id` int(11) default null,`webid` int(11) default null,`time` varchar(50) not null default \'\',`event` varchar(100) not null default \'\', `currency` varchar(10) not null default \'\', `impact` varchar(20) not null default \'\', `actual` varchar(20) not null default \'\', `forecast` varchar(20) not null default \'\', `previous` varchar(20) not null default \'\', PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;')
        print 'sql return:',num

        #print cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
    except MySQLdb.Warning, w:
        print 'warning:', w



if __name__ == '__main__':
    url = 'http://www.forexfactory.com/calendar.php'
    datebase_ip = '192.168.2.103'

    current_news_table_name = 'current_news'

    calender = parsePage(url)

    updateCurrentNewsTable(datebase_ip,current_news_table_name,calender)

    #updateAcutalValue(datebase_ip,current_news_table_name,70268,'0.5%')

    max_index = calender.index[-1]

    current_index = calender.index[0]

    df_result = pd.DataFrame(columns=['webid', 'time', 'record time', 'event', 'currency', 'impact', 'actual', 'forecast', 'previous'])

    while current_index <= max_index:
        secs_left, events = inspectEvent(calender)
        print events
        print secs_left, dt.datetime.now() + dt.timedelta(seconds=secs_left)

        if events.empty:
            break
        current_index = events.index[0]

        print 'sleeping...'

        time.sleep(secs_left)

        print 'fetching actual value...'
        while True:
            if findActual(events, df_result,datebase_ip,current_news_table_name):
                break
            else:
                time.sleep(2)
            criterion = events['actual'].map(lambda x:x is None)
            events = events[criterion]

    #print secs_left
    #print events
    #print events.loc[lambda df:df.id == int('65743')].empty

