import datetime as dt
import urllib2
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv
import MySQLdb
from sqlalchemy import create_engine
import logging


def parsePage(url):
    while True:
        try:
            r = urllib2.Request(url)
            page = urllib2.urlopen(r, data=None, timeout=10)

            soup = BeautifulSoup(page, 'lxml')
            break
        except Exception, e:
            logging.warning('exception(parse):{0}'.format(str(e)))
            #page.close()
            time.sleep(10)

    page.close()

    pre_id_str = ''

    time_current = dt.datetime.now()
    time_delta = dt.timedelta(days=time_current.weekday() + 1)
    year = (time_current - time_delta).year

    # be aware of the switch between summer and winter time
    time_delta_for_timezone = dt.timedelta(hours=4)

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
    #print time_current
    criterion = df['forecast'].map(lambda x:x is not None)
    upcoming_event = df[criterion]
    upcoming_event = upcoming_event.loc[lambda df:df.time > time_current_str]
    #print upcoming_event

    if not upcoming_event.empty:
        upcoming_time = upcoming_event.iloc[0]['time']
        upcoming_event = upcoming_event.loc[lambda df:df.time == upcoming_time]

        #secs_left = (dt.datetime.strptime(upcoming_time, '%Y.%m.%d %H:%M') - time_current).seconds

        local_upcoming_time = dt.datetime.strptime(upcoming_time, '%Y.%m.%d %H:%M') + time_delta_for_localtime

        return  local_upcoming_time,upcoming_event
    else:
        #  unkown network error, causing no future event
        logging.warning('calander error: no future event')
        return dt.datetime(1970,1,1), upcoming_event

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

def findActual(df_events, df_result, url, datebase_ip, table_name): # web content change, return -1, cannot find actual return 0, find actual return 1
    while True:
        try:
            r = urllib2.Request(url)
            page = urllib2.urlopen(r, data=None, timeout=10)

            soup = BeautifulSoup(page, 'lxml')
            break
        except Exception,e:
            logging.warning('exception(find actual):{0}'.format(str(e)))
            #page.close()
            print url
            time.sleep(10)

    page.close()

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
                        # check whether the content of the website have changed
                        if child['class'][2] == 'event':
                            div = child.div
                            if div is not None:
                                if search_df.iloc[0]['event'] != div.span.string:
                                    print 'web content changed'
                                    return -1 # web content change
                        elif child['class'][2] == 'actual':
                            value = child.string
                            if value is None:
                                return  0 # cannot find actual
                            else:
                                temp_buffer['actual'] = value
                                updateAcutalValue(datebase_ip,table_name,id_num,value) # update database
                                df_events.loc[search_df.index[0],'actual'] = value # update buffer

                                print df_events

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
                                    return 1 # find all events
                    except TypeError, KeyError:
                        continue
            else:
                if is_found_event:
                    return 0 # already pass the events we want to find actual for, meaning not found actual value for them

def updateCurrentNewsTable(ip, table_name, df):
    engine = create_engine('mysql+mysqldb://root:@%s:3306/forex_news' % ip, pool_recycle = 300)
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=True, index_label='id')



def updateAcutalValue(ip,table_name, webid, actual_value):
    try:
        conn = MySQLdb.connect(host = ip, port=3306,user='root',passwd='',db='forex_news')
        cur = conn.cursor()

        sql_line = 'update %s set actual = \'%s\' where webid = %d;' % (table_name, actual_value,webid)
        #sql_line = 'select * from %s where webid = %d' %(table_name,webid)
        logging.debug(sql_line)

        num = cur.execute(sql_line)
        #'CREATE TABLE `current_news`(`id` int(11) default null,`webid` int(11) default null,`time` varchar(50) not null default \'\',`event` varchar(100) not null default \'\', `currency` varchar(10) not null default \'\', `impact` varchar(20) not null default \'\', `actual` varchar(20) not null default \'\', `forecast` varchar(20) not null default \'\', `previous` varchar(20) not null default \'\', PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;')
        logging.debug('sql return:%d'%num)

        #print cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
    except MySQLdb.Warning, w:
        logging.warning('warning:{0}'.format(str(w)) )



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='live_bug.log',
                        filemode='w')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    url = 'http://www.forexfactory.com/calendar.php'
    database_ip = '127.0.0.1'

    current_news_table_name = 'current_news'

    calender = parsePage(url)

    updateCurrentNewsTable(database_ip,current_news_table_name,calender)

    #updateAcutalValue(datebase_ip,current_news_table_name,70268,'0.5%')

    max_index = calender.index[-1]

    current_index = calender.index[0]

    df_result = pd.DataFrame(columns=['webid', 'time', 'record time', 'event', 'currency', 'impact', 'actual', 'forecast', 'previous'])

    while current_index <= max_index:
        #secs_left, events = inspectEvent(calender)
        next_event_time, events = inspectEvent(calender)

        print events

        #next_event_time = dt.datetime.now() + dt.timedelta(seconds=secs_left)
        #print secs_left, next_event_time
        logging.info('next event time:{0}'.format(dt.datetime.strftime(next_event_time,'%Y.%m.%d %H:%M')) )

        if events.empty:
            break
        current_index = events.index[-1] + 1

        logging.debug('next index:%d'%current_index)

        #print current_index, max_index

        logging.info('sleeping...')

        # time.sleep(secs_left)

        #sleep, check whether web content has changed every 5 mins
        while dt.datetime.now() < next_event_time:
            new_calander = parsePage(url)

            while new_calander.empty:
                time.sleep(60)
                new_calander = parsePage(url)

            # events num are different, content changed
            if calender.index.size != new_calander.index.size:
                logging.info('detect web content changed')

                next_event_time, events = inspectEvent(new_calander)

                #if web error occurs, may be no future events
                if events.empty:
                    #new_calender = parsePage(url)
                    #next_event_time, events = inspectEvent(new_calender)

                    #time.sleep(5)
                    break

                else:
                    print events

                    calender = new_calander

                    updateCurrentNewsTable(database_ip,current_news_table_name,new_calander)

                    current_index = events.index[-1] + 1

                    logging.debug('next index: %d'%current_index)

            #check web page every 5 mins
            time.sleep(300)

        # awake
        logging.info('fetching actual value...')
        while True:
            find_actual_signal = findActual(events, df_result,url,database_ip,current_news_table_name)
            #found actual value
            if find_actual_signal == 1:
                break

            elif find_actual_signal == 0:
                #in case partially found, but not all found
                criterion = events['actual'].map(lambda x: x is None)
                events = events[criterion]
                time.sleep(5)

            else:
                # web content changed, update database and buffer(dataframe)
                calender = parsePage(url)

                updateCurrentNewsTable(database_ip, current_news_table_name, calender)

                max_index = calender.index[-1]

                stop_id = events.index[0]
                logging.info('changed index %d: %s'% (stop_id, events.iloc[0]['event']))

                criterion = calender['forecast'].map(lambda x:x is not None)
                events = calender[criterion]
                events = events['time'].loc[lambda df:df.time == events['time'].loc[stop_id]]
                logging.info('new events...')
                print events




    #print secs_left
    #print events
    #print events.loc[lambda df:df.id == int('65743')].empty

