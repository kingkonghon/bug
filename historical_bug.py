import datetime as dt
import urllib
from bs4 import BeautifulSoup
import pandas as pd
import time

def parsePage(url):

    page = urllib.urlopen(url)
    soup = BeautifulSoup(page)

    pre_id_str = ''

    year = int(url[-4:])
    print year

    time_delta_for_timezone = dt.timedelta(hours=5)

    temp_buffer = {}

    event_num = 0

    df = pd.DataFrame(columns=['id','time','event','currency','impact','actual','forecast','previous'])

    for event_id in soup.find_all('tr'):
        id_str = event_id.get('data-eventid')
        if id_str == pre_id_str:
            continue
        if id_str is not None:
            pre_id_str = id_str
            try:
                temp_buffer['id'] = int(id_str)
            except ValueError:
                print event_id
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
                                #print '%s %d %s'%(date_str,year,temp_time_str)
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

    return df

def getHistoricalNews(year,month,day,currency_pair):
    start_date = dt.datetime(year,month,day)
    end_date = dt.datetime.now()

    #get the start of the week for the start date
    day_num = start_date.weekday() + 1
    time_delta = dt.timedelta(days=day_num)
    real_start_date = start_date - time_delta

    #get the end of the week for last week
    m = end_date.minute
    h = end_date.hour
    s = end_date.second
    ms = end_date.microsecond
    day_num = end_date.weekday() + 1
    time_delta = dt.timedelta(days=day_num,seconds=s,minutes=m,hours=h,microseconds=ms)
    real_end_date = end_date - time_delta

    #html = open('E:\\forex_factory\\Forex Calendar @ Forex Factory.html')
    #str_html = html.read()

    start_of_week = real_start_date
    is_write_header = True
    write_mode = 'w'
    while start_of_week < real_end_date:
        str_start_of_week = start_of_week.strftime('%b%d.%Y')
        url = "http://www.forexfactory.com/calendar.php?week=" + str_start_of_week
        print url
        events_this_week = parsePage(url)

        if currency_pair != 'ALL':
            criterion = events_this_week['currency'].map(lambda x:x == currency_pair[0:3] or x == currency_pair[3:6])
            events_this_week = events_this_week[criterion]

        print events_this_week
        events_this_week.to_csv('E:\\forex_factory\\event_version2.csv',header=is_write_header,mode=write_mode)
        is_write_header = False
        write_mode = 'a'
        time_delta = dt.timedelta(days=7)
        start_of_week += time_delta


if __name__ == '__main__':
    year = 2014
    month = 1
    day = 1
    currency = 'ALL'
    getHistoricalNews(year,month,day,currency)