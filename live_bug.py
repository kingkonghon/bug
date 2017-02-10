import datetime as dt
import urllib
from bs4 import BeautifulSoup
import pandas as pd


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

    df = pd.DataFrame(columns=['id','time','event','currency','impact','actual','forecast','previous'])

    for event_id in soup.find_all('tr'):
        id_str = event_id.get('data-eventid')
        if id_str == pre_id_str:
            continue
        if id_str is not None:
            pre_id_str = id_str
            temp_buffer['id'] = int(id_str)
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
                            temp_str = span.span.string
                            if temp_str is not None:
                                if temp_str == 'Jan 1':
                                    year += 1
                                date_str = temp_str
                            #temp_buffer[child['class'][2]] = '%s %d'%(date_current,year)
                    elif child['class'][2] == 'time':
                        temp_str = child.string
                        if temp_str is not None:
                            if temp_str.find(':') == -1:
                                temp_dt = dt.datetime.strptime('%s %d'%(date_str,year),'%b %d %Y')
                            else:
                                print temp_str
                                temp_dt = dt.datetime.strptime('%s %d %s'%(date_str,year,temp_str),'%b %d %Y %I:%M%p')
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
            print temp_buffer
            df.loc[event_num] = temp_buffer
            event_num += 1

    print df

if __name__ == '__main__':
    url = 'http://www.forexfactory.com/calendar.php'
    parsePage(url)