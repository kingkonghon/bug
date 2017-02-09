import urllib
import numpy as np
import datetime as dt
import pandas as pd
def getHtml(url):
    page = urllib.urlopen(url)
    html = page.read()
    return html

def analyzeHtml(str_html,year):
    timezone = dt.timedelta(hours=5)

    #event id
    str_id_tab_head = '" data-eventid="'
    str_id_tab_len = len(str_id_tab_head)
    str_id_tab_tail = '"'
    #weekday and date
    str_date_tab_head = '<td class="calendar__cell calendar__date date"><span class="date">'
    str_date_tab_len = len(str_date_tab_head)
    str_date_tab_middle = '<span>'
    str_date_tab_len2 = len(str_date_tab_middle)
    str_date_tab_tail = '</span>'
    #event time
    str_time_tab_head = '<td class="calendar__cell calendar__time time">'
    str_time_tab_len = len(str_time_tab_head)
    str_time_tab_tail = '</td>'
    # currency
    str_currency_tab_head = '<td class="calendar__cell calendar__currency currency">'
    str_currency_tab_len = len(str_currency_tab_head)
    str_currency_tab_tail = '</td>'
    #impact
    str_impact_tab_head = '<td class="calendar__cell calendar__impact impact calendar__impact calendar__impact--'
    str_impact_tab_len = len(str_impact_tab_head)
    str_impact_tab_tail = '">'
    #description
    str_description_tab_head = '<span class="calendar__event-title">'
    str_description_tab_len = len(str_description_tab_head)
    str_description_tab_tail = '</span>'
    #actual
    str_actual_tab_head = '<td class="calendar__cell calendar__actual actual">'
    str_actual_tab_len = len(str_actual_tab_head)
    str_actual_tab_sub_head = '">'
    str_actual_tab_sub_len = len(str_actual_tab_sub_head)
    str_actual_tab_sub_tail = '</span>'
    str_actual_tab_tail = '</td>'
    #forecast
    str_forecast_tab_head = '<td class="calendar__cell calendar__forecast forecast">'
    str_forecast_tab_len = len(str_forecast_tab_head)
    str_forecast_tab_tail = '</td>'
    #previous
    str_previous_tab_head = '<td class="calendar__cell calendar__previous previous">'
    str_previous_tab_len = len(str_previous_tab_head)
    str_previous_tab_sub_head = '">'
    str_previous_tab_sub_len = len(str_previous_tab_sub_head)
    str_previous_tab_sub_tail = '</span>'
    str_previous_tab_tail = '</td>'

    #calculate event id num and pos
    id_pos = [0]
    id_content = [-1]
    id_num = 0
    event_num = 0
    last_id = id_content[0]
    while True:
        next_id_starting_pos = str_html.find(str_id_tab_head, id_pos[id_num])
        next_id_ending_pos = str_html.find(str_id_tab_tail,next_id_starting_pos + str_id_tab_len)
        if next_id_starting_pos == -1 or next_id_ending_pos == -1:
            break
        id_pos.append(next_id_ending_pos)
        str_this_content = str_html[next_id_starting_pos+str_id_tab_len:next_id_ending_pos]
        if str_this_content == '':
            this_content = 0
        else:
            this_content = int(str_this_content)
        id_content.append(this_content)
        if this_content != last_id:
            event_num += 1
            last_id = this_content
        id_num += 1
    #print range(1,id_num,2)
    #print id_num
    #print id_content

    #find event details
    event_time = [dt.datetime(2000,1,1)] * event_num
    event_weekday = ['SUN'] * event_num
    event_description = [''] * event_num
    event_currency = [''] * event_num
    event_actual = [0] * event_num
    event_forecast = [0] * event_num
    event_previous = [0] * event_num
    event_impact = [''] * event_num

    str_current_date = ''
    str_current_weekday = ''
    event_count = 0
    last_id = id_content[0]
    for i in range(1,id_num):
        if id_content[i] == last_id:
            continue
        else:
            last_id = id_content[i]
        current_start_pos = id_pos[i]
        current_end_pos = id_pos[i+1]
        #print current_start_pos,current_end_pos

        #find date (not necessarily exist)
        element_start_pos = str_html.find(str_date_tab_head,current_start_pos,current_end_pos)
        if element_start_pos != -1:
            element_middle_pos = str_html.find(str_date_tab_middle,element_start_pos + str_date_tab_len,current_end_pos)
            element_end_pos = str_html.find(str_date_tab_tail,element_middle_pos + str_date_tab_len2,current_end_pos)
            if element_end_pos == -1:
                print 'error:find date'
            else:
                str_current_weekday = str_html[element_start_pos + str_date_tab_len : element_middle_pos]
                str_current_date = str_html[element_middle_pos + str_date_tab_len2 : element_end_pos]
                #print str_current_date, str_current_weekday
                if str_current_date == 'Jan 1':
                    year += 1

        #find time
        is_find_event = False
        element_start_pos = str_html.find(str_time_tab_head,current_start_pos,current_end_pos)
        element_end_pos = str_html.find(str_time_tab_tail,element_start_pos + str_time_tab_len,current_end_pos)
        if element_end_pos == -1:
            print 'error find time'
        else:
            event_weekday[event_count] = str_current_weekday
            str_current_time = str_html[element_start_pos + str_time_tab_len : element_end_pos]
            #print str_current_time
            if len(str_current_time) == 0:
                event_time[event_count] = event_time[event_count-1]
            elif str_current_time.find(':') == -1:
                event_time[event_count] = dt.datetime.strptime(str_current_date + ' %s'%year, '%b %d %Y') + timezone
            else:
                #print str_current_time
                event_time[event_count] = dt.datetime.strptime(str_current_date + ' %s '%year + str_current_time, '%b %d %Y %I:%M%p') + timezone
            is_find_event = True

        # find currency
        element_start_pos = str_html.find(str_currency_tab_head, current_start_pos, current_end_pos)
        element_end_pos = str_html.find(str_currency_tab_tail, element_start_pos + str_currency_tab_len,current_end_pos)
        if element_end_pos == -1:
            print 'error find currency'
        else:
            event_currency[event_count] = str_html[element_start_pos + str_currency_tab_len: element_end_pos]

        #find description
        element_start_pos = str_html.find(str_description_tab_head, current_start_pos, current_end_pos)
        element_end_pos = str_html.find(str_description_tab_tail, element_start_pos + str_description_tab_len, current_end_pos)
        if element_end_pos == -1:
            print 'error find description'
        else:
            event_description[event_count] = str_html[element_start_pos + str_description_tab_len: element_end_pos]

        #find actual
        element_start_pos = str_html.find(str_actual_tab_head, current_start_pos, current_end_pos)
        element_end_pos = str_html.find(str_actual_tab_tail, element_start_pos + str_actual_tab_len, current_end_pos)
        if element_end_pos == -1:
            print 'error find actual'
        else:
            temp_str = str_html[element_start_pos + str_actual_tab_len: element_end_pos]
            if temp_str.find(str_actual_tab_sub_head) != -1:
                element_start_pos = temp_str.find(str_actual_tab_sub_head)
                element_end_pos = temp_str.find(str_actual_tab_sub_tail, element_start_pos + str_actual_tab_sub_len)
                event_actual[event_count] = temp_str[element_start_pos + str_actual_tab_sub_len: element_end_pos]
            else:
                event_actual[event_count] = str_html[element_start_pos + str_actual_tab_len: element_end_pos]

        #find forecast
        element_start_pos = str_html.find(str_forecast_tab_head, current_start_pos, current_end_pos)
        element_end_pos = str_html.find(str_forecast_tab_tail, element_start_pos + str_forecast_tab_len, current_end_pos)
        if element_end_pos == -1:
            print 'error find forecast'
        else:
            event_forecast[event_count] = str_html[element_start_pos + str_forecast_tab_len: element_end_pos]

        #find previous
        element_start_pos = str_html.find(str_previous_tab_head, current_start_pos, current_end_pos)
        element_end_pos = str_html.find(str_previous_tab_tail, element_start_pos + str_previous_tab_len, current_end_pos)
        if element_end_pos == -1:
            print 'error find previous'
        else:
            temp_str = str_html[element_start_pos + str_previous_tab_len: element_end_pos]
            if temp_str.find(str_previous_tab_sub_head) != -1:
                element_start_pos = temp_str.find(str_previous_tab_sub_head)
                element_end_pos = temp_str.find(str_previous_tab_sub_tail, element_start_pos + str_previous_tab_sub_len)
                event_previous[event_count] = temp_str[element_start_pos + str_previous_tab_sub_len: element_end_pos]
            else:
                event_previous[event_count] = str_html[element_start_pos + str_previous_tab_len: element_end_pos]

        # find impact
        element_start_pos = str_html.find(str_impact_tab_head, current_start_pos, current_end_pos)
        element_end_pos = str_html.find(str_impact_tab_tail, element_start_pos + str_impact_tab_len,
                                        current_end_pos)
        if element_end_pos == -1:
            print 'error find forecast'
        else:
            event_impact[event_count] = str_html[element_start_pos + str_impact_tab_len: element_end_pos]



        #-------------------------------
        if is_find_event:
            event_count += 1


    d = {'weekday':event_weekday, 'description':event_description, 'currency':event_currency, 'actual':event_actual, 'previous':event_previous,'forecast':event_forecast,'impact':event_impact}
    event_details = pd.DataFrame(data=d,index=event_time)

    return event_details

def getEvent(year,month,day,currency_pair):
    start_date = dt.datetime(year,month,day)
    end_date = dt.datetime.now()

    if len(currency_pair) != 6:
        print 'illegal currency pair'
        return

    day_num = start_date.weekday() + 1
    time_delta = dt.timedelta(days=day_num)
    real_start_date = start_date - time_delta

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
        start_year = start_of_week.year
        url = "http://www.forexfactory.com/calendar.php?week=" + str_start_of_week
        print url
        str_html = getHtml(url)
        past_event = analyzeHtml(str_html,start_year)

        if currency_pair != 'ALL':
            criterion = past_event['currency'].map(lambda x:x == currency_pair[0:3] or x == currency_pair[3:6])
            past_event = past_event[criterion]

        print past_event
        past_event.to_csv('E:\\forex_factory\\event.csv',header=is_write_header,mode=write_mode)
        is_write_header = False
        write_mode = 'a'
        time_delta = dt.timedelta(days=7)
        start_of_week += time_delta

if __name__ == '__main__':
    #cfg = open('cfg.txt')
    #currency = cfg.readline().strip('\n')
    #year = int(cfg.readline())
    #month = int(cfg.readline())
    #day = int(cfg.readline())
    year = 2014
    month = 1
    day = 1
    currency = 'AUDCAD'

    getEvent(year,month,day,currency)