import urllib
from bs4 import BeautifulSoup

url = 'http://www.forexfactory.com/calendar.php'
page = urllib.urlopen(url)
soup = BeautifulSoup(page)

pre_id_str = ''

tag_name = []

for event_id in soup.find_all('tr'):
    id_str = event_id.get('data-eventid')
    if id_str == pre_id_str:
        continue
    if id_str is not None:
        pre_id_str = id_str
        print id_str
        temp_buffer = {}
        for child in event_id.children:
            print child
            try:
                cell = child.string
                print child['class'],cell
                temp_buffer[child['class'][2]] = cell
            except TypeError, KeyError:
                continue
        print temp_buffer