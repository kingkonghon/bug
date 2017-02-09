import urllib
from bs4 import BeautifulSoup

url = 'http://www.forexfactory.com/calendar.php'
page = urllib.urlopen(url)
soup = BeautifulSoup(page)

for event_id in soup.find_all('tr'):
    if event_id.get('data-eventid') is not None:
        print event_id.get('data-eventid')
        for child in event_id.descendants:
            print child