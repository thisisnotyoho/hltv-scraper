import pandas as pa
import scrape
import datetime
import concurrent.futures as cf

eventsdf = pa.read_pickle('events.gz')

blacklist = [] 

ts = eventsdf.sort_values(by='timestamp',ascending=False)['timestamp'].iloc[0]



# note: we go to the future because their site does dumb shit with
# timezones
newest = datetime.date.fromtimestamp(ts)
now = datetime.date.today() + datetime.timedelta(1)

events = scrape.get_events_daterange(newest,now)

updatedf = pa.DataFrame(events)
updatecount = len(updatedf[~updatedf['id'].isin(eventsdf['id'])])
print('updating %i records' % updatecount)
if updatecount > 0:
    eventsdf = eventsdf.append(updatedf, ignore_index=True)
    eventsdf = eventsdf[~eventsdf.duplicated(subset='id',keep='last')]
    eventsdf = eventsdf[~eventsdf['id'].isin(blacklist)]
    eventsdf.reset_index(drop=True,inplace=True)
    eventsdf.to_pickle('events.gz') 

