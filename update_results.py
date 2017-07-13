import pandas as pa
import scrape
import datetime
import concurrent.futures as cf

resultsdf = pa.read_pickle('results.gz')

blacklist = ['2304474', 
             '2302286', 
             '2300492', 
             '2300369', 
             '2300466', 
             '2291761', 
             '2290858', 
             '2290859', 
             '2290784', 
             '2291488', 
             '2290789', 
             '2290607', 
             '2290727', 
             '2290613'] 

ts = resultsdf.sort_values(by='timestamp',ascending=False)['timestamp'].iloc[0]



# note: we go to the future because their site does dumb shit with
# timezones
newest = datetime.date.fromtimestamp(ts)
now = datetime.date.today() + datetime.timedelta(1)

results = scrape.get_results_daterange(newest,now)

updatedf = pa.DataFrame(results)
updatecount = len(updatedf[~updatedf['matchid'].isin(resultsdf['matchid'])])
print('updating %i records' % updatecount)
if updatecount > 0:
    resultsdf = resultsdf.append(updatedf, ignore_index=True)
    resultsdf = resultsdf[~resultsdf.duplicated(subset='matchid',keep='last')]
    resultsdf = resultsdf[~resultsdf['matchid'].isin(blacklist)]
    resultsdf.reset_index(drop=True,inplace=True)
    resultsdf.to_pickle('results.gz') 

