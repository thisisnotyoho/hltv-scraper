import pandas as pa
import os.path as path
import datetime

_results = path.dirname(__file__) + '/results.gz'
_matches = path.dirname(__file__) + '/matches.gz'
_events = path.dirname(__file__) + '/events.gz'

resultsdf = pa.read_pickle(_results)
matchesdf = pa.read_pickle(_matches)
eventsdf = pa.read_pickle(_events)


def get_teams_from_results():
    team1 = resultsdf[['id1','name1','timestamp']]
    team2 = resultsdf[['id2','name2','timestamp']]
    team1.columns = ['id','name','timestamp']
    team2.columns = ['id','name','timestamp']
    teams = teams1.append(team2,ignore_index=True)
    grouped = teams.groupby('id')
    names = grouped.names.unique()
    names = names.apply(lambda x : x[0])
    timestart = tmp.timestamp.min()
    timeend = tmp.timestamp.max()
    ret = pa.DataFrame({'names':names,'start':timestart,'end':timeend})
    ret = ret[['names','start','end']]
    ret['end'] = ret['end'].apply(lambda x :
                        datetime.datetime.fromtimestamp(x))
    ret['start'] = ret['start'].apply(lambda x :
                        datetime.datetime.fromtimestamp(x))
    return ret
   
def get_matchstats_from_matches():
    ret = []
    tmp = zip(list(matchesdf['mid']),
              list(matchesdf['stats']),
              list(matchesdf['timestamp']))
    for x in tmp
        if(len(x[1]) < 1): continue
        for i in x[1]:
            i.update({'mid':x[0]})
            i.update({'timestamp':x[2]})
            ret.append(i)
    retdf = pa.DataFrame(ret)
    retdf['timestamp'] = \
            retdf['timestamp'].apply(datetime.datetime.fromtimestamp)
    retdf['name'] = retdf['players'].apply(lambda x: x.split('\n')[0])
    retdf['nick'] = retdf['players'].apply(lambda x: x.split('\n')[1])
    del(retdf['players'])
    return retdf


