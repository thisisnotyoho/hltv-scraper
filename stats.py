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
    teams = team1.append(team2,ignore_index=True)
    grouped = teams.groupby('id')
    names = grouped.name.unique()
    names = names.apply(lambda x : x[0])
    timestart = grouped.timestamp.min()
    timeend = grouped.timestamp.max()
    ret = pa.DataFrame({'names':names,'start':timestart,'end':timeend})
    ret = ret[['names','start','end']]
    ret['end'] = ret['end'].apply(lambda x :
                        datetime.datetime.fromtimestamp(x))
    ret['start'] = ret['start'].apply(lambda x :
                        datetime.datetime.fromtimestamp(x))
    return ret
   
def get_matchstats_from_matches(matches=matchesdf):
    ret = []
    tmp = zip(list(matches['mid']),
              list(matches['stats']),
              list(matches['time']),
              list(matches['map'])
    for x in tmp:
        if(len(x[1]) < 1): continue
        for i in x[1]:
            i.update({'mid':x[0]})
            i.update({'time':x[2]})
            i.update({'map':x[3]})
            ret.append(i)
    retdf = pa.DataFrame(ret)
    retdf['time'] = \
            retdf['time'].apply(datetime.datetime.fromtimestamp)
    retdf['name'] = retdf['players'].apply(lambda x: x.split('\n')[0])
    retdf['nick'] = retdf['players'].apply(lambda x: x.split('\n')[1])
    del(retdf['players'])
    return retdf

def get_team_from_matchstats
