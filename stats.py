import pandas as pa
import os.path as path
import datetime
import re

_results = path.dirname(__file__) + '/results.gz'
_matches = path.dirname(__file__) + '/matches.gz'
_events = path.dirname(__file__) + '/events.gz'

resultsdf = pa.read_pickle(_results)
matchesdf = pa.read_pickle(_matches)
eventsdf = pa.read_pickle(_events)

pa.set_option('precision',2)

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
   
teamsdf = get_teams_from_results()

def get_matchstats_from_matches(matches=matchesdf):
    ret = []
    tmp = zip(list(matches['mid']),
              list(matches['stats']),
              list(matches['time']),
              list(matches['map']))
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
    retdf['k'] = retdf['kd'].apply(lambda x: int(x.split('-')[0]))
    retdf['d'] = retdf['kd'].apply(lambda x: int(x.split('-')[1]))
    del(retdf['kd'])
    retdf['adr'] = pa.to_numeric(retdf['adr'],errors='coerce')
    retdf['plus-minus'] = pa.to_numeric(retdf['plus-minus'],errors='coerce')
    retdf['rating'] = pa.to_numeric(retdf['rating'],errors='coerce')
    retdf['kast'] = pa.to_numeric(
                retdf['kast'].apply(lambda x:x.replace('%','')),errors='coerce')
    return retdf

def find_team(regex):
    return teamsdf[
        teamsdf['names'].apply( 
            lambda x: bool(re.search(regex,x,re.IGNORECASE)))]

def format_results(res):
    t = [x.split(';') for x in list(res)]
    t = [[x.split(':') for x in l] for l in t]
    ret=[]
    for x in t:
        res={}
        res['result']=x[0][0]
        res['score']=x[0][1]
        if(len(x)>1):
            if(x[1][0]=='ct'):
                res.update({'start':'ct'})
            elif(x[1][0]=='t'):
                res.update({'start':'t'})
            else:
                res.update({'start':'?'})
            [res.update({y[0]:y[1]}) for y in x[1:]]
        ret.append(res)
    return ret

def get_matchresults_from_matches(matches):
    matches = matches.copy()
    t1 = format_results(matches['t1'])
    t2 = format_results(matches['t2'])
    t1df=pa.DataFrame(t1)
    t1df['id']=list(matches['id1'])
    t1df['mid']=list(matches['mid'])
    t1df['map']=list(matches['map'])
    t2df=pa.DataFrame(t2)
    t2df['id']=list(matches['id2'])
    t2df['mid']=list(matches['mid'])
    t2df['map']=list(matches['map'])
    ret=t1df.append(t2df,ignore_index=True)
    ret['score'] = pa.to_numeric(ret['score'],errors='coerce')
    ret['ct'] = pa.to_numeric(ret['ct'],errors='coerce')
    ret['ot'] = pa.to_numeric(ret['ot'],errors='coerce')
    ret['t'] = pa.to_numeric(ret['t'],errors='coerce')
    ret = ret[['mid','id','map','result','score','start','ct','t','ot']]
    return ret

def filter_out_online(matches):
    evdf=eventsdf[eventsdf['type'] == 'Online']
    return matches[~matches['eventid'].isin(evdf['id'])]

def filter_after_date(matches,year,month,day):
    dt = datetime.datetime(year,month,day)
    return matches[matches['time'] > dt.timestamp()]

def filter_by_team(matches,teamids):
    if type(teamids) is not list:
        teamids=list(teamids)
    teamids = [str(x) for x in teamids]
    return matches[matches['id1'].isin(teamids) | 
                   matches['id2'].isin(teamids) ]

def get_matches_by_team(teamid,matches):
    shortdf = matches
    t1df = shortdf[shortdf['id1'] == str(teamid)].copy()
    t2df = shortdf[shortdf['id2'] == str(teamid)].copy()
    t2df.rename(inplace=True,columns={'id1':'tmpid1',
                                      'name1':'tmpname1',
                                      't1':'tmpt1'})
    t2df.rename(inplace=True,columns={'id2':'id1',
                                      'name2':'name1',
                                      't2':'t1'})
    t2df.rename(inplace=True,columns={'tmpid1':'id2',
                                      'tmpname1':'name2',
                                      'tmpt1':'t2'})
    return t1df.append(t2df,ignore_index=True)

def get_vetos(matches):
    alldf = matches[matches['map'] == 'all']
    tmp = [x.split('\n') for x in list(alldf['veto']) if type(x) == str]
    tmp = [x.split(' ') for l in tmp for x in l]
    tmp = [x for x in tmp if len(x) < 5]
    vetodf = pa.DataFrame(tmp,columns=['pos','team','action','map'])
    vetodf['pos'] = pa.to_numeric(vetodf['pos'],
                                  errors='coerce',
                                  downcast='integer')
    ret = vetodf.groupby(['team','map','action']).agg(['count','mean'])
    return ret

def analyze_team(matches,teamid):
    shortdf = get_matches_by_team(teamid,online,after)
    alldf = shortdf[shortdf['map'] == 'all']
    inddf = shortdf[shortdf['map'] != 'all']
    teamname = teamsdf[teamsdf.index == str(teamid)]['names'].iloc[0]
    indstats = get_matchstats_from_matches(inddf)
    indstats = indstats[indstats['tid'] == str(teamid)]
    groups = ['map','nick','adr','k','d','kast','rating']
    indgrp = indstats[groups].groupby(groups[0:1])
    #tmpdf.plot(kind='bar',title='%s Map Inclinations'%teamname)

