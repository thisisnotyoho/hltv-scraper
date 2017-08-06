import pandas as pa
import stats

teams = [5752,6137]
teams = [str(x) for x in teams]
teamnames = list(stats.teamsdf.loc[teams]['names'])

shortdf = stats.filter_out_online(stats.matchesdf)
shortdf = stats.filter_after_date(shortdf,2017,1,1)
shortdf = stats.filter_by_team(shortdf,teams)
res = stats.get_matchresults_from_matches(shortdf)
res = res[res['id'].isin(teams)]
resgrp=res.groupby(['id','map','result'])
rez=resgrp.mid.count()
tot=res.groupby(['id','map']).mid.count()
winrates=pa.Series([rez.loc[x]/tot.loc[x[0],x[1]] 
                        for x in rez.index],
                    index=rez.index)

vetodf = stats.get_vetos(shortdf)
for team in teams:
    print('*'*20+' '+stats.teamsdf.loc[team]['names']+' '+'*'*20)
    tmp = vetodf.loc[team,:,'removed'].sort_values(('pos','count',
    print(tmp.to_string())
    tmp = vetodf.loc[team,:,'picked'].sort_values(('pos','count',
    print(tmp.to_string())
    tmp = winrates.loc[team,:,'won'].sort_values(ascending=False)
    print(tmp.to_string())
    print('*'*40)

statsdf = stats.get_matchstats_from_matches(shortdf)
statsdf = statsdf[statsdf['tid'].isin(teams)]
groups = ['tid','map','nick','adr','k','d','kast','rating']
statsgrp = statsdf[groups].groupby(groups[0:3])
statsagg = statsgrp.describe()
maps = [x[1] for x in tmp.index.values]
for m in maps:
    print(m)
    for group in groups[3:]:
        print('*'*5+group)
        for team in teams:
            print(' '*10+stats.teamsdf.loc[team]['names'])
            print(statsagg.loc[team,m,:][group].to_string()) 


