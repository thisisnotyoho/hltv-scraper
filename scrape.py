import urllib.request
import gzip
import ssl
import bs4
import pandas as pa
import concurrent.futures as cf

class ParseError(Exception):
    pass

def get_url(url, timeout=120):
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0')
    with urllib.request.urlopen(req, timeout=timeout) as fd:
        return fd.read().decode()

def download_demo(demoid):
    url = 'https://www.hltv.org/download/demo/' + str(demoid)
    req = urllib.request.Request(url)
    req.add_header('User-Agent','Mozilla/5.0')
    with urllib.request.urlopen(req,timeout=120) as fd:
        return fd.read()


# Some helpful extra args:
# startDate=YYYY-MM-DD
# endDate=YYYY-MM-DD
# matchType=(Lan|Online)
# content=(highlights|demo|vod|stats)
# map=(de_cache|de_mirage|...)
# event=[event id]
# team = [team id]
# player = [player id]
def get_results(offset=0,**kwargs):
    url = 'https://www.hltv.org/results?'
    if offset > 0: url += 'offset=' + str(offset)
    for x in kwargs: url+='&' + x + '=' + str(kwargs[x])
    return get_url(url)

# startDate, endDate
# eventType = (MAJOR,INTLLAN,REGIONALLAN,ONLINE,LOCALLAN,OTHER)
# prizeMin, prizeMax
# team = [team id]
# player = [player id]
def get_events_archive(offset=0,**kwargs):
    url = 'https://www.hltv.org/events/archive?'
    if offset > 0: url += 'offset=' + str(offset)
    for x in kwargs: url+='&' + x + '=' + str(kwargs[x])
    return get_url(url)

def get_match(matchid):
    url = 'https://www.hltv.org/matches/'
    url += str(matchid)
    url += '/aaaa'
    return get_url(url)

def parse_match_team(t):
    ret = {}
    ret['name'] = t.a.img['alt']
    ret['id'] = t.a['href'].split('/')[-2]
    score = t.select_one('a + div')
    ret['score'] = score.text
    ret['finish'] = score['class'][0]
    return ret


def parse_map(soup):
    ret = {}
    ret['map'] = soup.select_one('.mapname').text.strip()
    results = soup.select('span')
    tmp = []
    def flatten(x):
        if 'class' in x.attrs: return x['class'][0] + ':' + x.text
        else: return 'ot:' + x.text
    for res in results:
        if res.text == '': continue
        found = False
        for c in '():;':
            if c in res.text:
                found = True
                break
        if(found): continue
        tmp.append(flatten(res))

    ret['t1'] = ';'.join(tmp[0::2])
    ret['t2'] = ';'.join(tmp[1::2])
    results = soup.select('div.results')
    if len(results) > 0:
        ret['results']=results[0].text.strip()
    else:
        ret['results']=''
    return ret

def parse_match_statstable(soup):
    ret = {}
    if soup.name != 'table': 
        raise ParseError('should be a table')
    tmp = soup.parent.parent.select('div.dynamic-map-name-full')
    statsmap = {x['id']:x.text.strip() for x in tmp}
    headerrow = soup.select_one('tr.header-row')
    names=[x['class'][0] for x in headerrow.select('td')]
    datarows = soup.select('tr + tr')
    ret['id'] = soup.parent['id'][:-8]
    ret['teamid'] = headerrow.select_one('a.team')['href'].split('/')[-2]
    data = []
    for row in datarows:
        entry = {}
        entry['sid'] = ret['id']
        entry['tid'] = ret['teamid']
        #entry['map'] = statsmap[ret['id']]
        #entry['team'] = ret['team']
        for name in names:
            entry[name] = row.select_one('.' + name).text.strip()
        entry['pid'] = row.select_one('td.players a')['href'].split('/')[-2]
        data.append(entry)
    return data

def parse_match(rawdata,matchid):
    bs = bs4.BeautifulSoup(rawdata, 'lxml')
    if(bs.select('div.error-500')):
        raise ParseError('Server Failed to get matchpage')
    content = bs.select_one('div.contentCol')
    if(content.div['class'][0] != 'match-page'):
        raise ParseError('Not a match page, cannot parse')
    allmaps = {}
    allmaps['mid'] = matchid
    ret = []
    teamsbox=content.select_one('div.teamsBox')
    team1 = teamsbox.select_one('div.team1-gradient')
    team2 = teamsbox.select_one('div.team2-gradient')
    tmp = parse_match_team(team1)
    allmaps.update({x+'1': tmp[x] for x in tmp})
    tmp = parse_match_team(team2)
    allmaps.update({x+'2': tmp[x] for x in tmp})
    allmaps['time']=teamsbox.find('div',attrs={'class':'time'})['data-unix']
    allmaps['time']=int(allmaps['time'])//1000
    evt = teamsbox.find('div',attrs={'class':'event'})
    allmaps['eventid']=evt.a['href'].split('/')[-2]

    allmaps['map'] = 'all'
    allmaps['t1'] = allmaps['finish1']+':'+allmaps['score1']
    allmaps['t2'] = allmaps['finish2']+':'+allmaps['score2']
    del(allmaps['finish1'])
    del(allmaps['finish2'])
    del(allmaps['score1'])
    del(allmaps['score2'])

    demo = content.select('a.flexbox')
    if len(demo)>0:
        allmaps['demoid']=demo[0]['href'].split('/')[-1]

    vetobox=content.select('div.veto-box')
    if(len(vetobox) > 0): allmaps['mapinfo'] = vetobox[0].text.strip()
    if(len(vetobox) > 1): allmaps['veto'] = vetobox[1].text.strip()
    statsmap = content.select('.matchstats div.dynamic-map-name-full')
    statsmap = {x.text.strip():x['id'] for x in statsmap}

    matchstats = content.select('div.matchstats')
    if(len(matchstats) < 1):
        allmaps['advstats'] = False
        allmaps['stats'] = []
        return [allmaps]
    else: matchstats = matchstats[0]

    advstats = matchstats.find('div',attrs={'class':'stats-detailed-stats'})
    advstats = matchstats.select('.stats-detailed-stats')
    if len(advstats) > 0: 
        allmaps['advstats'] = True

    mapscontainer = []
    maps=content.select('div.mapholder')
    for m in maps:
        tmp = parse_map(m)
        if(tmp['results'] != '' and tmp['map'] in statsmap):
            mid='#'+statsmap[tmp['map']]+'-content'
            statscontent=content.select(mid)
            if(len(statscontent) < 1): continue
            statscontent = statscontent[0]
            statstables = [parse_match_statstable(x) for x in
                            statscontent.select('table')]
            statstables = statstables[0] + statstables[1]
            tmp['stats'] = statstables
            tmp['sid'] = statsmap[tmp['map']]
            for x in allmaps:
                if x in tmp: continue
                tmp.update({x:allmaps[x]})
            del(tmp['results'])
            mapscontainer.append(tmp)
    

    statstables = []
    tmp = content.select_one('#all-content')
    a = tmp.select('table')
    allmaps['stats'] = parse_match_statstable(a[0]) + \
                      parse_match_statstable(a[1])

    ret = [allmaps] + mapscontainer
    return ret

def get_parse_matches(matchids,workers=5):
    ret = []
    if type(matchids) is not list:
        raise TypeError("matchids must be list") 
    
    with cf.ProcessPoolExecutor(max_workers=workers) as exp:
        pfutures = {}
        with cf.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_matchid = {executor.submit(get_match,matchid):matchid
                for matchid in matchids}
            for future in cf.as_completed(future_to_matchid):
                matchid = future_to_matchid[future]
                try:
                    data = future.result()
                    pfutures[exp.submit(parse_match,data,matchid)] = matchid
                except Exception as exc:
                    print('%s generated an exception during download %s' 
                                % (matchid,exc))

        for future in cf.as_completed(pfutures):
            try:
                data = future.result()
                ret += data
            except Exception as exc:
                matchid = pfutures[future]
                print('%s generated an exception during parsing %s' %
                        (matchid,exc))

    return ret
"""
list(scrape.resultsdf[~scrape.resultsdf['matchid'].isin(matches['mid'].unique())]['matchid'])

for i in range(0,24000,100):
    with gzip.open('out/results' + str(i) + '.gz', 'w') as fd:
        url = 'https://www.hltv.org/results?offset='+str(i)
        req = urllib.request.Request(url)
        req.add_header('User-Agent','Mozilla/5.0')
        with urllib.request.urlopen(req) as fd2:
            fd.write(fd2.read())

parsed_results = []
for i in range(0,24000,100):
    print(i)
    filename = 'out/results' + str(i) + '.gz'
    if not os.path.isfile(filename): continue
    with gzip.open(filename, 'r') as fd:
        bs = bs4.BeautifulSoup(fd.read(), 'lxml')
        results = bs.find_all('div',attrs={'class':'result-con'})
        for result in results:
            parsed_results.append(parse_result_con(result))

parseddf = pa.DataFrame(parsed_results)

matches = []
for match in scrape.resultsdf['matchid']:
    try:
            matches += scrape.parse_match(scrape.get_match(match),match)
            print(match)
    except:
            print('failed on {}' % match)

"""
def parse_results(rawdata):
    bs = bs4.BeautifulSoup(rawdata, 'lxml')
    results = bs.select('div.result-con')
    return [parse_result_con(r) for r in results]

def get_pagination(rawdata):
    soup = bs4.BeautifulSoup(rawdata,'lxml')
    txt = soup.select_one('div.pagination-top').text.strip()
    split = txt.split(' ')
    ret = [int(x) for x in split[0::2]]
    return ret

def parse_events_archive(rawdata):
    soup = bs4.BeautifulSoup(rawdata, 'lxml')
    events = soup.select('a.small-event')
    ret = []
    for event in events:
        ev = {}
        ev['id'] = event['href'].split('/')[-2]
        td = event.find_all('td')
        ev['name'] = td[0].text.strip()
        ev['teams'] = td[1].text.strip()
        ev['prize'] = td[2].text.strip()
        ev['prize'] = ev['prize'].replace('$','')
        ev['prize'] = ev['prize'].replace(',','')
        if(ev['prize'] == 'Other'):
            ev['prize'] = -1
        else:
            ev['prize'] = int(ev['prize'])
        ev['type'] = td[3].text.strip()
        span = td[4].find_all('span')
        ev['flag'] = span[0].img['title']
        ev['loc'] = span[1].text.strip()[:-2]
        ev['timestamp'] = int(span[4]['data-unix']) // 1000
        ret.append(ev)
    return ret

def get_results_daterange(datefrom, dateto):
    resultsfrom = datefrom.strftime('%Y-%m-%d')
    resultsto = dateto.strftime('%Y-%m-%d')
    rawdata = get_results(startDate=resultsfrom,endDate=resultsto)
    tmpdata = rawdata
    *junk,total=get_pagination(rawdata)
    results = []
    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        scrp = lambda x : get_results(offset=x,
                                      startDate=resultsfrom,
                                      endDate=resultsto)
        for rawdata in ex.map(scrp,range(100,total,100)):
            *junk,totaltmp = get_pagination(rawdata)
            if(totaltmp != total): 
                raise ValueError('Results changed while downloading')
            results += parse_results(rawdata)

    tmp = parse_results(tmpdata)
    results = tmp + results
    return results
    

def get_events_daterange(datefrom, dateto):
    resultsfrom = datefrom.strftime('%Y-%m-%d')
    resultsto = dateto.strftime('%Y-%m-%d')
    rawdata = get_events_archive(startDate=resultsfrom,endDate=resultsto)
    tmpdata = rawdata
    *junk,total=get_pagination(rawdata)
    results = []
    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        scrp = lambda x : get_events_archive(offset=x,
                                             startDate=resultsfrom,
                                             endDate=resultsto)
        for rawdata in ex.map(scrp,range(50,total,50)):
            *junk,totaltmp = get_pagination(rawdata)
            if(totaltmp != total): 
                raise ValueError('Results changed while downloading')
            results += parse_events_archive(rawdata)

    tmp = parse_events_archive(tmpdata)
    results = tmp + results
    return results
    

def parse_result_con(result):
    entry = {}
    entry['timestamp'] = int(result['data-zonedgrouping-entry-unix'])//1000
    entry['matchid'] = result.a['href'].split('/')[2]
    for i in [1,2]:
        teamtag = result.find('div',attrs={'class' : 'line-align team'+str(i)})
        entry['name' + str(i)] = teamtag.text.strip()
        entry['id' + str(i)] = teamtag.img['src'].split('/')[-1]
    entry['score'] = result.find('td',attrs={'class' : 'result-score'}).text
    event = result.find('td',attrs={'class' : 'event' })
    entry['eventname'] = event.img['title']
    entry['eventid'] = event.img['src'].split('/')[-1][:-4]
    if(entry['eventid'] == 'noLogo'): entry['eventid'] = None
    entry['extra']  = result.find('td', attrs={'class':'star-cell'}).text
    entry['extra'] = entry['extra'].strip()
    return entry



