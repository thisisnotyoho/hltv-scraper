import pandas as pa
import scrape

CHUNK_SIZE=50

resultsdf = pa.read_pickle('results.gz')

matchesdf = pa.read_pickle('matches.gz')


#asdf2 = [asdf[i:i + 100] for i in range(0, len(asdf), 100)]

mids=matchesdf['mid'].unique()
missing = list(resultsdf[~resultsdf['matchid'].isin(mids)]['matchid'])


length = len(missing)
if(length == 0):
    print('matches are already up to date')
    quit()
else:
    print('Fetching %i matches' % length)

missing_chunks=[missing[i:i+CHUNK_SIZE] for i in range(0,length,CHUNK_SIZE)]

count = 0
try:
    for chunk in missing_chunks:
        tmp = scrape.get_parse_matches(chunk)
        tmpdf = pa.DataFrame(tmp)
        matchesdf = matchesdf.append(tmpdf,ignore_index=True)
        count += CHUNK_SIZE
        print("%i of %i" % (min(count,length),length))
except Exception as exc:
    print('Received exception %s, saving data and exiting' % exc)
finally:
    matchesdf.to_pickle('matches.gz')

