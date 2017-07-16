import gzip
import pickle
import os
import sys
sys.path.append('demoinfocsgo/')
import demodump
import concurrent.futures as cf
from demoinfocsgo.proto import netmessages_pb2 as NETMSG

_GAMEEVENT_TYPES = {2 : 'val_string',
                    3 : 'val_float',
                    4 : 'val_long',
                    5 : 'val_short',
                    6 : 'val_byte',
                    7 : 'val_bool',
                    8 : 'val_uint64',
                    9 : 'val_wstring' }

class DemoProcessor(object):
    def __init__(self,filename):
        self.dd = demodump.DemoDump()
        self.dd.open(filename)
        self.processed = {}
        self.events = []        
        self.dd.register_on_netmsg(NETMSG.svc_GameEvent, self.on_event)
        
    def on_event(self,cmd,data):
        gameevent = NETMSG.CSVCMSG_GameEvent()
        gameevent.ParseFromString(data)
        name = demo.descriptors[gameevent.eventid][1]
        if(name == 'player_footstep') return
        event = {'EVENTNAME':name}
        keys = demo.descriptors[gameevent.eventid][2]
        for i in range(0,len(keys)):
            keytype=_GAMEEVENT_TYPES[keys[i][1]+1]
            event[keys[i][0]]=getattr(gameevent.keys[i],keytype)
        event['TICK'] = demo.current_tick
        if(event['NAME'] == 'round_poststart'):
            event['PLAYERINFO'] = self.dd.playerinfo.copy()
        self.events.append(event)
    

def process_demofile(filename):
    dp = DemoProcessor(filename)
    dp.dd.dump()
    ret = {}
    ret['filename'] = filename
    ret['events'] = events
    ret['header'] = dd.demofile.demoheader
    print(filename)
    return ret

results = []
if name == '__main__':
    with cf.ProcessPoolExecutor(workers=6) as ex:
        tmpfiles = os.walk('demos/')
        files = [os.path.join(x[0],y) for x in tmpfiles 
                                      for y in x[2] 
                                      if x[2] != [] 
                                      if y.endswith('.dem')]
        for result in ex.map(process_demofile,files,chunksize=5):
            results += result

    with gzip.open('processed_results.gz','wb') as fd:
        pickle.dump(results,fd)

