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
        self.events = {}        
        self.dd.register_on_netmsg(NETMSG.svc_GameEvent, self.on_event)
        
    def on_event(self,cmd,data):
        gameevent = NETMSG.CSVCMsg_GameEvent()
        gameevent.ParseFromString(data)
        name = self.dd.descriptors[gameevent.eventid][1]
        if(name == 'player_footstep'): return
        event = {}
        keys =  self.dd.descriptors[gameevent.eventid][2]
        for i in range(0,len(keys)):
            keytype=_GAMEEVENT_TYPES[keys[i][1]+1]
            event[keys[i][0]]=getattr(gameevent.keys[i],keytype)
        event['TICK'] = self.dd.current_tick
        if(name == 'round_start'):
            event['PLAYERINFO'] = self.dd.playerinfo.copy()
        if name in self.events:
            self.events[name].append(event)
        else:
            self.events[name] = [event]

def process_demofile(filename):
    dp = DemoProcessor(filename)
    try:
        dp.dd.dump()
    except:
        pass
    ret = {}
    ret['filename'] = filename
    ret['events'] = dp.events
    ret['header'] = dp.dd.demofile.demoheader

    return ret

results = []
if __name__ == '__main__':
    with cf.ProcessPoolExecutor(max_workers=6) as ex:
        tmpfiles = os.walk('demos/')
        files = [os.path.join(x[0],y) for x in tmpfiles 
                                      for y in x[2] 
                                      if x[2] != [] 
                                      if y.endswith('.dem')]
        future_to_file = {ex.submit(process_demofile,f):f for f in files}
        for future in cf.as_completed(future_to_file):
            try:
                res = future.result()
                results.append(res)
                print(future_to_file[future])
            except Exception as exc:
                filename = future_to_file[future]
                print('%s generated exception %s' % (filename, exc))

    with gzip.open('processed_results.gz','wb') as fd:
        pickle.dump(results,fd)

