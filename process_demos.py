import gzip
import pickle
import sys
sys.path.append('demoinfocsgo/')
import demodump
import concurrent.futures as cf
from demoinfocsgo.proto import netmessages_pb2 as NETMSG

class DemoProcessor(object):
    def __init__(self,filename):
        self.dd = demodump.DemoDump()
        dd.open(filename)
        self.processed = {}
        self.events = []        
        dd.register_on_netmsg(NETMSG.svc_GameEvent, self.on_event)
        
    def on_event(self,cmd,data):
        gameevent = NETMSG.CSVCMSG_GameEvent()
        gameevent.ParseFromString(data)
        name = demo.descriptors[gameevent.eventid][1]
        event = {'EVENTNAME':name}
        keys = demo.descriptors[gameevent.eventid][2]
        for i in range(0,len(keys)):
            keytype=_GAMEEVENT_TYPES[keys[i][1]+1]
            event[keys[i][0]]=getattr(gameevent.keys[i],keytype)
        event['TICK'] = demo.current_tick
        self.events.append(event)
    
events = []
processed = {}
_GAMEEVENT_TYPES = {2 : 'val_string',
                    3 : 'val_float',
                    4 : 'val_long',
                    5 : 'val_short',
                    6 : 'val_byte',
                    7 : 'val_bool',
                    8 : 'val_uint64',
                    9 : 'val_wstring' }


    
