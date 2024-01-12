import pandas as pd
import numpy as np
from loguru import logger
from collections import deque
from collections import OrderedDict
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")

IRTT_TIME_MARGIN = 0.0010 # 1ms
MAX_DEPTH = 500

class FixSizeOrderedDict(OrderedDict):
    def __init__(self, *args, max=0, **kwargs):
        self._max = max
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        if self._max > 0:
            if len(self) > self._max:
                logger.debug("ul combine buffer is full, poping items")
                self.popitem(False)


def flatten_dict(d, parent_key='', sep='.'):
    items = {}
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep=sep))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    items.update(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep))
                else:
                    items[f"{new_key}{sep}{i}"] = item
        else:
            items[new_key] = value
    return items

def closest_nlmt_entry_uplink(ue_timestamp, nlmt_dict):
    for seqno in nlmt_dict:
        entry = nlmt_dict[seqno]
        send_timestamp = entry['send.timestamp']
        if abs(ue_timestamp-send_timestamp) < IRTT_TIME_MARGIN:
            return seqno,entry
    return None,None

class CombineUL:
    def __init__(self):
        self.gnbjourneys_dict = FixSizeOrderedDict(max=MAX_DEPTH)
        self.uejourneys_dict = FixSizeOrderedDict(max=MAX_DEPTH)
        self.nlmtjourneys_dict = FixSizeOrderedDict(max=MAX_DEPTH)

    def run(self,upfjourneys_data, gnbjourneys_data, uejourneys_data):

        for entry in gnbjourneys_data:
            try:
                self.gnbjourneys_dict[entry['gtp.out']['sn']] = entry
            except:
                pass

        for entry in uejourneys_data:
            try:
                self.uejourneys_dict[entry['rlc.queue']['segments'][0]['rlc.txpdu']['sn']] = entry
            except:
                pass

        for entry in upfjourneys_data:
            if 'st' in list(entry.keys()):
                self.nlmtjourneys_dict[entry['seq']] = {
                    'send.timestamp': np.float64(entry['st'])/1.0e9, 
                    'receive.timestamp': np.float64(entry['rt'])/1.0e9,
                }

        # Combine
        combined_dict = {}
        del_arr = []
        del_arr_nlmt = []
        for uekey in self.uejourneys_dict:
            ue_entry = self.uejourneys_dict[uekey]
            if uekey in self.gnbjourneys_dict:
                gnb_entry = self.gnbjourneys_dict[uekey]
                # find the closest irtt send and receive timestamps
                nlmt_key,nlmt_entry = closest_nlmt_entry_uplink(ue_entry['ip.in']['timestamp'], self.nlmtjourneys_dict)
                if nlmt_entry:
                    combined_dict[uekey] = flatten_dict(nlmt_entry, parent_key='', sep='.') | flatten_dict(ue_entry, parent_key='', sep='.') | flatten_dict(gnb_entry, parent_key='', sep='.')
                    del_arr.append(uekey)
                    del_arr_nlmt.append(nlmt_key)
                else:
                    logger.debug(f"Could not find ue entry in nlmt for sn {uekey}")
            else:
                logger.debug(f"Could not find ue entry in gnb for sn {uekey}")
        
        for delkey in del_arr:
            del self.uejourneys_dict[delkey]
            del self.gnbjourneys_dict[delkey]

        for delkey in del_arr_nlmt:
            del self.nlmtjourneys_dict[delkey]

        return pd.DataFrame(combined_dict).T  # Transpose to have keys as columns