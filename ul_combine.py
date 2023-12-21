import json
from jsonpath_ng import jsonpath, parse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import plotly.tools as tls  # Import plotly.tools
from plotly.subplots import make_subplots
from loguru import logger
import gzip
import sys

IRTT_TIME_MARGIN = 0.0010 # 1ms

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

def closest_irtt_entry_uplink(ue_timestamp, irtt_dict):
    for seqno in irtt_dict:
        entry = irtt_dict[seqno]
        send_timestamp = entry['send.timestamp']
        if abs(ue_timestamp-send_timestamp) < IRTT_TIME_MARGIN:
                return entry
    return None


if __name__ == "__main__":
    # Check if the file name is provided as a command-line argument
    if len(sys.argv) != 5:
        logger.error("Usage: python script_name.py <gnbjsonfile> <nruejsonfile> <irtt_server_jsonfile> <output_parquet_file>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    gnbjsonfile = sys.argv[1]
    nruejsonfile = sys.argv[2]
    irtt_server_jsonfile = sys.argv[3]
    journey_parquet_file = sys.argv[4]

    # Load the gnb JSON file (gnbjourneys.json)
    with open(gnbjsonfile, 'r') as gnbjourneys_file:
        gnbjourneys_data = json.load(gnbjourneys_file)

    # Load the ue JSON file (nruejourneys.json)
    with open(nruejsonfile, 'r') as nruejourneys_file:
        uejourneys_data = json.load(nruejourneys_file)

    if '.gz' in irtt_server_jsonfile:
        # Load the irtt SE JSON file
        with gzip.open(irtt_server_jsonfile) as irtt_server_file:
            data = irtt_server_file.read() # returns a byte string `b'`
            irtt_server_data = json.loads(data)['oneway_trips']
    else:
        # Load the ue JSON file (nruejourneys.json)
        with open(irtt_server_jsonfile, 'r') as irtt_server_file:
            irtt_server_data = json.load(irtt_server_file)['oneway_trips']

    # Create dicts
    gnbjourneys_dict = {entry['gtp.out']['sn']: entry for entry in gnbjourneys_data}
    uejourneys_dict = {entry['rlc.queue']['segments'][0]['rlc.txpdu']['sn']: entry for entry in uejourneys_data}

    irtt_dict = {
        entry['seqno']: {
            'send.timestamp': entry['timestamps']['client']['send']['wall']/1.0e9, 
            'receive.timestamp': entry['timestamps']['server']['receive']['wall']/1.0e9
        } for entry in irtt_server_data if entry['timestamps']['client']['send'].get('wall') != None
    }

    ADD_IRTT_TIMESTAMPS = True

    # combine ue and gnb
    combined_dict = {}
    processed_entries = 0
    for uekey in uejourneys_dict:
        ue_entry = uejourneys_dict[uekey]
        if uekey in gnbjourneys_dict:
            gnb_entry = gnbjourneys_dict[uekey]
            # find the closest irtt send and receive timestamps
            ue_timestamp, gnb_timestamp = ue_entry['ip.in']['timestamp'], gnb_entry['gtp.out']['timestamp']
            if ADD_IRTT_TIMESTAMPS==False:
                combined_dict[uekey] = flatten_dict(ue_entry, parent_key='', sep='.') | flatten_dict(gnb_entry, parent_key='', sep='.')
            else:
                irtt_entry = closest_irtt_entry_uplink(ue_entry['ip.in']['timestamp'], irtt_dict)
                if irtt_entry!=None:
                    combined_dict[uekey] = flatten_dict(irtt_entry, parent_key='', sep='.') | flatten_dict(ue_entry, parent_key='', sep='.') | flatten_dict(gnb_entry, parent_key='', sep='.')
                    processed_entries += 1
                    #print(f"processed_entries: {processed_entries}/{len(irtt_dict)}")
                else:
                    logger.error("Could not find irtt timestamp")

    df = pd.DataFrame(combined_dict).T  # Transpose to have keys as columns
    df.to_parquet(journey_parquet_file, engine='pyarrow')