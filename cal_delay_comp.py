import seaborn as sns
from sortedcontainers import SortedList, SortedDict
from loguru import logger
import os, sys, gzip, json
import sqlite3
import pandas as pd
from edaf.core.uplink.decomp import *
from edaf.core.uplink.preprocess import preprocess_ul
from edaf.core.uplink.analyze_packet import ULPacketAnalyzer
from edaf.core.uplink.analyze_channel import ULChannelAnalyzer
from edaf.core.uplink.analyze_scheduling import ULSchedulingAnalyzer
from edaf.utilities.data_helpers import *
from edaf.utilities.plot_helpers import *
import matplotlib.pyplot as plt
import matplotlib.patches as patches
#from IPython.display import JSON
from pathlib import Path
import ijson
import numpy as np
import csv
from itertools import zip_longest

# This file calculates the various delay components along with other relevant parameters from the preprocessed database file. 

# Configure logger
logger.remove()
logger.add(sys.stdout, level="ERROR")

   
# Provide location to the edaf preprocessed database file
DB_FILE = '/home/wilsonan/edaf_new/edaf/data/res3_db.sql'

# Provide location to the output CSV file where the calculated delays and parameters will be stored
CSV_FILE = '/home/wilsonan/edaf_new/edaf/data/delayComp3.csv'

# ====================================================================================================================
# Packet analyzer
analyzer = ULPacketAnalyzer(DB_FILE)

# Search range for packets to analyze. This can be adjusted based on the specific UE IP IDs present in the database. The current range is set to include all UE IP IDs from the first to the last recorded in the analyzer.
# uids_arr = range(analyzer.first_ueipid, analyzer.last_ueipid+1)
uids_arr = range(analyzer.first_ueipid, analyzer.first_ueipid+10)

# Get packets for the specified range and store them into the 'packets' variable. 
# The 'packets' is a strucure that contains all the relevant information for each packet.
packets = analyzer.figure_packettx_from_ueipids(uids_arr)

# Associate the packets with the corresponding RAN events using the NLMT timestamps. 
packets = analyzer.figure_nlmt_ran_association(uids_arr, packets)

# Extract the unique RNTIs from the packets to identify which UEs are involved in the analysis. 
packets_rnti_set = set([item['rlc.attempts'][0]['rnti'] for item in packets if item['rlc.attempts'][0]['rnti']!=None])
print(f'RNTIs in packets: {list(packets_rnti_set)}')

#======================================================================================================================
# Channel analyzer
chan_analyzer = ULChannelAnalyzer(DB_FILE)
begin_ts = chan_analyzer.first_ts
end_ts = chan_analyzer.last_ts
WINDOW_LEN_SECONDS = 2

mcs_arr_all = chan_analyzer.find_mcs_from_ts(begin_ts,end_ts)
set_rnti = set([item['rnti'] for item in mcs_arr_all])
# filter entries with rnti list(packets_rnti_set)[0] 
if list(packets_rnti_set)[0]!=None:
    mcs_arr = [mcs for mcs in mcs_arr_all if mcs['rnti']==list(packets_rnti_set)[0]]
mcs_sorted_dict = SortedDict({mcs['timestamp']: mcs for mcs in mcs_arr})

#======================================================================================================================
# Delay component calculation

# Calculate end-to-end delay
print('Calculating end-to-end delay...')
e2e_delays = np.array(
    [get_e2e_delay(packet) if get_e2e_delay(packet) is not None else np.nan
     for packet in packets],dtype=float)
print('DONE')

idt = np.array(list({packets[ind]['id']: packets[ind]['ip.in_t']-packets[ind-1]['ip.in_t'] for ind in range(1, len(packets)) if packets[ind]['ip.in_t']!=None and  packets[ind-1]['ip.in_t']!=None}.values()))

# Queueing delays
print('Calculating queueing delays...')
queueing_delays = np.array(
    list({packet['id']: queueing_delay
          for packet in packets
          for queueing_delay in [get_queueing_delay(packet)]}.values()))
print('DONE')


# Segmentation delay
print('Calculating segmentation delay...')
segmentation_delay = np.array(list({packet['id']: seg_delay for packet in packets
                                    for seg_delay in [get_segmentation_delay(packet)]}.values()))
print('DONE')

# Retransmission delay
print('Calculating retransmission delays...')
retx_delays = np.array(list({packet['id']: retx_delay for packet in packets
                             for retx_delay in [get_retx_delay(packet)]}.values()))
print('DONE')

# Transmission delay
print('Calculating transmission delays...')
tx_delays = np.array(list({packet['id']: tx_delay for packet in packets
                           for tx_delay in [get_tx_delay(packet)]}.values()))
print('DONE')

# MCS index array
mcss = np.array(list({packet['id']: get_mcs(packet, mcs_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_mcs(packet, mcs_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))

# Segments array
segments = np.array(list({packet['id']: get_segments(packet) for packet in packets if get_segments(packet)!=None}.values()))


# Define the list of delay variables
delay_variables = [
     'segmentation delay',
     'Retransmission delay',
     'Transmission delay',
     'End to End Delay', 
     'Queuing delay',
]
# print(delay_variables)

with open(CSV_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(delay_variables)
    for row in zip_longest(segmentation_delay, retx_delays, tx_delays, e2e_delays,  queueing_delays, fillvalue=""):
        writer.writerow(row)

# Calculate Packet SN, Packet ID, Packet length, No of RLC segments, No of Harq retransmissions, MCS Index
maxNoHarqAttempts = []
packetSN = []
packetID = []
packetLen = []
nRlcSegments = []
mcsIndex = []
for packet in packets:
    max_rlc_seg = get_max_rlc_seg(packet)
    mcsIdx = get_mcs(packet, mcs_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)
    mcsIndex.append(mcsIdx)
    maxNoHarqAttempts.append(len(max_rlc_seg['mac.attempts']))
    packetSN.append(packet['sn'])
    packetID.append(packet['id'])
    packetLen.append(packet['len'])
    nRlcSegments.append(len(packet['rlc.attempts']))

# Load CSV into a DataFrame
df = pd.read_csv(CSV_FILE)

# Align lengths by reindexing
# This ensures no matter the size, it fills missing rows with NaN

df["Packet SN"] = pd.Series(packetSN)
df["Packet ID"] = pd.Series(packetID)
df["Packet Length"] = pd.Series(packetLen)
df["No of RLC attempts"] = pd.Series(nRlcSegments)
df["mcs"] = pd.Series(mcsIndex)
df["Max No of MAC attempts"] = pd.Series(maxNoHarqAttempts)


# 3. Reorder columns to move new ones to the front
cols = ["Packet SN", "Packet ID","Packet Length","No of RLC attempts","mcs","Max No of MAC attempts"] + [c for c in df.columns if c not in ["Packet SN", "Packet ID","Packet Length","No of RLC attempts","mcs","Max No of MAC attempts"]]
df = df[cols]



# Reorder columns to move new ones to the end (or adjust as needed)
cols = ["Packet SN", "Packet ID","Packet Length","No of RLC attempts","mcs","Max No of MAC attempts", 
        "segmentation delay", "Retransmission delay", "Transmission delay", "Queuing delay", "End to End Delay"] + \
       [c for c in df.columns if c not in ["Packet SN", "Packet ID","Packet Length","No of RLC attempts","mcs",
                                              "Max No of MAC attempts", "segmentation delay", "Retransmission delay", 
                                              "Transmission delay", "Queuing delay", "End to End Delay"]]

# Save back to CSV (overwrites file)
df.to_csv(CSV_FILE, index=False)
