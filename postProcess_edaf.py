import seaborn as sns
from decomp import *
from sortedcontainers import SortedList, SortedDict
from loguru import logger
import os, sys, gzip, json
import sqlite3
import pandas as pd
from edaf.core.uplink.preprocess import preprocess_ul
from edaf.core.uplink.analyze_packet import ULPacketAnalyzer
from edaf.core.uplink.analyze_channel import ULChannelAnalyzer
from edaf.core.uplink.analyze_scheduling import ULSchedulingAnalyzer
import matplotlib.pyplot as plt
import matplotlib.patches as patches
#from IPython.display import JSON
from pathlib import Path
import ijson
import numpy as np
import csv
from itertools import zip_longest


# Configure logger
logger.remove()
logger.add(sys.stdout, level="ERROR")

DB_FILE = '/home/wilsonan/edaf_new/edaf/nov9_results/database_09112025.db'
CSV_FILE = '/home/wilsonan/edaf_new/edaf/nov9_results/delayCal_09112025_v2.csv'

# Packet analyzer
analyzer = ULPacketAnalyzer(DB_FILE)
uids_arr = range(analyzer.first_ueipid, analyzer.last_ueipid+1)
# uids_arr = range(analyzer.first_ueipid, analyzer.first_ueipid+10000)
packets = analyzer.figure_packettx_from_ueipids(uids_arr)
#packets_rnti_set = set([item['rlc.attempts'][0]['rnti'] for item in packets if item['rlc.attempts'][0]['rnti']==list(packets_rnti_set)[0] or item['rlc.attempts'][0]['rnti']==None])
packets_rnti_set = set([item['rlc.attempts'][0]['rnti'] for item in packets if item['rlc.attempts'][0]['rnti']!=None])
print(f'RNTIs in packets: {list(packets_rnti_set)}')


#===============================================================================================================================================
# Schedule analyzer
sched_analyzer = ULSchedulingAnalyzer(
    total_prbs_num = 106, 
    symbols_per_slot = 14, 
    slots_per_frame = 20, 
    slots_duration_ms = 0.5, 
    scheduling_map_num_integers = 4,
    max_num_frames=100,
    db_addr = DB_FILE
)
begin_ts = sched_analyzer.first_ts
end_ts = sched_analyzer.last_ts
sched_arr = sched_analyzer.find_resource_schedules_from_ts(begin_ts, end_ts)
#sched_sorted_dict = SortedDict({sched['decision_ts']: sched for sched in sched_arr})
sched_sorted_dict = SortedDict({sched['schedule_ts']: sched for sched in sched_arr 
                                if sched['cause'].get('rnti')==list(packets_rnti_set)[0]
                                or sched['cause'].get('rnti')==None})

bsrupd_arr = sched_analyzer.find_bsr_upd_from_ts(begin_ts, end_ts)
sr_tx_arr = sched_analyzer.find_sr_tx_from_ts(begin_ts, end_ts)
bsr_tx_arr = sched_analyzer.find_bsr_tx_from_ts(begin_ts, end_ts)
sr_bsr_tx_sorted_list = SortedList([sr_tx['timestamp'] for sr_tx in sr_tx_arr]+[bsr_tx['timestamp'] for bsr_tx in bsr_tx_arr])

#===============================================================================================================================================
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

# To check and verify
# # # Calculate the transport block variables (suppposedly) array 
# tb_arr_all = chan_analyzer.find_mac_attempts_from_ts(begin_ts,end_ts)
# set_rnti = set([item['rnti'] for item in tb_arr_all])
# if list(packets_rnti_set)[0]!=None:
#     tb_arr = [tb for tb in tb_arr_all if tb['rnti']==list(packets_rnti_set)[0]]
# tb_sorted_dict = SortedDict({tb['timestamp']: tb for tb in tb_arr})

#print(f'RNTIs in channel: {list(set_rnti)}')

# Calculate end-to-end delay
send_ts = analyzer.nlmt_df['timestamps.client.send.wall'].to_numpy()
send_ts = (send_ts)/1e6
receive_ts = analyzer.nlmt_df['timestamps.server.receive.wall'].to_numpy()
receive_ts = (receive_ts)/1e6
idt = (send_ts[1:-1]-send_ts[0:-2])
e2e_delays = receive_ts-send_ts

idt = np.array(list({packets[ind]['id']: packets[ind]['ip.in_t']-packets[ind-1]['ip.in_t'] for ind in range(1, len(packets)) if packets[ind]['ip.in_t']!=None and  packets[ind-1]['ip.in_t']!=None}.values()))

# Frame alignment delays
frame_alignment_delays = np.array(list({packet['id']: get_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))

# Scheduling delays
scheduling_delays = np.array(list({packet['id']: get_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))

# Ran delays
ran_delays = np.array(list({packet['id']: get_ran_delay(packet) for packet in packets if get_ran_delay(packet)!=None}.values()))
# Ran delay without frame alignment delay
ran_delays_wo_frame_alignment_delay = np.array(list({packet['id']: get_ran_delay_wo_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_ran_delay_wo_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))
# Ran delay without scheduling delay
ran_delays_wo_scheduling_delay = np.array(list({packet['id']: get_ran_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_ran_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))

# Queueing delays
queueing_delays = np.array(list({packet['id']: get_queueing_delay(packet) for packet in packets if get_queueing_delay(packet)!=None}.values()))
# Queueing delay without scheduling delay
queueing_delays_wo_scheduling_delay = np.array(list({packet['id']: get_queueing_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_queueing_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))

# Segmentation delay
segmentation_delay = np.array(list({packet['id']: get_segmentation_delay(packet) for packet in packets if get_segmentation_delay(packet)!=None}.values()))
print('Segmentation delay is:',segmentation_delay)
print('Length of segmentation delay is:', len(segmentation_delay))
# Segmentation delay without scheduling delay
segmentation_delays_wo_scheduling_delay = np.array(list({packet['id']: get_segmentation_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_segmentation_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))

# Segments array
segments = np.array(list({packet['id']: get_segments(packet) for packet in packets if get_segments(packet)!=None}.values()))

# Retransmission delay
retx_delays = np.array(list({packet['id']: get_retx_delay(packet) for packet in packets if get_retx_delay(packet)!=None}.values()))

# Transmission delay
tx_delays = np.array(list({packet['id']: get_tx_delay(packet) for packet in packets if get_tx_delay(packet)!=None}.values()))

# MCS index array
mcss = np.array(list({packet['id']: get_mcs(packet, mcs_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5) for packet in packets if get_mcs(packet, mcs_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)!=None}.values()))


# # Define the list of delay variables
# delay_variables = [
#     'frame_alignment_delays',
#     'scheduling_delays',
#     'ran_delays',
#     'ran_delays_wo_frame_alignment_delay',
#     'ran_delays_wo_scheduling_delay',
#     'queueing_delays',
#     'queueing_delays_wo_scheduling_delay',
#     'segmentation_delay',
#     'segmentation_delays_wo_scheduling_delay',
#     'retx_delays',
#     'tx_delays',
# 'e2e_delays'
# ]
# print(delay_variables)

# with open(CSV_FILE, "w", newline="") as f:
#     writer = csv.writer(f)
#     writer.writerow(delay_variables)
#     for row in zip_longest(frame_alignment_delays, scheduling_delays, ran_delays, ran_delays_wo_frame_alignment_delay, ran_delays_wo_scheduling_delay, queueing_delays, queueing_delays_wo_scheduling_delay, segmentation_delay, segmentation_delays_wo_scheduling_delay, retx_delays, tx_delays, e2e_delays, fillvalue=""):
#             writer.writerow(row)


# Define the list of delay variables
delay_variables = [
     'segmentation delay',
     'Retransmission delay',
     'Transmission delay',
     'End to End Delay',
     'Frame alignment delay',
     'Scheduling delay',
     'Ran delay',
     'Queuing delay',
     'ran_delays_wo_frame_alignment_delay',
     'ran_delays_wo_scheduling_delay',
     'queueing_delays_wo_scheduling_delay',
     'segmentation_delays_wo_scheduling_delay',
]
print(delay_variables)

with open(CSV_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(delay_variables)
    for row in zip_longest(segmentation_delay, retx_delays, tx_delays, e2e_delays, frame_alignment_delays, scheduling_delays, ran_delays, queueing_delays, ran_delays_wo_frame_alignment_delay, ran_delays_wo_scheduling_delay, queueing_delays_wo_scheduling_delay, segmentation_delays_wo_scheduling_delay, fillvalue=""):
        writer.writerow(row)

# Calculate Packet SN, Packet ID, Packet length, No of RLC segments, No of Harq retransmissions, MCS Index
# Get transport block size
# We use here the mac.sdu.tbs parameter for the rlc segment with max delay
maxNoHarqAttempts = []
packetSN = []
packetID = []
packetLen = []
nRlcSegments = []
mcsIndex = []
tbSize = []     # in bytes
avgRbSize = []
avgNoisePwr = []
avgRssi = []
avgRxPwr = []
avgWidebandCqi = []
for packet in packets:
    max_rlc_seg = get_max_rlc_seg(packet)
    mcsIdx = get_mcs(packet, mcs_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)
    mcsIndex.append(mcsIdx)
    maxNoHarqAttempts.append(len(max_rlc_seg['mac.attempts']))
    packetSN.append(packet['sn'])
    packetID.append(packet['id'])
    packetLen.append(packet['len'])
    nRlcSegments.append(len(packet['rlc.attempts']))
    tbSize.append(max_rlc_seg['mac.sdu.tbs'])
    avgRbSize.append(get_rbs(max_rlc_seg))
    avgNoisePwr.append(get_noisePwr(max_rlc_seg))
    avgRssi.append(get_rssi(max_rlc_seg))
    avgRxPwr.append(get_rxPwr(max_rlc_seg))
    avgWidebandCqi.append(get_wbandCqi(max_rlc_seg))
    

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
df["Transport block size (max delay rlc seg)"] = pd.Series(tbSize)
df["Resource block size (Avg)"] = pd.Series(avgRbSize)
df["Noise Power (Avg)"] = pd.Series(avgNoisePwr)
df["Received Power (Avg)"] = pd.Series(avgRxPwr)
df["RSSI (Avg)"] = pd.Series(avgRssi)
df["Wideband CQI (Avg)"] = pd.Series(avgWidebandCqi)


# 3. Reorder columns to move new ones to the front
cols = ["Packet SN", "Packet ID","Packet Length","No of RLC attempts","mcs","Max No of MAC attempts"] + [c for c in df.columns if c not in ["Packet SN", "Packet ID","Packet Length","No of RLC attempts","mcs","Max No of MAC attempts"]]
df = df[cols]

# Save back to CSV (overwrites file)
df.to_csv(CSV_FILE, index=False)


# TODO: Find out ways to debug the database file in minimum time, else this will take a long time than anticipated.
# TODO: Add postprocessing utils folder containing relevant utility scripts like finding retransmission number, excel conversion, etc.
# TODO: The last values added: noise power, received power, rssi, wideband cqi show values such as 'None'. This needs to be handled. This is maybe due to unsuccessful harq retransmissions.
