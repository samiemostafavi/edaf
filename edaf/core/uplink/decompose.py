import numpy as np
import math
import pandas as pd

MAX_SEGMENTS = 20
MAX_HQROUND = 5
RU_LATENCY_MS = 0.5 #ms
SLOT_DUR = 0.5 # ms

# Add a new column: rlc.reassembled.num_segments
def get_num_segments(row):
    last_seg = 0
    for i in range(MAX_SEGMENTS):
        #if f'rlc.queue.segments.{i}.rlc.txpdu.timestamp' in row:
        if f'rlc.reassembled.{i}.mac.demuxed.hqround' in row:
            #if row[f'rlc.queue.segments.{i}.rlc.txpdu.timestamp'] > 0:
            if row[f'rlc.reassembled.{i}.mac.demuxed.hqround'] > 0:
                last_seg = i
    return last_seg

# Add a new row: last_segment_txpdu_ts
def get_last_segment_txpdu_ts(row):
    last_seg = 0
    for i in range(MAX_SEGMENTS):
        if f'rlc.queue.segments.{i}.rlc.txpdu.timestamp' in row:
            if row[f'rlc.queue.segments.{i}.rlc.txpdu.timestamp'] > 0:
                last_seg = i
    last_seg_column = f'rlc.queue.segments.{last_seg}.rlc.txpdu.timestamp'
    return row[last_seg_column]

# Add a new column: rlc.reassembled.first
def get_first_segment_reassembly_ts(row):
    first_seg_column = f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.mac.decoded.0.timestamp"
    return row[first_seg_column]

# Add a new column: first_hqround_rx_ts
def get_first_hqround_rx_ts(row,seg):
    if seg != -1:
        hqround_value = row[f'rlc.reassembled.{seg}.mac.demuxed.hqround']
        if hqround_value >= 0:
            decoded_slot_column = f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.{int(hqround_value)}.timestamp'
            return row[decoded_slot_column]
        else:
            return np.NaN
    else:
        hqround_value = row[f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.hqround"]
        if hqround_value >= 0:
            decoded_slot_column = f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.mac.decoded.{int(hqround_value)}.timestamp"
            return row[decoded_slot_column]
        else:
            return np.NaN

# Add a new column: last_segment_txpdu_ts
def get_last_segment_txpdu_ts(row):
    last_seg = 0
    for i in range(MAX_SEGMENTS):
        if f'rlc.queue.segments.{i}.rlc.txpdu.timestamp' in row:
            if row[f'rlc.queue.segments.{i}.rlc.txpdu.timestamp'] > 0:
                last_seg = i
    last_seg_column = f'rlc.queue.segments.{last_seg}.rlc.txpdu.timestamp'
    return row[last_seg_column]

# Add a new column: tdd_ts_offset_ms
def get_tdd_ts_offset_ms(row):
    slot_num = row['rlc.reassembled.0.mac.demuxed.slot']
    # 0.2 ms delay for demodulation and decoding
    slot_ref_offset = slot_num*SLOT_DUR+RU_LATENCY_MS

    tdd_offset_ms = slot_ref_offset - row['rx_ts_ms']
    return tdd_offset_ms

# Add a new column: find the longest segment service delay (tx + retx), save its gnb index
def get_max_delay_segment_gnbind(row):
    segment_delays = []
    for i in range(row['rlc.reassembled.num_segments']+1):
        ue_ind = row['rlc.reassembled.num_segments']-i
        gnb_ind = i
        timestamp1 = row[f"rlc.queue.segments.{ue_ind}.rlc.txpdu.timestamp"]
        timestamp2 = row[f"rlc.reassembled.{gnb_ind}.mac.demuxed.mac.decoded.0.timestamp"]
        segment_delays.append((timestamp2-timestamp1)*1000)

    max_index = segment_delays.index(max(segment_delays))
    return max_index

# Add a new column: get longest_segment_tx_delay
def get_longest_segment_tx_delay(row):
    ue_ind = row['rlc.reassembled.num_segments']-row['max_delay_segment_gnbind']
    gnb_ind = row['max_delay_segment_gnbind']
    timestamp1 = row[f"rlc.queue.segments.{ue_ind}.rlc.txpdu.timestamp"]
    timestamp2 = row[f"rlc.reassembled.{gnb_ind}.mac.demuxed.mac.decoded.first.timestamp"]
    return (timestamp2-timestamp1)*1000

def get_longest_segment_retx_delay(row):
    ind = row['max_delay_segment_gnbind']
    timestamp1 = row[f"rlc.reassembled.{ind}.mac.demuxed.mac.decoded.first.timestamp"]
    timestamp2 = row[f"rlc.reassembled.{ind}.mac.demuxed.mac.decoded.0.timestamp"]
    return (timestamp2-timestamp1)*1000

# Add a new column: retransmission_delay
def calc_full_retransmission_delay(row):
    sum_delay = 0.0
    for seg in range(row['rlc.reassembled.num_segments']):
        timestamp1 = row[f"rlc.reassembled.{seg}.mac.demuxed.mac.decoded.first.timestamp"]
        timestamp2 = row[f"rlc.reassembled.{seg}.mac.demuxed.mac.decoded.0.timestamp"]
        sum_delay += ((timestamp2 - timestamp1) * 1000)
    return sum_delay

def myfloor(inp_arr):
    out_arr = []
    for item in inp_arr:
        if not np.isnan(item):
            out_arr.append(math.floor(item))
        else:
            out_arr.append(math.nan)
    return np.array(out_arr,dtype=np.float64)

def process_ul_journeys(df, ignore_core=False):
    if len(df) == 0:
        return df

    ################### POST PROCESS ###################
    # find num_segments
    df['rlc.reassembled.num_segments'] = df.apply(get_num_segments, axis=1)

    # find last_segment_txpdu_ts
    df['rlc.queue.segments.last.rlc.txpdu.timestamp'] = df.apply(get_last_segment_txpdu_ts, axis=1)

    # find first_hqround_rx_ts
    for seg in range(MAX_SEGMENTS):
        if f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.0.timestamp' in df:
            df[f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.first.timestamp'] = df.apply(get_first_hqround_rx_ts, args=(seg,), axis=1)

    df['rlc.reassembled.first.mac.demuxed.mac.decoded.first.timestamp'] = df.apply(get_first_hqround_rx_ts, args=(-1,), axis=1)

    # find rlc.reassembled.first
    df["rlc.reassembled.first.mac.demuxed.mac.decoded.0.timestamp"] = df.apply(get_first_segment_reassembly_ts, axis=1)

    # find last_segment_txpdu_ts
    df['rlc.queue.segments.last.rlc.txpdu.timestamp'] = df.apply(get_last_segment_txpdu_ts, axis=1)

    #print(len(df))
    ################### End to End Delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    if ignore_core:
        timestamp1 = df["gtp.out.timestamp"]
        timestamp2 = df["send.timestamp"]
    else:
        timestamp1 = df["receive.timestamp"]
        timestamp2 = df["send.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = ((timestamp1 - timestamp2) * 1000) #-32.0
    df['e2e_delay'] = timestamp_difference
    df = df[df['e2e_delay'] >= 0]
    #print(len(df))

    ################### Core Delay ###################
    if not ignore_core:
        # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
        # Extract timestamp columns as series
        timestamp1 = df["receive.timestamp"]
        timestamp2 = df["gtp.out.timestamp"]

        # Convert timestamps to milliseconds and calculate the difference
        timestamp_difference = ((timestamp1 - timestamp2) * 1000)#-32.0
        df['core_delay'] = timestamp_difference
        #df = df[df['core_delay'] >= 0]
        df['core_delay_perc'] = timestamp_difference / df['e2e_delay']
    df = df[df['core_delay'] >= 0]
    #print(len(df))

    ################### RAN Delay ###################
    if not ignore_core:
        df['ran_delay'] = df['e2e_delay'] - df['core_delay']
    else:
        df['ran_delay'] = df['e2e_delay']
    df = df[df['ran_delay'] >= 0]
    #print(len(df))

    ################### Queuing Delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.queue.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000
    df['queuing_delay'] = timestamp_difference
    df = df[df['queuing_delay'] >= 0]
    df['queuing_delay_perc'] = timestamp_difference / df['e2e_delay']
    #print(len(df))

    ################### Link Delay ###################
    # "rlc.reassembled.0.mac.demuxed.mac.decoded.timestamp" - "rlc.queue.segments.0.rlc.txpdu.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.0.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp2 - timestamp1) * 1000
    df['link_delay'] = timestamp_difference
    df = df[df['link_delay'] >= 0]
    df['link_delay_perc'] = timestamp_difference / df['e2e_delay']
    #print(len(df))

    ################### Transmission delay ###################

    # find the longest segment, store its gnb index
    df['max_delay_segment_gnbind'] = df.apply(get_max_delay_segment_gnbind, axis=1)

    # find longest segment tx delay
    df['longest_segment_tx_delay_ms'] = df.apply(get_longest_segment_tx_delay, axis=1)

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = df['longest_segment_tx_delay_ms']
    df['transmission_delay'] = timestamp_difference

    # Remove rows where 'transmission_delay' is less than 0
    df = df[df['transmission_delay'] >= 0]
    df['transmission_delay_perc'] = timestamp_difference / df['e2e_delay']
    #print(len(df))

    ################### Retransmissions delay ###################

    # find longest segment retx delay
    df['longest_segment_retx_delay_ms'] = df.apply(get_longest_segment_retx_delay, axis=1)

    # find retx delay
    df["full_retransmission_delay"] = df.apply(calc_full_retransmission_delay, axis=1)

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = df['longest_segment_retx_delay_ms']
    df["retransmission_delay"] = timestamp_difference
    df = df[df['retransmission_delay'] >= 0]
    df['retransmission_delay_perc'] = df['retransmission_delay'] / df['e2e_delay']
    #print(len(df))

    ################### Segmentation delay ###################
    timestamp1 = df["rlc.reassembled.first.mac.demuxed.mac.decoded.first.timestamp"]
    timestamp2 = df["rlc.reassembled.0.mac.demuxed.mac.decoded.first.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = df['link_delay']-(df['longest_segment_retx_delay_ms']+df['longest_segment_tx_delay_ms'])
    df['segmentation_delay'] = timestamp_difference

    # Remove rows where 'segmentation_delay' is less than 0
    df = df[df['segmentation_delay'] >= 0]
    df['segmentation_delay_perc'] = df['segmentation_delay'] / df['e2e_delay']
    #print(len(df))

    ################### ABSOLUTE TIMING CALCULATIONS ###################
    ################### FRAME OFFSET ###################################

    # Calculate 5G TDD Frames Time Offset
    timestamp_str = "rlc.reassembled.0.mac.demuxed.timestamp"
    rx_tss_fn = np.floor(df[timestamp_str] * 100)
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (rx_tss_fn*10) )
    df['rx_ts_ms'] = tx_tss_ms
    # Apply the function to filter rows
    df['tdd_ts_offset_ms'] = df.apply(get_tdd_ts_offset_ms, axis=1)

    ################### Radio Arrival Time ###################

    timestamp_str = 'ip.in.timestamp'

    # Add the tdd sync offset to all timestamps
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)

    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    df['arrival_ref_time'] = tx_tss_fn*10

    # Calculate ms offset within the frame
    tx_tss_ms = list((df[timestamp_str] * 1000) - (tx_tss_fn*10))
    df['radio_arrival_time_os'] = tx_tss_ms

    # filter arrival times
    #mask = ((df['radio_arrival_time_os'] >= 5) & (df['radio_arrival_time_os'] <= 6.5))
    #df = df[mask]

    ################### Service Time ###################

    timestamp_str = 'rlc.queue.segments.0.rlc.txpdu.timestamp'
    # Add the tdd sync offset to all timestamps        
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
    df['service_time_os'] = tx_tss_ms
    df['service_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    timestamp_str = 'rlc.queue.segments.1.rlc.txpdu.timestamp'
    if timestamp_str in df:
        # Add the tdd sync offset to all timestamps        
        df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
        # Calculate frame number
        tx_tss_fn = myfloor(df[timestamp_str] * 100.0)
        # Calculate ms offset within the frame
        tx_tss_ms = list( (df[timestamp_str] * 1000.0) - (tx_tss_fn*10.0) )
        df['service_time_seg2_os'] = tx_tss_ms
        df['service_time_seg2'] = (df[timestamp_str] * 1000.0) - df['arrival_ref_time']

    timestamp_str = 'rlc.queue.segments.2.rlc.txpdu.timestamp'
    if timestamp_str in df:
        # Add the tdd sync offset to all timestamps        
        df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
        # Calculate frame number
        tx_tss_fn = myfloor(df[timestamp_str] * 100.0)
        # Calculate ms offset within the frame
        tx_tss_ms = list( (df[timestamp_str] * 1000.0) - (tx_tss_fn*10.0) )
        df['service_time_seg3_os'] = tx_tss_ms
        df['service_time_seg3'] = (df[timestamp_str] * 1000.0) - df['arrival_ref_time']

    timestamp_str = 'rlc.queue.segments.3.rlc.txpdu.timestamp'
    if timestamp_str in df:
        # Add the tdd sync offset to all timestamps        
        df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
        # Calculate frame number
        tx_tss_fn = myfloor(df[timestamp_str] * 100.0)
        # Calculate ms offset within the frame
        tx_tss_ms = list( (df[timestamp_str] * 1000.0) - (tx_tss_fn*10.0) )
        df['service_time_seg4_os'] = tx_tss_ms
        df['service_time_seg4'] = (df[timestamp_str] * 1000.0) - df['arrival_ref_time']

    ################### Radio Departure Time ###################

    timestamp_str = 'rlc.reassembled.0.mac.demuxed.timestamp'

    # Add the tdd sync offset to all timestamps
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)

    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)

    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )

    df['radio_departure_time_os'] = tx_tss_ms
    df['radio_departure_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']


    ################### Core Departure Time ###################
    timestamp_str = 'receive.timestamp'

    # Add the tdd sync offset to all timestamps
    df[timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str] * 100)
    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[timestamp_str] * 1000) - (tx_tss_fn*10) )
    df['core_departure_time'] = (df[timestamp_str] * 1000) - df['arrival_ref_time']

    return df