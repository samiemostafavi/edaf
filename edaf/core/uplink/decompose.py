import numpy as np
import math
import pandas as pd
from loguru import logger

MAX_SEGMENTS = 20
MAX_HQROUND = 5
RU_LATENCY_MS = 0.5 #ms
SLOT_DUR = 0.5 # ms

# Add a new column: rlc.reassembled.num_segments
def get_num_segments_gnb(row):
    last_seg = 0
    for i in range(MAX_SEGMENTS):
        if f'rlc.reassembled.{i}.mac.demuxed.timestamp' in row:
            if row[f'rlc.reassembled.{i}.mac.demuxed.timestamp'] > 0:
                last_seg = i
    return last_seg

# Add a new column: rlc.queue.segments.num_segments
def get_num_segments_ue(row):
    last_seg = 0
    for i in range(MAX_SEGMENTS):
        if f'rlc.queue.segments.{i}.rlc.txpdu.timestamp' in row:
            if row[f'rlc.queue.segments.{i}.rlc.txpdu.timestamp'] > 0:
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
            return np.nan
    else:
        hqround_value = row[f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.hqround"]
        if hqround_value >= 0:
            decoded_slot_column = f"rlc.reassembled.{row['rlc.reassembled.num_segments']}.mac.demuxed.mac.decoded.{int(hqround_value)}.timestamp"
            return row[decoded_slot_column]
        else:
            return np.nan

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
def get_max_service_delay_segment_gnb_idx(row):
    segments_service_delays = []
    for i in range(row['rlc.reassembled.num_segments']+1):
        ue_ind = row['rlc.reassembled.num_segments']-i
        gnb_ind = i
        timestamp1 = row[f"rlc.queue.segments.{ue_ind}.rlc.txpdu.timestamp"]
        timestamp2 = row[f"rlc.reassembled.{gnb_ind}.mac.demuxed.mac.decoded.0.timestamp"]
        segments_service_delays.append((timestamp2-timestamp1)*1000)

    max_index = segments_service_delays.index(max(segments_service_delays))
    return max_index

# Add a new column: get longest_segment_tx_delay
def get_max_service_delay_segment_tx_delay(row):
    ue_ind = row['rlc.reassembled.num_segments']-row['max_service_delay_segment_gnb_idx']
    gnb_ind = row['max_service_delay_segment_gnb_idx']
    timestamp1 = row[f"rlc.queue.segments.{ue_ind}.rlc.txpdu.timestamp"]
    timestamp2 = row[f"rlc.reassembled.{gnb_ind}.mac.demuxed.mac.decoded.first.timestamp"]
    return (timestamp2-timestamp1)*1000

def get_max_service_delay_segment_retx_delay(row):
    idx = row['max_service_delay_segment_gnb_idx']
    timestamp1 = row[f"rlc.reassembled.{idx}.mac.demuxed.mac.decoded.first.timestamp"]
    timestamp2 = row[f"rlc.reassembled.{idx}.mac.demuxed.mac.decoded.0.timestamp"]
    return (timestamp2-timestamp1)*1000

def myfloor(inp_arr):
    out_arr = []
    for item in inp_arr:
        if not np.isnan(item):
            out_arr.append(math.floor(item))
        else:
            out_arr.append(math.nan)
    return np.array(out_arr,dtype=np.float64)

def process_ul_journeys(df, ignore_core=False, standalone=False):
    if df is None:
        return None

    if len(df) == 0:
        return df

    if standalone:
        ########### STANDALONE End to End Delay #########
        timestamp1 = df["receive.timestamp"]
        timestamp2 = df["send.timestamp"]
        # Convert timestamps to milliseconds and calculate the difference
        timestamp_difference = ((timestamp1 - timestamp2) * 1000) #-32.0
        df['e2e_delay'] = timestamp_difference
        filtered_df = df[df['e2e_delay'] < 0]
        if filtered_df.shape[0] > 0:
            logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative e2e delay")
        df = df[df['e2e_delay'] >= 0]
        return df

    ################### POST PROCESS ###################
    # find the number of segments for each packet
    df['rlc.reassembled.num_segments'] = df.apply(get_num_segments_gnb, axis=1)
    df['rlc.queue.segments.num_segments'] = df.apply(get_num_segments_ue, axis=1)

    # find the last segment's service time
    df['rlc.queue.segments.last.rlc.txpdu.timestamp'] = df.apply(get_last_segment_txpdu_ts, axis=1)

    # find first harq attempt decode time of all segments
    for seg in range(MAX_SEGMENTS):
        if f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.0.timestamp' in df:
            df[f'rlc.reassembled.{seg}.mac.demuxed.mac.decoded.first.timestamp'] = df.apply(get_first_hqround_rx_ts, args=(seg,), axis=1)

    # find first harq attempt decode time of the first segment
    df['rlc.reassembled.first.mac.demuxed.mac.decoded.first.timestamp'] = df.apply(get_first_hqround_rx_ts, args=(-1,), axis=1)

    # find first segment's complete reassembly time
    df["rlc.reassembled.first.mac.demuxed.mac.decoded.0.timestamp"] = df.apply(get_first_segment_reassembly_ts, axis=1)

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
    filtered_df = df[df['e2e_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative e2e delay")
    df = df[df['e2e_delay'] >= 0]
    if df.shape[0] == 0:
        return df;
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

    filtered_df = df[df['core_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative core delay")
    df = df[df['core_delay'] >= 0]
    if df.shape[0] == 0:
        return df;
    #print(len(df))

    ################### RAN Delay ###################
    if not ignore_core:
        df['ran_delay'] = df['e2e_delay'] - df['core_delay']
    else:
        df['ran_delay'] = df['e2e_delay']

    filtered_df = df[df['ran_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative ran delay")
    df = df[df['ran_delay'] >= 0]
    if df.shape[0] == 0:
        return df;
    #print(len(df))

    ################### Queuing Delay ###################
    # "rlc.queue.segments.0.rlc.txpdu.timestamp" - "rlc.queue.timestamp"
    # Extract timestamp columns as series
    timestamp1 = df["rlc.queue.segments.0.rlc.txpdu.timestamp"]
    timestamp2 = df["rlc.queue.timestamp"]

    # Convert timestamps to milliseconds and calculate the difference
    timestamp_difference = (timestamp1 - timestamp2) * 1000
    df['queuing_delay'] = timestamp_difference
    filtered_df = df[df['queuing_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative queuing delay")
    df = df[df['queuing_delay'] >= 0]
    if df.shape[0] == 0:
        return df;

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
    filtered_df = df[df['link_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative link delay")
    df = df[df['link_delay'] >= 0]
    if df.shape[0] == 0:
        return df;

    df['link_delay_perc'] = timestamp_difference / df['e2e_delay']
    #print(len(df))

    ################### Transmission delay ###################
    # filter the rows where their number of segments on UE is not matched with gnb:
    # 'rlc.reassembled.num_segments' != 'rlc.queue.segments.num_segments'
    filtered_df = df[df['rlc.reassembled.num_segments'] != df['rlc.queue.segments.num_segments']]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to the number of segments on UE is not matched with gnb")
    
    df = df[df['rlc.reassembled.num_segments'] == df['rlc.queue.segments.num_segments']]
    if df.shape[0] == 0:
        return df;

    # find the segment with longest tx+retx (service) delay, then store its gnb index
    df['max_service_delay_segment_gnb_idx'] = df.apply(get_max_service_delay_segment_gnb_idx, axis=1)

    # save the tx delay of the previously discovered segment as the transmission delay
    df['transmission_delay'] = df.apply(get_max_service_delay_segment_tx_delay, axis=1)

    # Remove rows where 'transmission_delay' is less than 0
    filtered_df = df[df['transmission_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative transmission delay")
    df = df[df['transmission_delay'] >= 0]
    if df.shape[0] == 0:
        return df;

    df['transmission_delay_perc'] = timestamp_difference / df['e2e_delay']
    #print(len(df))

    ################### Retransmissions delay ###################
    # save the retx delay of the previously discovered segment as the retransmission delay
    df['retransmission_delay'] = df.apply(get_max_service_delay_segment_retx_delay, axis=1)
    filtered_df = df[df['retransmission_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative retransmission delay")
    df = df[df['retransmission_delay'] >= 0]
    if df.shape[0] == 0:
        return df;

    df['retransmission_delay_perc'] = df['retransmission_delay'] / df['e2e_delay']
    #print(len(df))

    ################### Segmentation delay ###################
    df['segmentation_delay'] = df['link_delay']-(df['transmission_delay']+df['retransmission_delay'])
    filtered_df = df[df['segmentation_delay'] < 0]
    if filtered_df.shape[0] > 0:
        logger.warning(f"{filtered_df.shape[0]} out of {df.shape[0]} got filtered due to negative segmentation delay")
    df = df[df['segmentation_delay'] >= 0]
    if df.shape[0] == 0:
        return df;

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
    new_timestamp_str = 'ip.in.timestamp_no_offset'

    # Add the tdd sync offset to all timestamps
    df[new_timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)

    # Calculate frame number
    tx_tss_fn = np.floor(df[new_timestamp_str] * 100)
    df['arrival_ref_time'] = tx_tss_fn*10

    # Calculate ms offset within the frame
    tx_tss_ms = list((df[new_timestamp_str] * 1000) - (tx_tss_fn*10))
    df['radio_arrival_time_os'] = tx_tss_ms

    # filter arrival times
    #mask = ((df['radio_arrival_time_os'] >= 5) & (df['radio_arrival_time_os'] <= 6.5))
    #df = df[mask]

    ################### Service Time ###################

    for seg in range(MAX_SEGMENTS):
        timestamp_str = f'rlc.queue.segments.{seg}.rlc.txpdu.timestamp'
        new_timestamp_str = f'rlc.queue.segments.{seg}.rlc.txpdu.timestamp_no_offset'
        if timestamp_str in df:
            # Add the tdd sync offset to all timestamps        
            df[new_timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
            # Calculate frame number
            tx_tss_fn = myfloor(df[new_timestamp_str] * 100)
            # Calculate ms offset within the frame
            tx_tss_ms = list( (df[new_timestamp_str] * 1000) - (tx_tss_fn*10) )
            if seg == 0:
                df['service_time_os'] = tx_tss_ms
                df['service_time'] = (df[new_timestamp_str] * 1000) - df['arrival_ref_time']
            else:
                df[f'service_time_seg{seg}_os'] = tx_tss_ms
                df[f'service_time_seg{seg}'] = (df[timestamp_str] * 1000.0) - df['arrival_ref_time']


    ################### Radio Departure Time ###################
    timestamp_str = 'rlc.reassembled.0.mac.demuxed.timestamp'
    new_timestamp_str = 'rlc.reassembled.0.mac.demuxed.timestamp_no_offset'
    # Add the tdd sync offset to all timestamps
    df[new_timestamp_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    # Calculate frame number
    tx_tss_fn = np.floor(df[new_timestamp_str] * 100)
    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[new_timestamp_str] * 1000) - (tx_tss_fn*10) )
    df['radio_departure_time_os'] = tx_tss_ms
    df['radio_departure_time'] = (df[new_timestamp_str] * 1000) - df['arrival_ref_time']

    ################### Core Departure Time ###################
    timestamp_str = 'receive.timestamp'
    timestamp_str_str = 'receive.timestamp_no_offset'
    # Add the tdd sync offset to all timestamps
    df[timestamp_str_str] = df[timestamp_str] + (df['tdd_ts_offset_ms']/1000.0)
    # Calculate frame number
    tx_tss_fn = np.floor(df[timestamp_str_str] * 100)
    # Calculate ms offset within the frame
    tx_tss_ms = list( (df[timestamp_str_str] * 1000) - (tx_tss_fn*10) )
    df['core_departure_time'] = (df[timestamp_str_str] * 1000) - df['arrival_ref_time']

    return df
