
import os, sys, gzip, json
from pathlib import Path
from loguru import logger
import pandas as pd
from edaf.core.uplink.preprocess import preprocess_ul
from edaf.core.uplink.analyze_packet import ULPacketAnalyzer
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np

PACKET_IN_DECISION_DELAY_MIN = 10 # slots delay between decision and in packet

def get_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
    idx=sched_sorted_dict.bisect_right(packet['ip.in_t']+PACKET_IN_DECISION_DELAY_MIN*slots_duration_ms*0.001)
    if idx!=None and idx < len(sched_sorted_dict):
        schedule_ts = sched_sorted_dict[sched_sorted_dict.keys()[idx]]['schedule_ts']
        return (schedule_ts-packet['ip.in_t'])*1000
    else:
        return None

def get_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5):
    idx=sr_bsr_tx_sorted_list.bisect_right(packet['ip.in_t'])
    if idx!=None and idx < len(sr_bsr_tx_sorted_list):
        return (sr_bsr_tx_sorted_list[idx]-packet['ip.in_t'])*1000
    else:
        return None
    

def get_buffer_len(packet, bsrupd_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
    idx=bsrupd_sorted_dict.bisect_right(packet['ip.in_t'])
    if idx!=None and idx < len(bsrupd_sorted_dict):
        return bsrupd_sorted_dict[bsrupd_sorted_dict.keys()[idx]]['len']
    else:
        return None

# delay between ip.in and first segment mac.in  
def get_queueing_delay(packet):
    min_delay = np.inf
    for rlc_seg in packet['rlc.attempts']:
        if rlc_seg.get('mac.in_t')!=None and packet.get('ip.in_t')!=None and packet.get('rlc.attempts')!=None:
            min_delay = min(min_delay, rlc_seg['mac.in_t']-packet['ip.in_t'])
        else:
            logger.error(f"Packet {packet['id']} Either mac.in_t, ip.in_t or rlc.attempts not present")
            return None
    return min_delay*1000


# delay between ip.in and first segment mac.in  - frame alignment delay
def get_queueing_delay_wo_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5):
    queueing_delay = get_queueing_delay(packet)
    frame_alignment_delay = get_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5)
    if queueing_delay!=None and frame_alignment_delay!=None and queueing_delay>frame_alignment_delay:
        return queueing_delay-frame_alignment_delay
    else:
        return None

# delay between ip.in and first segment mac.in  - scheduling delay
def get_queueing_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
    queueing_delay = get_queueing_delay(packet)
    scheduling_delay = get_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)
    if queueing_delay!=None and scheduling_delay!=None and queueing_delay>scheduling_delay:
        return queueing_delay-scheduling_delay
    else:
        return None
            
# return ran delay in milliseconds: 
def get_ran_delay(packet):
    if packet.get('rlc.out_t')!=None and packet.get('ip.in_t')!=None:
        return (packet['rlc.out_t']-packet['ip.in_t'])*1000
    else:
        logger.error(f"Packet {packet['id']} either ip.in_t or rlc.out_t not present")
        return None
    
# Return end to end delay in milliseconds: 
def get_e2e_delay(packet):
    if packet.get('app.ip.out_t')!=None and packet.get('app.ip.in_t')!=None:
        return (packet['app.ip.out_t']-packet['app.ip.in_t'])*1000
    else:
        logger.error(f"Packet {packet['id']} with application packet id {packet['app.sn']} either app.ip.in_t or app.ip.out_t not present")
        return None

# Return time difference between send time from the application and the in time in the RAN in milliseconds: 
def get_app_ran_time_diff(packet):
    if packet.get('nlmt_gtp_timediff_ms')!=None:
        return packet['nlmt_gtp_timediff_ms']
    else:
        logger.error(f"Packet {packet['id']} doesn't contain nlmt_gtp_timediff_ms field")
        return None
    

def get_ran_delay_wo_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5):
    if get_ran_delay(packet)>0 and get_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5)>0:
        return get_ran_delay(packet)-get_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5)
    else:
        return None

def get_ran_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
    if get_ran_delay(packet)>0 and get_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
        return get_ran_delay(packet)-get_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)
    else:
        return None

def get_retx_delay_seg(packet, rlc_seg):
    max_delay, min_delay = -np.inf, np.inf
    for mac_attempt in rlc_seg['mac.attempts']:
        if mac_attempt.get('phy.in_t')!=None:
            max_delay = max(max_delay, mac_attempt['phy.in_t'])
            min_delay = min(min_delay, mac_attempt['phy.in_t'])
        else:
            logger.error(f"Packet {packet['id']} phy.in_t not present")
            return None
    return (max_delay-min_delay)*1000

# Identify the rlc segment that experiences the maximum service delay among all segments of the packet.
# The maximum service delay is calculated as the maximum retransmission delay among all the segments
def get_max_rlc_seg(packet):
    max_delay = -np.inf
    max_rlc_seg = None
    for rlc_seg in packet['rlc.attempts']:
        if len(rlc_seg['mac.attempts'])>0 and get_retx_delay_seg(packet, rlc_seg)!=None:
            if get_retx_delay_seg(packet, rlc_seg) > max_delay:
                max_delay = get_retx_delay_seg(packet, rlc_seg)
                max_rlc_seg = rlc_seg
        else:
            logger.error(f"Packet {packet['id']} mac.attempts not present")
            return None
    return  max_rlc_seg

# Calculate the retransmission delay for the segment that experiences maximum service delay.
def get_retx_delay(packet):
    max_delay = -np.inf
    max_rlc_seg = get_max_rlc_seg(packet)
    if max_rlc_seg!=None and get_retx_delay_seg(packet, max_rlc_seg)!=None:
        return get_retx_delay_seg(packet, max_rlc_seg)
    else:
        return None


# Calculate the transmission delay for the segment that experiences maximum service delay.
# The transmission delay is calculated as the time difference between mac.attempt that experiences maximum delay between phy.in and phy.out or phy.decode (if phy.out is not present). 
# There is not much delay difference between phy.out and phy.decode, as the decoding is done immediately after receiving the last symbol of the segment. So we can consider phy.decode as the end of transmission if phy.out is not present.            
def get_tx_delay(packet):
    max_rlc_seg = get_max_rlc_seg(packet)
    max_delay = -np.inf
    for mac_attempt in max_rlc_seg['mac.attempts']:
        if mac_attempt.get('phy.in_t')!=None :
            if mac_attempt['phy.out_t']!=None:
                max_delay = max(max_delay, (mac_attempt['phy.out_t']-mac_attempt['phy.in_t']))
            else:
                if mac_attempt['phy.decode_t']!=None:
                    max_delay = max(max_delay, (mac_attempt['phy.decode_t']-mac_attempt['phy.in_t']))
                else:
                    logger.error(f"Packet {packet['id']} phy.out_t and phy.decode_t not present")
        else:
            logger.error(f"Packet {packet['id']} phy.in_t not present")
    if max_delay>-np.inf:
        return max_delay*1000
    else:
        return None
    
def get_segmentation_delay(packet):
    retx_delay = get_retx_delay(packet)
    tx_delay =  get_tx_delay(packet)

    first_rlc_seg_in = np.inf
    # find the segment which experiences the first mac attempt
    for rlc_seg in packet['rlc.attempts']:
        if rlc_seg['mac.in_t']!=None:
            first_rlc_seg_in = min(first_rlc_seg_in, rlc_seg['mac.in_t'])
        else:
            logger.error(f"Packet {packet['id']} mac.in_t not present")
            return None

    if tx_delay!=None and retx_delay!=None and first_rlc_seg_in<np.inf and packet['rlc.out_t']!=None:
        return (packet['rlc.out_t']-first_rlc_seg_in)*1000-tx_delay-retx_delay
    else:
        return None

def get_segmentation_delay_wo_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5):
    segmentation_delay = get_segmentation_delay(packet)
    frame_alignment_delay = get_frame_alignment_delay(packet, sr_bsr_tx_sorted_list, slots_per_frame=20, slots_duration_ms=0.5)
    if segmentation_delay!=None and frame_alignment_delay!=None and segmentation_delay>=frame_alignment_delay:
        return segmentation_delay-frame_alignment_delay
    else:
        return None
    
def get_segmentation_delay_wo_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
    segmentation_delay = get_segmentation_delay(packet)
    scheduling_delay = get_scheduling_delay(packet, sched_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5)
    if segmentation_delay!=None and scheduling_delay!=None and segmentation_delay>=scheduling_delay:
        return segmentation_delay-scheduling_delay
    else:
        return None
                        
def get_segments(packet):
    return len(set([rlc_seg['so'] for rlc_seg in packet['rlc.attempts']]))

def get_mcs(packet, mcs_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
    idx=mcs_sorted_dict.bisect_right(packet['ip.in_t']+PACKET_IN_DECISION_DELAY_MIN*slots_duration_ms*0.001)
    if idx < len(mcs_sorted_dict):
        return mcs_sorted_dict[mcs_sorted_dict.keys()[idx]]['mcs']
    else:
        return None    

def get_tb(packet, tb_sorted_dict, slots_per_frame=20, slots_duration_ms=0.5):
    idx=tb_sorted_dict.bisect_right(packet['ip.in_t']+PACKET_IN_DECISION_DELAY_MIN*slots_duration_ms*0.001)
    if idx < len(tb_sorted_dict):
        return tb_sorted_dict[tb_sorted_dict.keys()[idx]]
    else:
        return None
    
# check this
def get_rlc_reassembely_delay(packet):
    return (packet['rlc.out_t']-packet['rlc.out_t'])*1000


def get_segment_attempt_delay(rlc_seg):
    return (rlc_seg['mac.out_t']-rlc_seg['mac.in_t'])*1000

def get_rlc_segment_len(rlc_seg):
    return rlc_seg['len']

def get_rlc_segment_mac_attempts(rlc_seg):
    return len(rlc_seg['mac.attempts'])

def get_rbs(max_rlc_seg):
    rbsVal = 0
    lenMacAttempts = len(max_rlc_seg['mac.attempts'])
    for i in range(lenMacAttempts):
        rbsVal = rbsVal + max_rlc_seg['mac.attempts'][i]['rbs']
    
    # return the average resource block size for the rlc segment with maximum delay
    avgRbs = int(rbsVal/lenMacAttempts)
    return avgRbs

# Get the average noise power for the rlc segment that experiences maximum delay
def get_noisePwr(max_rlc_seg):
    noisePwrVal = 0
    lenMacAttempts = len(max_rlc_seg['mac.attempts'])
    for i in range(lenMacAttempts):
        max_rlc_seg_noisePwr = max_rlc_seg['mac.attempts'][i]['noise_pwr']
        if max_rlc_seg_noisePwr == None:
            max_rlc_seg_noisePwr = int(0)
        noisePwrVal = noisePwrVal + max_rlc_seg_noisePwr
    
    # return the average noise power for the rlc segment with maximum delay
    avgNoisePwr = float(noisePwrVal/lenMacAttempts)
    return avgNoisePwr

# Get the average rssi for the rlc segment that experiences maximum delay
def get_rssi(max_rlc_seg):
    rssiVal = 0
    lenMacAttempts = len(max_rlc_seg['mac.attempts'])
    for i in range(lenMacAttempts):
        max_rlc_seg_rssi = max_rlc_seg['mac.attempts'][i]['rssi']
        if max_rlc_seg_rssi == None:
            max_rlc_seg_rssi = int(0)
        rssiVal = rssiVal + max_rlc_seg_rssi
    
    # return the average rssi for the rlc segment with maximum delay
    avgRssi = float(rssiVal/lenMacAttempts)
    return avgRssi

# Get the average wideband cqi for the rlc segment that experiences maximum delay
def get_wbandCqi(max_rlc_seg):
    wbandCqiVal = 0
    lenMacAttempts = len(max_rlc_seg['mac.attempts'])
    for i in range(lenMacAttempts):
        max_rlc_seg_wbandCqi = max_rlc_seg['mac.attempts'][i]['wideband_cqi']
        if max_rlc_seg_wbandCqi == None:
            max_rlc_seg_wbandCqi = int(0)
        wbandCqiVal = wbandCqiVal + max_rlc_seg_wbandCqi
    
    # return the average wideband cqi for the rlc segment with maximum delay
    avgWidebandCqi = float(wbandCqiVal/lenMacAttempts)
    return avgWidebandCqi

# Get the average ulcqi for the rlc segment that experiences maximum delay
def get_ulcqi(max_rlc_seg):
    ulcqiVal = 0
    lenMacAttempts = len(max_rlc_seg['mac.attempts'])
    for i in range(lenMacAttempts):
        max_rlc_seg_ulcqi = max_rlc_seg['mac.attempts'][i]['ul_cqi']
        if max_rlc_seg_ulcqi == None:
            max_rlc_seg_ulcqi = int(0)
        ulcqiVal = ulcqiVal + max_rlc_seg_ulcqi
    
    # return the average ulcqi for the rlc segment with maximum delay
    avgUlcqi = float(ulcqiVal/lenMacAttempts)
    return avgUlcqi


# Get the average received power for the rlc segment that experiences maximum delay
def get_rxPwr(max_rlc_seg):
    rxPwrVal = 0
    lenMacAttempts = len(max_rlc_seg['mac.attempts'])
    for i in range(lenMacAttempts):
        max_rlc_seg_rxPwr = max_rlc_seg['mac.attempts'][i]['rx_pwr']
        if max_rlc_seg_rxPwr == None:
            max_rlc_seg_rxPwr = int(0)
        rxPwrVal = rxPwrVal + max_rlc_seg_rxPwr
    
    # return the average received power for the rlc segment with maximum delay
    avgRxPwr = float(rxPwrVal/lenMacAttempts)
    return avgRxPwr

# Return average snr value for the packet
def get_snr_for_packet(packet, snr_list):
    for snr in snr_list:
        if snr['sn'] == packet['sn'] and snr['id'] == packet['id']:
            return snr['mean_snr']    # Found match
    return None  # No match

# Return average rsrp value for the packet
def get_rsrp_for_packet(packet, rsrp_list):
    for rsrp in rsrp_list:
        if rsrp['sn'] == packet['sn'] and rsrp['id'] == packet['id']:
            return rsrp['mean_rsrp']    # Found match
    return None  # No match