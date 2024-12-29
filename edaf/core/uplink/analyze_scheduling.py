import os, sys
import sqlite3
from loguru import logger
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None

if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

class ULSchedulingAnalyzer:
    def __init__(self, total_prbs_num, symbols_per_slot, slots_per_frame, slots_duration_ms, scheduling_map_num_integers, max_num_frames, db_addr):

        # example for config params:
        #self.conf_total_prbs_num = 106
        #self.conf_symbols_per_slot = 14
        #self.conf_slots_per_frame = 20
        #self.conf_slots_duration_ms = 0.5 #ms
        #self.conf_scheduling_map_num_integers = 4

        self.conf_total_prbs_num = total_prbs_num
        self.conf_symbols_per_slot = symbols_per_slot
        self.conf_slots_per_frame = slots_per_frame
        self.conf_slots_duration_ms = slots_duration_ms
        self.conf_scheduling_map_num_integers = scheduling_map_num_integers
        self.conf_max_num_frames = max_num_frames

        # Open a connection to the SQLite database
        conn = sqlite3.connect(db_addr)

        # Read each table from the SQLite database into pandas DataFrames
        self.gnb_ip_packets_df = pd.read_sql('SELECT * FROM gnb_ip_packets', conn)
        logger.info(f"gnb_ip_packets_df: {self.gnb_ip_packets_df.columns.tolist()}")

        self.gnb_rlc_segments_df = pd.read_sql('SELECT * FROM gnb_rlc_segments', conn)
        logger.info(f"gnb_rlc_segments_df: {self.gnb_rlc_segments_df.columns.tolist()}")

        self.gnb_iprlc_rel_df = pd.read_sql('SELECT * FROM gnb_iprlc_rel', conn)
        logger.info(f"gnb_iprlc_rel_df: {self.gnb_iprlc_rel_df.columns.tolist()}")

        self.gnb_mac_attempts_df = pd.read_sql('SELECT * FROM gnb_mac_attempts', conn)
        logger.info(f"gnb_mac_attempts_df: {self.gnb_mac_attempts_df.columns.tolist()}")

        self.gnb_sched_maps_df = pd.read_sql('SELECT * FROM gnb_sched_maps', conn)
        logger.info(f"gnb_sched_maps_df: {self.gnb_sched_maps_df.columns.tolist()}")

        self.gnb_sched_reports_df = pd.read_sql('SELECT * FROM gnb_sched_reports', conn)
        logger.info(f"gnb_sched_reports_df: {self.gnb_sched_reports_df.columns.tolist()}")
        # add schedule_id to gnb_sched_reports_df
        self.gnb_sched_reports_df['schedule_id'] = self.gnb_sched_reports_df.index

        self.ue_ip_packets_df = pd.read_sql('SELECT * FROM ue_ip_packets', conn)
        logger.info(f"ue_ip_packets_df: {self.ue_ip_packets_df.columns.tolist()}")

        self.ue_rlc_segments_df = pd.read_sql('SELECT * FROM ue_rlc_segments', conn)
        logger.info(f"ue_rlc_segments_df: {self.ue_rlc_segments_df.columns.tolist()}")

        self.ue_mac_attempts_df = pd.read_sql('SELECT * FROM ue_mac_attempts', conn)
        logger.info(f"ue_mac_attempts_df: {self.ue_mac_attempts_df.columns.tolist()}")

        self.ue_uldcis_df = pd.read_sql('SELECT * FROM ue_uldcis', conn)
        logger.info(f"ue_uldcis_df: {self.ue_uldcis_df.columns.tolist()}")

        self.ue_iprlc_rel_df = pd.read_sql('SELECT * FROM ue_iprlc_rel', conn)
        logger.info(f"ue_iprlc_rel_df: {self.ue_iprlc_rel_df.columns.tolist()}")

        self.ue_srtxs_df = pd.read_sql('SELECT * FROM ue_srtxs', conn)
        logger.info(f"ue_srtxs_df: {self.ue_srtxs_df.columns.tolist()}")

        self.ue_bsrupds_df = pd.read_sql('SELECT * FROM ue_bsrupds', conn)
        logger.info(f"ue_bsrupds_df: {self.ue_bsrupds_df.columns.tolist()}")

        self.ue_bsrtxs_df = pd.read_sql('SELECT * FROM ue_bsrtxs', conn)
        logger.info(f"ue_bsrtxs_df: {self.ue_bsrtxs_df.columns.tolist()}")

        conn.close()

        # check and report the first and last timestamps
        self.first_ts = self.gnb_sched_maps_df['sched.map.pr.timestamp'].min()
        self.last_ts = self.gnb_sched_maps_df['sched.map.pr.timestamp'].max()

        # check and report the ue_ip_ids and gnb sns
        #logger.success(f"Imported database '{db_addr}', with UE IDs ranging from {self.ue_ip_packets_df['ip_id'].min()} to {self.ue_ip_packets_df['ip_id'].max()}, and GNB SNs ranging from {self.gnb_ip_packets_df['gtp.out.sn'].min()} to {self.gnb_ip_packets_df['gtp.out.sn'].max()}")

    def decode_scheduling_map(self, map_row):
        # find RBs structure
        ins_pr = []
        ins_po = []
        for i in range(self.conf_scheduling_map_num_integers):
            ins_pr.append(int(map_row[f'sched.map.pr.i{self.conf_scheduling_map_num_integers-i-1}m']))
            ins_po.append(int(map_row[f'sched.map.po.i{self.conf_scheduling_map_num_integers-i-1}m']))

        binary_string = ''.join(format(num, '032b') for num in ins_pr)[::-1]
        first_106_bits = binary_string[:self.conf_total_prbs_num]
        pr_bit_list = [int(bit) for bit in first_106_bits]

        binary_string = ''.join(format(num, '032b') for num in ins_po)[::-1]
        first_106_bits = binary_string[:self.conf_total_prbs_num]
        po_bit_list = [int(bit) for bit in first_106_bits]
        
        return pr_bit_list, po_bit_list

    def find_resource_schedules_from_ts(self, begin_ts, end_ts):

        schedules_arr = []
        # bring all sched.map.pr and sched.map.po within this frame (10ms earlier)
        maps = self.gnb_sched_maps_df[
            (self.gnb_sched_maps_df['sched.map.pr.timestamp'] >= begin_ts) &
            (self.gnb_sched_maps_df['sched.map.pr.timestamp'] < end_ts)
        ]
        for i in range(maps.shape[0]):
            schedule = {
                'decision_ts' : None,
                'schedule_ts' : None,
                'symbols_start' : None,
                'symbols_num' : None,
                'prbs_start' : None,
                'prbs_num' : None,
                'cause' : {},
            }
            map_row = maps.iloc[i]

            # find fm, sl and fmtx, sltx
            schedule['decision_ts'] = map_row['sched.map.pr.timestamp']
            abs_sltx_po = int(map_row[f'sched.map.po.frametx'])*self.conf_slots_per_frame +int(map_row[f'sched.map.po.slottx'])
            abs_sl_po = int(map_row[f'sched.map.po.frame'])*self.conf_slots_per_frame +int(map_row[f'sched.map.po.slot'])
            sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*self.conf_slots_duration_ms
            schedule['schedule_ts'] = map_row['sched.map.pr.timestamp']+sltx_tsdif_ms/1000

            # find RBs structure
            pr_bit_list, po_bit_list = self.decode_scheduling_map(map_row)
            if len(po_bit_list) == 0 or len(pr_bit_list) != len(po_bit_list):
                logger.warning("Wrong schedule map codes")
                continue
            #blocked_bits_list = [bit1 & bit2 for bit1, bit2 in zip(pr_bit_list, po_bit_list)]
            toggled_to_zero_array = [1 if pr_bit_list[i] == 1 and po_bit_list[i] == 0 else 0 for i in range(len(pr_bit_list))]
            prbs_num = 0
            prbs_start = np.inf
            for i, bit in enumerate(toggled_to_zero_array):
                if bit == 1:
                    prbs_start = min(i,prbs_start)
                    prbs_num = prbs_num + 1
            if prbs_num == 0:
                # no resources were scheduled
                continue
            schedule['prbs_start'] = prbs_start
            schedule['prbs_num'] = prbs_num
                    
            # find symbols structure
            schedule['symbols_start'] = int(map_row[f'sched.map.po.sb'])
            schedule['symbols_num'] = int(map_row[f'sched.map.po.ss'])

            # look for the cause
            schedule['cause'] = self.find_sched_cause(int(map_row[f'sched.map.po.frametx']), int(map_row[f'sched.map.po.slottx']), map_row['sched.map.pr.timestamp'])

            schedules_arr.append(schedule)

        return schedules_arr

    def find_frame_start_ts_from_ts(self, 
        timestamp, 
        SCHED_OFFSET_S = 0 # 4*SLOT_DURATION_S #2ms or 4 slots is this sl_ahead?
    ):

        NUM_SLOTS_PER_FRAME = self.conf_slots_per_frame
        SLOT_DURATION_S = self.conf_slots_duration_ms/1000
        CLOSENESS_LIMIT_S = SLOT_DURATION_S*NUM_SLOTS_PER_FRAME #10ms (one full frame)

        # find the closest sched.pr map prior to this timestamp
        # bring all sched.map.pr within this frame (10ms earlier)
        maps = self.gnb_sched_maps_df[
            ( self.gnb_sched_maps_df['sched.map.pr.timestamp'] >= timestamp-(CLOSENESS_LIMIT_S) ) &
            ( self.gnb_sched_maps_df['sched.map.pr.timestamp'] < timestamp )
        ]
        if maps.shape[0] == 0:
            logger.error("Did not find any scheduling map for this interval.")
            return (None, None, None)
        
        # sort them by timestamp
        maps = maps.sort_values(by='sched.map.pr.timestamp', ascending=True)

        # just pick the first one
        pr_map_row = maps.iloc[0]

        return float(pr_map_row['sched.map.pr.timestamp']+SCHED_OFFSET_S)


    def find_frame_slot_from_ts(self, 
        timestamp, 
        SCHED_OFFSET_S = 0 # 4*SLOT_DURATION_S #2ms or 4 slots is this sl_ahead?
    ):

        MAX_NUM_FRAMES = self.conf_max_num_frames
        NUM_SLOTS_PER_FRAME = self.conf_slots_per_frame
        SLOT_DURATION_S = self.conf_slots_duration_ms/1000
        CLOSENESS_LIMIT_S = 0.01 #10ms

        # find the closest sched.pr map to this timestamp
        # bring all sched.map.pr within this frame (10ms earlier)
        maps = self.gnb_sched_maps_df[
            (self.gnb_sched_maps_df['sched.map.pr.timestamp'] < timestamp+(CLOSENESS_LIMIT_S/2) ) &
            (self.gnb_sched_maps_df['sched.map.pr.timestamp'] >= timestamp-(CLOSENESS_LIMIT_S/2) )
        ]
        if maps.shape[0] == 0:
            logger.error("Did not find any scheduling map for this interval.")
            return (None, None)

        # just pick the first one
        pr_map_row = maps.iloc[0]
        slots_diff = int(np.floor((timestamp - (pr_map_row['sched.map.pr.timestamp']+SCHED_OFFSET_S))/SLOT_DURATION_S))
        pr_abs_slot_num = pr_map_row['sched.map.po.frame']*NUM_SLOTS_PER_FRAME + pr_map_row['sched.map.po.slot']
        new_abs_slot_num = pr_abs_slot_num + slots_diff
        if new_abs_slot_num < 0:
            new_abs_slot_num = MAX_NUM_FRAMES*NUM_SLOTS_PER_FRAME + new_abs_slot_num

        new_frame_num = new_abs_slot_num // NUM_SLOTS_PER_FRAME
        new_slot_num = new_abs_slot_num % NUM_SLOTS_PER_FRAME

        # calculate the fraction inside the slot that the packet arrived on
        slot_frac = (timestamp - ((pr_map_row['sched.map.pr.timestamp']+SCHED_OFFSET_S) + slots_diff*SLOT_DURATION_S))/SLOT_DURATION_S
        assert slot_frac < 1, f"Slot fraction is greater than 1: {slot_frac}"
        assert slot_frac >= 0, f"Slot fraction is negative: {slot_frac}"

        # calculate the beginning of the frame timestamp
        frame_start_ts = (pr_map_row['sched.map.pr.timestamp']+SCHED_OFFSET_S) - (pr_map_row['sched.map.po.slot']*SLOT_DURATION_S)

        return frame_start_ts, new_frame_num, new_slot_num + slot_frac


    def find_sched_cause(self, frametx, slottx, decision_ts):
        CLOSENESS_SECONDS = 0.005 #5ms

        # find sched.ue for this frame and slot number
        sched_ue_list = self.gnb_sched_reports_df[
            (self.gnb_sched_reports_df['sched.ue.frametx'] == frametx) &
            (self.gnb_sched_reports_df['sched.ue.slottx'] == slottx)
        ]
        sched_ue_list_new = []

        for i in range(sched_ue_list.shape[0]):
            sched_ue_row = sched_ue_list.iloc[i]
            if abs(float(sched_ue_row['sched.cause.timestamp']) - float(decision_ts)) <= CLOSENESS_SECONDS: #5ms close
                sched_ue_list_new.append(sched_ue_row)

        if len(sched_ue_list_new) == 0:
            logger.warning("Did not find scheduling cause for this scheduling map.")
            return {}
        elif len(sched_ue_list_new) > 1:
            logger.warning(f"Found more than one scheduling cause for this frame and slot within {CLOSENESS_SECONDS*1000} ms window.")
            return {}
        
        ue_sched_row = sched_ue_list_new[0]

        return {     
            'rnti' : ue_sched_row['sched.ue.rnti'],
            'tbs' : ue_sched_row['sched.ue.tbs'],
            'mcs' : ue_sched_row['sched.ue.mcs'],
            'rbs' : ue_sched_row['sched.ue.rbs'],
            'type' : ue_sched_row['sched.cause.type'],
            'diff' : ue_sched_row['sched.cause.diff'],
            'buf' : ue_sched_row['sched.cause.buf'],
            'sched' : ue_sched_row['sched.cause.sched'], 
            'hqround' : ue_sched_row['sched.cause.hqround'],
            'hqpid' : ue_sched_row['sched.cause.hqpid']
        }

    def find_latest_bsrupd_before_ts(self, timestamp):

        # bring all bsr.upd within this frame
        # find bsr updates transmitted 'bsr.tx'
        bsr_upd_list = self.ue_bsrupds_df[
            (self.ue_bsrupds_df['timestamp'] < timestamp)
        ]
        if bsr_upd_list.shape[0] == 0:
            logger.warning("Did not find any bsr upd for this interval.")
            return []

        max_timestamp_row = bsr_upd_list.loc[bsr_upd_list['timestamp'].idxmax()]
        return max_timestamp_row


    def find_bsr_upd_from_ts(self, begin_ts, end_ts):

        # bring all bsr.upd within this frame
        # find bsr updates transmitted 'bsr.tx'
        bsr_upd_list = self.ue_bsrupds_df[
            (self.ue_bsrupds_df['timestamp'] >= begin_ts) &
            (self.ue_bsrupds_df['timestamp'] < end_ts)
        ]
        if bsr_upd_list.shape[0] == 0:
            logger.warning("Did not find any bsr upd for this interval.")
            return []

        bsr_upd_rows = []
        for i in range(bsr_upd_list.shape[0]):
            bsr_upd_row = bsr_upd_list.iloc[i]
            bsr_upd_rows.append(dict(bsr_upd_row))

        return sorted(bsr_upd_rows, key=lambda x: x['timestamp'])
    
    def find_bsr_tx_from_ts(self, begin_ts, end_ts):

        # bring all bsr.upd within this frame
        # find bsr updates transmitted 'bsr.tx'
        bsr_tx_list = self.ue_bsrtxs_df[
            (self.ue_bsrtxs_df['timestamp'] >= begin_ts) &
            (self.ue_bsrtxs_df['timestamp'] < end_ts)
        ]
        if bsr_tx_list.shape[0] == 0:
            logger.warning("Did not find any bsr tx for this interval.")
            return []

        bsr_tx_rows = []
        for i in range(bsr_tx_list.shape[0]):
            bsr_tx_row = bsr_tx_list.iloc[i]
            bsr_tx_rows.append(bsr_tx_row)

        return sorted(bsr_tx_rows, key=lambda x: x['timestamp'])

    def find_sr_tx_from_ts(self, begin_ts, end_ts):

        # bring all sr.tx within this frame
        # find bsr updates transmitted 'sr.tx'
        srtx_list = self.ue_srtxs_df[
            (self.ue_srtxs_df['timestamp'] >= begin_ts) &
            (self.ue_srtxs_df['timestamp'] < end_ts)
        ]
        if srtx_list.shape[0] == 0:
            logger.warning("Did not find any sr tx for this interval.")
            return []
        
        sr_tx_rows = []
        for i in range(srtx_list.shape[0]):
            sr_tx_row = srtx_list.iloc[i]
            sr_tx_rows.append(sr_tx_row)

        return sorted(sr_tx_rows, key=lambda x: x['timestamp'])

    def find_all_schedules_from_ts(self, begin_ts : float, end_ts : float, rnti : str, SCHED_OFFSET_S : float = 0):

        gnb_ul_schedules_df = self.gnb_sched_reports_df[
            (self.gnb_sched_reports_df['sched.ue.timestamp'] < end_ts) &
            (self.gnb_sched_reports_df['sched.ue.timestamp'] >= begin_ts) &
            (self.gnb_sched_reports_df['sched.ue.rnti'] == rnti)
        ]
        all_ul_schedules = []
        for j in range(gnb_ul_schedules_df.shape[0]):
            gnb_ul_schedule = dict(gnb_ul_schedules_df.iloc[j])

            # calculate the scheduled timestamp
            abs_sltx_po = int(gnb_ul_schedule[f'sched.ue.frametx'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slottx'])
            abs_sl_po = int(gnb_ul_schedule[f'sched.ue.frame'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slot'])
            sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*self.conf_slots_duration_ms
            ue_scheduled_ts = gnb_ul_schedule['sched.ue.timestamp']+sltx_tsdif_ms/1000+SCHED_OFFSET_S

            # set it to the dict
            gnb_ul_schedule['ue_scheduled_ts'] = ue_scheduled_ts

            all_ul_schedules.append(gnb_ul_schedule)

        return all_ul_schedules

    def find_failed_schedules_from_ts(self, begin_ts : float, end_ts : float, rnti : str, SCHED_OFFSET_S : float = 0, filter_ue_buffer_0 : bool = False) -> list:

        GNB_MAC_GNB_SCHED_MATCH_MS = 20
        GNB_SCHED_UE_ULDCI_MATCH_MS = 10
        SCHED_GNB_RLC_MATCH_MS = 20
        MAC_RETX_TS_DIFF_MS = 20

        # if there is an uplink resource allocation for a certain frame and slot, 
        # but no data comes out of rlc in gnb at that frame and slot, then it was a failed schedule
        # NOTE: this can be just an excess scheduling, but we can't know that
        # maybe gnb allocates resources for the UE due to scheduling request, but UE does not have any data to send
        # we see that in many experiments
        # we try to filter those out by checking the buffer status of the UE at the time of ULDCI arrival
        # if the buffer is empty, then we can say that the scheduling was not a failed one
        # however we see that sometimes for the first attempt the buffer is empty and there is no ue_mac_attempt, but there is a second attempt!
        # we consider these cases one transmission attempt

        # add a new column to gnb_sched_reports_df to keep the retx_group_id
        self.gnb_sched_reports_df['ue_mac_attempt_id'] = np.nan
        self.gnb_sched_reports_df['ue_prev_mac_attempt_id'] = np.nan

        # gnb_sched_reports_df: ['sched.ue.rnti', 'sched.ue.frame', 'sched.ue.slot', 'sched.ue.frametx', 'sched.ue.slottx', 'sched.ue.tbs', 'sched.ue.mcs', 'sched.ue.timestamp', 'sched.ue.rbs', 'sched.cause.type', 'sched.cause.frame', 'sched.cause.slot', 'sched.cause.diff', 'sched.cause.timestamp', 'sched.cause.buf', 'sched.cause.sched', 'sched.cause.hqround', 'sched.cause.hqpid']
        # find all schedulings withing this time span and this rnti
        gnb_ul_schedules = self.gnb_sched_reports_df[
            (self.gnb_sched_reports_df['sched.ue.timestamp'] < end_ts) &
            (self.gnb_sched_reports_df['sched.ue.timestamp'] >= begin_ts) &
            (self.gnb_sched_reports_df['sched.ue.rnti'] == rnti)
        ]

        num_gnb_ul_schedules = gnb_ul_schedules.shape[0]
        logger.info(f"Number of ul schedules discovered: {num_gnb_ul_schedules}, checking failures: ", end="")

        res_list = []
        for j in range(num_gnb_ul_schedules):
            gnb_ul_schedule = gnb_ul_schedules.iloc[j]

            progress = (j + 1) / num_gnb_ul_schedules * 100
            print(f"\rProgress: {progress:.2f}%", end="")

            frame = int(gnb_ul_schedule['sched.ue.frametx'])
            slot = int(gnb_ul_schedule['sched.ue.slottx'])
            hqpid = int(gnb_ul_schedule['sched.cause.hqpid'])

            # check #1: if data comes out on gnb side mac
            # now we can find the corresponding rlc segment on gnb side
            # gnb_mac_attempts_df: ['phy.decodeend.timestamp', 'phy.decodeend.rbb', 'phy.decodeend.rbs', 'phy.decodeend.tbs', 'phy.decodeend.mcs', 'phy.decodeend.rnti', 'phy.decodeend.suc', 'phy.detectend.timestamp', 'phy.detectend.frame', 'phy.detectend.slot', 'phy.detectend.hqpid', 'phy.detectend.hqround', 'phy.detectend.hbuf', 'phy.detectend.ptot', 'phy.detectend.pn', 'phy.detectend.pth', 'phy.detectend.suc', 'phy.detectstart.timestamp']
            found_gnb_mac_out = False
            gnb_mac_attempts_arr = self.gnb_mac_attempts_df[
                (self.gnb_mac_attempts_df['phy.decodeend.frame'] == frame) &
                (self.gnb_mac_attempts_df['phy.decodeend.slot'] == slot) &
                (self.gnb_mac_attempts_df['phy.decodeend.hqpid'] == hqpid) &
                (self.gnb_mac_attempts_df['phy.decodeend.suc'] == 1) &
                (self.gnb_mac_attempts_df['phy.decodeend.rnti'] == rnti)
            ]
            if gnb_mac_attempts_arr.shape[0] > 0:
                for k in range(gnb_mac_attempts_arr.shape[0]):
                    pot_gnb_mac = gnb_mac_attempts_arr.iloc[k]
                    if abs(pot_gnb_mac['phy.decodeend.timestamp'] - gnb_ul_schedule['sched.ue.timestamp'])*1000 < GNB_MAC_GNB_SCHED_MATCH_MS:
                        found_gnb_mac_out = True
                        break

            if found_gnb_mac_out:
                continue

            # check #1: if there is data out on gnb rlc side
            # find the corresponding rlc segment on the gnb side
            found_gnb_rlc_out = False
            #gnb_rlc_segment_arr = self.gnb_rlc_segments_df[
            #    (self.gnb_rlc_segments_df['rlc.decoded.frame'] == frame) &
            #    (self.gnb_rlc_segments_df['rlc.decoded.slot'] == slot) &
            #    (self.gnb_rlc_segments_df['rlc.decoded.rnti'] == rnti)
            #]
            #if gnb_rlc_segment_arr.shape[0] > 0:
            #    for k in range(gnb_rlc_segment_arr.shape[0]):
            #        pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
            #        if abs(pot_gnb_seg['rlc.reassembled.timestamp'] - gnb_ul_schedule['sched.ue.timestamp'])*1000 < SCHED_GNB_RLC_MATCH_MS:
            #            found_gnb_rlc_out = True
            #            break
            
            if found_gnb_rlc_out:
                continue

            # check #2: check if the ue buffer was empty at the time of scheduled transmission
            # calculate the ue scheduled timestamp for this gnb schedule and check if at that time the buffer was empty or no
            found_empty_ue_buffer = False
            # calculate the scheduled timestamp
            abs_sltx_po = int(gnb_ul_schedule[f'sched.ue.frametx'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slottx'])
            abs_sl_po = int(gnb_ul_schedule[f'sched.ue.frame'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slot'])
            sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*self.conf_slots_duration_ms
            ue_scheduled_ts = gnb_ul_schedule['sched.ue.timestamp']+sltx_tsdif_ms/1000+SCHED_OFFSET_S

            # find latest ue buffer status update
            if filter_ue_buffer_0:
                # ue_bsrupds_df: ['frame', 'slot', 'timestamp', 'lcid', 'lcgid', 'len']
                ue_bsr_upds = self.ue_bsrupds_df[
                    (self.ue_bsrupds_df['timestamp'] <= ue_scheduled_ts)
                ]
                if ue_bsr_upds.shape[0] > 0:
                    # find the one with max timestamp
                    ue_bsr_upd = ue_bsr_upds.loc[ue_bsr_upds['timestamp'].idxmax()]
                    if ue_bsr_upd['len'] == 0:
                        found_empty_ue_buffer = True

            if found_empty_ue_buffer:
                continue
            
            # now we can say that this is a failed scheduling
            cp_gnb_ul_schedule = dict(gnb_ul_schedule)
            cp_gnb_ul_schedule['ue_scheduled_ts'] = ue_scheduled_ts
            res_list.append(cp_gnb_ul_schedule)

        print("\n", end="")
        logger.info(f"Number of failed schedullings discovered: {len(res_list)}")

        return res_list

    def find_retx_schedules_from_ts(self, begin_ts : float, end_ts : float, rnti, SCHED_OFFSET_S : float, only_successful = False) -> list:
        """
        finds all UL schedules where generated for retransmissions
        returns a list of dict
        """

        GNB_MAC_GNB_SCHED_MATCH_MS = 20

        # there must be at least one entry in ue mac attempts with this info
        gnb_retx_ul_schedules = self.gnb_sched_reports_df[
            (self.gnb_sched_reports_df['sched.ue.timestamp'] < end_ts) &
            (self.gnb_sched_reports_df['sched.ue.timestamp'] >= begin_ts) &
            (self.gnb_sched_reports_df['sched.ue.rnti'] == rnti) &
            (self.gnb_sched_reports_df['sched.cause.type'] == 4) # retx scheduling
        ]
        if gnb_retx_ul_schedules.shape[0] == 0:
            logger.warning("No retx schedules found")
            return []

        logger.info(f"Number of retx schedules found: {gnb_retx_ul_schedules.shape[0]}.")

        res_arr = []
        for i in range(gnb_retx_ul_schedules.shape[0]):
            gnb_ul_schedule = gnb_retx_ul_schedules.iloc[i].to_dict()

            if only_successful:
                frame = int(gnb_ul_schedule['sched.ue.frametx'])
                slot = int(gnb_ul_schedule['sched.ue.slottx'])
                hqpid = int(gnb_ul_schedule['sched.cause.hqpid'])

                # check #1: if data comes out on gnb side mac
                # now we can find the corresponding rlc segment on gnb side
                # gnb_mac_attempts_df: ['phy.decodeend.timestamp', 'phy.decodeend.rbb', 'phy.decodeend.rbs', 'phy.decodeend.tbs', 'phy.decodeend.mcs', 'phy.decodeend.rnti', 'phy.decodeend.suc', 'phy.detectend.timestamp', 'phy.detectend.frame', 'phy.detectend.slot', 'phy.detectend.hqpid', 'phy.detectend.hqround', 'phy.detectend.hbuf', 'phy.detectend.ptot', 'phy.detectend.pn', 'phy.detectend.pth', 'phy.detectend.suc', 'phy.detectstart.timestamp']
                found_gnb_mac_out = False
                gnb_mac_attempts_arr = self.gnb_mac_attempts_df[
                    (self.gnb_mac_attempts_df['phy.decodeend.frame'] == frame) &
                    (self.gnb_mac_attempts_df['phy.decodeend.slot'] == slot) &
                    (self.gnb_mac_attempts_df['phy.decodeend.hqpid'] == hqpid) &
                    (self.gnb_mac_attempts_df['phy.decodeend.suc'] == 1) &
                    (self.gnb_mac_attempts_df['phy.decodeend.rnti'] == rnti)
                ]
                if gnb_mac_attempts_arr.shape[0] > 0:
                    for k in range(gnb_mac_attempts_arr.shape[0]):
                        pot_gnb_mac = gnb_mac_attempts_arr.iloc[k]
                        if abs(pot_gnb_mac['phy.decodeend.timestamp'] - gnb_ul_schedule['sched.ue.timestamp'])*1000 < GNB_MAC_GNB_SCHED_MATCH_MS:
                            found_gnb_mac_out = True
                            break

                if not found_gnb_mac_out:
                    continue

            abs_sltx_po = int(gnb_ul_schedule[f'sched.ue.frametx'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slottx'])
            abs_sl_po = int(gnb_ul_schedule[f'sched.ue.frame'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slot'])
            sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*self.conf_slots_duration_ms
            scheduled_timestamp = gnb_ul_schedule['sched.ue.timestamp']+sltx_tsdif_ms/1000+SCHED_OFFSET_S
            gnb_ul_schedule['ue_scheduled_ts'] = scheduled_timestamp
            res_arr.append(gnb_ul_schedule)

        return res_arr


    def find_ue_mac_attempt_from_gnb_schedule(self, gnb_ul_schedule : dict, SCHED_OFFSET_S : float) -> dict:
            
            SCHED_GNB_MAC_UE_MATCH_MS = 5

            # gnb_sched_reports_df: ['sched.ue.rnti', 'sched.ue.frame', 'sched.ue.slot', 'sched.ue.frametx', 'sched.ue.slottx', 'sched.ue.tbs', 'sched.ue.mcs', 'sched.ue.timestamp', 'sched.ue.rbs', 'sched.cause.type', 'sched.cause.frame', 'sched.cause.slot', 'sched.cause.diff', 'sched.cause.timestamp', 'sched.cause.buf', 'sched.cause.sched', 'sched.cause.hqround', 'sched.cause.hqpid']
            # find scheduled timestamp
            abs_sltx_po = int(gnb_ul_schedule[f'sched.ue.frametx'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slottx'])
            abs_sl_po = int(gnb_ul_schedule[f'sched.ue.frame'])*self.conf_slots_per_frame +int(gnb_ul_schedule[f'sched.ue.slot'])
            sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*self.conf_slots_duration_ms
            scheduled_timestamp = gnb_ul_schedule['sched.ue.timestamp']+sltx_tsdif_ms/1000+SCHED_OFFSET_S

            # find the corresponding mac attempt
            # ue_mac_attempts_df: ['mac_id', 'phy.tx.timestamp', 'phy.tx.Hbuf', 'phy.tx.rvi', 'phy.tx.fm', 'phy.tx.sl', 'phy.tx.nb_rb', 'phy.tx.nb_sym', 'phy.tx.mod_or', 'phy.tx.len', 'phy.tx.rnti', 'phy.tx.hqpid', 'mac.harq.timestamp', 'mac.harq.hqpid', 'mac.harq.rvi', 'mac.harq.len', 'mac.harq.ndi', 'mac.harq.M3buf']
            ue_mac_attempts_arr = self.ue_mac_attempts_df[
                (self.ue_mac_attempts_df['phy.tx.fm'] == gnb_ul_schedule['sched.ue.frametx']) &
                (self.ue_mac_attempts_df['phy.tx.sl'] == gnb_ul_schedule['sched.ue.slottx']) &
                (self.ue_mac_attempts_df['phy.tx.rnti'] == gnb_ul_schedule['sched.ue.rnti'])
            ]
            ue_mac_attempt = {}
            if ue_mac_attempts_arr.shape[0] > 0:
                for k in range(ue_mac_attempts_arr.shape[0]):
                    pot_ue_mac_attempt = ue_mac_attempts_arr.iloc[k]
                    if abs(pot_ue_mac_attempt['phy.tx.timestamp'] - scheduled_timestamp)*1000 < SCHED_GNB_MAC_UE_MATCH_MS:
                        ue_mac_attempt = dict(pot_ue_mac_attempt)
                        break

            return ue_mac_attempt
    

    def find_gnb_schedule_from_ue_mac_attempt(self, ue_mac_attempt : dict, SCHED_OFFSET_S : float) -> dict:
            
            SCHED_GNB_MAC_UE_MATCH_MS = 5

            # gnb_sched_reports_df: ['sched.ue.rnti', 'sched.ue.frame', 'sched.ue.slot', 'sched.ue.frametx', 'sched.ue.slottx', 'sched.ue.tbs', 'sched.ue.mcs', 'sched.ue.timestamp', 'sched.ue.rbs', 'sched.cause.type', 'sched.cause.frame', 'sched.cause.slot', 'sched.cause.diff', 'sched.cause.timestamp', 'sched.cause.buf', 'sched.cause.sched', 'sched.cause.hqround', 'sched.cause.hqpid']
            # find scheduled timestamp
            # ue_mac_attempts_df: ['mac_id', 'phy.tx.timestamp', 'phy.tx.Hbuf', 'phy.tx.rvi', 'phy.tx.fm', 'phy.tx.sl', 'phy.tx.nb_rb', 'phy.tx.nb_sym', 'phy.tx.mod_or', 'phy.tx.len', 'phy.tx.rnti', 'phy.tx.hqpid', 'mac.harq.timestamp', 'mac.harq.hqpid', 'mac.harq.rvi', 'mac.harq.len', 'mac.harq.ndi', 'mac.harq.M3buf']

            # find the corresponding gnb schedule
            gnb_sched_reports_list = self.gnb_sched_reports_df[
                (self.gnb_sched_reports_df['sched.ue.frametx'] == ue_mac_attempt['phy.tx.fm']) &
                (self.gnb_sched_reports_df['sched.ue.slottx'] == ue_mac_attempt['phy.tx.sl']) &
                (self.gnb_sched_reports_df['sched.cause.hqpid'] == ue_mac_attempt['mac.harq.hqpid']) &
                (self.gnb_sched_reports_df['sched.ue.rnti'] == ue_mac_attempt['phy.tx.rnti'])
            ]
            gnb_sched_report = {}
            if gnb_sched_reports_list.shape[0] > 0:
                for k in range(gnb_sched_reports_list.shape[0]):
                    pot_gnb_sched_report = gnb_sched_reports_list.iloc[k]
                    # find scheduled timestamp
                    abs_sltx_po = int(pot_gnb_sched_report[f'sched.ue.frametx'])*self.conf_slots_per_frame +int(pot_gnb_sched_report[f'sched.ue.slottx'])
                    abs_sl_po = int(pot_gnb_sched_report[f'sched.ue.frame'])*self.conf_slots_per_frame +int(pot_gnb_sched_report[f'sched.ue.slot'])
                    sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*self.conf_slots_duration_ms
                    scheduled_timestamp = pot_gnb_sched_report['sched.ue.timestamp']+sltx_tsdif_ms/1000+SCHED_OFFSET_S
                    if abs(ue_mac_attempt['phy.tx.timestamp'] - scheduled_timestamp)*1000 < SCHED_GNB_MAC_UE_MATCH_MS:
                        gnb_sched_report = dict(pot_gnb_sched_report)
                        break

            return gnb_sched_report