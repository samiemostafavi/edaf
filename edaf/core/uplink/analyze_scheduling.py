import os, sys
import sqlite3
from loguru import logger
import pandas as pd
import numpy as np

if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

class ULSchedulingAnalyzer:
    def __init__(self, total_prbs_num, symbols_per_slot, slots_per_frame, slots_duration_ms, scheduling_map_num_integers, db_addr):

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
            schedule['schedule_ts'] = map_row['sched.map.pr.timestamp']+sltx_tsdif_ms

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

    def find_frame_slot_from_ts(self, timestamp):
        MAX_NUM_FRAMES = 1024
        NUM_SLOTS_PER_FRAME = 20
        SLOT_DURATION_S = 0.0005
        CLOSENESS_LIMIT_S = 0.1 #100ms  
        SCHED_OFFSET_S = 4*SLOT_DURATION_S #2ms or 4 slots

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
        slots_diff = int((timestamp - (pr_map_row['sched.map.pr.timestamp']+SCHED_OFFSET_S))/SLOT_DURATION_S)
        pr_abs_slot_num = pr_map_row['sched.map.po.frame']*NUM_SLOTS_PER_FRAME + pr_map_row['sched.map.po.slot']
        new_abs_slot_num = pr_abs_slot_num + slots_diff
        if new_abs_slot_num < 0:
            new_abs_slot_num = MAX_NUM_FRAMES*NUM_SLOTS_PER_FRAME + new_abs_slot_num

        new_frame_num = new_abs_slot_num // NUM_SLOTS_PER_FRAME
        new_slot_num = new_abs_slot_num % NUM_SLOTS_PER_FRAME

        return new_frame_num, new_slot_num


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
