import os, sys
import sqlite3
from loguru import logger
import pandas as pd

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

        # check and report the ue_ip_ids and gnb sns
        #logger.success(f"Imported database '{db_addr}', with UE IDs ranging from {self.ue_ip_packets_df['ip_id'].min()} to {self.ue_ip_packets_df['ip_id'].max()}, and GNB SNs ranging from {self.gnb_ip_packets_df['gtp.out.sn'].min()} to {self.gnb_ip_packets_df['gtp.out.sn'].max()}")

    def decode_scheduling_map(self, map_row):
    
        # find RBs structure
        ins_pr = []
        ins_po = []
        for i in range(self.conf_scheduling_map_num_integers):
            ins_pr.append(int(map_row[f'sched.map.pr.i{self.conf_scheduling_map_num_integers-i-1}m']))
            ins_po.append(int(map_row[f'sched.map.po.i{self.conf_scheduling_map_num_integers-i-1}m']))
        
        binary_string = ''.join(format(num, '016b') for num in ins_pr)[::-1]
        first_106_bits = binary_string[:self.conf_total_prbs_num]
        pr_bit_list = [int(bit) for bit in first_106_bits]

        binary_string = ''.join(format(num, '016b') for num in ins_po)[::-1]
        first_106_bits = binary_string[:self.conf_symbols_per_slot]
        po_bit_list = [int(bit) for bit in first_106_bits]

        return pr_bit_list, po_bit_list

    def figure_ulresourceblocks_from_ts(self, begin_ts, end_ts):



        # bring all sched.map.pr and sched.map.po within this frame (10ms earlier)
        maps = self.gnb_sched_maps_df[
            (self.gnb_sched_maps_df['sched.map.pr.timestamp'] >= begin_ts) &
            (self.gnb_sched_maps_df['sched.map.pr.timestamp'] < end_ts)
        ]
        for i in range(maps.shape[0]):
            map_row = maps.iloc[i]
            map_ts = map_row['sched.map.pr.timestamp']
            dl_slot_time = (map_ts-begin_ts)*1000+self.conf_slots_duration_ms*4
            #rect = patches.Rectangle((dl_slot_time-(SLOT_LENGTH/2), 3), SLOT_LENGTH, 1, color='blue')
            #ax.add_patch(rect)

            # find RBs structure
            pr_bit_list, po_bit_list = self.decode_scheduling_map(map_row)
            blocked_bits_list = [bit1 & bit2 for bit1, bit2 in zip(pr_bit_list, po_bit_list)]

            # find symbols structure
            sym_begin = int(map_row[f'sched.map.po.sb'])
            sym_size = int(map_row[f'sched.map.po.ss'])

            # find fm, sl and fmtx, sltx
            abs_sltx_po = int(map_row[f'sched.map.po.frametx'])*self.conf_slots_per_frame +int(map_row[f'sched.map.po.slottx']) 
            abs_sl_po = int(map_row[f'sched.map.po.frame'])*self.conf_slots_per_frame +int(map_row[f'sched.map.po.slot'])
            sltx_tsdif_ms = (abs_sltx_po - abs_sl_po)*self.conf_slots_duration_ms

            offset = sym_begin/self.conf_symbols_per_slot*self.conf_slots_duration_ms
            width = sym_size/self.conf_symbols_per_slot*self.conf_slots_duration_ms
            #rect = patches.Rectangle((dl_slot_time+sltx_tsdif_ms+offset, 3), width, 1, color='green')
            #ax.add_patch(rect)

            #tot_height = 1
            #height = tot_height/NUM_PRBS*
            #rect = patches.Rectangle((dl_slot_time+sltx_tsdif_ms+offset, 3), width, 1, color='purple')
            #ax.add_patch(rect)

            #tot_height = 1
            #segment_height = tot_height / len(blocked_bits_list)
            #for i, bit in enumerate(blocked_bits_list):
            #    color = 'grey' if bit == 1 else 'green'
            #    y_pos = i * segment_height 
            #    rect = patches.Rectangle((dl_slot_time+sltx_tsdif_ms+offset, 3+y_pos), width, segment_height, color=color)
            #    ax.add_patch(rect)