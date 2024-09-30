import os, sys
import sqlite3
from loguru import logger
import pandas as pd
import numpy as np

if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")


class ULChannelAnalyzer:
    def __init__(self, db_addr):
        # Open a connection to the SQLite database
        conn = sqlite3.connect(db_addr)

        # Read each table from the SQLite database into pandas DataFrames
        self.gnb_mac_attempts_df = pd.read_sql('SELECT * FROM gnb_mac_attempts', conn)
        logger.info(f"gnb_mac_attempts_df: {self.gnb_mac_attempts_df.columns.tolist()}")

        self.gnb_rlc_segments_df = pd.read_sql('SELECT * FROM gnb_rlc_segments', conn)
        logger.info(f"gnb_rlc_segments_df: {self.gnb_rlc_segments_df.columns.tolist()}")

        self.gnb_mcs_reports_df = pd.read_sql('SELECT * FROM gnb_mcs_reports', conn)
        logger.info(f"gnb_mcs_reports_df: {self.gnb_mcs_reports_df.columns.tolist()}")

        self.ue_mac_attempts_df = pd.read_sql('SELECT * FROM ue_mac_attempts', conn)
        logger.info(f"ue_mac_attempts_df: {self.ue_mac_attempts_df.columns.tolist()}")

        conn.close()

        # check and report the first and last timestamps
        self.first_ts = self.gnb_mcs_reports_df['timestamp'].min()
        self.last_ts = self.gnb_mcs_reports_df['timestamp'].max()


    def find_mcs_from_ts(self, begin_ts : float, end_ts : float) -> list:
        """
        finds the UL MCS indices within the timestamps
        returns a list of MCS reports
        """

        # find all attempts with timestamps less than this
        gnb_mcs_reports = self.gnb_mcs_reports_df[
            (self.gnb_mcs_reports_df['timestamp'] < end_ts) &
            (self.gnb_mcs_reports_df['timestamp'] >= begin_ts)
        ]
        num_mcs_reports = gnb_mcs_reports.shape[0]
        logger.info(f"Number of GNB mcs reports discovered: {num_mcs_reports}")

        res_arr = []
        for j in range(num_mcs_reports):
            mcs_report = dict(gnb_mcs_reports.iloc[j])
            res_arr.append(mcs_report)

        return res_arr


    def find_mac_attempts_from_ts(self, begin_ts : float, end_ts : float) -> list:
        """
        finds all the mac attempts between UE and gnb
        returns a list of dict
        """

        GNB_MAC_RLC_MATCH_MS = 5

        # find all attempts with timestamps less than this
        ue_mac_attempts = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.timestamp'] < end_ts) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= begin_ts)
        ]
        num_ue_mac_attempts = ue_mac_attempts.shape[0]
        logger.info(f"Number of UE mac attempts discovered: {num_ue_mac_attempts}")

        res_arr = []
        for j in range(num_ue_mac_attempts):
            ue_mac_attempt = ue_mac_attempts.iloc[j]

            macattempt = {
                'len' : ue_mac_attempt['phy.tx.len'],
                'id' : ue_mac_attempt['mac_id'],
                'frame' : int(ue_mac_attempt[f'phy.tx.fm']),
                'slot' : int(ue_mac_attempt[f'phy.tx.sl']),
                'hqpid' : int(ue_mac_attempt[f'phy.tx.hqpid']),
                'phy.in_t' : float(ue_mac_attempt[f'phy.tx.timestamp']),
                'rvi': int(ue_mac_attempt[f'phy.tx.rvi']),
                'phy.out_t' : None,
                'acked' : False,
            }

            # now we can find the corresponding mac attempt on gnb side
            gnb_mac_attempt_arr = self.gnb_mac_attempts_df[
                (self.gnb_mac_attempts_df['phy.detectend.frame'] == ue_mac_attempt['phy.tx.fm']) &
                (self.gnb_mac_attempts_df['phy.detectend.slot'] == ue_mac_attempt['phy.tx.sl']) &
                (self.gnb_mac_attempts_df['phy.detectend.hqpid'] == ue_mac_attempt['phy.tx.hqpid'])
            ]
            if gnb_mac_attempt_arr.shape[0] == 0:
                # unsuccessful harq attempt 
                pass
            elif gnb_mac_attempt_arr.shape[0] > 1:
                logger.warning(f"UE MAC attempt {j}, looking for the corresponding gnb mac attempt. Found {gnb_mac_attempt_arr.shape[0]} (more than one) possible gnb mac attempt matches. We pick the closest one.")
                min_diff = np.inf
                gnb_mac_attempt = None
                for k in range(gnb_mac_attempt_arr.shape[0]):
                    gnb_pot_mac_attempt = gnb_mac_attempt_arr.iloc[k]
                    if not pd.isna(gnb_pot_mac_attempt['phy.decodeend.timestamp']):
                        diff = abs(float(gnb_pot_mac_attempt['phy.decodeend.timestamp'] - ue_mac_attempt[f'phy.tx.timestamp']))
                        if diff < min_diff:
                            min_diff = diff
                            gnb_mac_attempt = gnb_pot_mac_attempt
            else:
                gnb_mac_attempt = gnb_mac_attempt_arr.iloc[0]

            if gnb_mac_attempt is not None:
                if pd.isna(gnb_mac_attempt['phy.decodeend.timestamp']):
                    # unsuccessful harq attempt 
                    pass
                else:
                    # possibly successful harq attempt
                    macattempt['phy.out_t'] = float(gnb_mac_attempt['phy.decodeend.timestamp'])
                    hq_s = int(gnb_mac_attempt['phy.detectend.hqpid'])
                    fm_s = int(gnb_mac_attempt['phy.detectend.frame'])
                    sl_s = int(gnb_mac_attempt['phy.detectend.slot'])

                    # find the rlc segment on the gnb side of this mac attempt
                    # use hq_s, fm_s, and sl_s
                    # the possible hq, fm, and sl of that rlc segment in gnb
                    gnb_rlc_segment_arr = self.gnb_rlc_segments_df[
                        (self.gnb_rlc_segments_df['rlc.decoded.frame'] == fm_s) &
                        (self.gnb_rlc_segments_df['rlc.decoded.slot'] == sl_s) &
                        (self.gnb_rlc_segments_df['rlc.decoded.hqpid'] == hq_s)
                    ]
                    if gnb_rlc_segment_arr.shape[0] == 1:
                        macattempt['acked'] = True
                    elif gnb_rlc_segment_arr.shape[0] > 1:
                        logger.warning(f"UE RLC attempt {macattempt['id']} - found {gnb_rlc_segment_arr.shape[0]} (more than one) possible gnb rlc segment matches. We reject the ones farther than {GNB_MAC_RLC_MATCH_MS} ms.")
                        for k in range(gnb_rlc_segment_arr.shape[0]):
                            pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
                            if abs(pot_gnb_seg['rlc.reassembled.timestamp'] - gnb_mac_attempt['phy.decodeend.timestamp'])*1000 < GNB_MAC_RLC_MATCH_MS:
                                macattempt['acked'] = True
                                break

            res_arr.append(macattempt)
    
        return res_arr