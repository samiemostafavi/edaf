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

        self.ue_rlc_segments_df = pd.read_sql('SELECT * FROM ue_rlc_segments', conn)
        logger.info(f"ue_rlc_segments_df: {self.ue_rlc_segments_df.columns.tolist()}")

        self.ue_uldcis_df = pd.read_sql('SELECT * FROM ue_uldcis', conn)
        logger.info(f"ue_uldcis_df: {self.ue_uldcis_df.columns.tolist()}")

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

        res_arr = []
        for j in range(num_mcs_reports):
            mcs_report = dict(gnb_mcs_reports.iloc[j])
            res_arr.append(mcs_report)

        return res_arr


    def find_harq_attempts_from_ts(self, begin_ts : float, end_ts : float) -> list:
        """
        finds all the mac attempts between UE and gnb
        returns a list of dict
        """

        GNB_MAC_RLC_MATCH_MS = 5

        # find all attempts with timestamps less than this
        ue_harq_attempts = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.timestamp'] < end_ts) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= begin_ts)
        ]
        num_ue_harq_attempts = ue_harq_attempts.shape[0]
        logger.info(f"Number of UE mac attempts discovered: {num_ue_harq_attempts}")

        res_arr = []
        for j in range(num_ue_harq_attempts):
            ue_harq_attempt = ue_harq_attempts.iloc[j]

            harqattempt = {
                'len' : ue_harq_attempt['phy.tx.len'],
                'id' : ue_harq_attempt['mac_id'],
                'frame' : int(ue_harq_attempt[f'phy.tx.fm']),
                'slot' : int(ue_harq_attempt[f'phy.tx.sl']),
                'hqpid' : int(ue_harq_attempt[f'phy.tx.hqpid']),
                'rnti' : ue_harq_attempt['phy.tx.rnti'],
                'phy.in_t' : float(ue_harq_attempt[f'phy.tx.timestamp']),
                'rvi': int(ue_harq_attempt[f'phy.tx.rvi']),
                'phy.out_t' : None,
                'acked' : False,
            }

            # now we can find the corresponding mac attempt on gnb side
            gnb_harq_attempt_arr = self.gnb_mac_attempts_df[
                (self.gnb_mac_attempts_df['phy.detectend.frame'] == ue_harq_attempt['phy.tx.fm']) &
                (self.gnb_mac_attempts_df['phy.detectend.slot'] == ue_harq_attempt['phy.tx.sl']) &
                (self.gnb_mac_attempts_df['phy.detectend.hqpid'] == ue_harq_attempt['phy.tx.hqpid']) #&
                # (self.gnb_mac_attempts_df['phy.detectend.rnti'] == ue_harq_attempt['phy.tx.rnti']) # TODO gnb mac attempts do not have rnti
            ]
            gnb_harq_attempt = None
            if gnb_harq_attempt_arr.shape[0] == 0:
                # unsuccessful harq attempt 
                pass
            elif gnb_harq_attempt_arr.shape[0] > 1:
                logger.warning(f"UE MAC attempt {j}, looking for the corresponding gnb mac attempt. Found {gnb_harq_attempt_arr.shape[0]} (more than one) possible gnb mac attempt matches. We pick the closest one.")
                min_diff = np.inf
                for k in range(gnb_harq_attempt_arr.shape[0]):
                    gnb_pot_harq_attempt = gnb_harq_attempt_arr.iloc[k]
                    if not pd.isna(gnb_pot_harq_attempt['phy.decodeend.timestamp']):
                        diff = abs(float(gnb_pot_harq_attempt['phy.decodeend.timestamp'] - ue_harq_attempt[f'phy.tx.timestamp']))
                        if diff < min_diff:
                            min_diff = diff
                            gnb_harq_attempt = gnb_pot_harq_attempt
            else:
                gnb_harq_attempt = gnb_harq_attempt_arr.iloc[0]

            if gnb_harq_attempt is not None:
                if pd.isna(gnb_harq_attempt['phy.decodeend.timestamp']):
                    # unsuccessful harq attempt 
                    pass
                else:
                    # possibly successful harq attempt
                    harqattempt['phy.out_t'] = float(gnb_harq_attempt['phy.decodeend.timestamp'])
                    hq_s = int(gnb_harq_attempt['phy.detectend.hqpid'])
                    fm_s = int(gnb_harq_attempt['phy.detectend.frame'])
                    sl_s = int(gnb_harq_attempt['phy.detectend.slot'])

                    # find the rlc segment on the gnb side of this mac attempt
                    # use hq_s, fm_s, and sl_s
                    # the possible hq, fm, and sl of that rlc segment in gnb
                    gnb_rlc_segment_arr = self.gnb_rlc_segments_df[
                        (self.gnb_rlc_segments_df['rlc.decoded.frame'] == fm_s) &
                        (self.gnb_rlc_segments_df['rlc.decoded.slot'] == sl_s) &
                        (self.gnb_rlc_segments_df['rlc.decoded.hqpid'] == hq_s)
                    ]
                    if gnb_rlc_segment_arr.shape[0] >= 1:
                        for k in range(gnb_rlc_segment_arr.shape[0]):
                            pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
                            if abs(pot_gnb_seg['rlc.reassembled.timestamp'] - gnb_harq_attempt['phy.decodeend.timestamp'])*1000 < GNB_MAC_RLC_MATCH_MS:
                                harqattempt['acked'] = True
                                break

            res_arr.append(harqattempt)
    
        return res_arr
    

    def find_mac_attempts_from_ts(self, begin_ts : float, end_ts : float) -> list:
        """
        finds all the mac attempts between UE and gnb
        returns a list of dict
        """

        GNB_MAC_RLC_MATCH_MS = 5

        # (m2buf_value >= m3buf_value) and ((m2buf_value + m2len) <= (m3buf_value + m3len)
        # there must be at least one entry in ue mac attempts with this info
        ue_mac_attempts_0 = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= begin_ts) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] <= end_ts)
        ]
        if ue_mac_attempts_0.shape[0] == 0:
            logger.error("No harq attempts found")
            return []

        logger.info(f"UE MAC attempts found: {ue_mac_attempts_0.shape[0]}.")

        res_arr = []
        already_observed_mac_ids = set()
        for i in range(ue_mac_attempts_0.shape[0]):
            ue_mac_attempt_0 = ue_mac_attempts_0.iloc[i]
            if ue_mac_attempt_0['mac_id'] in already_observed_mac_ids:
                continue
            # make sure to only start from the first rvi
            if int(ue_mac_attempt_0[f'phy.tx.rvi']) != 0:
                continue

            hq = ue_mac_attempt_0['phy.tx.hqpid']
            at_0_ts = float(ue_mac_attempt_0['phy.tx.timestamp'])

            # find all attempts with this hq
            hq_attempts = self.ue_mac_attempts_df[
                (self.ue_mac_attempts_df['phy.tx.hqpid'] == hq) &
                (self.ue_mac_attempts_df['phy.tx.timestamp'] > at_0_ts)
            ]
            sorted_hq_attempts = hq_attempts.sort_values(by='phy.tx.timestamp', ascending=True, inplace=False)
            first_ndi_row = sorted_hq_attempts[sorted_hq_attempts['mac.harq.ndi'] == 1]
            if first_ndi_row.shape[0] == 0:
                continue
            #first_ndi1_attempt_ts = float(first_ndi_row.head(1)['phy.tx.timestamp'])
            first_ndi1_attempt_ts = float(first_ndi_row['phy.tx.timestamp'].iloc[0])

            # find all attempts with timestamps less than this
            ue_mac_attempts = self.ue_mac_attempts_df[
                (self.ue_mac_attempts_df['phy.tx.hqpid'] == hq) &
                (self.ue_mac_attempts_df['phy.tx.timestamp'] < first_ndi1_attempt_ts) &
                (self.ue_mac_attempts_df['phy.tx.timestamp'] >= at_0_ts)
            ]
            num_ue_mac_attempts = ue_mac_attempts.shape[0]
            logger.debug(f"Number of harq attempts discovered: {num_ue_mac_attempts}")

            harqattempts = []

            # frame and slot number of the last mac attempt
            for j in range(num_ue_mac_attempts):
                ue_mac_attempt = ue_mac_attempts.iloc[j]
                if ue_mac_attempt['mac_id'] in already_observed_mac_ids:
                    continue
                already_observed_mac_ids.add(ue_mac_attempt['mac_id'])

                # check if this is a rlc segment harq attempt
                ue_rlc_segment = None
                ue_rlc_segment_found = False
                ue_rlc_segments = self.ue_rlc_segments_df[ 
                    (self.ue_rlc_segments_df['mac.sdu.frame'] ==  ue_mac_attempt['phy.tx.fm']) &
                    (self.ue_rlc_segments_df['mac.sdu.slot'] ==  ue_mac_attempt['phy.tx.sl'])
                ]
                if ue_rlc_segments.shape[0] >= 1:
                    for k in range(ue_rlc_segments.shape[0]):
                        ue_rlc_segment_pot = ue_rlc_segments.iloc[k]
                        if abs(ue_rlc_segment_pot['mac.sdu.timestamp']-ue_mac_attempt['phy.tx.timestamp']) < (0.001*GNB_MAC_RLC_MATCH_MS):
                            ue_rlc_segment = ue_rlc_segment_pot
                            ue_rlc_segment_found = True

                real_rvi = int(ue_mac_attempt[f'phy.tx.rvi'])-1 if int(ue_mac_attempt[f'phy.tx.rvi'])>0 else 0
                harqattempt = {
                    'len' : ue_mac_attempt['phy.tx.len'],
                    'id' : ue_mac_attempt['mac_id'],
                    'frame' : int(ue_mac_attempt[f'phy.tx.fm']),
                    'slot' : int(ue_mac_attempt[f'phy.tx.sl']),
                    'hqpid' : int(ue_mac_attempt[f'phy.tx.hqpid']),
                    'phy.in_t' : float(ue_mac_attempt[f'phy.tx.timestamp']),
                    'rnti' : ue_mac_attempt['phy.tx.rnti'],
                    'rvi': real_rvi,
                    'phy.out_t' : None,
                    'ndi' : ue_mac_attempt['mac.harq.ndi'],
                    'rlc_in' : ue_rlc_segment_found,
                    'rlc_out' : False,
                    'rlc_ack' : bool(ue_rlc_segment['rlc.report.ack']) if ue_rlc_segment_found else None,
                }

                # now we can find the corresponding mac attempt on gnb side
                gnb_mac_attempt_arr = self.gnb_mac_attempts_df[
                    (self.gnb_mac_attempts_df['phy.detectend.frame'] == ue_mac_attempt['phy.tx.fm']) &
                    (self.gnb_mac_attempts_df['phy.detectend.slot'] == ue_mac_attempt['phy.tx.sl']) &
                    (self.gnb_mac_attempts_df['phy.detectend.hqpid'] == ue_mac_attempt['phy.tx.hqpid']) #& 
                    # (self.gnb_mac_attempts_df['phy.detectend.rnti'] == ue_mac_attempt['phy.tx.rnti']) # TODO gnb mac attempts do not have rnti
                ]
                
                gnb_mac_attempt = None
                if gnb_mac_attempt_arr.shape[0] == 0:
                    # unsuccessful harq attempt 
                    pass
                elif gnb_mac_attempt_arr.shape[0] >= 1:
                    logger.debug(f"UE Harq attempt {j}, looking for the corresponding gnb mac attempt. Found {gnb_mac_attempt_arr.shape[0]} (more than one) possible gnb mac attempt matches. We pick the one within {GNB_MAC_RLC_MATCH_MS} ms.")
                    for k in range(gnb_mac_attempt_arr.shape[0]):
                        gnb_pot_mac_attempt = gnb_mac_attempt_arr.iloc[k]
                        if abs(gnb_pot_mac_attempt['phy.decodeend.timestamp']-ue_mac_attempt['phy.tx.timestamp']) < (0.001*GNB_MAC_RLC_MATCH_MS):
                            gnb_mac_attempt = gnb_pot_mac_attempt

                if gnb_mac_attempt is not None:
                    if pd.isna(gnb_mac_attempt['phy.decodeend.timestamp']):
                        # unsuccessful harq attempt 
                        pass
                    else:
                        # successful harq attempt
                        harqattempt['phy.out_t'] = float(gnb_mac_attempt['phy.decodeend.timestamp'])

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
                        if gnb_rlc_segment_arr.shape[0] >= 1:
                            logger.debug(f"UE RLC attempt {harqattempt['id']} - found {gnb_rlc_segment_arr.shape[0]} (more than one) possible gnb rlc segment matches. We reject the ones farther than {GNB_MAC_RLC_MATCH_MS} ms.")
                            for k in range(gnb_rlc_segment_arr.shape[0]):
                                pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
                                if abs(pot_gnb_seg['rlc.reassembled.timestamp'] - gnb_mac_attempt['phy.decodeend.timestamp'])*1000 < GNB_MAC_RLC_MATCH_MS:
                                    harqattempt['rlc_out'] = True
                                    harqattempt['rlc_ack'] = True
                                    break

                harqattempts.append(harqattempt)

            # sort harq attempts based on their timestamp
            harqattempts = sorted(harqattempts, key=lambda x: x['phy.in_t'])
            res_arr.append(harqattempts)

        return res_arr
    
    def find_ue_mac_attempt_after_ts(self, timestamp : float) -> dict:
        """
        finds the mac id of the closest UE MAC attempt after the given timestamp
        """

        # there must be at least one entry in ue mac attempts with this info
        ue_mac_attempts_0 = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= timestamp)
        ]
        if ue_mac_attempts_0.shape[0] == 0:
            logger.warning("No failed MAC attempts found")
            return {}

        return dict(ue_mac_attempts_0.shape[0])


    def find_failed_ue_mac_attempts_from_ts(self, begin_ts : float, end_ts : float, rnti : str) -> list:
        
        # finds all the mac attempts on the UE side where no data comes out of mac in gnb
        # returns a list of dict
        
        GNB_MAC_UE_MAC_MATCH_MS = 10

        # there must be at least one entry in ue mac attempts with this info
        # ue_mac_attempts_df: ['mac_id', 'phy.tx.timestamp', 'phy.tx.Hbuf', 'phy.tx.rvi', 'phy.tx.fm', 'phy.tx.sl', 'phy.tx.nb_rb', 'phy.tx.nb_sym', 'phy.tx.mod_or', 'phy.tx.len', 'phy.tx.rnti', 'phy.tx.hqpid', 'mac.harq.timestamp', 'mac.harq.hqpid', 'mac.harq.rvi', 'mac.harq.len', 'mac.harq.ndi', 'mac.harq.M3buf']
        ue_mac_attempts_0 = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= begin_ts) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] <= end_ts)
        ]
        if ue_mac_attempts_0.shape[0] == 0:
            logger.error("No harq attempts found")
            return []

        logger.info(f"Number of ul mac attempts discovered: {ue_mac_attempts_0.shape[0]}, checking failures: ", end="")

        res_arr = []
        for i in range(ue_mac_attempts_0.shape[0]):
            ue_mac_attempt = ue_mac_attempts_0.iloc[i]
            progress = (i + 1) / ue_mac_attempts_0.shape[0] * 100
            print(f"\rProgress: {progress:.2f}%", end="")

            # check #1: if data comes out on gnb side mac
            # now we can find the corresponding rlc segment on gnb side
            # gnb_mac_attempts_df: ['phy.decodeend.timestamp', 'phy.decodeend.rbb', 'phy.decodeend.rbs', 'phy.decodeend.tbs', 'phy.decodeend.mcs', 'phy.decodeend.rnti', 'phy.decodeend.suc', 'phy.detectend.timestamp', 'phy.detectend.frame', 'phy.detectend.slot', 'phy.detectend.hqpid', 'phy.detectend.hqround', 'phy.detectend.hbuf', 'phy.detectend.ptot', 'phy.detectend.pn', 'phy.detectend.pth', 'phy.detectend.suc', 'phy.detectstart.timestamp']
            found_gnb_mac_out = False
            gnb_mac_attempts_arr = self.gnb_mac_attempts_df[
                (self.gnb_mac_attempts_df['phy.decodeend.frame'] == ue_mac_attempt['phy.tx.fm']) &
                (self.gnb_mac_attempts_df['phy.decodeend.slot'] == ue_mac_attempt['phy.tx.sl']) &
                (self.gnb_mac_attempts_df['phy.decodeend.hqpid'] == ue_mac_attempt['phy.tx.hqpid']) &
                (self.gnb_mac_attempts_df['phy.decodeend.suc'] == 1) &
                (self.gnb_mac_attempts_df['phy.decodeend.rnti'] == rnti)
            ]
            if gnb_mac_attempts_arr.shape[0] > 0:
                for k in range(gnb_mac_attempts_arr.shape[0]):
                    pot_gnb_mac = gnb_mac_attempts_arr.iloc[k]
                    if abs(pot_gnb_mac['phy.decodeend.timestamp'] - ue_mac_attempt['phy.tx.timestamp'])*1000 < GNB_MAC_UE_MAC_MATCH_MS:
                        found_gnb_mac_out = True
                        break

            if found_gnb_mac_out:
                continue

            # check #2: if data comes out on rlc segment on gnb side
            # gnb_rlc_segments_df: ['sdu_id', 'rlc.reassembled.MRbuf', 'rlc.reassembled.timestamp', 'rlc.reassembled.length', 'rlc.reassembled.sn', 'rlc.reassembled.so', 'rlc.decoded.lcid', 'rlc.decoded.hqpid', 'rlc.decoded.frame', 'rlc.decoded.slot', 'rlc.decoded.timestamp', 'rlc.decoded.length', 'rlc.decoded.rnti', 'rlc.decoded.Hbuf']
            #found_gnb_rlc_out = False
            #gnb_rlc_segment_arr = self.gnb_rlc_segments_df[
            #    (self.gnb_rlc_segments_df['rlc.decoded.frame'] == ue_mac_attempt['phy.tx.fm']) &
            #    (self.gnb_rlc_segments_df['rlc.decoded.slot'] == ue_mac_attempt['phy.tx.sl']) &
            #    (self.gnb_rlc_segments_df['rlc.decoded.hqpid'] == ue_mac_attempt['phy.tx.hqpid']) &
            #    (self.gnb_rlc_segments_df['rlc.decoded.rnti'] == rnti)
            #]
            #if gnb_rlc_segment_arr.shape[0] > 0:
            #    for k in range(gnb_rlc_segment_arr.shape[0]):
            #        pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
            #        if abs(pot_gnb_seg['rlc.reassembled.timestamp'] - ue_mac_attempt['phy.tx.timestamp'])*1000 < GNB_RLC_UE_MAC_MATCH_MS:
            #            found_gnb_rlc_out = True
            #            break

            #if found_gnb_rlc_out:
            #    continue

            # if we are here, then this is a failed mac attempt
            ue_mac_attempt_dict = ue_mac_attempt.to_dict()
            # fix rvi
            if int(ue_mac_attempt['phy.tx.rvi']) == 1:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 3
            elif int(ue_mac_attempt['phy.tx.rvi']) == 3:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 2
            elif int(ue_mac_attempt['phy.tx.rvi']) == 2:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 1
            else:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 0

            # final check on TS
            if ue_mac_attempt_dict['phy.tx.timestamp'] > end_ts or ue_mac_attempt_dict['phy.tx.timestamp'] < begin_ts:
                continue

            res_arr.append(ue_mac_attempt_dict)
        
        print("\n", end="")
        logger.info(f"Failed UE MAC attempts found: {len(res_arr)}.")
        return res_arr

    def find_ndi0_ue_mac_attempts_from_ts(self, begin_ts : float, end_ts : float) -> list:
        """
        finds all the mac attempts between UE and gnb where on the harq ndi (new data indicator) is set to zero
        returns a list of dict
        """

        # there must be at least one entry in ue mac attempts with this info
        ue_mac_attempts_0 = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= begin_ts) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] <= end_ts) &
            (self.ue_mac_attempts_df['mac.harq.ndi'] == 0)
        ]
        if ue_mac_attempts_0.shape[0] == 0:
            logger.warning("No failed MAC attempts found")
            return []

        logger.info(f"UE failed MAC attempts found: {ue_mac_attempts_0.shape[0]}.")

        res_arr = []
        for i in range(ue_mac_attempts_0.shape[0]):
            ue_mac_attempt = ue_mac_attempts_0.iloc[i]
            progress = (i + 1) / ue_mac_attempts_0.shape[0] * 100
            print(f"\rProgress: {progress:.2f}%", end="")

            ue_mac_attempt_dict = ue_mac_attempt.to_dict()
            # fix rvi
            if int(ue_mac_attempt['phy.tx.rvi']) == 1:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 3
            elif int(ue_mac_attempt['phy.tx.rvi']) == 3:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 2
            elif int(ue_mac_attempt['phy.tx.rvi']) == 2:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 1
            else:
                ue_mac_attempt_dict['phy.tx.real_rvi'] = 0
            res_arr.append(ue_mac_attempt_dict)

        print("\n")
        return res_arr

    def find_failed_uldci_from_ts(self, begin_ts : float, end_ts : float, rnti : str) -> list:

        UL_DCI_GNB_RLC_MATCH_MS = 10

        # if there is an uplink dci, but no data comes out of rlc in gnb, then it was a failed uldci
        # if there is an uplink resource allocation for a certain frame and slot, 
        # but no data comes out of rlc in gnb at that frame and slot, then it was a failed schedule
        # NOTE: this can be just an excess scheduling, but we can't know that here
        # maybe gnb allocates resources for the UE due to scheduling request, but UE does not have any data to send
        # we see that in many experiments

        # ue_uldcis_df: ['rnti', 'frame', 'slot', 'frametx', 'slottx', 'timestamp', 'rbb', 'rbs', 'sb', 'ss', 'uldci_ent']
        # find all attempts with timestamps less than this
        ue_ul_dcis = self.ue_uldcis_df[
            (self.ue_uldcis_df['timestamp'] < end_ts) &
            (self.ue_uldcis_df['timestamp'] >= begin_ts) &
            (self.ue_uldcis_df['rnti'] == rnti)
        ]

        num_ue_ul_dcis = ue_ul_dcis.shape[0]
        logger.info(f"Number of UE ul DCIs discovered: {num_ue_ul_dcis}")

        res_list = []
        for j in range(num_ue_ul_dcis):
            ue_ul_dci = ue_ul_dcis.iloc[j]
            frame = int(ue_ul_dci['frametx'])
            slot = int(ue_ul_dci['slottx'])

            found_gnb_rlc_out = False

            # find the corresponding rlc segment on the gnb side
            gnb_rlc_segment_arr = self.gnb_rlc_segments_df[
                (self.gnb_rlc_segments_df['rlc.decoded.frame'] == frame) &
                (self.gnb_rlc_segments_df['rlc.decoded.slot'] == slot) &
                (self.gnb_rlc_segments_df['rlc.decoded.rnti'] == rnti)
            ]
            if gnb_rlc_segment_arr.shape[0] > 0:
                for k in range(gnb_rlc_segment_arr.shape[0]):
                    pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
                    if abs(pot_gnb_seg['rlc.reassembled.timestamp'] - ue_ul_dci['timestamp'])*1000 < UL_DCI_GNB_RLC_MATCH_MS:
                        found_gnb_rlc_out = True
                        break

            if not found_gnb_rlc_out:
                res_list.append(ue_ul_dci)

        logger.info(f"Number of failed ul DCIs discovered: {len(res_list)}")

        return res_list


    def find_repeated_ue_rlc_attempts_from_ts(self, begin_ts : float, end_ts : float) -> list:
        """
        finds all the repeated rlc attempts between UE and gnb
        returns a list of dict
        """
        
        # ue_rlc_segments_df: ['txpdu_id', 'rlc.txpdu.M1buf', 'rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.srn', 'rlc.txpdu.so', 'rlc.txpdu.tbs', 'rlc.txpdu.timestamp', 'rlc.txpdu.length', 'rlc.txpdu.leno', 'rlc.txpdu.ENTno', 'rlc.txpdu.retx', 'rlc.txpdu.retxc', 'rlc.report.timestamp', 'rlc.report.num', 'rlc.report.ack', 'rlc.report.tpollex', 'mac.sdu.lcid', 'mac.sdu.tbs', 'mac.sdu.frame', 'mac.sdu.slot', 'mac.sdu.timestamp', 'mac.sdu.length', 'mac.sdu.M2buf', 'rlc.resegment.old_leno', 'rlc.resegment.old_so', 'rlc.resegment.other_seg_leno', 'rlc.resegment.other_seg_so', 'rlc.resegment.pdu_header_len', 'rlc.resegment.pdu_len']
        # find all attempts with timestamps less than this

        repeated_ue_rlc_segments = self.ue_rlc_segments_df[
            (self.ue_rlc_segments_df['mac.sdu.timestamp'] < end_ts) &
            (self.ue_rlc_segments_df['mac.sdu.timestamp'] >= begin_ts) &
            (self.ue_rlc_segments_df['rlc.txpdu.retx'] == True)
        ]
        num_repeated_ue_rlc_segments = repeated_ue_rlc_segments.shape[0]
        logger.info(f"Number of repeated UE RLC segments discovered: {num_repeated_ue_rlc_segments}")
        return [ dict(repeated_ue_rlc_segments.iloc[j]) for j in range(num_repeated_ue_rlc_segments) ]
    

    def find_ue_mac_attempt_from_ue_rlc(self, ue_rlc_row : dict) -> dict:
        CLOSENESS_SECONDS = 0.005 #5ms

        fm = ue_rlc_row['mac.sdu.frame']
        sl = ue_rlc_row['mac.sdu.slot']
        m2buf = ue_rlc_row['mac.sdu.M2buf']
        m2len = ue_rlc_row['mac.sdu.length']

        # (m2buf_value >= m3buf_value) and ((m2buf_value + m2len) <= (m3buf_value + m3len)
        # there must be at least one entry in ue mac attempts with this info
        poss_mac_attempt_0s = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.fm'] == fm) &
            (self.ue_mac_attempts_df['phy.tx.sl'] == sl)
        ]
        if poss_mac_attempt_0s.shape[0] == 0:
            logger.error(f"No UE MAC attempts found for the UE RLC attempt {dict(ue_rlc_row)}.")
            return {}
        
        mac_attempt_0 = None
        attempt_found = False
        for i in range(poss_mac_attempt_0s.shape[0]):
            poss_mac_attempt_0 = poss_mac_attempt_0s.iloc[i]
            if (poss_mac_attempt_0['mac.harq.M3buf'] <= m2buf) and \
                ((m2buf+m2len) <= (poss_mac_attempt_0['mac.harq.M3buf']+poss_mac_attempt_0['mac.harq.len'])) and \
                ( abs(poss_mac_attempt_0['phy.tx.timestamp']-ue_rlc_row['rlc.txpdu.timestamp']) < CLOSENESS_SECONDS ) :
                
                mac_attempt_0 = poss_mac_attempt_0s.iloc[i]
                attempt_found = True
                break

        if not attempt_found:
            logger.error(f"No UE MAC attempts found for the UE RLC attempt {dict(ue_rlc_row)}.")
            return {}

        # fix rvi
        if int(mac_attempt_0['phy.tx.rvi']) == 1:
            mac_attempt_0['phy.tx.real_rvi'] = 3
        elif int(mac_attempt_0['phy.tx.rvi']) == 3:
            mac_attempt_0['phy.tx.real_rvi'] = 2
        elif int(mac_attempt_0['phy.tx.rvi']) == 2:
            mac_attempt_0['phy.tx.real_rvi'] = 1
        else:
            mac_attempt_0['phy.tx.real_rvi'] = 0
        return dict(mac_attempt_0)

    def find_failed_ue_rlc_segments_from_ts(self, begin_ts : float, end_ts : float) -> list:
        """
        first, finds all the repeated rlc attempts between UE and gnb
        returns a list of dict
        """
        
        # ue_rlc_segments_df: ['txpdu_id', 'rlc.txpdu.M1buf', 'rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.srn', 'rlc.txpdu.so', 'rlc.txpdu.tbs', 'rlc.txpdu.timestamp', 'rlc.txpdu.length', 'rlc.txpdu.leno', 'rlc.txpdu.ENTno', 'rlc.txpdu.retx', 'rlc.txpdu.retxc', 'rlc.report.timestamp', 'rlc.report.num', 'rlc.report.ack', 'rlc.report.tpollex', 'mac.sdu.lcid', 'mac.sdu.tbs', 'mac.sdu.frame', 'mac.sdu.slot', 'mac.sdu.timestamp', 'mac.sdu.length', 'mac.sdu.M2buf', 'rlc.resegment.old_leno', 'rlc.resegment.old_so', 'rlc.resegment.other_seg_leno', 'rlc.resegment.other_seg_so', 'rlc.resegment.pdu_header_len', 'rlc.resegment.pdu_len']
        # find all attempts with timestamps less than this

        # add the end offset to the dataframe
        self.ue_rlc_segments_df['rlc.txpdu.end_offset'] = self.ue_rlc_segments_df['rlc.txpdu.so'] + self.ue_rlc_segments_df['rlc.txpdu.leno']

        repeated_ue_rlc_segments = self.ue_rlc_segments_df[
            (self.ue_rlc_segments_df['mac.sdu.timestamp'] < end_ts) &
            (self.ue_rlc_segments_df['mac.sdu.timestamp'] >= begin_ts) &
            (self.ue_rlc_segments_df['rlc.txpdu.retx'] == True)
        ]

        # in the end, we filter out the repeated ones
        # so create a set of ids
        unique_txpdu_ids = set()

        num_repeated_ue_rlc_segments = repeated_ue_rlc_segments.shape[0]
        logger.info(f"Number of repeated UE RLC segments discovered: {num_repeated_ue_rlc_segments}")
        results = []
        for j in range(num_repeated_ue_rlc_segments):
            repeated_ue_rlc_segment = repeated_ue_rlc_segments.iloc[j]
            # find all previous segments overlapping with this segment

            parents_of_the_rlc_segment = self.ue_rlc_segments_df[
                (self.ue_rlc_segments_df['rlc.txpdu.so'] <= repeated_ue_rlc_segment['rlc.txpdu.so']) &
                (self.ue_rlc_segments_df['rlc.txpdu.end_offset'] >= repeated_ue_rlc_segment['rlc.txpdu.end_offset']) &
                (self.ue_rlc_segments_df['rlc.txpdu.srn'] == repeated_ue_rlc_segment['rlc.txpdu.srn']) &
                (self.ue_rlc_segments_df['txpdu_id'] != repeated_ue_rlc_segment['txpdu_id'])
            ]

            num_parents_found = parents_of_the_rlc_segment.shape[0]
            if num_parents_found == 0:
                logger.warning(f"Repeated UE RLC segment {j} has no parents.")
                continue

            # sort parents_of_the_rlc_segment by timestamp
            parent_of_the_rlc_segment = dict(parents_of_the_rlc_segment.sort_values(by='rlc.txpdu.timestamp', ascending=False, inplace=False).iloc[0])

            # find the mac attempt for this parent_of_the_rlc_segment
            parent_of_the_rlc_segment['ue_mac_attempt_0'] = self.find_ue_mac_attempt_from_ue_rlc(parent_of_the_rlc_segment)

            # for this parent_of_the_rlc_segment, find the last harq attempt
            if int(parent_of_the_rlc_segment['txpdu_id']) not in unique_txpdu_ids:
                results.append(parent_of_the_rlc_segment)
                unique_txpdu_ids.add(int(parent_of_the_rlc_segment['txpdu_id']))

        logger.info(f"Number of unique failed UE RLC segments discovered: {len(results)}")
        return results
