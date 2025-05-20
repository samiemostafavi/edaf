import os, sys
import sqlite3
from loguru import logger
import pandas as pd

if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

class ULPacketAnalyzer:
    def __init__(self, db_addr):
        # Open a connection to the SQLite database
        conn = sqlite3.connect(db_addr)

        # Read each table from the SQLite database into pandas DataFrames

        self.nlmt_df = pd.read_sql('SELECT * FROM nlmt_ip_packets', conn)
        logger.debug(f"nlmt_df: {self.nlmt_df.columns.tolist()}")

        self.gnb_mcs_reports_df = pd.read_sql('SELECT * FROM gnb_mcs_reports', conn)
        logger.debug(f"gnb_mcs_reports_df: {self.gnb_mcs_reports_df.columns.tolist()}")

        self.gnb_ip_packets_df = pd.read_sql('SELECT * FROM gnb_ip_packets', conn)
        logger.debug(f"gnb_ip_packets_df: {self.gnb_ip_packets_df.columns.tolist()}")

        self.gnb_rlc_segments_df = pd.read_sql('SELECT * FROM gnb_rlc_segments', conn)
        logger.debug(f"gnb_rlc_segments_df: {self.gnb_rlc_segments_df.columns.tolist()}")

        self.gnb_iprlc_rel_df = pd.read_sql('SELECT * FROM gnb_iprlc_rel', conn)
        logger.debug(f"gnb_iprlc_rel_df: {self.gnb_iprlc_rel_df.columns.tolist()}")

        self.gnb_mac_attempts_df = pd.read_sql('SELECT * FROM gnb_mac_attempts', conn)
        logger.debug(f"gnb_mac_attempts_df: {self.gnb_mac_attempts_df.columns.tolist()}")

        self.ue_ip_packets_df = pd.read_sql('SELECT * FROM ue_ip_packets', conn)
        logger.debug(f"ue_ip_packets_df: {self.ue_ip_packets_df.columns.tolist()}")

        self.ue_rlc_segments_df = pd.read_sql('SELECT * FROM ue_rlc_segments', conn)
        logger.debug(f"ue_rlc_segments_df: {self.ue_rlc_segments_df.columns.tolist()}")

        self.ue_mac_attempts_df = pd.read_sql('SELECT * FROM ue_mac_attempts', conn)
        logger.debug(f"ue_mac_attempts_df: {self.ue_mac_attempts_df.columns.tolist()}")

        self.ue_iprlc_rel_df = pd.read_sql('SELECT * FROM ue_iprlc_rel', conn)
        logger.debug(f"ue_iprlc_rel_df: {self.ue_iprlc_rel_df.columns.tolist()}")

        logger.info(f"Database '{db_addr}' imported successfully.")

        conn.close()

        # check and report the first and last ue ip ids
        self.first_ueipid = self.ue_ip_packets_df['ip_id'].min()
        self.last_ueipid = self.ue_ip_packets_df['ip_id'].max()

        # check and report the first and last rlc.txpdu.srn
        self.first_rlcsrn = self.ue_rlc_segments_df["rlc.txpdu.srn"].min()
        self.last_rlcsrn = self.ue_rlc_segments_df["rlc.txpdu.srn"].max()

        # check and report the first and last gnb sns
        self.first_gnbsn = self.gnb_ip_packets_df['gtp.out.sn'].min()
        self.last_gnbsn = self.gnb_ip_packets_df['gtp.out.sn'].max()
        # check and report the first and last timestamps
        self.first_ueip_ts = self.ue_ip_packets_df['ip.in.timestamp'].min()
        self.last_ueip_ts = self.ue_ip_packets_df['ip.in.timestamp'].max()


        # check and report the ue_ip_ids and gnb sns
        logger.success(f"Imported database '{db_addr}', with UE IDs ranging from {self.ue_ip_packets_df['ip_id'].min()} to {self.ue_ip_packets_df['ip_id'].max()}, and GNB SNs ranging from {self.gnb_ip_packets_df['gtp.out.sn'].min()} to {self.gnb_ip_packets_df['gtp.out.sn'].max()}")

    def __init__(self, nlmt_df, gnb_ip_packets_df, gnb_rlc_segments_df, gnb_iprlc_rel_df, gnb_mac_attempts_df, gnb_mcs_reports_df, ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df, ue_iprlc_rel_df):
        self.nlmt_df = nlmt_df
        self.gnb_ip_packets_df = gnb_ip_packets_df
        self.gnb_rlc_segments_df = gnb_rlc_segments_df
        self.gnb_iprlc_rel_df = gnb_iprlc_rel_df
        self.gnb_mac_attempts_df = gnb_mac_attempts_df
        self.gnb_mcs_reports_df = gnb_mcs_reports_df
        self.gnb_mcs_reports_df = gnb_mcs_reports_df
        self.ue_ip_packets_df = ue_ip_packets_df
        self.ue_rlc_segments_df = ue_rlc_segments_df
        self.ue_mac_attempts_df = ue_mac_attempts_df
        self.ue_iprlc_rel_df = ue_iprlc_rel_df

        # check and report the first and last ue ip ids
        self.first_ueipid = self.ue_ip_packets_df['ip_id'].min()
        self.last_ueipid = self.ue_ip_packets_df['ip_id'].max()

        # check and report the first and last rlc.txpdu.srn
        self.first_rlcsrn = self.ue_rlc_segments_df["rlc.txpdu.srn"].min()
        self.last_rlcsrn = self.ue_rlc_segments_df["rlc.txpdu.srn"].max()

        # check and report the first and last gnb sns
        self.first_gnbsn = self.gnb_ip_packets_df['gtp.out.sn'].min()
        self.last_gnbsn = self.gnb_ip_packets_df['gtp.out.sn'].max()

        # check and report the first and last timestamps
        self.first_ueip_ts = self.ue_ip_packets_df['ip.in.timestamp'].min()
        self.last_ueip_ts = self.ue_ip_packets_df['ip.in.timestamp'].max()

        # check and report the ue_ip_ids and gnb sns
        logger.debug(f"Imported online data, with UE IDs ranging from {self.ue_ip_packets_df['ip_id'].min()} to {self.ue_ip_packets_df['ip_id'].max()}, and GNB SNs ranging from {self.gnb_ip_packets_df['gtp.out.sn'].min()} to {self.gnb_ip_packets_df['gtp.out.sn'].max()}")


    def figure_packet_arrivals_from_ts(self, ts_begin, ts_end):

        ueippackets = self.ue_ip_packets_df[
            (self.ue_ip_packets_df['ip.in.timestamp'] <= ts_end) &
            (self.ue_ip_packets_df['ip.in.timestamp'] >= ts_begin)
        ]     
        return [ dict(ueippackets.iloc[i]) for i in range(ueippackets.shape[0]) ]


    def figure_packettx_from_ts(self, ts_begin, ts_end):

        poss_ueippackets = self.ue_ip_packets_df[
            (self.ue_ip_packets_df['ip.in.timestamp'] <= ts_end) &
            (self.ue_ip_packets_df['ip.in.timestamp'] >= ts_begin)
        ]     
        ue_ipid_list = []
        for i in range(poss_ueippackets.shape[0]):
            row_ueippacket = poss_ueippackets.iloc[i]
            ue_ipid_list.append(row_ueippacket['ip_id'])

        return self.figure_packettx_from_ueipids(ue_ipid_list)


    def figure_packettx_from_ue_rlc_srn(self, ue_rlc_srn_list : list, silent = False):

        packets = []
        for idx, ue_rlc_srn in enumerate(ue_rlc_srn_list):
            if not silent:
                print(f"\rProcessing packet {idx + 1}/{len(ue_rlc_srn_list)} ({(idx + 1) / len(ue_rlc_srn_list) * 100:.2f}%) with ip_id: {ue_rlc_srn}", end="")
            filtered_df = self.ue_iprlc_rel_df[self.ue_iprlc_rel_df['rlc.txpdu.srn'] == ue_rlc_srn]
            ipid_set = set()
            txpdu_id_set = set()
            for i in range(filtered_df.shape[0]):
                ipid_set.add(filtered_df.iloc[i]['ip_id'])
                txpdu_id_set.add(filtered_df.iloc[i]['txpdu_id'])

            logger.debug(f"Found {len(ipid_set)} related ipid(s) and {len(txpdu_id_set)} TXPDU(s) for UE rlc srn:{ue_rlc_srn}")

            if len(ipid_set) > 1:
                logger.error(f"More than one related ue ipids: {ipid_set} for UE rlc srn:{ue_rlc_srn}.")
                continue

            if len(ipid_set) == 0:
                logger.error(f"No related ue ipids for UE rlc srn:{ue_rlc_srn}.")
                continue

            if len(txpdu_id_set) == 0:
                logger.error(f"No related ue TXPDU ids for UE rlc srn:{ue_rlc_srn}.")
                continue

            ip_id = ipid_set.pop()
            sn = ue_rlc_srn
            logger.debug(f"The UE ipid found: {ip_id}")

            ue_rlc_rows = []
            for txpdu_id in txpdu_id_set:
                ue_rlc_rows.append(self.ue_rlc_segments_df[self.ue_rlc_segments_df['txpdu_id'] == txpdu_id].iloc[0])
            if len(ue_rlc_rows) == 0:
                logger.error(f"No related ue rlc rows found for txpdu set: {txpdu_id_set}.")
                continue
            ue_rlc_row0 = ue_rlc_rows[0]

            # get the ue ip row
            result_df = self.ue_ip_packets_df[self.ue_ip_packets_df['ip_id'] == ip_id]
            if result_df.shape[0] == 0:
                logger.error(f"For UE SN {sn}, UE IP ID {ip_id} could not be found.")
                continue
            elif result_df.shape[0] >= 1:
                logger.debug(f"Looking for the ue_ip_row with ip_id {ip_id}. Found {result_df.shape[0]} of them. We pick the closest but after the ue_rlc_row0.")
                
                min_time_diff = float('inf')
                ue_ip_row = None
                found = False

                for k in range(result_df.shape[0]):
                    pot_ue_ip_row = result_df.iloc[k]
                    time_diff = ue_rlc_row0['rlc.txpdu.timestamp'] - pot_ue_ip_row['ip.in.timestamp']
                    if time_diff > 0 and time_diff < min_time_diff:
                        found = True
                        min_time_diff = time_diff
                        ue_ip_row = pot_ue_ip_row

                if not found:
                    logger.error(f"For UE SN {sn}, UE IP ID {ip_id} could not be found, the potential ones all had negative timeings.")
                    continue
            
            result_df = self.gnb_ip_packets_df[self.gnb_ip_packets_df['gtp.out.sn'] == ue_rlc_srn]
            if result_df.shape[0] == 0:
                logger.error(f"UE SN {sn} for UE IP ID {ip_id} could not be found on GNB side. Dropped packet?")
                continue
            gnb_ip_row = result_df.iloc[0]

            filtered_df = self.gnb_iprlc_rel_df[self.gnb_iprlc_rel_df['gtp.out.sn'] == ue_rlc_srn]
            gnb_rlc_rows = []
            for i in range(filtered_df.shape[0]):
                sdu_id = int(filtered_df.iloc[i]['sdu_id'])
                gnb_rlc_rows.append(self.gnb_rlc_segments_df[self.gnb_rlc_segments_df['sdu_id'] == sdu_id].iloc[0])

            logger.debug(f"Found {len(txpdu_id_set)} gnb sdu_id(s) for SN:{ue_rlc_srn}")

            if len(txpdu_id_set) == 0 :
                logger.error(f"No related gnb txpdu ids found for UE ip_id:{ip_id} and sn:{ue_rlc_srn}")
                continue

            # start packet dict
            packet = {
                'sn' : gnb_ip_row['gtp.out.sn'],
                'id' : ip_id,
                'len' : int(ue_ip_row['ip.in.length']),
                'ip.in_t' : float(ue_ip_row['ip.in.timestamp']),
                'ip.out_t' : float(gnb_ip_row['gtp.out.timestamp']),  
                'rlc.in_t' : float(ue_ip_row['rlc.queue.timestamp']),
                'rlc.out_t' : None,
                'backlog' : int(ue_ip_row['rlc.queue.queue']),
                'rlc.attempts' : [],
            }
            # find rlc and mac attempts
            packet = self.figure_rlc_attempts(packet, gnb_rlc_rows, ue_rlc_rows)
            packets.append(packet)

        if not silent:
            print("\n", end="")
        return packets


    def figure_packettx_from_ueipids(self, ue_ipid_list : list, silent = False):
    
        packets = []
        # first sort the ipids based on the packets arrival time
        ids_ts_list = []
        for ip_id in ue_ipid_list:
            ue_ip_row = self.ue_ip_packets_df[self.ue_ip_packets_df['ip_id'] == ip_id].iloc[0]
            ids_ts_list.append({ 'id':ip_id, 'ts':ue_ip_row['ip.in.timestamp']})

        sorted_ids_ts_list = sorted(ids_ts_list, key=lambda x: x['ts'])
        sorted_ids_list = [ di['id'] for di in sorted_ids_ts_list ]

        # then do the actual work
        for idx, ip_id in enumerate(sorted_ids_list):
            if not silent:
                print(f"\rProcessing packet {idx + 1}/{len(sorted_ids_list)} ({(idx + 1) / len(sorted_ids_list) * 100:.2f}%) with ip_id: {ip_id}", end="")
            ue_ip_row = self.ue_ip_packets_df[self.ue_ip_packets_df['ip_id'] == ip_id].iloc[0]
            filtered_df = self.ue_iprlc_rel_df[self.ue_iprlc_rel_df['ip_id'] == ip_id]
            sn_set = set()
            txpdu_id_set = set()
            for i in range(filtered_df.shape[0]):
                sn_set.add(filtered_df.iloc[i]['rlc.txpdu.srn'])
                txpdu_id_set.add(filtered_df.iloc[i]['txpdu_id'])

            logger.debug(f"Found {len(sn_set)} related SN(s) and {len(txpdu_id_set)} TXPDU(s) for UE ip_id:{ip_id}")

            if len(sn_set) > 1:
                logger.error(f"More than one related ue SNs: {sn_set} for UE ip_id:{ip_id}.")
                continue

            if len(sn_set) == 0:
                logger.error(f"No related ue SNs for UE ip_id:{ip_id}.")
                continue

            if len(txpdu_id_set) == 0:
                logger.error(f"No related ue TXPDU ids for UE ip_id:{ip_id}.")
                continue

            sn = sn_set.pop()
            logger.debug(f"The UE SN found: {sn}")

            ue_rlc_rows = []
            for txpdu_id in txpdu_id_set:
                ue_rlc_rows.append(self.ue_rlc_segments_df[self.ue_rlc_segments_df['txpdu_id'] == txpdu_id].iloc[0])

            result_df = self.gnb_ip_packets_df[self.gnb_ip_packets_df['gtp.out.sn'] == sn]
            if result_df.shape[0] == 0:
                logger.error(f"UE SN {sn} for UE IP ID {ip_id} could not be found on GNB side. Dropped packet?")
                continue
            gnb_ip_row = result_df.iloc[0]

            filtered_df = self.gnb_iprlc_rel_df[self.gnb_iprlc_rel_df['gtp.out.sn'] == sn]
            gnb_rlc_rows = []
            for i in range(filtered_df.shape[0]):
                sdu_id = int(filtered_df.iloc[i]['sdu_id'])
                gnb_rlc_rows.append(self.gnb_rlc_segments_df[self.gnb_rlc_segments_df['sdu_id'] == sdu_id].iloc[0])

            logger.debug(f"Found {len(txpdu_id_set)} gnb sdu_id(s) for SN:{sn}")

            if len(txpdu_id_set) == 0 :
                logger.error(f"No related gnb txpdu ids found for UE ip_id:{ip_id} and sn:{sn}")
                continue

            # start packet dict
            packet = {
                'sn' : gnb_ip_row['gtp.out.sn'],
                'id' : ip_id,
                'len' : int(ue_ip_row['ip.in.length']),
                'ip.in_t' : float(ue_ip_row['ip.in.timestamp']),
                'ip.out_t' : float(gnb_ip_row['gtp.out.timestamp']),
                'rlc.in_t' : float(ue_ip_row['rlc.queue.timestamp']),
                'rlc.out_t' : None,
                'backlog' : int(ue_ip_row['rlc.queue.queue']),
                'rlc.attempts' : [],
            }
            # find rlc and mac attempts
            packet = self.figure_rlc_attempts(packet, gnb_rlc_rows, ue_rlc_rows)
            packets.append(packet)

        if not silent:
            print("\n", end="")
        return packets


    def figure_mac_attempts(self, rlcattempt, ue_rlc_row, ue_ip_in_ts, ue_ip_out_ts):

        CLOSENESS_SECONDS = 0.005 #5ms

        # find the first attempt's frame, slot, and hqpid
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
            return rlcattempt
        
        sorted_poss_mac_attempt_0s = poss_mac_attempt_0s.sort_values(by='phy.tx.timestamp', ascending=True, inplace=False)

        mac_attempt_0 = None
        attempt_found = False
        for i in range(sorted_poss_mac_attempt_0s.shape[0]):
            poss_mac_attempt_0 = sorted_poss_mac_attempt_0s.iloc[i]
            if (poss_mac_attempt_0['mac.harq.M3buf'] <= m2buf) and \
                ((m2buf+m2len) <= (poss_mac_attempt_0['mac.harq.M3buf']+poss_mac_attempt_0['mac.harq.len'])) and \
                ( abs(poss_mac_attempt_0['phy.tx.timestamp']-ue_rlc_row['rlc.txpdu.timestamp']) < CLOSENESS_SECONDS ) :
                
                mac_attempt_0 = sorted_poss_mac_attempt_0s.iloc[i]
                attempt_found = True
                break

        if not attempt_found:
            logger.error(f"No UE MAC attempts found for the UE RLC attempt {dict(ue_rlc_row)}.")
            return rlcattempt

        # ue_mac_attempts_df: ['mac_id', 'phy.tx.timestamp', 'phy.tx.Hbuf', 'phy.tx.rvi', 'phy.tx.fm', 'phy.tx.sl', 'phy.tx.nb_rb', 'phy.tx.nb_sym', 'phy.tx.mod_or', 'phy.tx.len', 'phy.tx.rnti', 'phy.tx.hqpid', 'mac.harq.timestamp', 'mac.harq.hqpid', 'mac.harq.rvi', 'mac.harq.len', 'mac.harq.ndi', 'mac.harq.M3buf']

        hq = mac_attempt_0['phy.tx.hqpid']
        at_0_ts = float(mac_attempt_0['phy.tx.timestamp'])

        # find all attempts with this hq
        hq_attempts = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.hqpid'] == hq) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] > at_0_ts)
        ]
        sorted_hq_attempts = hq_attempts.sort_values(by='phy.tx.timestamp', ascending=True, inplace=False)
        first_ndi_row = sorted_hq_attempts[sorted_hq_attempts['mac.harq.ndi'] == 1]

        if not first_ndi_row.empty:
            #first_ndi1_attempt_ts = float(first_ndi_row.head(1)['phy.tx.timestamp'])
            first_ndi1_attempt_ts = float(first_ndi_row['phy.tx.timestamp'].iloc[0])
        else:
            logger.error(f"No rows with mac.harq.ndi == 1 found: {dict(ue_rlc_row)}.")
            return rlcattempt

        # find all attempts with timestamps less than this
        ue_mac_attempts = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.hqpid'] == hq) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] < first_ndi1_attempt_ts) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= at_0_ts)
        ]
        num_ue_mac_attempts = ue_mac_attempts.shape[0]
        logger.debug(f"UE RLC attempt {rlcattempt['id']} - number of mac attempts discovered: {num_ue_mac_attempts}")

        # frame and slot number of the last mac attempt
        hq_s = None
        fm_s = None
        sl_s = None
        for j in range(num_ue_mac_attempts):
            ue_mac_attempt = ue_mac_attempts.iloc[j]

            # find MCS index
            # find closest mcs item in mcs_df in terms of timestamp
            # mcs item: ['timestamp', 'frame', 'slot', 'frametx', 'slottx', 'rnti', 'mcs']
            mac_mcs_value = 0
            gnb_mcs_reports = self.gnb_mcs_reports_df[
                (self.gnb_mcs_reports_df['frametx'] == ue_mac_attempt['phy.tx.fm']) &
                (self.gnb_mcs_reports_df['slottx'] == ue_mac_attempt['phy.tx.sl']) &
                (self.gnb_mcs_reports_df['rnti'] == ue_mac_attempt['phy.tx.rnti'])
            ]
            for k in range(gnb_mcs_reports.shape[0]):
                gnb_pot_mcs_attempt = gnb_mcs_reports.iloc[k]
                if abs(gnb_pot_mcs_attempt['timestamp'] - ue_mac_attempt['phy.tx.timestamp']) < 0.02:
                    mac_mcs_value = gnb_pot_mcs_attempt['mcs']
                    break

            # set the rest of the mac attempt
            macattempt = {
                'len' : ue_mac_attempt['phy.tx.len'],
                'id' : ue_mac_attempt['mac_id'],
                'rnti' : ue_mac_attempt['phy.tx.rnti'],
                'frame' : int(ue_mac_attempt[f'phy.tx.fm']),
                'slot' : int(ue_mac_attempt[f'phy.tx.sl']),
                'hqpid' : int(ue_mac_attempt[f'phy.tx.hqpid']),
                'phy.in_t' : float(ue_mac_attempt[f'phy.tx.timestamp']),
                'rbs' : int(ue_mac_attempt[f'phy.tx.nb_rb']),
                'symbols' : int(ue_mac_attempt[f'phy.tx.nb_sym']),
                'mcs' : int(mac_mcs_value),
                'phy.decode_t' : None,
                'phy.out_t' : None,
                'acked' : False,
                'hqround' : None,
                'next_id' : None,
                'prev_id' : None,
            }
            rlcattempt['rnti'] = ue_mac_attempt['phy.tx.rnti']

            # now we can find the corresponding mac attempt on gnb side
            # gnb_mac_attempts_df: ['phy.decodeend.timestamp', 'phy.decodeend.rbb', 'phy.decodeend.rbs', 'phy.decodeend.tbs', 'phy.decodeend.mcs', 'phy.decodeend.rnti', 'phy.decodeend.suc', 'phy.decodeend.frame', 'phy.decodeend.slot', 'phy.decodeend.hqpid', 'phy.decodeend.hqround', 'phy.detectend.timestamp', 'phy.detectend.frame', 'phy.detectend.slot', 'phy.detectend.hqpid', 'phy.detectend.hqround', 'phy.detectend.hbuf', 'phy.detectend.ptot', 'phy.detectend.pn', 'phy.detectend.pth', 'phy.detectend.suc', 'phy.detectstart.timestamp', 'phy.decodeend.sb', 'phy.decodeend.ss']
            gnb_mac_attempt_arr = self.gnb_mac_attempts_df[
                (self.gnb_mac_attempts_df['phy.detectend.frame'] == ue_mac_attempt['phy.tx.fm']) &
                (self.gnb_mac_attempts_df['phy.detectend.slot'] == ue_mac_attempt['phy.tx.sl']) &
                (self.gnb_mac_attempts_df['phy.decodeend.rnti'] == ue_mac_attempt['phy.tx.rnti']) &
                (self.gnb_mac_attempts_df['phy.detectend.hqpid'] == ue_mac_attempt['phy.tx.hqpid'])
            ]
            
            gnb_mac_attempt = None
            if gnb_mac_attempt_arr.shape[0] == 0:
                # unsuccessful harq attempt 
                pass
            elif gnb_mac_attempt_arr.shape[0] >= 1:
                logger.debug(f"UE RLC attempt {rlcattempt['id']} - UE MAC attempt {j}, looking for the corresponding gnb mac attempt. Found {gnb_mac_attempt_arr.shape[0]} (more than one) possible gnb mac attempt matches. We pick the one closer than {CLOSENESS_SECONDS} seconds.")
                for k in range(gnb_mac_attempt_arr.shape[0]):
                    gnb_pot_mac_attempt = gnb_mac_attempt_arr.iloc[k]
                    if abs(gnb_pot_mac_attempt['phy.decodeend.timestamp'] - ue_mac_attempt['phy.tx.timestamp']) < CLOSENESS_SECONDS:
                        gnb_mac_attempt = gnb_pot_mac_attempt
                        break

            if gnb_mac_attempt is not None:
                if pd.isna(gnb_mac_attempt['phy.decodeend.timestamp']):
                    # unsuccessful harq attempt 
                    pass
                else:
                    # possibly successful harq attempt
                    macattempt['phy.decode_t'] = float(gnb_mac_attempt['phy.decodeend.timestamp'])
                    if gnb_mac_attempt['phy.decodeend.suc']:
                        # possibly successful gnb harq attempt

                        macattempt['phy.out_t'] = float(gnb_mac_attempt['phy.decodeend.timestamp']) 
                        hq_s = int(gnb_mac_attempt['phy.detectend.hqpid'])
                        fm_s = int(gnb_mac_attempt['phy.detectend.frame'])
                        sl_s = int(gnb_mac_attempt['phy.detectend.slot'])

                        # find rlc segment of this mac attempt
                        # use hq_s, fm_s, and sl_s which belong to the last mac attempt
                        # the possible hq, fm, and sl of that rlc segment in gnb
                        gnb_rlc_segment_arr = self.gnb_rlc_segments_df[
                            (self.gnb_rlc_segments_df['rlc.decoded.frame'] == fm_s) &
                            (self.gnb_rlc_segments_df['rlc.decoded.slot'] == sl_s) &
                            (self.gnb_rlc_segments_df['rlc.decoded.hqpid'] == hq_s)
                        ]

                        if gnb_rlc_segment_arr.shape[0] >= 1:
                            logger.debug(f"UE RLC attempt {rlcattempt['id']} - found {gnb_rlc_segment_arr.shape[0]} (more than one) possible gnb rlc segment matches. We pick the one closer than {CLOSENESS_SECONDS} seconds.")
                            for k in range(gnb_rlc_segment_arr.shape[0]):
                                pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
                                if abs(pot_gnb_seg['rlc.reassembled.timestamp']-gnb_mac_attempt['phy.decodeend.timestamp']) < CLOSENESS_SECONDS:
                                    gnb_rlc_segment = pot_gnb_seg
                                    rlcattempt['mac.out_t'] = gnb_rlc_segment['rlc.reassembled.timestamp']
                                    rlcattempt['acked'] = True
                                    break

            rlcattempt['mac.attempts'].append(macattempt)

        # sort harq attempts based on their timestamp
        rlcattempt['mac.attempts'] = sorted(rlcattempt['mac.attempts'], key=lambda x: x['phy.in_t'])
        
        # set the 'hqround', and 'next_id'
        for index, macatt in enumerate(rlcattempt['mac.attempts']):
            macatt['hqround'] = index
            if index < len(rlcattempt['mac.attempts'])-1:
                macatt['next_id'] = rlcattempt['mac.attempts'][index+1]['id']
            if index > 0:
                macatt['prev_id'] = rlcattempt['mac.attempts'][index-1]['id']

        return rlcattempt

    def figure_rlc_attempts(self, packet, gnb_rlc_rows, ue_rlc_rows):

        # Get the number of rlc segments
        num_gnb_rlc_segments = len(gnb_rlc_rows)
        logger.debug(f"Number of gnb RLC segments {num_gnb_rlc_segments}")

        # Get the number of rlc attempts
        num_ue_rlc_attempts = len(ue_rlc_rows)
        logger.debug(f"Number of ue RLC attempts {num_ue_rlc_attempts}")

        # Iterate over each ue rlc attempt
        # ue_rlc_segments_df: ['txpdu_id', 'rlc.txpdu.M1buf', 'rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.srn', 'rlc.txpdu.so', 'rlc.txpdu.tbs', 'rlc.txpdu.timestamp', 'rlc.txpdu.length', 'rlc.txpdu.leno', 'rlc.txpdu.ENTno', 'rlc.txpdu.retx', 'rlc.txpdu.retxc', 'rlc.report.timestamp', 'rlc.report.num', 'rlc.report.ack', 'rlc.report.tpollex', 'mac.sdu.lcid', 'mac.sdu.tbs', 'mac.sdu.frame', 'mac.sdu.slot', 'mac.sdu.timestamp', 'mac.sdu.length', 'mac.sdu.M2buf', 'rlc.resegment.old_leno', 'rlc.resegment.old_so', 'rlc.resegment.other_seg_leno', 'rlc.resegment.other_seg_so', 'rlc.resegment.pdu_header_len', 'rlc.resegment.pdu_len']
        for i in range(num_ue_rlc_attempts):
            rlcattempt = {
                'id' : i,
                'so' : int(ue_rlc_rows[i]['rlc.txpdu.so']),
                'len' : int(ue_rlc_rows[i]['rlc.txpdu.leno']),
                'mac.in_t' : None,
                'mac.out_t' : None,
                'rnti' : None,
                'frame' : None,
                'slot' : None,
                'acked' : False,
                'repeated' : False,
                'mac.attempts' : [],
            }
            rlcattempt['mac.in_t'] = ue_rlc_rows[i]['rlc.txpdu.timestamp']

            # frame and slot number of the first harq attempt
            fm_0 = int(ue_rlc_rows[i]['mac.sdu.frame'])
            sl_0 = int(ue_rlc_rows[i]['mac.sdu.slot'])
            rlcattempt['frame'] = fm_0
            rlcattempt['slot'] = sl_0
            
            # find mac attempts for this ue rlc attempt
            rlcattempt = self.figure_mac_attempts(rlcattempt, ue_rlc_rows[i], packet['ip.in_t'], packet['ip.out_t'])

            # here we decide not to append the rlc segment if there is no mac attempt
            #if len(rlcattempt['mac.attempts']) > 0:
                # append the rlc segment
            packet['rlc.attempts'].append(rlcattempt)

        # sort rlc segments based on their timestamp
        packet['rlc.attempts'] = sorted(packet['rlc.attempts'], key=lambda x: x['mac.in_t'])

        # fix acked flag for harq attempts
        for rlcattempt in packet['rlc.attempts']:
            if rlcattempt['acked']:
                for id, mac_att in enumerate(rlcattempt['mac.attempts']):
                    if id < len(rlcattempt['mac.attempts'])-1:
                        mac_att['acked'] = False
                    else:
                        mac_att['acked'] = True
            else:
                for id, mac_att in enumerate(rlcattempt['mac.attempts']):
                        mac_att['acked'] = False

        # fix repeated flag for rlc attempts
        for idx, rlcattempt in enumerate(packet['rlc.attempts']):
            # check if (rlcattempt['so'],rlcattempt['so']+rlcattempt['len']) is included in any of packet['rlc.attempts'][0:idx] so and so+lens
            for prev_rlcattempt in packet['rlc.attempts'][:idx]:
                if prev_rlcattempt['so'] <= rlcattempt['so'] and (prev_rlcattempt['so'] + prev_rlcattempt['len']) >= (rlcattempt['so'] + rlcattempt['len']):
                    rlcattempt['repeated'] = True
                    break

        # fix rlc.out_t, which is the latest rlc.attempts
        rlc_out = 0
        for i in range(len(packet['rlc.attempts'])):
            if packet['rlc.attempts'][i]['mac.out_t']:
                rlc_out = max(packet['rlc.attempts'][i]['mac.out_t'],rlc_out)
        packet['rlc.out_t'] = rlc_out
        #print(packet)
        return packet