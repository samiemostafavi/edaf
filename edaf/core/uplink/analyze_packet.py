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
        self.gnb_ip_packets_df = pd.read_sql('SELECT * FROM gnb_ip_packets', conn)
        logger.info(f"gnb_ip_packets_df: {self.gnb_ip_packets_df.columns.tolist()}")

        self.gnb_rlc_segments_df = pd.read_sql('SELECT * FROM gnb_rlc_segments', conn)
        logger.info(f"gnb_rlc_segments_df: {self.gnb_rlc_segments_df.columns.tolist()}")

        self.gnb_iprlc_rel_df = pd.read_sql('SELECT * FROM gnb_iprlc_rel', conn)
        logger.info(f"gnb_iprlc_rel_df: {self.gnb_iprlc_rel_df.columns.tolist()}")

        self.gnb_mac_attempts_df = pd.read_sql('SELECT * FROM gnb_mac_attempts', conn)
        logger.info(f"gnb_mac_attempts_df: {self.gnb_mac_attempts_df.columns.tolist()}")

        self.ue_ip_packets_df = pd.read_sql('SELECT * FROM ue_ip_packets', conn)
        logger.info(f"ue_ip_packets_df: {self.ue_ip_packets_df.columns.tolist()}")

        self.ue_rlc_segments_df = pd.read_sql('SELECT * FROM ue_rlc_segments', conn)
        logger.info(f"ue_rlc_segments_df: {self.ue_rlc_segments_df.columns.tolist()}")

        self.ue_mac_attempts_df = pd.read_sql('SELECT * FROM ue_mac_attempts', conn)
        logger.info(f"ue_mac_attempts_df: {self.ue_mac_attempts_df.columns.tolist()}")

        self.ue_iprlc_rel_df = pd.read_sql('SELECT * FROM ue_iprlc_rel', conn)
        logger.info(f"ue_iprlc_rel_df: {self.ue_iprlc_rel_df.columns.tolist()}")

        conn.close()

        # check and report the first and last ue ip ids
        self.first_ueipid = self.ue_ip_packets_df['ip_id'].min()
        self.last_ueipid = self.ue_ip_packets_df['ip_id'].max()
        # check and report the first and last gnb sns
        self.first_gnbsn = self.gnb_ip_packets_df['gtp.out.sn'].min()
        self.last_gnbsn = self.gnb_ip_packets_df['gtp.out.sn'].max()

        # check and report the ue_ip_ids and gnb sns
        logger.success(f"Imported database '{db_addr}', with UE IDs ranging from {self.ue_ip_packets_df['ip_id'].min()} to {self.ue_ip_packets_df['ip_id'].max()}, and GNB SNs ranging from {self.gnb_ip_packets_df['gtp.out.sn'].min()} to {self.gnb_ip_packets_df['gtp.out.sn'].max()}")

    def figure_packettx_from_ueipids(self, ue_ipid_list : list):
    
        packets = []
        # first sort the ipids based on the packets arrival time
        ids_ts_list = []
        for ip_id in ue_ipid_list:
            ue_ip_row = self.ue_ip_packets_df[self.ue_ip_packets_df['ip_id'] == ip_id].iloc[0]
            ids_ts_list.append({ 'id':ip_id, 'ts':ue_ip_row['ip.in.timestamp']})

        sorted_ids_ts_list = sorted(ids_ts_list, key=lambda x: x['ts'])
        sorted_ids_list = [ di['id'] for di in sorted_ids_ts_list ]

        # then do the actual work
        for ip_id in sorted_ids_list:
            ue_ip_row = self.ue_ip_packets_df[self.ue_ip_packets_df['ip_id'] == ip_id].iloc[0]
            filtered_df = self.ue_iprlc_rel_df[self.ue_iprlc_rel_df['ip_id'] == ip_id]
            sn_set = set()
            txpdu_id_set = set()
            for i in range(filtered_df.shape[0]):
                sn_set.add(filtered_df.iloc[i]['rlc.txpdu.srn'])
                txpdu_id_set.add(filtered_df.iloc[i]['txpdu_id'])

            logger.info(f"Found {len(sn_set)} related SN(s) and {len(txpdu_id_set)} TXPDU(s) for UE ip_id:{ip_id}")

            if len(sn_set) > 1:
                logger.error(f"More than one related ue SNs for UE ip_id:{ip_id}.")
                continue

            if len(sn_set) == 0:
                logger.error(f"No related ue SNs for UE ip_id:{ip_id}.")
                continue

            if len(txpdu_id_set) == 0:
                logger.error(f"No related ue TXPDU ids for UE ip_id:{ip_id}.")
                continue

            sn = sn_set.pop()
            logger.info(f"The UE SN found: {sn}")

            ue_rlc_rows = []
            for txpdu_id in txpdu_id_set:
                ue_rlc_rows.append(self.ue_rlc_segments_df[self.ue_rlc_segments_df['txpdu_id'] == txpdu_id].iloc[0])

            gnb_ip_row = self.gnb_ip_packets_df[self.gnb_ip_packets_df['gtp.out.sn'] == sn].iloc[0]
            filtered_df = self.gnb_iprlc_rel_df[self.gnb_iprlc_rel_df['gtp.out.sn'] == sn]
            gnb_rlc_rows = []
            for i in range(filtered_df.shape[0]):
                sdu_id = int(filtered_df.iloc[i]['sdu_id'])
                gnb_rlc_rows.append(self.gnb_rlc_segments_df[self.gnb_rlc_segments_df['sdu_id'] == sdu_id].iloc[0])

            logger.info(f"Found {len(txpdu_id_set)} gnb sdu_id(s) for SN:{sn}")

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
                'rlc.attempts' : [],
            }
            # find rlc and mac attempts
            packet = self.figure_rlc_attempts(packet, gnb_rlc_rows, ue_rlc_rows)
            packets.append(packet)

        return packets

    def figure_mac_attempts(self, rlcattempt, ue_rlc_row, ue_ip_in_ts, ue_ip_out_ts):

        # find the first attempt's frame, slot, and hqpid
        fm = ue_rlc_row['mac.sdu.frame']
        sl = ue_rlc_row['mac.sdu.slot']
        m2buf = ue_rlc_row['mac.sdu.M2buf']
        m2len = ue_rlc_row['mac.sdu.length']

        # (m2buf_value >= m3buf_value) and ((m2buf_value + m2len) <= (m3buf_value + m3len)
        # there must be at least one entry in ue mac attempts with this info
        poss_mac_attempt_0s = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.fm'] == fm) &
            (self.ue_mac_attempts_df['phy.tx.sl'] == sl) &
            (self.ue_mac_attempts_df['mac.harq.M3buf'] <= m2buf) &
            ((m2buf+m2len) <= (self.ue_mac_attempts_df['mac.harq.M3buf']+self.ue_mac_attempts_df['mac.harq.len'])) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= ue_rlc_row['rlc.txpdu.timestamp']) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] <= ue_ip_out_ts)
        ]
        if poss_mac_attempt_0s.shape[0] == 0:
            logger.error("No harq attempts found")
            return rlcattempt
        elif poss_mac_attempt_0s.shape[0] > 1:
            logger.error(f"Multiple harq attempts found: {poss_mac_attempt_0s.shape[0]}.")
            return rlcattempt
        
        mac_attempt_0 = poss_mac_attempt_0s.iloc[0]
        # logger.info(f"Found harq attempt: {mac_attempt_0}")

        hq = mac_attempt_0['phy.tx.hqpid']
        at_0_ts = float(mac_attempt_0['phy.tx.timestamp'])

        # find all attempts with this hq
        hq_attempts = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.hqpid'] == hq) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] > at_0_ts)
        ]
        sorted_hq_attempts = hq_attempts.sort_values(by='phy.tx.timestamp', ascending=True, inplace=False)
        first_ndi_row = sorted_hq_attempts[sorted_hq_attempts['mac.harq.ndi'] == 1]
        #first_ndi1_attempt_ts = float(first_ndi_row.head(1)['phy.tx.timestamp'])
        first_ndi1_attempt_ts = float(first_ndi_row['phy.tx.timestamp'].iloc[0])

        # find all attempts with timestamps less than this
        ue_mac_attempts = self.ue_mac_attempts_df[
            (self.ue_mac_attempts_df['phy.tx.hqpid'] == hq) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] < first_ndi1_attempt_ts) &
            (self.ue_mac_attempts_df['phy.tx.timestamp'] >= at_0_ts)
        ]
        num_ue_mac_attempts = ue_mac_attempts.shape[0]
        logger.info(f"UE RLC attempt {rlcattempt['id']} - number of mac attempts discovered: {num_ue_mac_attempts}")

        # frame and slot number of the last mac attempt
        hq_s = None
        fm_s = None
        sl_s = None
        for j in range(num_ue_mac_attempts):
            ue_mac_attempt = ue_mac_attempts.iloc[j]

            macattempt = {
                'len' : ue_mac_attempt['phy.tx.len'],
                'id' : ue_mac_attempt['mac_id'],
                'frame' : int(ue_mac_attempt[f'phy.tx.fm']),
                'slot' : int(ue_mac_attempt[f'phy.tx.sl']),
                'hqpid' : int(ue_mac_attempt[f'phy.tx.hqpid']),
                'phy.in_t' : float(ue_mac_attempt[f'phy.tx.timestamp']),
                'phy.out_t' : None,
                'acked' : False,
                'hqround' : None,
                'next_id' : None,
                'prev_id' : None,
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
                logger.warning(f"UE RLC attempt {rlcattempt['id']} - UE MAC attempt {j}, looking for the corresponding gnb mac attempt. Found {gnb_mac_attempt_arr.shape[0]} (more than one) possible gnb mac attempt matches. We pick the one between packet arrival and departure times.")
                for k in range(gnb_mac_attempt_arr.shape[0]):
                    gnb_pot_mac_attempt = gnb_mac_attempt_arr.iloc[k]
                    if gnb_pot_mac_attempt['phy.decodeend.timestamp'] >= ue_ip_in_ts and gnb_pot_mac_attempt['phy.decodeend.timestamp'] <= ue_ip_out_ts:
                        gnb_mac_attempt = gnb_pot_mac_attempt
            else:
                gnb_mac_attempt = gnb_mac_attempt_arr.iloc[0]

            if pd.isna(gnb_mac_attempt['phy.decodeend.timestamp']):
                # unsuccessful harq attempt 
                pass
            else:
                # possibly successful harq attempt
                macattempt['phy.out_t'] = float(gnb_mac_attempt['phy.decodeend.timestamp'])
                hq_s = int(gnb_mac_attempt['phy.detectend.hqpid'])
                fm_s = int(gnb_mac_attempt['phy.detectend.frame'])
                sl_s = int(gnb_mac_attempt['phy.detectend.slot'])

            # find gnb side of this rlc segment
            # use hq_s, fm_s, and sl_s which belong to the last mac attempt
            # the possible hq, fm, and sl of that rlc segment in gnb
            gnb_rlc_segment_arr = self.gnb_rlc_segments_df[
                (self.gnb_rlc_segments_df['rlc.decoded.frame'] == fm_s) &
                (self.gnb_rlc_segments_df['rlc.decoded.slot'] == sl_s) &
                (self.gnb_rlc_segments_df['rlc.decoded.hqpid'] == hq_s)
            ]

            if gnb_rlc_segment_arr.shape[0] == 1:
                gnb_rlc_segment = gnb_rlc_segment_arr.iloc[0]
                rlcattempt['mac.out_t'] = gnb_rlc_segment['rlc.reassembled.timestamp']
                rlcattempt['acked'] = True
            elif gnb_rlc_segment_arr.shape[0] > 1:
                logger.warning(f"UE RLC attempt {rlcattempt['id']} - found {gnb_rlc_segment_arr.shape[0]} (more than one) possible gnb rlc segment matches. We pick the one between packet arrival and departure times.")
                for k in range(gnb_rlc_segment_arr.shape[0]):
                    pot_gnb_seg = gnb_rlc_segment_arr.iloc[k]
                    if pot_gnb_seg['rlc.reassembled.timestamp'] <= ue_ip_out_ts and pot_gnb_seg['rlc.reassembled.timestamp'] >= ue_ip_in_ts:
                        gnb_rlc_segment = pot_gnb_seg
                        rlcattempt['mac.out_t'] = gnb_rlc_segment['rlc.reassembled.timestamp']
                        rlcattempt['acked'] = True

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
        num_rlc_segments = len(gnb_rlc_rows)
        logger.info(f"Number of gnb RLC segments {num_rlc_segments}")

        # Get the number of rlc attempts
        num_rlc_attempts = len(ue_rlc_rows)
        logger.info(f"Number of ue RLC attempts {num_rlc_attempts}")

        # Iterate over each rlc attempt
        for i in range(num_rlc_attempts):
            rlcattempt = {
                'id' : i,
                'mac.in_t' : None,
                'mac.out_t' : None,
                'frame' : None,
                'slot' : None,
                'acked' : False,
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

        # fix rlc.out_t, which is the latest rlc.attempts
        rlc_out = 0
        for i in range(len(packet['rlc.attempts'])):
            if packet['rlc.attempts'][i]['mac.out_t']:
                rlc_out = max(packet['rlc.attempts'][i]['mac.out_t'],rlc_out)
        packet['rlc.out_t'] = rlc_out
        #print(packet)
        return packet