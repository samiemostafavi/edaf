import os, sys
from loguru import logger
import pandas as pd
from edaf.core.common.timestamp import rdtsctotsOnline
from edaf.core.uplink.gnb.gnb import ProcessULGNB
from edaf.core.uplink.ue.ue import ProcessULUE
from sqlite3 import Connection
from edaf.core.common.utils import flatten_dict

if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

# receives lines from latseq files and nlmt, processes them and inserts them into database
def preprocess_ul(
        sqlite_conn : Connection, 
        gnb_lines : list, 
        ue_lines : list, 
        nlmt_records : list
    ):

    gnbrdts = rdtsctotsOnline("GNB")
    gnbproc = ProcessULGNB()
    uerdts = rdtsctotsOnline("UE")
    ueproc = ProcessULUE()

    # NLMT preprocess
    nlmt_flat_records = []
    for nlmt_rec in nlmt_records:
        nlmt_flat_records.append(flatten_dict(nlmt_rec))
    logger.info(f"Extracted {len(nlmt_flat_records)} nlmt records.")
    nlmt_df = pd.DataFrame(nlmt_flat_records)
    logger.success(f"Processed NLMT records")
    
    # GNB preprocess
    l1linesgnb = gnbrdts.return_rdtsctots(gnb_lines)
    if len(l1linesgnb) > 0:
        gnb_ip_packets_df, gnb_rlc_segments_df, gnb_sched_reports_df, gnb_sched_maps_df, gnb_rlc_reports_df, gnb_mac_attempts_df = gnbproc.run(l1linesgnb)
    logger.success(f"Processed GNB lines")
    
    # UE preprocess
    l1linesue = uerdts.return_rdtsctots(ue_lines)
    l1linesue.reverse()
    if len(l1linesue) > 0:
        ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df, ue_uldcis_df, ue_bsrupds_df, ue_bsrtxs_df, ue_srtrigs_df, ue_srtxs_df = ueproc.run(l1linesue)
    logger.success(f"Processed UE lines")

    # Save each DataFrame to a separate table in the SQLite database

    # NLMT
    nlmt_df.to_sql('nlmt_ip_packets', sqlite_conn, if_exists='replace', index=False)

    # GNB
    gnb_ip_packets_df.to_sql('gnb_ip_packets', sqlite_conn, if_exists='replace', index=False)
    gnb_rlc_segments_df.to_sql('gnb_rlc_segments', sqlite_conn, if_exists='replace', index=False)
    gnb_sched_reports_df.to_sql('gnb_sched_reports', sqlite_conn, if_exists='replace', index=False)
    gnb_sched_maps_df.to_sql('gnb_sched_maps', sqlite_conn, if_exists='replace', index=False)
    gnb_rlc_reports_df.to_sql('gnb_rlc_reports', sqlite_conn, if_exists='replace', index=False)
    gnb_mac_attempts_df.to_sql('gnb_mac_attempts', sqlite_conn, if_exists='replace', index=False)

    # Create gnb databases relationship
    # For each 'gtp.out.sn' in gnb_ip_packets_df, find corresponding 'sdu_id' entries in gnb_rlc_segments_df
    gnb_iprlc_rel_df = pd.merge(gnb_ip_packets_df[['gtp.out.sn']],
                            gnb_rlc_segments_df[['rlc.reassembled.sn', 'sdu_id']],
                            left_on='gtp.out.sn', right_on='rlc.reassembled.sn')
    gnb_iprlc_rel_df = gnb_iprlc_rel_df.drop(columns=['rlc.reassembled.sn'])
    gnb_iprlc_rel_df.to_sql('gnb_iprlc_rel', sqlite_conn, if_exists='replace', index=False)

    # UE
    ue_ip_packets_df.to_sql('ue_ip_packets', sqlite_conn, if_exists='replace', index=False)
    ue_rlc_segments_df.to_sql('ue_rlc_segments', sqlite_conn, if_exists='replace', index=False)
    ue_mac_attempts_df.to_sql('ue_mac_attempts', sqlite_conn, if_exists='replace', index=False)
    ue_uldcis_df.to_sql('ue_uldcis', sqlite_conn, if_exists='replace', index=False)
    ue_bsrupds_df.to_sql('ue_bsrupds', sqlite_conn, if_exists='replace', index=False)
    ue_bsrtxs_df.to_sql('ue_bsrtxs', sqlite_conn, if_exists='replace', index=False)
    ue_srtrigs_df.to_sql('ue_srtrigs', sqlite_conn, if_exists='replace', index=False)
    ue_srtxs_df.to_sql('ue_srtxs', sqlite_conn, if_exists='replace', index=False)

    # For each pair of ['rlc.queue.R2buf', 'rlc.queue.sn'] in ue_ip_packets_df,
    # find corresponding entries in ue_rlc_segments_df with the same values for ['rlc.txpdu.R2buf', 'rlc.txpdu.sn']
    ue_iprlc_rel_df = pd.merge(ue_ip_packets_df[['rlc.queue.R2buf', 'rlc.queue.sn',  'ip_id']],
                            ue_rlc_segments_df[['rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.srn','rlc.txpdu.timestamp', 'rlc.txpdu.length', 'txpdu_id']],  # Additional columns from ue_rlc_segments_df
                            left_on=['rlc.queue.R2buf', 'rlc.queue.sn'],
                            right_on=['rlc.txpdu.R2buf', 'rlc.txpdu.sn'])
    ue_iprlc_rel_df = ue_iprlc_rel_df.drop(columns=['rlc.queue.R2buf', 'rlc.queue.sn' , 'rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.timestamp', 'rlc.txpdu.length'])
    ue_iprlc_rel_df.to_sql('ue_iprlc_rel', sqlite_conn, if_exists='replace', index=False)

    # logger.success(f"Tables successfully saved to sqlite database.")
