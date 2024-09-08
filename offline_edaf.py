import os, sys, gzip, json
from pathlib import Path
from loguru import logger
from collections import deque
import pandas as pd
from edaf.core.common.timestamp import rdtsctotsOnline
from edaf.core.uplink.gnb.gnb import ProcessULGNB
from edaf.core.uplink.ue.ue import ProcessULUE
from edaf.core.uplink.nlmt import process_ul_nlmt
from edaf.core.uplink.combine import CombineUL
from edaf.core.uplink.decompose import process_ul_journeys
import sqlite3
    
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

NUM_POP = 10000

# in case you have offline parquet journey files, you can use this script to decompose delay
# pass the address of a folder in argv with the following structure:
# FOLDER_ADDR/
# -- gnb/
# ---- latseq.*.lseq
# -- ue/
# ---- latseq.*.lseq
# -- upf/
# ---- se_*.json.gz

class RingBuffer:
    def __init__(self, size):
        self.size = size
        self.buffer = deque(maxlen=size)

    def append(self, item):
        if len(self.buffer) == self.size:
            logger.warning("RingBuffer is being overwritten. Consider increasing the buffer size.")
        self.buffer.append(item)

    def get_items(self):
        return list(self.buffer)

    def reverse_items(self):
        return list(reversed(self.buffer))

    def pop_items(self, n):
        popped_items = []
        for _ in range(min(n, len(self.buffer))):
            popped_items.append(self.buffer.popleft())
        return popped_items
        
    def get_length(self):
        return len(self.buffer)

if __name__ == "__main__":

    if len(sys.argv) != 3:
        logger.error("Usage: python offline_edaf.py <source_folder_address> <result_parquetfile>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    folder_path = Path(sys.argv[1])
    result_database_file = Path(sys.argv[2])

    gnb_path = folder_path.joinpath("gnb")
    gnb_lseq_file = list(gnb_path.glob("*.lseq"))[0]
    logger.info(f"found gnb lseq file: {gnb_lseq_file}")

    ue_path = folder_path.joinpath("ue")
    ue_lseq_file = list(ue_path.glob("*.lseq"))[0]
    logger.info(f"found ue lseq file: {ue_lseq_file}")

    upf_path = folder_path.joinpath("upf")
    upf_file = list(upf_path.glob("se_*.json.gz"))[0]
    logger.info(f"found upf json file: {upf_file}")

    gnbrdts = rdtsctotsOnline("GNB")
    gnbproc = ProcessULGNB()
    uerdts = rdtsctotsOnline("UE")
    ueproc = ProcessULUE()
    combineul = CombineUL(max_depth=NUM_POP)

    # NLMT
    with gzip.open(upf_file, 'rt', encoding='utf-8') as file:
        nlmt_journeys = json.load(file)['oneway_trips']
    logger.info(f"Loaded {len(nlmt_journeys)} NLMT packets")

    # GNB
    gnb_lseq_file = open(gnb_lseq_file, 'r')
    gnb_lines = gnb_lseq_file.readlines()
    l1linesgnb = gnbrdts.return_rdtsctots(gnb_lines)
    if len(l1linesgnb) > 0:
        gnb_ip_packets_df, gnb_rlc_segments_df, gnb_sched_reports_df, gnb_sched_maps_df, gnb_rlc_reports_df, gnb_mac_attempts_df = gnbproc.run(l1linesgnb)
    logger.info(f"Processes GNB file")
    
    # UE
    ue_lseq_file = open(ue_lseq_file, 'r')
    ue_lines = ue_lseq_file.readlines()
    l1linesue = uerdts.return_rdtsctots(ue_lines)
    l1linesue.reverse()
    if len(l1linesue) > 0:
        ue_ip_packets_df, ue_rlc_segments_df, ue_mac_attempts_df, ue_uldcis_df, ue_bsrupds_df, ue_bsrtxs_df, ue_srtrigs_df, ue_srtxs_df = ueproc.run(l1linesue)
    
    logger.info(f"Processed UE file")

    # Open a connection to the SQLite database
    conn = sqlite3.connect(result_database_file)

    # Save each DataFrame to a separate table in the SQLite database
    gnb_ip_packets_df.to_sql('gnb_ip_packets', conn, if_exists='replace', index=False)
    gnb_rlc_segments_df.to_sql('gnb_rlc_segments', conn, if_exists='replace', index=False)
    gnb_sched_reports_df.to_sql('gnb_sched_reports', conn, if_exists='replace', index=False)
    gnb_sched_maps_df.to_sql('gnb_sched_maps', conn, if_exists='replace', index=False)
    gnb_rlc_reports_df.to_sql('gnb_rlc_reports', conn, if_exists='replace', index=False)
    gnb_mac_attempts_df.to_sql('gnb_mac_attempts', conn, if_exists='replace', index=False)

    # Create gnb databases relationship
    # For each 'gtp.out.sn' in gnb_ip_packets_df, find corresponding 'sdu_id' entries in gnb_rlc_segments_df
    gnb_iprlc_rel_df = pd.merge(gnb_ip_packets_df[['gtp.out.sn']],
                            gnb_rlc_segments_df[['rlc.reassembled.sn', 'sdu_id']],
                            left_on='gtp.out.sn', right_on='rlc.reassembled.sn')
    gnb_iprlc_rel_df = gnb_iprlc_rel_df.drop(columns=['rlc.reassembled.sn'])
    gnb_iprlc_rel_df.to_sql('gnb_iprlc_rel', conn, if_exists='replace', index=False)

    ue_ip_packets_df.to_sql('ue_ip_packets', conn, if_exists='replace', index=False)
    ue_rlc_segments_df.to_sql('ue_rlc_segments', conn, if_exists='replace', index=False)
    ue_mac_attempts_df.to_sql('ue_mac_attempts', conn, if_exists='replace', index=False)
    ue_uldcis_df.to_sql('ue_uldcis', conn, if_exists='replace', index=False)
    ue_bsrupds_df.to_sql('ue_bsrupds', conn, if_exists='replace', index=False)
    ue_bsrtxs_df.to_sql('ue_bsrtxs', conn, if_exists='replace', index=False)
    ue_srtrigs_df.to_sql('ue_srtrigs', conn, if_exists='replace', index=False)
    ue_srtxs_df.to_sql('ue_srtxs', conn, if_exists='replace', index=False)

    # For each pair of ['rlc.queue.R2buf', 'rlc.queue.sn'] in ue_ip_packets_df,
    # find corresponding entries in ue_rlc_segments_df with the same values for ['rlc.txpdu.R2buf', 'rlc.txpdu.sn']
    ue_iprlc_rel_df = pd.merge(ue_ip_packets_df[['rlc.queue.R2buf', 'rlc.queue.sn',  'ip_id']],
                            ue_rlc_segments_df[['rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.srn','rlc.txpdu.timestamp', 'rlc.txpdu.length', 'txpdu_id']],  # Additional columns from ue_rlc_segments_df
                            left_on=['rlc.queue.R2buf', 'rlc.queue.sn'],
                            right_on=['rlc.txpdu.R2buf', 'rlc.txpdu.sn'])
    ue_iprlc_rel_df = ue_iprlc_rel_df.drop(columns=['rlc.queue.R2buf', 'rlc.queue.sn' , 'rlc.txpdu.R2buf', 'rlc.txpdu.sn', 'rlc.txpdu.timestamp', 'rlc.txpdu.length'])
    ue_iprlc_rel_df.to_sql('ue_iprlc_rel', conn, if_exists='replace', index=False)

    # Close the connection when done
    conn.close()
    logger.info(f"DataFrames saved successfully to '{result_database_file}'.")

    exit(0)
    ue_journeys.reverse()

    ind = 0
    df = pd.DataFrame()
    while True:
        if ind+NUM_POP > len(nlmt_journeys) or ind+NUM_POP > len(nlmt_journeys) or ind+NUM_POP > len(nlmt_journeys):
            # process all the rest
            df_combined = combineul.run(
                nlmt_journeys[ind:-1],
                gnb_journeys[ind:-1],
                ue_journeys[ind:-1]
            )
        else:
            # process a batch of size NUM_POP
            df_combined = combineul.run(
                nlmt_journeys[ind:ind+NUM_POP],
                gnb_journeys[ind:ind+NUM_POP],
                ue_journeys[ind:ind+NUM_POP]
            )

        logger.info(f'Combined len: {len(df_combined)}')
        df_to_append = process_ul_journeys(df_combined)
        logger.info(f'Processed len: {len(df_to_append)}')
        df = pd.concat([df, df_to_append], ignore_index=True)
        
        ind += NUM_POP
        if ind > len(nlmt_journeys) or ind > len(nlmt_journeys) or ind > len(nlmt_journeys):
            break
    
    logger.info(f"Combines logs, created a df with {len(df)} entries.")
    df.to_parquet(result_parquet_file, engine='pyarrow')

