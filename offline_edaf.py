import os, sys, gzip, json
from pathlib import Path
from loguru import logger
import pandas as pd
from edaf.core.uplink.preprocess import preprocess_ul
from edaf.core.uplink.analyze_packet import ULPacketAnalyzer
from edaf.core.uplink.analyze_channel import ULChannelAnalyzer
from edaf.core.uplink.analyze_scheduling import ULSchedulingAnalyzer
import sqlite3
import numpy as np
    
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

# in case you have offline parquet journey files, you can use this script to decompose delay
# pass the address of a folder in argv with the following structure:
# FOLDER_ADDR/
# -- gnb/
# ---- latseq.*.lseq
# -- ue/
# ---- latseq.*.lseq
# -- upf/
# ---- se_*.json.gz

# create database file by running
# python offline_edaf.py results/240928_082545_results results/240928_082545_results/database.db

if __name__ == "__main__":

    if len(sys.argv) != 3:
        logger.error("Usage: python offline_edaf.py <source_folder_address> <result_parquetfile>")
        sys.exit(1)

    # Get the Parquet file name from the command-line argument
    folder_path = Path(sys.argv[1])
    result_database_file = Path(sys.argv[2])

    # GNB
    gnb_path = folder_path.joinpath("gnb")
    gnb_lseq_file = list(gnb_path.glob("*.lseq"))[0]
    logger.info(f"found gnb lseq file: {gnb_lseq_file}")
    gnb_lseq_file = open(gnb_lseq_file, 'r')
    gnb_lines = gnb_lseq_file.readlines()
    
    # UE
    ue_path = folder_path.joinpath("ue")
    ue_lseq_file = list(ue_path.glob("*.lseq"))[0]
    logger.info(f"found ue lseq file: {ue_lseq_file}")
    ue_lseq_file = open(ue_lseq_file, 'r')
    ue_lines = ue_lseq_file.readlines()

    # NLMT
    nlmt_path = folder_path.joinpath("upf")
    nlmt_file = list(nlmt_path.glob("se_*"))[0]
    if nlmt_file.suffix == '.json':
        with open(nlmt_file, 'r') as file:
            nlmt_records = json.load(file)['oneway_trips']
    elif nlmt_file.suffix == '.gz':
        with gzip.open(nlmt_file, 'rt', encoding='utf-8') as file:
            nlmt_records = json.load(file)['oneway_trips']
    else:
        logger.error(f"NLMT file format not supported: {nlmt_file.suffix}")
    logger.info(f"found nlmt file: {nlmt_file}")

    # Open a connection to the SQLite database
    conn = sqlite3.connect(result_database_file)
    # process the lines
    preprocess_ul(conn, gnb_lines, ue_lines, nlmt_records)
    # Close the connection when done
    conn.close()
    logger.success(f"Tables successfully saved to '{result_database_file}'.")


    # Post process examples:

    # 1) Packet analyzer
    packet_analyzer = ULPacketAnalyzer(result_database_file)
    UE_PACKET_INSERTIONS = 100
    uids_arr = list(range(packet_analyzer.first_ueipid, packet_analyzer.first_ueipid + UE_PACKET_INSERTIONS))
    packets_dict = packet_analyzer.figure_packettx_from_ueipids(uids_arr)
    print(packets_dict)

    # 2) Channel analyzer
    chan_analyzer = ULChannelAnalyzer(result_database_file)
    begin_ts = chan_analyzer.first_ts
    end_ts = chan_analyzer.last_ts
    logger.info(f"Duration uploaded: {(end_ts-begin_ts)*1000} ms")
    WINDOW_LEN_SECONDS = 2
    mcs_arr = chan_analyzer.find_mcs_from_ts(begin_ts,begin_ts+WINDOW_LEN_SECONDS)
    tb_arr = chan_analyzer.find_mac_attempts_from_ts(begin_ts,begin_ts+WINDOW_LEN_SECONDS)
    print(mcs_arr)
    print(tb_arr)

    # 3) Scheduling analyzer
    sched_analyzer = ULSchedulingAnalyzer(
        total_prbs_num = 106, 
        symbols_per_slot = 14, 
        slots_per_frame = 20, 
        slots_duration_ms = 0.5, 
        scheduling_map_num_integers = 4,
        db_addr = result_database_file
    )
    begin_ts = sched_analyzer.first_ts
    end_ts = sched_analyzer.last_ts
    logger.info("Scheduling events at GNB:")
    sched_arr = sched_analyzer.find_resource_schedules_from_ts(begin_ts+10, begin_ts+10+0.1)
    print(sched_arr)
    logger.info("Buffer status updates:")
    bsrupd_arr = sched_analyzer.find_bsr_upd_from_ts(begin_ts+10, begin_ts+10+0.1)
    print(bsrupd_arr)

    