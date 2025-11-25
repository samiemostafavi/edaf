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
    
# np.set_printoptions(suppress=True)

if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

# This file is for associating the different parameters that are saved in the database files. 


result_database_file = '/home/wilsonan/edaf_new/edaf/nov13_results/database1.db'

if __name__ == "__main__":

    # Post process examples:
    # 1) Packet analyzer
    packet_analyzer = ULPacketAnalyzer(db_addr=result_database_file)
    uids_arr = range(packet_analyzer.first_ueipid, packet_analyzer.first_ueipid+100)
    # uids_arr = range(packet_analyzer.first_ueipid, packet_analyzer.last_ueipid+1)
    # UE_PACKET_INSERTIONS = 100
    # uids_arr = list(range(packet_analyzer.first_ueipid, packet_analyzer.first_ueipid + UE_PACKET_INSERTIONS))
    packets_dict = packet_analyzer.figure_packettx_from_ueipids(uids_arr)
    snr_dict = packet_analyzer.figure_snr_from_packets(packets_dict)
    rsrp_dict = packet_analyzer.figure_rsrp_from_packets(packets_dict)
    print(packets_dict)

    # # 2) Channel analyzer
    # chan_analyzer = ULChannelAnalyzer(result_database_file)
    # begin_ts = chan_analyzer.first_ts
    # end_ts = chan_analyzer.last_ts
    # logger.info(f"Duration uploaded: {(end_ts-begin_ts)*1000} ms")
    # WINDOW_LEN_SECONDS = 2
    # mcs_arr = chan_analyzer.find_mcs_from_ts(begin_ts,begin_ts+WINDOW_LEN_SECONDS)
    # tb_arr = chan_analyzer.find_mac_attempts_from_ts(begin_ts,begin_ts+WINDOW_LEN_SECONDS)
    # print(mcs_arr)
    # print(tb_arr)

    # # 3) Scheduling analyzer
    # sched_analyzer = ULSchedulingAnalyzer(
    #     total_prbs_num = 106, 
    #     symbols_per_slot = 14, 
    #     slots_per_frame = 20, 
    #     slots_duration_ms = 0.5, 
    #     scheduling_map_num_integers = 4,
    #     db_addr = result_database_file
    # )
    # begin_ts = sched_analyzer.first_ts
    # end_ts = sched_analyzer.last_ts
    # logger.info("Scheduling events at GNB:")
    # sched_arr = sched_analyzer.find_resource_schedules_from_ts(begin_ts+10, begin_ts+10+0.1)
    # print(sched_arr)
    # logger.info("Buffer status updates:")
    # bsrupd_arr = sched_analyzer.find_bsr_upd_from_ts(begin_ts+10, begin_ts+10+0.1)
    # print(bsrupd_arr)

    
