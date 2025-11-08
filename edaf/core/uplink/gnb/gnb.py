import sys
from loguru import logger
from edaf.core.common.utils import RingBuffer
from edaf.core.uplink.gnb.ip import find_ip_packets
from edaf.core.uplink.gnb.rlc import find_rlc_reports, find_rlc_segments
from edaf.core.uplink.gnb.sched import find_sched_events, find_sched_maps, find_mcs_reports
from edaf.core.uplink.gnb.mac import find_mac_successful_attempts, find_mac_failed_attempts, find_rssi_values, find_ulcqi_values
import pandas as pd

import os
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

class ProcessULGNB:
    def __init__(
            self,
            enable_ip_packets = True,
            enable_rlc_segments = True,
            enable_sched_reports = True,
            enable_sched_maps = True,
            enable_rlc_reports = True,
            enable_mac_attempts = True,
            enable_mcs_reports = True,
            enable_rssi_values = True,
            enable_ulcqi_values = True,
            silent = False
        ):
        self.enable_ip_packets = enable_ip_packets
        self.enable_rlc_segments = enable_rlc_segments
        self.enable_sched_reports = enable_sched_reports
        self.enable_sched_maps = enable_sched_maps
        self.enable_rlc_reports = enable_rlc_reports
        self.enable_mac_attempts = enable_mac_attempts
        self.enable_mcs_reports = enable_mcs_reports
        self.enable_rssi_values = enable_rssi_values
        self.enable_ulcqi_values = enable_ulcqi_values

        # maximum number of lines to check
        self.previous_lines_ip = RingBuffer(500)
        self.previous_lines_rlc = RingBuffer(500)
        self.sdu_id_count = 0
        self.previous_lines_sched = RingBuffer(500)
        self.previous_lines_maps = RingBuffer(500)
        self.previous_lines_mac1 = RingBuffer(100)
        self.previous_lines_mac2 = RingBuffer(100)

        self.silent = silent

    def run(self, lines):
        if self.enable_ip_packets:
            ip_packets_df = find_ip_packets(self.previous_lines_ip, lines, self.silent)  # KEY: 'gtp.out.sn'
        else:
            ip_packets_df = None
        
        if self.enable_rlc_segments:
            rlc_segments_df = find_rlc_segments(self.previous_lines_rlc, lines, self.sdu_id_count, self.silent) # KEY: 'rlc.reassembled.sn' and 'rlc.reassembled.so' 
        else:
            rlc_segments_df = None

        if self.enable_sched_reports:
            sched_reports_df = find_sched_events(self.previous_lines_sched, lines, self.silent) # KEY: 'sched.ue.frametx' 'sched.ue.slottx'
        else:
            sched_reports_df = None

        if self.enable_sched_maps:
            sched_maps_df = find_sched_maps(self.previous_lines_maps, lines, self.silent)
        else:
            sched_maps_df = None

        if self.enable_rlc_reports:
            rlc_reports_df = find_rlc_reports(lines, self.silent) # dict keys: sn
        else:
            rlc_reports_df = None

        if self.enable_mac_attempts:
            mac_s_attempts_df = find_mac_successful_attempts(self.previous_lines_mac1, lines, self.silent)
            mac_u_attempts_df = find_mac_failed_attempts(self.previous_lines_mac2, lines, self.silent)
            mac_attempts_df = pd.concat([mac_s_attempts_df, mac_u_attempts_df], ignore_index=True)
        else:
            mac_attempts_df = None

        if self.enable_mcs_reports:
            mcs_reports_df = find_mcs_reports(lines, self.silent)
        else:
            mcs_reports_df = None

        if self.enable_rssi_values:
            rssi_values_df = find_rssi_values(self.previous_lines_mac1, lines, self.silent)
        else:
            rssi_values_df = None

        if self.enable_ulcqi_values:
            ulcqi_values_df = find_ulcqi_values(self.previous_lines_mac1, lines, self.silent)
        else:
            ulcqi_values_df = None


        return ip_packets_df, rlc_segments_df, sched_reports_df, sched_maps_df, rlc_reports_df, mac_attempts_df, mcs_reports_df, rssi_values_df, ulcqi_values_df
