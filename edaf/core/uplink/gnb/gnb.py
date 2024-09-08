import sys
from loguru import logger
from edaf.core.common.utils import RingBuffer
from edaf.core.uplink.gnb.ip import find_ip_packets
from edaf.core.uplink.gnb.rlc import find_rlc_reports, find_rlc_segments
from edaf.core.uplink.gnb.sched import find_sched_events, find_sched_maps
from edaf.core.uplink.gnb.mac import find_mac_successful_attempts, find_mac_failed_attempts
import pandas as pd

import os
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

class ProcessULGNB:
    def __init__(self):
        # maximum number of lines to check
        self.previous_lines_ip = RingBuffer(500)
        self.previous_lines_rlc = RingBuffer(500)
        self.previous_lines_sched = RingBuffer(500)
        self.previous_lines_maps = RingBuffer(500)
        self.previous_lines_mac1 = RingBuffer(100)
        self.previous_lines_mac2 = RingBuffer(100)

    def run(self, lines):
        ip_packets_df = find_ip_packets(self.previous_lines_ip, lines)  # KEY: 'gtp.out.sn'
        rlc_segments_df = find_rlc_segments(self.previous_lines_rlc, lines) # KEY: 'rlc.reassembled.sn' and 'rlc.reassembled.so' 
        sched_reports_df = find_sched_events(self.previous_lines_sched, lines) # KEY: 'sched.ue.frametx' 'sched.ue.slottx'
        sched_maps_df = find_sched_maps(self.previous_lines_maps, lines)
        rlc_reports_df = find_rlc_reports(lines) # dict keys: sn
        mac_s_attempts_df = find_mac_successful_attempts(self.previous_lines_mac1, lines)
        mac_u_attempts_df = find_mac_failed_attempts(self.previous_lines_mac2, lines)
        mac_attempts_df = pd.concat([mac_s_attempts_df, mac_u_attempts_df], ignore_index=True)

        return ip_packets_df, rlc_segments_df, sched_reports_df, sched_maps_df, rlc_reports_df, mac_attempts_df
