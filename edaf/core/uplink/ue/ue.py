import sys
import re
from loguru import logger
from edaf.core.common.utils import RingBuffer
from edaf.core.uplink.ue.ip import find_ip_packets
from edaf.core.uplink.ue.rlc import find_rlc_segments
from edaf.core.uplink.ue.mac import find_mac_attempts
from edaf.core.uplink.ue.sched import find_uldci_reports
from edaf.core.uplink.ue.sched import find_sched_reports

import os
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")


PREV_LINES_MAX = 900

# NOTE: we need to keep some earlier lines in "window_lines", 
# as in some cases, ul.dci could have arrived sooner than 
# even when the packet enters the system
PRIOR_LINES_NUM = 50

#def sort_key(line):
#    return float(line.split()[0])

class ProcessULUE:
    def __init__(
            self,
            enable_ip_packets = True,
            enable_rlc_segments = True,
            enable_mac_attempts = True,
            enable_uldcis_reports = True,
            enable_sched_reports = True,
            silent = False
        ):

        self.enable_ip_packets = enable_ip_packets
        self.enable_rlc_segments = enable_rlc_segments
        self.enable_mac_attempts = enable_mac_attempts
        self.enable_uldcis_reports = enable_uldcis_reports
        self.enable_sched_reports = enable_sched_reports

        self.previous_lines_ip = RingBuffer(PREV_LINES_MAX)
        self.ip_id_count = 0
        self.previous_lines_rlc = RingBuffer(PREV_LINES_MAX)
        self.txpdu_id_count = 0
        self.previous_lines_mac = RingBuffer(PREV_LINES_MAX)
        self.mac_id_count = 0

        self.silent = silent
        
    def run(self, lines):
        if self.enable_ip_packets:
            ip_packets_df = find_ip_packets(self.previous_lines_ip, lines, self.ip_id_count, self.silent)
        else:
            ip_packets_df = None
        
        if self.enable_rlc_segments:
            rlc_segments_df = find_rlc_segments(self.previous_lines_rlc, lines, self.txpdu_id_count, self.silent)
        else:
            rlc_segments_df = None
        
        if self.enable_mac_attempts:
            mac_attempts_df = find_mac_attempts(self.previous_lines_mac, lines, self.mac_id_count, self.silent)
        else:
            mac_attempts_df = None

        if self.enable_uldcis_reports:
            uldcis_df = find_uldci_reports(lines, self.silent)
        else:
            uldcis_df = None
        
        if self.enable_sched_reports:
            bsrupds_df, bsrtxs_df, srtrigs_df, srtxs_df = find_sched_reports(lines, self.silent)
        else:
            bsrupds_df, bsrtxs_df, srtrigs_df, srtxs_df = None, None, None, None
            
        return ip_packets_df, rlc_segments_df, mac_attempts_df, uldcis_df, bsrupds_df, bsrtxs_df, srtrigs_df, srtxs_df

