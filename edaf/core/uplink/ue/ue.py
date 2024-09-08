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
    def __init__(self):
        self.previous_lines_ip = RingBuffer(PREV_LINES_MAX)
        self.previous_lines_rlc = RingBuffer(PREV_LINES_MAX)
        self.previous_lines_mac = RingBuffer(PREV_LINES_MAX)
        
    def run(self, lines):
        ip_packets_df = find_ip_packets(self.previous_lines_ip, lines)
        rlc_segments_df = find_rlc_segments(self.previous_lines_rlc, lines)
        mac_attempts_df = find_mac_attempts(self.previous_lines_mac, lines)
        uldcis_df = find_uldci_reports(lines)
        bsrupds_df, bsrtxs_df, srtrigs_df, srtxs_df = find_sched_reports(lines)
        return ip_packets_df, rlc_segments_df, mac_attempts_df, uldcis_df, bsrupds_df, bsrtxs_df, srtrigs_df, srtxs_df

