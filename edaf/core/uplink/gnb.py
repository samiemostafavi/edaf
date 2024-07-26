import sys
import re
from loguru import logger
from collections import deque

#logger.remove()
#logger.add(sys.stderr, level="INFO")

class RingBuffer:
    def __init__(self, size):
        self.size = size
        self.buffer = deque(maxlen=size)

    def append(self, item):
        self.buffer.append(item)

    def get_items(self):
        return list(self.buffer)
    
    def reverse_items(self):
        return list(reversed(self.buffer))


KW_R = 'gtp.out'    # first, find the lines including this

# go back a few lines, find the first line that includes
KW_SDAP = 'sdap.sdu'
# and 'snXX'.

# go back more lines, find the first line that includes
KW_PDCP = 'pdcp.decoded'
# and 'snXX'. In this line, check lenYY

# go back more lines, find all lines that include
KW_RLC = 'rlc.reassembled'
# and 'snXX'. Read their 'lenZZ' and sum them up. Stop looking for more lines until the sum equals lenYY.

KW_RLC_DC = 'rlc.decoded'

KW_MAC_DEM = 'mac.demuxed'

KW_MAC_DEC = 'mac.decoded' # this is the key to recognize the retransmissions

# maximum number of lines to check
MAX_DEPTH = 100


def find_MAC_DEC(hqpid_value,hqround,prev_lines,line_number):
    # return an array

    hqround_counter = hqround
    mac_dec_arr = []
    for jd,prev_ljne in enumerate(prev_lines):
        hqstr = f'hqpid{hqpid_value}.hqround{hqround_counter}'
        if ('--'+KW_MAC_DEC in prev_ljne) and (hqstr in prev_ljne):
            timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
            fm_match = re.search(r'fm(\d+)', prev_ljne)
            sl_match = re.search(r'sl(\d+)', prev_ljne)
            if timestamp_match and fm_match and sl_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
            else:
                logger.warning(f"For {KW_MAC_DEC} and {hqstr}, could not find timestamp or frame or slot in line {line_number-jd-1}.")
                break

            logger.debug(f"find '{KW_MAC_DEC}' and '{hqstr}' in line {line_number-jd-1}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")
            mac_dec_arr.append({
                'timestamp':timestamp,
                'frame':fm_value,
                'slot':sl_value,
                'hqpid':hqpid_value,
                'hqround':hqround_counter
            })

            hqround_counter = hqround_counter-1
            if hqround_counter<0:
                break
        
    if len(mac_dec_arr) != hqround+1:
        logger.warning(f"Could not find all '{KW_MAC_DEC}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
        return []
    
    return mac_dec_arr

#def sort_key(line):
#    return float(line.split()[0])

class ProcessULGNB:
    def __init__(self):
        self.previous_lines = RingBuffer(MAX_DEPTH)

    def run(self, lines):

        #lines = sorted(unsortedlines, key=sort_key, reverse=False)
        journeys = []
        ip_packets_counter = 0
        for line_number, line in enumerate(lines):
            self.previous_lines.append(line)

            if KW_R in line:
                line = line.replace('\n', '')
            
                # Use regular expressions to extract the numbers
                timestamp_match = re.search(r'^(\d+\.\d+)', line)
                len_match = re.search(r'len(\d+)', line)
                sn_match = re.search(r'sn(\d+)', line)

                if len_match and sn_match and timestamp_match:

                    timestamp = float(timestamp_match.group(1))
                    len_value = int(len_match.group(1))
                    sn_value = int(sn_match.group(1))

                    logger.debug(f"[GNB] Found '{KW_R}' in line {line_number}, len:{len_value}, sn: {sn_value}, ts: {timestamp}")

                    journey = {
                        KW_R : {
                            'timestamp' : timestamp,
                            'length' : len_value,
                            'sn' : sn_value
                        }
                    }
                    sngtp = f"sn{sn_value}"

                    # lets go back in lines
                    prev_lines = self.previous_lines.reverse_items()

                    # check for KW_SDAP
                    found_KW_SDAP = False
                    for id,prev_line in enumerate(prev_lines):    
                        if ('--'+KW_SDAP in prev_line) and (sngtp in prev_line):
                            
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            if len_match and timestamp_match:
                                timestamp = float(timestamp_match.group(1))
                                len_value = int(len_match.group(1))
                            else:
                                logger.warning(f"[GNB] For {KW_SDAP}, could not find timestamp or length in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                                break

                            logger.debug(f"[GNB] Found '{KW_SDAP}' and '{sngtp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}")
                            journey[KW_SDAP] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                            }
                            found_KW_SDAP = True
                            break

                    if not found_KW_SDAP:
                        logger.warning(f"[GNB] Could not find '{KW_SDAP}' and '{sngtp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue

                    # check for KW_PDCP
                    found_KW_PDCP = False
                    for id,prev_line in enumerate(prev_lines):
                        if ('--'+KW_PDCP in prev_line) and (sngtp in prev_line):
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            if len_match and timestamp_match:
                                timestamp = float(timestamp_match.group(1))
                                len_value = int(len_match.group(1))
                            else:
                                logger.warning(f"[GNB] For {KW_PDCP}, could not find timestamp or length in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                                break

                            logger.debug(f"[GNB] Found '{KW_PDCP}' and '{sngtp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}")
                            journey[KW_PDCP] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                            }
                            found_KW_PDCP = True
                            break
                    
                    if not found_KW_PDCP:
                        logger.warning(f"[GNB] Could not find '{KW_PDCP}' and '{sngtp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue

                    
                    # check for KW_RLC
                    RLC_ARR = []
                    lengths = []
                    for id,prev_line in enumerate(prev_lines):
                        if ('--'+KW_RLC in prev_line) and (sngtp in prev_line):
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            mrbuf_match = re.search(r'MRbuf(\d+)\.', prev_line)
                            if len_match and timestamp_match and mrbuf_match:
                                timestamp = float(timestamp_match.group(1))
                                len_value = int(len_match.group(1))
                                mrbuf_value = mrbuf_match.group(1)
                            else:
                                logger.warning(f"[GNB] For {KW_RLC}, could not find timestamp, length, or MRBuf in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                                break

                            logger.debug(f"[GNB] Found '{KW_RLC}' and '{sngtp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, MRBuf:{mrbuf_value}")
                            lengths.append(len_value)
                            rlc_reass_dict = {
                                'MRbuf': mrbuf_value,
                                'timestamp' : timestamp,
                                'length' : len_value,
                            }

                            # Check RLC_decoded for each RLC_reassembeled
                            mrbufstr = 'MRbuf'+mrbuf_value
                            found_RLC_DC = False
                            for jd,prev_ljne in enumerate(prev_lines):
                                if ('--'+KW_RLC_DC in prev_ljne) and (mrbufstr in prev_ljne):
                                    if 'sn46.' in prev_line:
                                        print(prev_line, prev_ljne)
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    len_match = re.search(r'len(\d+)', prev_ljne)
                                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                                    lcid_match = re.search(r'lcid(\d+)', prev_ljne)
                                    hqpid_match = re.search(r'hqpid(\d+)', prev_ljne)
                                    if len_match and timestamp_match and fm_match and sl_match and lcid_match and hqpid_match:
                                        timestamp = float(timestamp_match.group(1))
                                        len_value = int(len_match.group(1))
                                        fm_value = int(fm_match.group(1))
                                        sl_value = int(sl_match.group(1))
                                        lcid_value = int(lcid_match.group(1))
                                        hqpid_value = int(hqpid_match.group(1))
                                    else:
                                        logger.warning(f"[GNB] For {KW_RLC_DC}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_R}' journey")
                                        break

                                    logger.debug(f"[GNB] Found '{KW_RLC_DC}' and '{mrbufstr}' in line {line_number-jd-1}, len:{len_value}, timestamp: {timestamp}, harq pid: {hqpid_value}")

                                    rlc_decode_dict = {
                                        'lcid': lcid_value,
                                        'hqpid': hqpid_value,
                                        'frame': fm_value,
                                        'slot': sl_value,
                                        'timestamp' : timestamp,
                                        'length' : len_value,
                                    }
                                    found_RLC_DC = True
                                    break

                            if not found_RLC_DC:
                                logger.warning(f"[GNB] Could not find '{KW_RLC_DC}' and '{mrbufstr}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                                continue

                            # Check MAC_demuxed for each RLC_decoded
                            frstr = 'fm' + str(fm_value)
                            slstr = 'sl' + str(sl_value)
                            hqstr = 'hqpid' + str(hqpid_value)
                            found_MAC_DEM = False
                            for jd,prev_ljne in enumerate(prev_lines):
                                if ('--'+KW_MAC_DEM in prev_ljne) and (frstr in prev_ljne) and (slstr in prev_ljne) and (hqstr in prev_ljne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    len_match = re.search(r'len(\d+)', prev_ljne)
                                    ldpc_match = re.search(r'ldpciter(\d+)', prev_ljne)
                                    mcs_match = re.search(r'mcs(\d+)', prev_ljne)
                                    hq_match = re.search(r'hqround(\d+)', prev_ljne)
                                    if len_match and timestamp_match and ldpc_match and mcs_match and hq_match:
                                        timestamp = float(timestamp_match.group(1))
                                        len_value = int(len_match.group(1))
                                        ldpc_value = int(ldpc_match.group(1))
                                        mcs_value = int(mcs_match.group(1))
                                        hq_value = int(hq_match.group(1))
                                    else:
                                        logger.warning(f"[GNB] For {KW_MAC_DEM}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_R}' journey")
                                        break

                                    logger.debug(f"[GNB] Found '{KW_MAC_DEM}', '{frstr}', '{slstr}', and '{hqstr}' in line {line_number-jd-1}, len:{len_value}, timestamp: {timestamp}, harq attempts: {hq_value+1}")

                                    found_MAC_DEM = True
                                    break

                            if not found_MAC_DEM:
                                logger.warning(f"[GNB] Could not find '{KW_MAC_DEM}', '{frstr}', '{slstr}', and '{hqstr}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                                continue

                            mac_demuxed_dict = {
                                'frame':fm_value,
                                'slot':sl_value,
                                'ldpciter': ldpc_value,
                                'mcs': mcs_value,
                                'hqpid': hqpid_value,
                                'hqround': hq_value,
                                'timestamp' : timestamp,
                                'length' : len_value,
                                KW_MAC_DEC : find_MAC_DEC(hqpid_value,hq_value,prev_lines,line_number),
                            }

                            RLC_ARR.append(
                                {
                                    KW_RLC : rlc_reass_dict,
                                    KW_RLC_DC : rlc_decode_dict,
                                    KW_MAC_DEM : mac_demuxed_dict
                                }
                            )
                            if sum(lengths) >= journey[KW_PDCP]['length']:
                                break
                    
                    journey[KW_RLC] = RLC_ARR
                    
                    # result
                    journeys.append(journey)
                    ip_packets_counter = ip_packets_counter+1

                else:
                    logger.error(f"[GNB] Couldn't extract values from the line {line_number}")
                    break
        else:
            logger.debug(f"[GNB] '{KW_R}' no more in the file.")

        logger.info(f"[GNB] Found {ip_packets_counter} ip packets.")
        return journeys