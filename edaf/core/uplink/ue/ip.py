import sys
import re
from loguru import logger
from edaf.core.common.utils import RingBuffer
from edaf.core.common.utils import flatten_dict
import pandas as pd

import os
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")


PRIOR_LINES_NUM = 20

def find_ip_packets(previous_lines : RingBuffer, lines, ip_id_count):
    # we sort in the rdt process instead
    #lines = sorted(unsortedlines, key=sort_key, reverse=False)

    journeys = []
    for line_number, line in enumerate(lines):
        #print(line.replace('\n', ''))
        previous_lines.append(line)
        #set_exit = False

        # check for ip.in lines:
        # ip.in--pdcp.sdu len128::rb1.sduid0.Pbuf1615939680
        KW_R = 'ip.in'
        if KW_R in line:

            # extract the properties
            line = line.replace('\n', '')
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            len_match = re.search(r'len(\d+)', line)
            pbuf_match = re.search(r'Pbuf(\d+)', line)
            if len_match and pbuf_match and timestamp_match:
                timestamp = float(timestamp_match.group(1))
                len_value = int(len_match.group(1))
                pbuf_value = int(pbuf_match.group(1))
                logger.debug(f"[UE] Found '{KW_R}' in line {line_number}, len:{len_value}, PBuf: {pbuf_value}, ts: {timestamp}")
                journey = {
                    'ip_id' : ip_id_count,
                    KW_R : {
                        'timestamp' : timestamp,
                        'length' : len_value,
                        'PBuf' : pbuf_value
                    }
                }
                pbufp = f"Pbuf{pbuf_value}"
                ip_timestamp = timestamp

                # lets go back in lines
                #prev_lines = previous_lines.reverse_items()

                # prepare the analysis window with [-20 to 500] lines around ip.in line
                window_lines = list()
                last_win_line_n = min(len(lines),line_number-1+PRIOR_LINES_NUM)
                #print(last_win_line_n)
                for pl in reversed(lines[line_number+1:last_win_line_n]):
                    window_lines.append(pl)
                for l in list(reversed(previous_lines.get_items())):
                    window_lines.append(l)            
                # the window lines
                prev_lines = window_lines

                # check for pdcp.cipher lines
                # pdcp.sdu--pdcp.cipher len131::rb1.sduid0.Pbuf1615939680.PCbuf1615928608
                KW_PDCPC = 'pdcp.cipher'
                found_KW_PDCPC = False
                for id,prev_line in enumerate(prev_lines):
                    if ("--"+KW_PDCPC in prev_line) and (pbufp in prev_line):
                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                        len_match = re.search(r'len(\d+)', prev_line)
                        pcbuf_match = re.search(r'PCbuf(\d+)', prev_line)
                        if len_match and pcbuf_match and timestamp_match:
                            timestamp = float(timestamp_match.group(1))
                            if ip_timestamp >= timestamp + 0.0001: # 100us is ok
                                # picked up a pdcp.pdu line before ip.in
                                continue
                            len_value = int(len_match.group(1))
                            pcbuf_value = int(pcbuf_match.group(1))
                        else:
                            logger.warning(f"[UE] For {KW_PDCPC}, could not found timestamp or length in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                            continue
                        
                        if not found_KW_PDCPC:
                            logger.debug(f"[UE] Found '{KW_PDCPC}' and '{pbufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, PCbuf: {pcbuf_value}")
                            journey[KW_PDCPC] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'PCBuf' : pcbuf_value
                            }
                            pdcp_timestamp = timestamp
                            pcbufp = f"PCbuf{pcbuf_value}"
                            found_KW_PDCPC = True
                        else:
                            logger.debug(f"[UE] Found duplicate '{KW_PDCPC}' and '{pbufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, PCbuf: {pcbuf_value}, we choose the one with the timestamp closer to ip_timestamp.")

                            # compare journey[KW_PDCPC]['timestamp'] vs timestamp against ip_timestamp
                            if abs(timestamp-ip_timestamp) < abs(journey[KW_PDCPC]['timestamp']-ip_timestamp):
                                # replace
                                journey[KW_PDCPC] = {
                                    'timestamp' : timestamp,
                                    'length' : len_value,
                                    'PCBuf' : pcbuf_value
                                }
                                pdcp_timestamp = timestamp
                                pcbufp = f"PCbuf{pcbuf_value}"
                            else:
                                # do nothing
                                pass
                        
                if not found_KW_PDCPC:
                    logger.warning(f"[UE] Could not find '{KW_PDCPC}' and '{pbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                    continue

                # check for KW_PDCP
                # pdcp.cipher--pdcp.pdu len131::rb1.sduid0.PCbuf1615928608.R1buf4026606016
                found_KW_PDCP = False
                KW_PDCP = 'pdcp.pdu'
                for id,prev_line in enumerate(prev_lines):
                    if ("--"+KW_PDCP in prev_line) and (pcbufp in prev_line):
                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                        len_match = re.search(r'len(\d+)', prev_line)
                        r1buf_match = re.search(r'R1buf(\d+)', prev_line)
                        if len_match and timestamp_match and r1buf_match:
                            timestamp = float(timestamp_match.group(1))
                            if pdcp_timestamp >= timestamp + 0.0001: # 100us is ok
                                # picked up a pdcp.cipher line before pdcp.pdu
                                continue
                            len_value = int(len_match.group(1))
                            r1buf_value = int(r1buf_match.group(1))
                        else:
                            logger.warning(f"[UE] For {KW_PDCP}, could not found timestamp or length in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                            continue

                        if not found_KW_PDCP:
                            logger.debug(f"[UE] Found '{KW_PDCP}' and '{pcbufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}")
                            journey[KW_PDCP] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'R1buf' : r1buf_value
                            }
                            cipher_timestamp = timestamp
                            r1bufp = f"R1buf{r1buf_value}"
                            found_KW_PDCP = True
                        else:
                            logger.debug(f"[UE] Found duplicate '{KW_PDCP}' and '{pcbufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, we choose the one with the timestamp closer to pdcp_timestamp.")
                            # compare journey[KW_PDCP]['timestamp'] vs timestamp against pdcp_timestamp
                            if abs(timestamp-pdcp_timestamp) < abs(journey[KW_PDCP]['timestamp']-pdcp_timestamp):
                                # replace
                                journey[KW_PDCP] = {
                                    'timestamp' : timestamp,
                                    'length' : len_value,
                                    'R1buf' : r1buf_value
                                }
                                cipher_timestamp = timestamp
                                r1bufp = f"R1buf{r1buf_value}"
                            else:
                                # do nothing
                                pass

                
                if not found_KW_PDCP:
                    logger.warning(f"[UE] Could not find '{KW_PDCP}' and '{pcbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                    continue


                # check for KW_RLC
                # pdcp.pdu--rlc.queue len131::sduid0.queue262.sn4232556276.R1buf16576.R2buf134225152
                found_KW_RLC = False
                KW_RLC = 'rlc.queue'
                for id,prev_line in enumerate(prev_lines):
                    if ("--"+KW_RLC in prev_line) and (r1bufp in prev_line):
                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                        len_match = re.search(r'len(\d+)', prev_line)
                        r2buf_match = re.search(r'R2buf(\d+)', prev_line)
                        sn_match = re.search(r'sn(\d+)', prev_line)
                        q_match = re.search(r'queue(\d+)', prev_line)
                        if len_match and timestamp_match and r2buf_match and q_match and sn_match:
                            timestamp = float(timestamp_match.group(1))
                            if cipher_timestamp >= timestamp + 0.0001: # 100us is ok
                                # picked up a rlc.queue line before pdcp.cipher
                                continue
                            len_value = int(len_match.group(1))
                            r2buf_value = int(r2buf_match.group(1))
                            queue_value = int(q_match.group(1))
                            sn_value = int(sn_match.group(1))
                        else:
                            logger.warning(f"For {KW_RLC}, could not found timestamp or length in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                            break

                        logger.debug(f"[UE] Found '{KW_RLC}' and '{r1bufp}' in line {line_number-id-1}, len:{len_value}, sn:{sn_value}, timestamp: {timestamp}")
                        journey[KW_RLC] = {
                            'timestamp' : timestamp,
                            'length' : len_value,
                            'R2buf' : r2buf_value,
                            'queue' : queue_value,
                            'sn' : sn_value,
                            'segments' : {},
                        }
                        found_KW_RLC = True
                        break
                
                if not found_KW_RLC:
                    logger.warning(f"[UE] Could not find '{KW_RLC}' and '{r1bufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                    continue

            journeys.append(flatten_dict(journey))
            ip_id_count = ip_id_count + 1
        
    logger.info(f"Extracted {len(journeys)} ip packet deliveries on UE.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(journeys)
    return df