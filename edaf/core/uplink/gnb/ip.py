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

# maximum number of lines to check
MAX_DEPTH = 500

def find_ip_packets(previous_lines : RingBuffer, lines):

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    journeys = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # look for the ip packets
        # sdap.sdu--gtp.out len128::SBuf805311360.sn2
        # as it indicates one packet delivery in uplink on gnb
        KW_R = 'gtp.out'
        if KW_R in line:
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            len_match = re.search(r'len(\d+)', line)
            sbuf_match = re.search(r'SBuf(\d+)', line)
            sn_match = re.search(r'sn(\d+)', line)
            if len_match and sbuf_match and timestamp_match and sn_match:
                timestamp = float(timestamp_match.group(1))
                len_value = int(len_match.group(1))
                sbuf_value = sbuf_match.group(1)
                sn_value = int(sn_match.group(1))
                journey = {
                    KW_R : {
                        'timestamp' : timestamp,
                        'length' : len_value,
                        'SBuf' : sbuf_value,
                        'sn' : sn_value
                    }
                }
                logger.debug(f"[GNB] Found '{KW_R}' in line {line_number}, {journey}")
                snp = f"sn{sn_value}"
                sbufp = f"SBuf{sbuf_value}"
            else:
                logger.warning(f"[GNB] Found '{KW_R}' in line {line_number}, but properties did not match, skipping this {KW_R}.")
                continue

            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # check for '--sdap.sdu' lines 
            # pdcp.decoded--sdap.sdu len128::PBuf1147745664.SBuf1073749440.sn1527
            KW_SDAP = 'sdap.sdu'
            found_KW_SDAP = False
            for id,prev_line in enumerate(prev_lines):    
                if ('--'+KW_SDAP in prev_line) and (sbufp in prev_line) and (snp in prev_line):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                    len_match = re.search(r'len(\d+)', prev_line)
                    pbuf_match = re.search(r'PBuf(\d+)', prev_line)
                    if len_match and timestamp_match and pbuf_match:
                        timestamp = float(timestamp_match.group(1))
                        len_value = int(len_match.group(1))
                        pbuf_value = pbuf_match.group(1)
                    else:
                        logger.warning(f"[GNB] For {KW_SDAP}, could not find timestamp, length, or PBuf in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                        break
                    journey[KW_SDAP] = {
                        'timestamp' : timestamp,
                        'length' : len_value,
                        'PBuf' : pbuf_value,
                    }
                    logger.debug(f"[GNB] Found '{KW_SDAP}','{sbufp}', and '{snp}' in line {line_number-id-1}, {journey[KW_SDAP]}")
                    pbufp = f"PBuf{pbuf_value}"
                    found_KW_SDAP = True
                    break

            if not found_KW_SDAP:
                logger.warning(f"[GNB] Could not find '{KW_SDAP}' and '{sbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                continue

            # check for 'pdcp.decoded' lines
            # pdcp.ind--pdcp.decoded len131::PIBuf1446872704.PBuf1147745664.sn1527
            KW_PDCP = 'pdcp.decoded'
            found_KW_PDCP = False
            for id,prev_line in enumerate(prev_lines):
                if ('--'+KW_PDCP in prev_line) and (pbufp in prev_line) and (snp in prev_line):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                    len_match = re.search(r'len(\d+)', prev_line)
                    pibuf_match = re.search(r'PIBuf(\d+)', prev_line)
                    if len_match and timestamp_match and pibuf_match:
                        timestamp = float(timestamp_match.group(1))
                        len_value = int(len_match.group(1))
                        pibuf_value = pibuf_match.group(1)
                    else:
                        logger.warning(f"[GNB] For {KW_PDCP}, could not find timestamp, length, PIBuf, or sn in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                        break
                    journey[KW_PDCP] = {
                        'timestamp' : timestamp,
                        'length' : len_value,
                        'PIBuf' : pibuf_value,
                    }
                    logger.debug(f"[GNB] Found '{KW_PDCP}', '{pbufp}', and '{snp}' in line {line_number-id-1}, {journey[KW_PDCP]}")
                    pibufp = f"PIBuf{pibuf_value}"
                    found_KW_PDCP = True
                    break
            
            if not found_KW_PDCP:
                logger.warning(f"[GNB] Could not find '{KW_PDCP}' and '{pbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                continue

            # check for 'pdcp.ind'
            # rlc.reassembled--pdcp.ind len131::sn1527.PIBuf1446872704
            KW_PDCPIND = 'pdcp.ind'
            found_KW_PDCPIND = False
            for id,prev_line in enumerate(prev_lines):
                if ('--'+KW_PDCPIND in prev_line) and (pibufp in prev_line) and (snp in prev_line):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                    len_match = re.search(r'len(\d+)', prev_line)
                    if len_match and timestamp_match and sn_match:
                        timestamp = float(timestamp_match.group(1))
                        len_value = int(len_match.group(1))
                    else:
                        logger.warning(f"[GNB] For {KW_PDCPIND}, could not find timestamp, or length in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                        break

                    logger.debug(f"[GNB] Found '{KW_PDCPIND}', '{pibufp}', and '{snp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}")
                    journey[KW_PDCPIND] = {
                        'timestamp' : timestamp,
                        'length' : len_value,
                    }
                    found_KW_PDCPIND = True
                    break

            if not found_KW_PDCPIND:
                logger.warning(f"[GNB] Could not find '{KW_PDCPIND}', '{pibufp}', or '{snp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                continue
        
            journeys.append(flatten_dict(journey))
    
    logger.info(f"Extracted {len(journeys)} ip packet deliveries on GNB.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(journeys)
    return df