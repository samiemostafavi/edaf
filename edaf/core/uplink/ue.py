import sys
import re
from loguru import logger
from collections import deque

import os
if not os.getenv('DEBUG'):
    logger.remove()
    logger.add(sys.stdout, level="INFO")

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


KW_R = 'ip.in'    # first, find the lines including this, then read 'PbufXXXXXX'.

# go back a few lines, find the first line that includes
KW_PDCPC = 'pdcp.cipher'
# and the same 'PbufXXXXXX'.

# go back more lines, find the first line that includes
KW_PDCP = 'pdcp.pdu'
# and 'snXX'. In this line, check lenYY

# go back more lines, find all lines that include
KW_RLC = 'rlc.queue'
# and 'snXX'. Read their 'lenZZ' and sum them up. Stop looking for more lines until the sum equals lenYY.

KW_RLC_TX = 'rlc.txpdu'

KW_MAC_1 = 'mac.sdu'
KW_MAC_2 = 'mac.harq'
KW_MAC_3 = 'phy.tx'

MAX_DEPTH = 500

#def sort_key(line):
#    return float(line.split()[0])

class ProcessULUE:
    def __init__(self):
        self.previous_lines = RingBuffer(MAX_DEPTH)

    def run(self, lines):
        # we sort in the rdt process instead
        #lines = sorted(unsortedlines, key=sort_key, reverse=False)

        journeys = []
        ip_packets_counter = 0
        for line_number, line in enumerate(lines):
            self.previous_lines.append(line)
            #set_exit = False
            if KW_R in line:
                line = line.replace('\n', '')
                
                # Use regular expressions to extract the numbers
                timestamp_match = re.search(r'^(\d+\.\d+)', line)
                len_match = re.search(r'len(\d+)', line)
                pbuf_match = re.search(r'Pbuf(\d+)', line)

                if len_match and pbuf_match and timestamp_match:

                    timestamp = float(timestamp_match.group(1))
                    len_value = int(len_match.group(1))
                    pbuf_value = int(pbuf_match.group(1))

                    logger.debug(f"[UE] Found '{KW_R}' in line {line_number}, len:{len_value}, PBuf: {pbuf_value}, ts: {timestamp}")

                    journey = {
                        KW_R : {
                            'timestamp' : timestamp,
                            'length' : len_value,
                            'PBuf' : pbuf_value
                        }
                    }
                    pbufp = f"Pbuf{pbuf_value}"

                    # lets go back in lines
                    prev_lines = self.previous_lines.reverse_items()

                    # check for KW_PDCPC
                    found_KW_PDCPC = False
                    for id,prev_line in enumerate(prev_lines):
                        if ("--"+KW_PDCPC in prev_line) and (pbufp in prev_line):
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            pcbuf_match = re.search(r'PCbuf(\d+)', prev_line)
                            if len_match and pcbuf_match and timestamp_match:
                                timestamp = float(timestamp_match.group(1))
                                len_value = int(len_match.group(1))
                                pcbuf_value = int(pcbuf_match.group(1))
                            else:
                                logger.warning(f"[UE] For {KW_PDCPC}, could not found timestamp or length in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                                break

                            logger.debug(f"[UE] Found '{KW_PDCPC}' and '{pbufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, PCbuf: {pcbuf_value}")
                            journey[KW_PDCPC] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'PCBuf' : pcbuf_value
                            }
                            pcbufp = f"PCbuf{pcbuf_value}"
                            found_KW_PDCPC = True
                            break

                    if not found_KW_PDCPC:
                        logger.warning(f"[UE] Could not find '{KW_PDCPC}' and '{pbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue

                    # check for KW_PDCP
                    found_KW_PDCP = False
                    for id,prev_line in enumerate(prev_lines):
                        if ("--"+KW_PDCP in prev_line) and (pcbufp in prev_line):
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            r1buf_match = re.search(r'R1buf(\d+)', prev_line)
                            if len_match and timestamp_match and r1buf_match:
                                timestamp = float(timestamp_match.group(1))
                                len_value = int(len_match.group(1))
                                r1buf_value = int(r1buf_match.group(1))
                            else:
                                logger.warning(f"[UE] For {KW_PDCP}, could not found timestamp or length in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                                break

                            logger.debug(f"[UE] Found '{KW_PDCP}' and '{pcbufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}")
                            journey[KW_PDCP] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'R1buf' : r1buf_value
                            }
                            r1bufp = f"R1buf{r1buf_value}"
                            found_KW_PDCP = True
                            break
                    
                    if not found_KW_PDCP:
                        logger.warning(f"[UE] Could not find '{KW_PDCP}' and '{pcbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue


                    # check for KW_RLC
                    found_KW_RLC = False
                    for id,prev_line in enumerate(prev_lines):
                        if ("--"+KW_RLC in prev_line) and (r1bufp in prev_line):
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            r2buf_match = re.search(r'R2buf(\d+)', prev_line)
                            sn_match = re.search(r'sn(\d+)', prev_line)
                            q_match = re.search(r'queue(\d+)', prev_line)
                            if len_match and timestamp_match and r2buf_match and q_match and sn_match:
                                timestamp = float(timestamp_match.group(1))
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
                            r2bufp = f"R2buf{r2buf_value}"
                            snp = f"sn{sn_value}"
                            found_KW_RLC = True
                            break
                    
                    if not found_KW_RLC:
                        logger.warning(f"[UE] Could not find '{KW_RLC}' and '{r1bufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue

                    
                    # check for KW_RLC_TX
                    RLC_ARR = []
                    lengths = []
                    for id,prev_line in enumerate(prev_lines):
                        if ("--"+KW_RLC_TX in prev_line) and (r2bufp in prev_line) and (snp in prev_line):
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            leno_match = re.search(r'leno(\d+)', prev_line)
                            tbs_match = re.search(r'tbs(\d+)\.', prev_line)
                            sn_match = re.search(r'sn(\d+)\.', prev_line)
                            srn_match = re.search(r'srn(\d+)\.', prev_line)
                            m1buf_match = re.search(r'M1buf(\d+)\.', prev_line)
                            ent_match = re.search(r'ENTno(\d+)', prev_line)
                            if len_match and timestamp_match and tbs_match and sn_match and m1buf_match and ent_match and srn_match:
                                timestamp = float(timestamp_match.group(1))
                                len_value = int(len_match.group(1))
                                leno_value = int(leno_match.group(1))
                                tbs_value = int(tbs_match.group(1))
                                sn_value = int(sn_match.group(1))
                                srn_value = int(srn_match.group(1))
                                m1buf_value = int(m1buf_match.group(1))
                                ent_value = int(ent_match.group(1))
                            else:
                                logger.warning(f"[UE] For {KW_RLC_TX}, could not found timestamp, length, M1buf, or ENTno in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                                break

                            logger.debug(f"[UE] Found '{KW_RLC_TX}' and '{r2bufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, Mbuf:{m1buf_value}, sn: {sn_value}, srn: {srn_value}, tbs: {tbs_value}, ENTno: {ent_value}")
                            #lengths.append(len_value)
                            lengths.append(leno_value)
                            rlc_tx_reass_dict = {
                                'M1buf' : m1buf_value,
                                'sn' : sn_value,
                                'srn' : srn_value,
                                'tbs' : tbs_value,
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'leno' : leno_value,
                                'ENTno' : ent_value,
                            }
                            m1bufp = f"M1buf{m1buf_value}"
                            entnop = f"ENTno{ent_value}"

                            # Check RLC_decoded for each RLC_reassembeled
                            found_MAC_1 = False
                            for jd,prev_ljne in enumerate(prev_lines):
                                if ("--"+KW_MAC_1 in prev_ljne) and (m1bufp in prev_ljne) and (entnop in prev_ljne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    len_match = re.search(r'len(\d+)', prev_ljne)
                                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                                    lcid_match = re.search(r'lcid(\d+)', prev_ljne)
                                    tbs_match = re.search(r'tbs(\d+)', prev_ljne)
                                    m2buf_match = re.search(r'M2buf(\d+)', prev_ljne)
                                    if len_match and timestamp_match and fm_match and sl_match and lcid_match and tbs_match and m2buf_match:
                                        timestamp = float(timestamp_match.group(1))
                                        len_value = int(len_match.group(1))
                                        fm_value = int(fm_match.group(1))
                                        sl_value = int(sl_match.group(1))
                                        lcid_value = int(lcid_match.group(1))
                                        tbs_value = int(tbs_match.group(1))
                                        m2buf_value = int(m2buf_match.group(1))
                                    else:
                                        logger.warning(f"[UE] For {KW_MAC_1}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_R}' journey")
                                        break

                                    logger.debug(f"[UE] Found '{KW_MAC_1}' and '{m1bufp}' in line {line_number-jd-1}, len:{len_value}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")

                                    mac_1_dict = {
                                        'lcid': lcid_value,
                                        'tbs': tbs_value,
                                        'frame': fm_value,
                                        'slot': sl_value,
                                        'timestamp' : timestamp,
                                        'length' : len_value,
                                        'M2buf' : m2buf_value,
                                    }

                                    # NOTE: M3buf should not necessarily be equal to M2buf. 
                                    # It is important that [M2buf: M2buf+M2len] be inside [M3buf : M3buf+M3len].
                                    m2len = len_value
                                    frmp = f"fm{fm_value}"
                                    slp = f"sl{sl_value}"
                                    found_MAC_1 = True
                                    break

                            if not found_MAC_1:
                                logger.warning(f"[UE] Could not find '{KW_MAC_1}' and '{m1bufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                                continue

                            # Check RLC_decoded for each RLC_reassembeled
                            found_MAC_2 = False
                            for jd,prev_ljne in enumerate(prev_lines):
                                if ("--"+KW_MAC_2 in prev_ljne) and (frmp in prev_ljne) and (slp in prev_ljne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    len_match = re.search(r'len(\d+)', prev_ljne)
                                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                                    hqpid_match = re.search(r'hqpid(\d+)', prev_ljne)
                                    m3buf_match = re.search(r'M3buf(\d+)', prev_ljne)
                                    hbuf_match = re.search(r'Hbuf(\d+)', prev_ljne)
                                    if len_match and timestamp_match and fm_match and sl_match and hqpid_match and m3buf_match and hbuf_match:
                                        timestamp = float(timestamp_match.group(1))
                                        len_value = int(len_match.group(1))
                                        fm_value = int(fm_match.group(1))
                                        sl_value = int(sl_match.group(1))
                                        hqpid_value = int(hqpid_match.group(1))
                                        m3buf_value = int(m3buf_match.group(1))
                                        hbuf_value = int(hbuf_match.group(1))
                                    else:
                                        logger.warning(f"[UE] For {KW_MAC_2}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_R}' journey")
                                        break

                                    # important check: only [M2buf: M2buf+M2len] must be inside [M3buf : M3buf+M3len]
                                    # this is harq that can carry other drbs data
                                    m3len = len_value
                                    isInside = (m2buf_value >= m3buf_value) and ((m2buf_value + m2len) <= (m3buf_value + m3len))
                                    if (isInside):
                                        logger.debug(f"[UE] Found '{KW_MAC_2}', '{frmp}', and '{slp}' in a line where [M2buf: M2buf+M2len] was inside [M3buf : M3buf+M3len] {line_number-jd-1}, len:{len_value}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")

                                        mac_2_dict = {
                                            'hqpid': hqpid_value,
                                            'frame': fm_value,
                                            'slot': sl_value,
                                            'timestamp' : timestamp,
                                            'length' : len_value,
                                            'M3buf' : m3buf_value,
                                            'Hbuf' : hbuf_value,
                                        }
                                        hbufp = f"Hbuf{hbuf_value}"
                                        found_MAC_2 = True
                                        break

                            if not found_MAC_2:
                                logger.warning(f"[UE] Could not find '{KW_MAC_2}', '{frmp}', or '{slp}' in {len(prev_lines)} lines before {line_number} where [M2buf: M2buf+M2len] was inside [M3buf : M3buf+M3len]. MAC dicts of '{KW_R}' journey set empty.")
                                mac_2_dict = {}
                                mac_3_dict = {}
                            else:
                                # Check RLC_decoded for each RLC_reassembeled
                                found_MAC_3 = False
                                for jd,prev_ljne in enumerate(prev_lines):
                                    if ("--"+KW_MAC_3 in prev_ljne) and (hbufp in prev_ljne):
                                        timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                        len_match = re.search(r'len(\d+)', prev_ljne)
                                        fm_match = re.search(r'fm(\d+)', prev_ljne)
                                        sl_match = re.search(r'sl(\d+)', prev_ljne)
                                        hqpid_match = re.search(r'hqpid(\d+)', prev_ljne)
                                        mod_or_match = re.search(r'mod_or(\d+)', prev_ljne)
                                        nb_sym_match = re.search(r'nb_sym(\d+)', prev_ljne)
                                        nb_rb_match = re.search(r'nb_rb(\d+)', prev_ljne)
                                        rnti_match = re.search(r'rnti([0-9a-fA-F]+)', prev_ljne)
                                        if len_match and timestamp_match and fm_match and sl_match and hqpid_match and mod_or_match and nb_sym_match and nb_rb_match and rnti_match:
                                            timestamp = float(timestamp_match.group(1))
                                            len_value = int(len_match.group(1))
                                            fm_value = int(fm_match.group(1))
                                            sl_value = int(sl_match.group(1))
                                            hqpid_value = int(hqpid_match.group(1))
                                            mod_or_value = int(mod_or_match.group(1))
                                            nb_sym_value = int(nb_sym_match.group(1))
                                            nb_rb_value = int(nb_rb_match.group(1))
                                            rnti_value = rnti_match.group(1)
                                        else:
                                            logger.warning(f"[UE] For {KW_MAC_3}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_R}' journey")
                                            break
    
                                        logger.debug(f"[UE] Found '{KW_MAC_3}' and '{hbufp}' in line {line_number-jd-1}, len:{len_value}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")
    
                                        mac_3_dict = {
                                            'hqpid': hqpid_value,
                                            'frame': fm_value,
                                            'slot': sl_value,
                                            'timestamp' : timestamp,
                                            'length' : len_value,
                                            'mod_or' : mod_or_value,
                                            'nb_sym' : nb_sym_value,
                                            'nb_rb' : nb_rb_value,
                                            'rnti' : rnti_value,
                                        }
                                        found_MAC_3 = True
                                        break
    
                                if not found_MAC_3:
                                    logger.warning(f"[UE] Could not find '{KW_MAC_3}' and '{hbufp}' in {len(prev_lines)} lines before {line_number}. Mac dicts 3 of '{KW_R}' journey set empty.")
                                    mac_3_dict = {}

                            RLC_ARR.append(
                                {
                                    KW_RLC_TX : rlc_tx_reass_dict,
                                    KW_MAC_1 : mac_1_dict,
                                    KW_MAC_2 : mac_2_dict,
                                    KW_MAC_3 : mac_3_dict
                                }
                            )
                            if sum(lengths) >= journey[KW_RLC]['length']:
                                logger.debug(f"[UE] segments lengths parsed: {lengths}, total length: {journey[KW_RLC]['length']}, breaking segments search.")
                                break

                    if len(RLC_ARR) == 0:
                        logger.warning(f"[UE] Could not find any segments! no '{KW_RLC_TX}', '{r2bufp}', or '{snp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        journey[KW_RLC]['segments'] = []
                    else:
                        if sum(lengths) != journey[KW_RLC]['length']:
                            logger.warning(f"[UE] Sum of the segements' lengths: {lengths}, is not equal to the packet length: {journey[KW_RLC]['length']}")

                        journey[KW_RLC]['segments'] = RLC_ARR

                    # result
                    journeys.append(journey)
                    #if set_exit:
                    #    print(journey)
                    #    exit(0)

                    ip_packets_counter = ip_packets_counter+1

                else:
                    logger.error(f"[UE] Couldn't extract values from the line {line_number}")
                    break
        else:
            logger.debug(f"[UE] '{KW_R}' no more in the file.")

        logger.info(f"[UE] Found {ip_packets_counter} ip packets.")
        return journeys
