import sys
import re
from loguru import logger
from collections import deque
import numpy as np

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

PREV_LINES_MAX = 900

# NOTE: we need to keep some earlier lines in "window_lines", 
# as in some cases, ul.dci could have arrived sooner than 
# even when the packet enters the system
PRIOR_LINES_NUM = 50

#def sort_key(line):
#    return float(line.split()[0])

class ProcessULUE:
    def __init__(self):
        self.previous_lines = RingBuffer(PREV_LINES_MAX)

    def run(self, lines):
        # we sort in the rdt process instead
        #lines = sorted(unsortedlines, key=sort_key, reverse=False)

        journeys = []
        ip_packets_counter = 0
        for line_number, line in enumerate(lines):
            #print(line.replace('\n', ''))
            self.previous_lines.append(line)
            #set_exit = False

            # check for ip.in lines:
            # ip.in--pdcp.sdu len128::rb1.sduid0.Pbuf1615939680
            KW_R = 'ip.in'
            if KW_R in line:
                # if found one,
                # prepare the analysis window with [-20 to 500] lines around ip.in line
                self.window_lines = list()
                last_win_line_n = min(len(lines),line_number-1+PRIOR_LINES_NUM)
                #print(last_win_line_n)
                for pl in reversed(lines[line_number+1:last_win_line_n]):
                    self.window_lines.append(pl)
                for l in list(reversed(self.previous_lines.get_items())):
                    self.window_lines.append(l)            
                # the window lines
                prev_lines = self.window_lines
        
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
                        KW_R : {
                            'timestamp' : timestamp,
                            'length' : len_value,
                            'PBuf' : pbuf_value
                        }
                    }
                    pbufp = f"Pbuf{pbuf_value}"
                    ip_timestamp = timestamp

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
                                if ip_timestamp > timestamp:
                                    # picked up a pdcp.pdu line before ip.in
                                    continue
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
                            pdcp_timestamp = timestamp
                            pcbufp = f"PCbuf{pcbuf_value}"
                            found_KW_PDCPC = True
                            break

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
                                if pdcp_timestamp > timestamp:
                                    # picked up a pdcp.cipher line before pdcp.pdu
                                    continue
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
                            cipher_timestamp = timestamp
                            r1bufp = f"R1buf{r1buf_value}"
                            found_KW_PDCP = True
                            break
                    
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
                                if cipher_timestamp > timestamp:
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
                            rlc_seg_timestamp = timestamp
                            r2bufp = f"R2buf{r2buf_value}"
                            snp = f"sn{sn_value}"
                            found_KW_RLC = True
                            break
                    
                    if not found_KW_RLC:
                        logger.warning(f"[UE] Could not find '{KW_RLC}' and '{r1bufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue

                    # check for KW_RLC_TX
                    # here we look for the segments such as (in normal case):
                    # rlc.queue--rlc.txpdu len19::leno16.tbs19.sn4223186221.srn1429.so0.pdulen134.R2buf134223344.M1buf2020393731.ENTno2910
                    # rlc.queue--rlc.txpdu len19::leno14.tbs19.sn4223186221.srn1429.so16.pdulen120.R2buf134223344.M1buf2020393731.ENTno2911
                    # rlc.queue--rlc.txpdu len19::leno14.tbs19.sn4223186221.srn1429.so30.pdulen106.R2buf134223344.M1buf1986822915.ENTno2912
                    # rlc.queue--rlc.txpdu len92::leno87.tbs226.sn4223186221.srn1429.so44.pdulen92.R2buf134223344.M1buf1970037507.ENTno2913
                    # in case a segment gets retrasmissions, in addition to above, it will get entries such as:
                    # rlc.queue--rlc.txpdu retx.len19::leno16.tbs139.sn4223186221.srn1429.so0.retxc0.R2buf134223344.M1buf1995215619.ENTno2920
                    # what makes a segment unique, is (srn,so) combination, but then each retransmission will have a unique ENTno and retxc.
                    rlc_txpdus = []
                    lengths = []
                    KW_RLC_TX = 'rlc.txpdu'
                    for id,prev_line in enumerate(prev_lines):
                        if ("--"+KW_RLC_TX in prev_line) and (r2bufp in prev_line) and (snp in prev_line):
                            timestamp_match = re.search(r'^(\d+\.\d+)', prev_line)
                            len_match = re.search(r'len(\d+)', prev_line)
                            leno_match = re.search(r'leno(\d+)', prev_line)
                            tbs_match = re.search(r'tbs(\d+)\.', prev_line)
                            sn_match = re.search(r'sn(\d+)\.', prev_line)
                            so_match = re.search(r'so(\d+)\.', prev_line)
                            srn_match = re.search(r'srn(\d+)\.', prev_line)
                            m1buf_match = re.search(r'M1buf(\d+)\.', prev_line)
                            ent_match = re.search(r'ENTno(\d+)', prev_line)
                            if len_match and timestamp_match and tbs_match and sn_match and so_match and m1buf_match and ent_match and srn_match:
                                timestamp = float(timestamp_match.group(1))
                                len_value = int(len_match.group(1))
                                leno_value = int(leno_match.group(1))
                                tbs_value = int(tbs_match.group(1))
                                sn_value = int(sn_match.group(1))
                                srn_value = int(srn_match.group(1))
                                so_value = int(so_match.group(1))
                                m1buf_value = int(m1buf_match.group(1))
                                ent_value = int(ent_match.group(1))
                                retx_value = ("retx." in prev_line)
                                retxc_value = 0
                                if retx_value:
                                    retxc_match = re.search(r'retxc(\d+)', prev_line)
                                    if retxc_match:
                                        retxc_value = int(retxc_match.group(1))
                                    else:
                                        logger.warning(f"[UE] did not find retxc in {KW_RLC_TX} retx line")
                                        continue
                            else:
                                logger.warning(f"[UE] For {KW_RLC_TX}, could not found timestamp, length, M1buf, or ENTno in in line {line_number-id-1}. Skipping this '{KW_R}' journey")
                                continue
                            
                            logger.debug(f"[UE] Found '{KW_RLC_TX}' and '{r2bufp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, Mbuf:{m1buf_value}, sn: {sn_value}, srn: {srn_value}, tbs: {tbs_value}, ENTno: {ent_value}")

                            rlc_tx_reass_dict = {
                                'M1buf' : m1buf_value,
                                'sn' : sn_value,
                                'srn' : srn_value,
                                'so' : so_value,
                                'tbs' : tbs_value,
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'leno' : leno_value,
                                'ENTno' : ent_value,
                                'retx' : retx_value,
                                'retxc' : retxc_value,
                                'resegment' : {},
                                'report' : {},
                                'uldci' : {},
                            }
                            m1bufp = f"M1buf{m1buf_value}"
                            entnop = f"ENTno{ent_value}"
                            srnp = f"srn{srn_value}"
                            sop = f"so{so_value}"
                            if retx_value:
                                retxcp = f"retxc{retxc_value+1}"
                            else:
                                retxcp = f"retxc{retxc_value}"

                            # look for possible segmentation related to this txpdu
                            # rlc.resegment srn21.oleno22.oso109.n1leno16.n1so115.n2leno6.n2so109.pduhl5.pdul11
                            # rlc.queue--rlc.txpdu len11::leno6.tbs11.sn3531634549.srn21.so109.pdulen27.R2buf536878784.M1buf2217722627.ENTno95
                            # this shows that the rlc.txpdu has caused a segmentation, and the details of the segmentation is in the rlc.resegment line
                            re_srnp = f"srn{srn_value}"
                            re_sop = f"n2so{so_value}"
                            re_leno = f"n2leno{leno_value}"
                            RLC_RESEG_STR = 'rlc.resegment'
                            for jd,prev_ljne in enumerate(prev_lines):
                                if (RLC_RESEG_STR in prev_ljne) and (re_srnp in prev_ljne) and (re_sop in prev_ljne) and (re_leno in prev_ljne) and (entnop in prev_ljne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    oleno_match = re.search(r'oleno(\d+)', prev_ljne)
                                    oso_match = re.search(r'oso(\d+)', prev_ljne)
                                    n1leno_match = re.search(r'n1leno(\d+)', prev_ljne)
                                    n1so_match = re.search(r'n1so(\d+)', prev_ljne)
                                    pduhl_match = re.search(r'pduhl(\d+)', prev_ljne)
                                    pdul_match = re.search(r'pdul(\d+)', prev_ljne)
                                    if timestamp_match and oleno_match and oso_match and n1leno_match and n1so_match and pduhl_match and pdul_match:
                                        timestamp = float(timestamp_match.group(1))
                                        oleno_value = int(oleno_match.group(1))
                                        oso_value = int(oso_match.group(1))
                                        n1leno_value = int(n1leno_match.group(1))
                                        n1so_value = int(n1so_match.group(1))
                                        pduhl_value = int(pduhl_match.group(1))
                                        pdul_value = int(pdul_match.group(1))
                                        rlc_tx_reass_dict['resegment'] = {
                                            'old_leno' : oleno_value,
                                            'old_so' : oso_value,
                                            'other_seg_leno' : n1leno_value,
                                            'other_seg_so' : n1so_value,
                                            'pdu_header_len' : pduhl_value,
                                            'pdu_len' : pdul_value
                                        }
                                        logger.debug(f"[UE] Found '{RLC_RESEG_STR}', '{re_srnp}', '{re_sop}', and '{re_leno}' in line {line_number-jd-1}, old leno:{oleno_value}, old so: {oso_value}, other segment leno:{n1leno_value}, other segment so: {n1so_value}")
                                    else:
                                        continue

                            # look for the segment tranmission attempt reports:
                            # rlc.report--rlc.acked num1940.len16.sn1577979792.srn3759.so0.R2buf4160756624.retxc0
                            found_RLC_SEG_REPORT = False
                            RLC_SEG_REP_STR = 'rlc.report'
                            for jd,prev_ljne in enumerate(prev_lines):
                                if (RLC_SEG_REP_STR in prev_ljne) and (srnp in prev_ljne) and (sop in prev_ljne) and (retxcp in prev_ljne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    num_match = re.search(r'num(\d+)', prev_ljne)
                                    ack_value = ("rlc.acked" in prev_ljne)
                                    if timestamp_match and num_match:
                                        timestamp = float(timestamp_match.group(1))
                                        num_value = int(num_match.group(1))
                                        rlc_seg_txpdu_report = {
                                            'timestamp': timestamp,
                                            'num': num_value,
                                            'ack': ack_value,
                                            'tpollex': False,
                                        }
                                        found_RLC_SEG_REPORT = True
                                        logger.debug(f"[UE] Found '{RLC_SEG_REP_STR}', '{srnp}', '{sop}', and '{retxcp}' in line {line_number-jd-1}, {rlc_seg_txpdu_report}")
                                        break
                                    else:
                                        logger.warning(f"Did not find timestamp or num in {RLC_SEG_REP_STR} line {jd}.")
                                        continue

                            # if there was no report, the poll retransmission timer expiers and we will retransmit
                            # we consider this a nack report as well (but with tpollex flag)
                            # rlc.pollretx len16.srn3760.sn1577989916.so0.retxc0        
                            RLC_SEG_POLL_STR = 'rlc.pollretx'
                            for jd,prev_ljne in enumerate(prev_lines):
                                if (RLC_SEG_POLL_STR in prev_ljne) and (srnp in prev_ljne) and (sop in prev_ljne) and (retxcp in prev_ljne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    len_match = re.search(r'len(\d+)', prev_ljne)
                                    if timestamp_match and len_match:
                                        timestamp = float(timestamp_match.group(1))
                                        len_value = int(len_match.group(1))
                                        rlc_seg_txpdu_report = {
                                            'timestamp': timestamp,
                                            'len': len_value,
                                            'ack': False,
                                            'tpollex': True
                                        }
                                        found_RLC_SEG_REPORT = True
                                        logger.debug(f"[UE] Found '{RLC_SEG_POLL_STR}', '{srnp}', '{sop}', and '{retxcp}' in line {line_number-jd-1}, {rlc_seg_txpdu_report}")
                                        break
                                    else:
                                        logger.warning(f"Did not find timestamp or len in {RLC_SEG_POLL_STR} line {jd}.")
                                        continue

                            if found_RLC_SEG_REPORT:
                                if ack_value:
                                    lengths.append(leno_value)
                                rlc_tx_reass_dict['report'] = rlc_seg_txpdu_report
                            else:
                                logger.warning(f"Could not find {RLC_SEG_REP_STR} for rlc segment with srn:{srn_value} and so:{so_value}, skipping this journey.")
                                break
                            
                            # Check mac.sdu for each RLC_reassembeled
                            found_MAC_1 = False
                            KW_MAC_1 = 'mac.sdu'
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
                                    frmptx = f"fmtx{fm_value}"
                                    slp = f"sl{sl_value}"
                                    slptx = f"sltx{sl_value}"
                                    found_MAC_1 = True
                                    break

                            if not found_MAC_1:
                                logger.warning(f"[UE] Could not find '{KW_MAC_1}' and '{m1bufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                                continue

                            # Find ul.dci for this segment
                            # ul.dci rntif58e.rbb0.rbs5.sb10.ss3.ENTno282.fm938.sl11.fmtx938.sltx17
                            found_ULDCI = False
                            ULDCI_str = "ul.dci"
                            for kd,prev_lkne in enumerate(prev_lines):
                                if (ULDCI_str in prev_lkne) and (frmptx in prev_lkne) and (slptx in prev_lkne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_lkne)
                                    rnti_match = re.search(r'rnti([0-9a-fA-F]{4})', prev_lkne)
                                    rbb_match = re.search(r'rbb(\d+)', prev_lkne)
                                    rbs_match = re.search(r'rbs(\d+)', prev_lkne)
                                    sb_match = re.search(r'sb(\d+)', prev_lkne)
                                    ss_match = re.search(r'ss(\d+)', prev_lkne)
                                    fm_match = re.search(r'fm(\d+)', prev_lkne)
                                    sl_match = re.search(r'sl(\d+)', prev_lkne)
                                    fmtx_match = re.search(r'fmtx(\d+)', prev_lkne)
                                    sltx_match = re.search(r'sltx(\d+)', prev_lkne)
                                    uldci_ent_match = re.search(r'ENTno(\d+)', prev_lkne)
                                    if timestamp_match and fm_match and sl_match and fmtx_match and sltx_match and rnti_match and rbb_match and rbs_match and sb_match and ss_match and uldci_ent_match:
                                        timestamp = float(timestamp_match.group(1))
                                        rnti_value = rnti_match.group(1)
                                        rbb_value = int(rbb_match.group(1))
                                        rbs_value = int(rbs_match.group(1))
                                        sb_value = int(sb_match.group(1))
                                        ss_value = int(ss_match.group(1))
                                        fm_value = int(fm_match.group(1))
                                        sl_value = int(sl_match.group(1))
                                        uldci_ent_value = int(uldci_ent_match.group(1))
                                        fmtx_value = int(fmtx_match.group(1))
                                        sltx_value = int(sltx_match.group(1))
                                    else:
                                        logger.warning(f"[UE] For {ULDCI_str}, could not find properties in line {line_number-kd-1}. Skipping this '{KW_R}' journey")
                                        continue

                                    logger.debug(f"[UE] Found '{ULDCI_str}', '{frmptx}', and '{slptx}' in line {line_number-kd-1}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")

                                    uldci_dict = {
                                        'rnti': rnti_value,
                                        'frame': fm_value,
                                        'slot': sl_value,
                                        'frametx': fmtx_value,
                                        'slottx': sltx_value,
                                        'timestamp' : timestamp,
                                        'rbb' : rbb_value,
                                        'rbs' : rbs_value,
                                        'sb' : sb_value,
                                        'ss' : ss_value,
                                        'uldci_ent' : uldci_ent_value,
                                    }
                                    uldci_entnop = f"ENTno{uldci_ent_value}"
                                    uldci_fr = fm_value
                                    uldci_sl = sl_value
                                    found_ULDCI = True
                                    break

                            if not found_ULDCI:
                                for li in prev_lines:
                                    if 'ul.dci' in li:
                                        print(li.replace('\n', ''))
                                print(rlc_tx_reass_dict)
                                logger.warning(f"[UE] Could not find '{ULDCI_str}', '{frmptx}', and '{slptx}' in {len(prev_lines)} lines before {line_number}. Skipping the scheduling section")
                                exit(0)
                            else:   
                                # finally set the rlc pdu dict
                                rlc_tx_reass_dict['uldci'] = uldci_dict


                            # Check mac.harq
                            found_MAC_2 = False
                            KW_MAC_2 = 'mac.harq'
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
                                KW_MAC_3 = 'phy.tx'
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

                            rlc_txpdus.append(
                                {
                                    'txpdu_id' : len(rlc_txpdus),
                                    KW_RLC_TX : rlc_tx_reass_dict,
                                    KW_MAC_1 : mac_1_dict,
                                    KW_MAC_2 : mac_2_dict,
                                    KW_MAC_3 : mac_3_dict
                                }
                            )
                            if sum(lengths) >= journey[KW_RLC]['length']:
                                logger.debug(f"[UE] segments lengths parsed: {lengths}, total length: {journey[KW_RLC]['length']}, breaking segments search.")
                                break

                    if len(rlc_txpdus) == 0:
                        logger.warning(f"[UE] Could not find any RLC segments! no '{KW_RLC_TX}', '{r2bufp}', or '{snp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        journey[KW_RLC]['segments'] = []
                    else:
                        if sum(lengths) != journey[KW_RLC]['length']:
                            logger.warning(f"[UE] Sum of the segements' lengths: {lengths}, is not equal to the packet length: {journey[KW_RLC]['length']}, empty segments")
                            journey[KW_RLC]['segments'] = []
                        else:
                            # post process segments
                            def covers(seg_big,seg_small):
                                if seg_big[KW_RLC_TX]['leno'] >= seg_small[KW_RLC_TX]['leno']:
                                    return ((seg_big[KW_RLC_TX]['srn'] == seg_small[KW_RLC_TX]['srn']) and (seg_big[KW_RLC_TX]['so'] <= seg_small[KW_RLC_TX]['so']) and (seg_big[KW_RLC_TX]['so']+seg_big[KW_RLC_TX]['leno'] >= seg_small[KW_RLC_TX]['so']+seg_small[KW_RLC_TX]['leno']))
                                else:
                                    return False

                            rlc_segments = []
                            rlc_attempts = []
                            for seg_small in rlc_txpdus:
                                seg_small[KW_RLC_TX]['parent_ids'] = []
                                if seg_small[KW_RLC_TX]['report']['ack']:
                                    rlc_segments.append(seg_small)
                                else:
                                    rlc_attempts.append(seg_small)
                                for seg_big in rlc_txpdus:
                                    if covers(seg_big,seg_small) and seg_big['txpdu_id'] != seg_small['txpdu_id']:
                                        if seg_big[KW_RLC_TX]['timestamp'] < seg_small[KW_RLC_TX]['timestamp']:
                                            seg_small[KW_RLC_TX]['parent_ids'].append(seg_big['txpdu_id'])
                                seg_small[KW_RLC_TX]['retxc'] = len(seg_small[KW_RLC_TX]['parent_ids'])
                        
                            journey[KW_RLC]['attempts'] = rlc_attempts
                            journey[KW_RLC]['segments'] = rlc_segments
                    
                    # result
                    journeys.append(journey)
                    ip_packets_counter = ip_packets_counter+1

                else:
                    logger.error(f"[UE] Couldn't extract values from the line {line_number}")
                    break
        else:
            logger.debug(f"[UE] '{KW_R}' no more in the file.")

        logger.info(f"[UE] Found {ip_packets_counter} ip packets.")
        return journeys
