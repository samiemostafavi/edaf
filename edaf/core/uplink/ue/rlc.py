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

# NOTE: we need to keep some earlier lines in "window_lines", 
# as in some cases, ul.dci could have arrived sooner than 
# even when the packet enters the system
PRIOR_LINES_NUM = 50

def find_rlc_segments(previous_lines : RingBuffer, lines, txpdu_id_count):
    # we sort in the rdt process instead
    #lines = sorted(unsortedlines, key=sort_key, reverse=False)

    txpdus = []
    for line_number, line in enumerate(lines):
        #print(line.replace('\n', ''))
        previous_lines.append(line)

        # check for KW_RLC_TX
        # here we look for the segments such as (in normal case):
        # rlc.queue--rlc.txpdu len19::leno16.tbs19.sn4223186221.srn1429.so0.pdulen134.R2buf134223344.M1buf2020393731.ENTno2910
        # rlc.queue--rlc.txpdu len19::leno14.tbs19.sn4223186221.srn1429.so16.pdulen120.R2buf134223344.M1buf2020393731.ENTno2911
        # rlc.queue--rlc.txpdu len19::leno14.tbs19.sn4223186221.srn1429.so30.pdulen106.R2buf134223344.M1buf1986822915.ENTno2912
        # rlc.queue--rlc.txpdu len92::leno87.tbs226.sn4223186221.srn1429.so44.pdulen92.R2buf134223344.M1buf1970037507.ENTno2913
        # in case a segment gets retrasmissions, in addition to above, it will get entries such as:
        # rlc.queue--rlc.txpdu retx.len19::leno16.tbs139.sn4223186221.srn1429.so0.retxc0.R2buf134223344.M1buf1995215619.ENTno2920
        # what makes a segment unique, is (srn,so) combination, but then each retransmission will have a unique ENTno and retxc.
        KW_RLC_TX = 'rlc.txpdu'
        if ("--"+KW_RLC_TX in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            len_match = re.search(r'len(\d+)', line)
            leno_match = re.search(r'leno(\d+)', line)
            tbs_match = re.search(r'tbs(\d+)', line)
            sn_match = re.search(r'sn(\d+)', line)
            so_match = re.search(r'so(\d+)', line)
            srn_match = re.search(r'srn(\d+)', line)
            r2buf_match = re.search(r'R2buf(\d+)', line)
            m1buf_match = re.search(r'M1buf(\d+)', line)
            ent_match = re.search(r'ENTno(\d+)', line)
            if len_match and timestamp_match and tbs_match and sn_match and so_match and m1buf_match and r2buf_match and ent_match and srn_match:
                timestamp = float(timestamp_match.group(1))
                len_value = int(len_match.group(1))
                leno_value = int(leno_match.group(1))
                tbs_value = int(tbs_match.group(1))
                sn_value = int(sn_match.group(1))
                srn_value = int(srn_match.group(1))
                so_value = int(so_match.group(1))
                m1buf_value = int(m1buf_match.group(1))
                r2buf_value = int(r2buf_match.group(1))
                ent_value = int(ent_match.group(1))
                retx_value = ("retx." in line)
                retxc_value = 0
                if retx_value:
                    retxc_match = re.search(r'retxc(\d+)', line)
                    if retxc_match:
                        retxc_value = int(retxc_match.group(1))
                    else:
                        logger.warning(f"[UE] did not find retxc in {KW_RLC_TX} retx line")
                        continue
            else:
                logger.warning(f"[UE] For {KW_RLC_TX}, could not found timestamp, length, M1buf, or ENTno in line {line_number}. Skipping this '{KW_RLC_TX}' txpdu")
                continue

            txpdu_report = {
                'txpdu_id' : txpdu_id_count,
                KW_RLC_TX : {
                    'M1buf' : m1buf_value,
                    'R2buf' : r2buf_value,
                    'sn' : sn_value,
                    'srn' : srn_value,
                    'so' : so_value,
                    'tbs' : tbs_value,
                    'timestamp' : timestamp,
                    'length' : len_value,
                    'leno' : leno_value,
                    'ENTno' : ent_value,
                    'retx' : retx_value,
                    'retxc' : retxc_value
                }
            }
            logger.debug(f"[UE] Found '{KW_RLC_TX}' in line {line_number}, {txpdu_report[KW_RLC_TX]}")

            m1bufp = f"M1buf{m1buf_value}"
            entnop = f"ENTno{ent_value}"
            srnp = f"srn{srn_value}"
            sop = f"so{so_value}"
            if retx_value:
                retxcp = f"retxc{retxc_value+1}"
            else:
                retxcp = f"retxc{retxc_value}"


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
                        txpdu_report[RLC_RESEG_STR] = {
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
                        txpdu_report[RLC_SEG_REP_STR] = {
                            'timestamp': timestamp,
                            'num': num_value,
                            'ack': ack_value,
                            'tpollex': False,
                        }
                        found_RLC_SEG_REPORT = True
                        logger.debug(f"[UE] Found '{RLC_SEG_REP_STR}', '{srnp}', '{sop}', and '{retxcp}' in line {line_number-jd-1}, {txpdu_report[RLC_SEG_REP_STR]}")
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
                        txpdu_report[RLC_SEG_REP_STR] = {
                            'timestamp': timestamp,
                            'len': len_value,
                            'ack': False,
                            'tpollex': True
                        }
                        found_RLC_SEG_REPORT = True
                        logger.debug(f"[UE] Found '{RLC_SEG_POLL_STR}', '{srnp}', '{sop}', and '{retxcp}' in line {line_number-jd-1}, {txpdu_report[RLC_SEG_REP_STR]}")
                        break
                    else:
                        logger.warning(f"Did not find timestamp or len in {RLC_SEG_POLL_STR} line {jd}.")
                        continue

            if not found_RLC_SEG_REPORT:
                logger.warning(f"Could not find {RLC_SEG_REP_STR} for rlc segment with srn:{srn_value} and so:{so_value}, skipping this txpdu.")
                continue
                        
            # find mac.sdu
            # rlc.pdu--mac.sdu len128::tbs145.lcid4.fm204.sl13.M1buf998224643.M2buf998224640.ENTno191
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
                        logger.warning(f"[UE] For {KW_MAC_1}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_RLC_TX}'.")
                        break

                    logger.debug(f"[UE] Found '{KW_MAC_1}', '{m1bufp}', and '{entnop}' in line {line_number-jd-1}, len:{len_value}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")

                    txpdu_report[KW_MAC_1] = {
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

                    # # important check: only [M2buf: M2buf+M2len] must be inside [M3buf : M3buf+M3len]
                    # this is harq that can carry other drbs data
                    # m3len = len_value
                    # isInside = (m2buf_value >= m3buf_value) and ((m2buf_value + m2len) <= (m3buf_value + m3len))
                    # if (isInside):
                    #    logger.debug(f"[UE] Found '{KW_MAC_2}', '{frmp}', and '{slp}' in a line where [M2buf: M2buf+M2len] was inside [M3buf : M3buf+M3len] {line_number-jd-1}, len:{len_value}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")

            if not found_MAC_1:
                logger.warning(f"[UE] Could not find '{KW_MAC_1}', '{m1bufp}', and '{entnop}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_RLC_TX}'")
                continue
                
            # result
            txpdus.append(flatten_dict(txpdu_report))
            txpdu_id_count = txpdu_id_count + 1

    logger.info(f"Extracted {len(txpdus)} rlc txpdus on UE.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(txpdus)
    return df