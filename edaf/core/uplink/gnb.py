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


# go back a few lines, find the first line that includes
KW_SDAP = 'sdap.sdu'
# and 'snXX'.

# go back more lines, find the first line that includes
KW_PDCP = 'pdcp.decoded'

# go back more lines, find the first line that includes
KW_PDCPIND = 'pdcp.ind'

KW_MAC_DEC = 'mac.decoded' # this is the key to recognize the retransmissions

# maximum number of lines to check
MAX_DEPTH = 500


def find_rlc_reports(lines):
    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    rlc_reports = {} # a hash table with report number as the key
    for line_number, line in enumerate(lines): 
        # look for the rlc reports
        KW_RLC_REPORT = 'rlc.report'
        if KW_RLC_REPORT in line:
            # skip TX side rlc.report
            if 'R2buf' in line:
                continue
            # look for the rlc segments that got nacked or dropped
            # case 1.e1) 
            # rlc.report--rlc.nacked num127.sn494.sos30.soe65535.range1
            # if soe is 65535, it is actually 0xFFFF, which means the whole packet is NACKED
            # case 1.e2)
            # rlc.report--rlc.nacked num2.sn1.sos14.soe63.range1
            # case 1.e3)
            # rlc.report--rlc.nacked num876.sn2560.sos26.soe7.range2
            # On UE side, this was translated into these two segments: (len105.srn2560.so26) (len8.srn2561.so0)
            KW_RLC_NACK = 'rlc.nacked'
            KW_RLC_ACK = 'rlc.acked'
            if KW_RLC_NACK in line:
                line = line.replace('\n', '')
                timestamp_match = re.search(r'^(\d+\.\d+)', line)
                num_match = re.search(r'num(\d+)', line)
                sn_match = re.search(r'sn(\d+)', line)
                sos_match = re.search(r'sos(\d+)', line)
                soe_match = re.search(r'soe(\d+)', line)
                range_match = re.search(r'range(\d+)', line)
                if num_match and timestamp_match and sn_match and sos_match and soe_match and range_match:
                    timestamp = float(timestamp_match.group(1))
                    num_value = int(num_match.group(1))
                    sn_value = int(sn_match.group(1))
                    sos_value = int(sos_match.group(1))
                    soe_value = int(soe_match.group(1))
                    range_value = int(range_match.group(1))
                else:
                    logger.warning(f"[GNB] For {KW_RLC_NACK}, could not find properties on line {line_number}. Skipping this '{KW_RLC_NACK}' journey")
                    continue
                
                rlc_nack_dict = {
                    'timestamp' : timestamp,
                    'sn' : sn_value,
                    'sos' : sos_value,
                    'soe' : soe_value,
                    'range' : range_value,
                    'discard' : {}
                }
                logger.debug(f"[GNB] Found '{KW_RLC_NACK}' in line {line_number}, num:{num_value}, {rlc_nack_dict}")
                
                # check if there was an RLC discard event associated with this NACK
                # rlc.discarded len14::type5.MRbuf3980642723.p0.si3.sn2441.so30.dc1
                # TODO
                if num_value not in rlc_reports:
                    rlc_reports[num_value] = {}
                rlc_reports[num_value]['nack'] = rlc_nack_dict

            elif KW_RLC_ACK in line:
                line = line.replace('\n', '')
                timestamp_match = re.search(r'^(\d+\.\d+)', line)
                num_match = re.search(r'num(\d+)', line)
                sn_match = re.search(r'sn(\d+)', line)
                if num_match and timestamp_match and sn_match:
                    timestamp = float(timestamp_match.group(1))
                    num_value = int(num_match.group(1))
                    sn_value = int(sn_match.group(1))
                else:
                    logger.warning(f"[GNB] For {KW_RLC_ACK}, could not find properties on line {line_number}. Skipping this '{KW_RLC_ACK}' journey")
                    continue
                rlc_ack_dict = {
                    'timestamp' : timestamp,
                    'sn' : sn_value
                }
                logger.debug(f"[GNB] Found '{KW_RLC_ACK}' in line {line_number}, num:{num_value}, {rlc_ack_dict}")
                if num_value not in rlc_reports:
                    rlc_reports[num_value] = {}
                rlc_reports[num_value]['ack'] = rlc_ack_dict

    logger.info(f"Extracted {max(rlc_reports.keys())} rlc reports.")
    # sort the reports
    rlc_reports = dict(sorted(rlc_reports.items()))

    # in for each item, that the ack and nacks timestamps are not far
    new_reports = {}
    brokens = []
    for num in rlc_reports.keys():
        rep = rlc_reports[num]
        if 'nack' in rep:
            if abs(rep['ack']['timestamp'] - rep['nack']['timestamp']) > 0.0001: #100us
                logger.warning(f"RLC report number {num} removed due to timestamp mismatch, skipping num {num}.")
                brokens.append(num)
                continue

        # find previous report
        num_prev = num - 1
        while num_prev not in rlc_reports:
            num_prev = num_prev - 1
            if num_prev < min(rlc_reports.keys()):
                break
        if num_prev in rlc_reports:
            prev_rep = rlc_reports[num-1]
        else:
            num_prev = None
            prev_rep = None
        
        # latest nack_sn always indicates what is missing
        # the rest cannot be confirmed
        # therefore ack_sn is always latest nack_sn+1
        if 'nack' in rep:
            sn_begin = rep['nack']['sn']
            sn_end = rep['nack']['sn'] + rep['nack']['range']-1
            for sn in range(sn_begin, sn_end+1):
                if sn not in new_reports:
                    new_reports[sn] = {}
                if num not in new_reports[sn]:
                    new_reports[sn][num] =  { 'ack' : False,  'nack': {}, 'timestamp': rep['ack']['timestamp'] }
                if sn == sn_begin and sn == sn_end:
                    new_reports[sn][num]['nack'] = {
                        'sos' : rep['nack']['sos'],
                        'soe' : rep['nack']['soe'],
                    }
                elif sn == sn_begin and sn != sn_end:
                    new_reports[sn][num]['nack'] = {
                        'sos' : rep['nack']['sos'],
                        'soe' : 65535,
                    }
                elif sn == sn_end and sn != sn_begin:
                    new_reports[sn][num]['nack'] = {
                        'sos' : 0,
                        'soe' : rep['nack']['soe'],
                    }
                else:
                    # middle sn (full nack)
                    new_reports[sn][num]['nack'] = {
                        'sos' : 0,
                        'soe' : 65535,
                    }

        if not prev_rep:
            if 'nack' not in rep:
                if rep['ack']['sn']-1 >= 0:
                    sn = rep['ack']['sn']-1
                    if sn not in new_reports:
                        new_reports[sn] = {}
                    if num not in new_reports[sn]:
                        new_reports[sn][num] =  { 'ack' : {},  'nack': {}, 'timestamp': rep['ack']['timestamp'] }
                    if not new_reports[sn][num]['nack']:
                        new_reports[sn][num]['ack'] = { 
                            'sos' : 0,
                            'soe' : 65535,
                        }
                    else:
                        if new_reports[sn][num]['nack']['sos'] == 0:
                            new_reports[sn][num]['ack'] = {}
                        else:
                            new_reports[sn][num]['ack'] = { 
                                'sos' : 0,
                                'soe' : new_reports[sn][num]['nack']['sos'],
                            }
                else:
                    # does not mean anything
                    pass
            else:
                # it is taken care before in NACK
                pass
        else:
            if rep['ack']['sn'] < prev_rep['ack']['sn']:
                logger.warning(f"status reports corrupt, report {rep}, skipping num {num}, prev num {num_prev}, prev rep {prev_rep}.")
                brokens.append(num)
                continue
            else:
                # for sure rep['ack']['sn'] > prev_rep['ack']['sn']
                # we calculate acks with the rest 
                sn_end = rep['ack']['sn'] - 1
                if 'nack' in prev_rep:
                    sn_begin = prev_rep['nack']['sn']
                else:
                    if ('nack' in rep) and (rep['ack']['sn'] == prev_rep['ack']['sn']):
                        logger.warning(f"status reports corrupt, report {rep}, skipping num {num}, prev num {num_prev}, prev rep {prev_rep}.")
                        brokens.append(num)
                        continue
                    if ('nack' not in rep) and (rep['ack']['sn'] == prev_rep['ack']['sn']):
                        sn_begin = sn_end
                    else:
                        sn_begin = prev_rep['ack']['sn']
                for sn in range(sn_begin, sn_end+1):  
                    if sn not in new_reports:
                        new_reports[sn] = {}
                    if num not in new_reports[sn]:
                        new_reports[sn][num] =  { 'ack' : False,  'nack': {}, 'timestamp': rep['ack']['timestamp'] }
                    if not new_reports[sn][num]['nack']:
                        new_reports[sn][num]['ack'] = { 
                            'sos' : 0,
                            'soe' : 65535,
                        }
                    else:
                        if new_reports[sn][num]['nack']['sos'] == 0:
                            new_reports[sn][num]['ack'] = {}
                        else:
                            new_reports[sn][num]['ack'] = { 
                                'sos' : 0,
                                'soe' : new_reports[sn][num]['nack']['sos'],
                            }

    # another pass to convert new_reports[sn][num] to just lists in new_reports[sn]
    new_reports_list = {}
    for sn, sn_item in new_reports.items():
        new_reports_list[sn] = []
        for num, rep in sn_item.items():
            new_reports_list[sn].append({ 'num':num,  **rep } )

    return rlc_reports, new_reports_list

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

            logger.debug(f"Found '{KW_MAC_DEC}' and '{hqstr}' in line {line_number-jd-1}, timestamp: {timestamp}, frame: {fm_value}, slot: {sl_value}")
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
        self.unfinished_rlc_segments = list()

    def run(self, lines):
        
        # sweep for rlc reports
        rlc_reports_num, rlc_reports_sn = find_rlc_reports(lines)

        #lines = sorted(unsortedlines, key=sort_key, reverse=False)
        journeys = []
        ip_packets_counter = 0
        for line_number, line in enumerate(lines):
            self.previous_lines.append(line)

            # look for the ip packets
            # sdap.sdu--gtp.out len128::SBuf805311360.sn2
            # as it indicates one packet delivery in uplink on gnb
            KW_R = 'gtp.out'
            if KW_R in line:
                line = line.replace('\n', '')
            
                # Use regular expressions to extract the numbers
                timestamp_match = re.search(r'^(\d+\.\d+)', line)
                len_match = re.search(r'len(\d+)', line)
                sbuf_match = re.search(r'SBuf(\d+)', line)
                sn_match = re.search(r'sn(\d+)', line)
                if len_match and sbuf_match and timestamp_match and sn_match:
                    timestamp = float(timestamp_match.group(1))
                    len_value = int(len_match.group(1))
                    sbuf_value = sbuf_match.group(1)
                    sn_value = int(sn_match.group(1))
                    logger.debug(f"[GNB] Found '{KW_R}' in line {line_number}, len:{len_value}, SBuf: {sbuf_value}, ts: {timestamp}, sn: {sn_value}")
                    journey = {
                        KW_R : {
                            'timestamp' : timestamp,
                            'length' : len_value,
                            'SBuf' : sbuf_value,
                            'sn' : sn_value
                        }
                    }
                    snp = f"sn{sn_value}"
                    sbufp = f"SBuf{sbuf_value}"

                    # lets go back in lines
                    prev_lines = self.previous_lines.reverse_items()

                    # check for KW_SDAP
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

                            logger.debug(f"[GNB] Found '{KW_SDAP}','{sbufp}', and '{snp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}")
                            journey[KW_SDAP] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'PBuf' : pbuf_value,
                            }
                            pbufp = f"PBuf{pbuf_value}"
                            found_KW_SDAP = True
                            break

                    if not found_KW_SDAP:
                        logger.warning(f"[GNB] Could not find '{KW_SDAP}' and '{sbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue

                    # check for KW_PDCP
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

                            logger.debug(f"[GNB] Found '{KW_PDCP}', '{pbufp}', and '{snp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, sn: {sn_value}")
                            journey[KW_PDCP] = {
                                'timestamp' : timestamp,
                                'length' : len_value,
                                'PIBuf' : pibuf_value,
                            }
                            pibufp = f"PIBuf{pibuf_value}"
                            found_KW_PDCP = True
                            break
                    
                    if not found_KW_PDCP:
                        logger.warning(f"[GNB] Could not find '{KW_PDCP}' and '{pbufp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue

                    # check for KW_PDCPIND
                    # This is tricky. We may find multiple lines we have to keep the one with smaller snp
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
                            if sn_value in rlc_reports_sn:
                                journey[KW_PDCPIND] = {
                                    'timestamp' : timestamp,
                                    'length' : len_value,
                                    'rlc_reports' : rlc_reports_sn[sn_value],
                                }
                            else:
                                journey[KW_PDCPIND] = {
                                    'timestamp' : timestamp,
                                    'length' : len_value,
                                    'rlc_reports' : {},
                                }
                            found_KW_PDCP = True
                            break

                    if not found_KW_PDCP:
                        logger.warning(f"[GNB] Could not find '{KW_PDCPIND}', '{pibufp}', or '{snp}' in {len(prev_lines)} lines before {line_number}. Skipping this '{KW_R}' journey")
                        continue
                    
                    # define observation function
                    def was_observed_before(rlc_arr : list, test_line : str):
                        found = False
                        for item in rlc_arr:
                            if (('fm'+str(item[KW_RLC_DC]['frame'])) in test_line) and (('sl'+str(item[KW_RLC_DC]['slot'])) in test_line):
                                found = True
                                break
                        return found

                    
                    RLC_ARR = []
                    lengths = []
                    # find all rlc.reassembled lines
                    # rlc.decoded--rlc.reassembled len16::MRbuf1103907235.p0.si1.sn1300.so0
                    KW_RLC = 'rlc.reassembled'
                    for id,prev_line in enumerate(prev_lines):
                        if ('--'+KW_RLC in prev_line) and (snp in prev_line):
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

                            logger.debug(f"[GNB] Found '{KW_RLC}' and '{snp}' in line {line_number-id-1}, len:{len_value}, timestamp: {timestamp}, MRBuf:{mrbuf_value}")
                            lengths.append(len_value)
                            rlc_reass_dict = {
                                'MRbuf': mrbuf_value,
                                'timestamp' : timestamp,
                                'length' : len_value,
                            }

                            # find an rlc.decoded for each rlc.reassembeled
                            mrbufstr = 'MRbuf'+mrbuf_value
                            found_RLC_DC = False
                            KW_RLC_DC = 'rlc.decoded'
                            for jd,prev_ljne in enumerate(prev_lines):
                                if ('--'+KW_RLC_DC in prev_ljne) and (mrbufstr in prev_ljne):
                                    if was_observed_before(RLC_ARR, prev_ljne):
                                        continue
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    len_match = re.search(r'len(\d+)', prev_ljne)
                                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                                    lcid_match = re.search(r'lcid(\d+)', prev_ljne)
                                    hqpid_match = re.search(r'hqpid(\d+)', prev_ljne)
                                    rnti_match = re.search(r'rnti([0-9a-fA-F]+)', prev_ljne)
                                    if len_match and timestamp_match and fm_match and sl_match and lcid_match and hqpid_match and rnti_match:
                                        timestamp = float(timestamp_match.group(1))
                                        len_value = int(len_match.group(1))
                                        fm_value = int(fm_match.group(1))
                                        sl_value = int(sl_match.group(1))
                                        lcid_value = int(lcid_match.group(1))
                                        hqpid_value = int(hqpid_match.group(1))
                                        rnti_value = rnti_match.group(1)
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
                                        'rnti' : rnti_value,
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
                            KW_MAC_DEM = 'mac.demuxed'
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

                            # decode scheduling process for this segment
                            # sched.ue rntif58e.tbs88.rbs7.mcs20.fm206.sl1.fmtx206.sltx7
                            frtxstr = 'fmtx' + str(fm_value)
                            sltxstr = 'sltx' + str(sl_value)
                            rntistr = 'rnti' + rnti_value
                            found_SCHED_UE = False
                            SCHED_UE_STR = 'sched.ue'
                            for jd,prev_ljne in enumerate(prev_lines):
                                if (SCHED_UE_STR in prev_ljne) and (frtxstr in prev_ljne) and (sltxstr in prev_ljne) and (rntistr in prev_ljne):
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    tbs_match = re.search(r'tbs(\d+)', prev_ljne)
                                    rbs_match = re.search(r'rbs(\d+)', prev_ljne)
                                    mcs_match = re.search(r'mcs(\d+)', prev_ljne)
                                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                                    if timestamp_match and tbs_match and rbs_match and mcs_match and fm_match and sl_match:
                                        timestamp = float(timestamp_match.group(1))
                                        tbs_value = int(tbs_match.group(1))
                                        rbs_value = int(rbs_match.group(1))
                                        mcs_value = int(mcs_match.group(1))
                                        sl_sched_value = int(sl_match.group(1))
                                        fm_sched_value = int(fm_match.group(1))
                                    else:
                                        logger.warning(f"[GNB] for {SCHED_UE_STR}, could not find properties in line {line_number-jd-1}. Skipping the schedulling parts")
                                        break

                                    logger.debug(f"[GNB] Found '{SCHED_UE_STR}', '{frtxstr}', '{sltxstr}', and '{rntistr}' in line {line_number-jd-1}, frame:{fm_sched_value}, slot: {sl_sched_value}, RBs: {rbs_value}")

                                    sched_dict = {
                                        'frame':fm_sched_value,
                                        'slot':sl_sched_value,
                                        'tbs': tbs_value,
                                        'mcs': mcs_value,
                                        'timestamp' : timestamp,
                                        'rbs' : rbs_value,
                                        'cause' : {}
                                    }

                                    found_SCHED_UE = True
                                    break

                            if not found_SCHED_UE:
                                logger.warning(f"[GNB] Could not find '{SCHED_UE_STR}', '{frtxstr}', '{sltxstr}', and '{rntistr}' in {len(prev_lines)} lines before {line_number}. Skipping the schedulling parts")
                                sched_dict = {}

                            # decode the cause of scheduling for this segment
                            # sched.cause--sched.ue rntif58e.type1.buf142.sched63.fm206.sl1.fmtx206.sltx7
                            # sched.cause--sched.ue rntif58e.type2.fm124.sl1.fmtx124.sltx7
                            # sched.cause--sched.ue rntif58e.type3.fm148.sl2.fmtx148.sltx8.diff200
                            # This will give us 3 types of causes that we discover later
                            found_SCHED_CAUSE = False
                            SCHED_CAUSE_STR = 'sched.cause'
                            for jd,prev_ljne in enumerate(prev_lines):
                                if (SCHED_CAUSE_STR in prev_ljne) and (frtxstr in prev_ljne) and (sltxstr in prev_ljne) and (rntistr in prev_ljne):
                                    property_loss = False
                                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                                    type_match = re.search(r'type(\d+)', prev_ljne)
                                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                                    if timestamp_match and type_match and fm_match and sl_match:
                                        type_value = int(type_match.group(1))
                                        timestamp = float(timestamp_match.group(1))
                                        sl_cause_value = int(sl_match.group(1))
                                        fm_cause_value = int(fm_match.group(1))
                                        if type_value == 1:
                                            buf_match = re.search(r'buf(\d+)', prev_ljne)
                                            sched_match = re.search(r'sched(\d+)', prev_ljne)
                                            if buf_match and sched_match:
                                                buf_value = int(buf_match.group(1))
                                                sched_value = int(sched_match.group(1))
                                            else:
                                                property_loss = True
                                        elif type_value == 3:
                                            diff_match = re.search(r'diff(\d+)', prev_ljne)
                                            if diff_match:
                                                diff_value = int(diff_match.group(1))
                                            else:
                                                property_loss = True
                                    else:
                                        property_loss = True

                                    if property_loss:
                                        logger.warning(f"[GNB] for {SCHED_CAUSE_STR}, could not find properties in line {line_number-jd-1}. Skipping the schedulling parts")
                                        break

                                    if type_value == 1:
                                        logger.debug(f"[GNB] found '{SCHED_CAUSE_STR}', '{frtxstr}', '{sltxstr}', and '{rntistr}' in line {line_number-jd-1}, frame:{fm_cause_value}, slot: {sl_cause_value}, type: {type_value}, buf:{buf_value}, sched:{sched_value}")

                                        sched_cause_dict = {
                                            'type': type_value,
                                            'frame':fm_cause_value,
                                            'slot':sl_cause_value,
                                            'buf': buf_value,
                                            'sched': sched_value,
                                            'timestamp' : timestamp,
                                        }
                                    elif type_value == 2:
                                        logger.debug(f"[GNB] found '{SCHED_CAUSE_STR}', '{frtxstr}', '{sltxstr}', and '{rntistr}' in line {line_number-jd-1}, frame:{fm_cause_value}, slot: {sl_cause_value}, type: {type_value}")

                                        sched_cause_dict = {
                                            'type': type_value,
                                            'frame':fm_cause_value,
                                            'slot':sl_cause_value,
                                            'timestamp' : timestamp,
                                        }
                                    elif type_value == 3:
                                        logger.debug(f"[GNB] found '{SCHED_CAUSE_STR}', '{frtxstr}', '{sltxstr}', and '{rntistr}' in line {line_number-jd-1}, frame:{fm_cause_value}, slot: {sl_cause_value}, type: {type_value}, diff: {diff_value}")

                                        sched_cause_dict = {
                                            'type': type_value,
                                            'frame':fm_cause_value,
                                            'slot':sl_cause_value,
                                            'diff':diff_value,
                                            'timestamp' : timestamp,
                                        }

                                    found_SCHED_CAUSE = True
                                    break

                            if not found_SCHED_CAUSE:
                                logger.warning(f"[GNB] Could not find '{SCHED_CAUSE_STR}', '{frtxstr}', '{sltxstr}', and '{rntistr}' in {len(prev_lines)} lines before {line_number}. Skipping the schedulling parts")
                                sched_cause_dict = {}

                            sched_dict['cause'] = sched_cause_dict
                            rlc_seg_dict = {
                                KW_RLC : rlc_reass_dict,
                                KW_RLC_DC : rlc_decode_dict,
                                KW_MAC_DEM : mac_demuxed_dict,
                                'schedule' : sched_dict
                            }

                            RLC_ARR.append(rlc_seg_dict)
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
