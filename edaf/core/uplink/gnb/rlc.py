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

# define observation function
def was_observed_before(rlc_arr : list, test_line : str):
    KW_RLC_DC = 'rlc.decoded'
    found = False
    for item in rlc_arr:
        if (('fm'+str(item[KW_RLC_DC]['frame'])) in test_line) and (('sl'+str(item[KW_RLC_DC]['slot'])) in test_line):
            found = True
            break
    return found

def find_rlc_segments(previous_lines : RingBuffer, lines):

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    rlc_reassemblies = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # find all rlc.reassembled lines
        # rlc.decoded--rlc.reassembled len16::MRbuf1103907235.p0.si1.sn1300.so0
        KW_RLC = 'rlc.reassembled'
        if ('--'+KW_RLC in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            len_match = re.search(r'len(\d+)', line)
            mrbuf_match = re.search(r'MRbuf(\d+)\.', line)
            sn_match = re.search(r'sn(\d+)', line)
            so_match = re.search(r'so(\d+)', line)
            if len_match and timestamp_match and mrbuf_match and sn_match and so_match:
                timestamp = float(timestamp_match.group(1))
                len_value = int(len_match.group(1))
                sn_value = int(sn_match.group(1))
                so_value = int(so_match.group(1))
                mrbuf_value = mrbuf_match.group(1)
            else:
                logger.warning(f"[GNB] For {KW_RLC}, could not find timestamp, length, or MRBuf in in line {line_number-id-1}. Skipping this '{KW_RLC}'.")
                continue
            rlc_sdu = {
                'sdu_id':len(rlc_reassemblies),
                KW_RLC: {
                    'MRbuf': mrbuf_value,
                    'timestamp' : timestamp,
                    'length' : len_value,
                    'sn' : sn_value,
                    'so' : so_value
                }
            }
            logger.debug(f"[GNB] Found '{KW_RLC}' in line {line_number}, {rlc_sdu[KW_RLC]}")
        
            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # find an rlc.decoded for each rlc.reassembeled
            # mac.demuxed--rlc.decoded len120::fm918.sl3.lcid4.hqpid8.Hbuf1523790496.MRbuf1523790499.rnticd6e
            mrbufstr = 'MRbuf'+mrbuf_value
            found_RLC_DC = False
            KW_RLC_DC = 'rlc.decoded'
            for jd,prev_ljne in enumerate(prev_lines):
                if ('--'+KW_RLC_DC in prev_ljne) and (mrbufstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    len_match = re.search(r'len(\d+)', prev_ljne)
                    fm_match = re.search(r'fm(\d+)', prev_ljne)
                    sl_match = re.search(r'sl(\d+)', prev_ljne)
                    lcid_match = re.search(r'lcid(\d+)', prev_ljne)
                    hqpid_match = re.search(r'hqpid(\d+)', prev_ljne)
                    hbuf_match = re.search(r'Hbuf(\d+)', prev_ljne)
                    rnti_match = re.search(r'rnti([0-9a-fA-F]+)', prev_ljne)
                    if len_match and timestamp_match and fm_match and sl_match and lcid_match and hqpid_match and hbuf_match and rnti_match:
                        timestamp = float(timestamp_match.group(1))
                        len_value = int(len_match.group(1))
                        fm_value = int(fm_match.group(1))
                        sl_value = int(sl_match.group(1))
                        lcid_value = int(lcid_match.group(1))
                        hqpid_value = int(hqpid_match.group(1))
                        hbuf_value = int(hbuf_match.group(1))
                        rnti_value = rnti_match.group(1)
                    else:
                        logger.warning(f"[GNB] For {KW_RLC_DC}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_RLC}' journey")
                        break
                    rlc_sdu[KW_RLC_DC] = {
                        'lcid': lcid_value,
                        'hqpid': hqpid_value,
                        'frame': fm_value,
                        'slot': sl_value,
                        'timestamp' : timestamp,
                        'length' : len_value,
                        'rnti' : rnti_value,
                        'Hbuf' : hbuf_value
                    }
                    logger.debug(f"[GNB] Found '{KW_RLC_DC}' and '{mrbufstr}' in line {line_number-jd-1}, {rlc_sdu[KW_RLC_DC]}")
                    found_RLC_DC = True
                    break

            if not found_RLC_DC:
                logger.warning(f"[GNB] Could not find '{KW_RLC_DC}' and '{mrbufstr}' in {len(prev_lines)} lines before {line_number}. Skipping this '{rlc_sdu[KW_RLC]}' journey")
                rlc_sdu[KW_RLC_DC] = {
                    'lcid': None,
                    'hqpid': None,
                    'frame': None,
                    'slot': None,
                    'timestamp' : None,
                    'length' : None,
                    'rnti' : None,
                    'Hbuf' : None
                }

            rlc_reassemblies.append(flatten_dict(rlc_sdu))

    logger.info(f"Extracted {len(rlc_reassemblies)} rlc segments on GNB.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(rlc_reassemblies)
    return df

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

    # convert it to a list and flatten it
    final_reports_list = []
    for sn, reps_list in new_reports_list.items():
        final_reports_list.append(flatten_dict({'sn':sn, 'ans_len':len(reps_list), 'ans':reps_list}))
            
    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(final_reports_list)
    return df