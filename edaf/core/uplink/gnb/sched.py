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

def find_sched_events(previous_lines : RingBuffer, lines):   

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    sched_reports = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # decode scheduling process for this segment
        # sched.ue rntif58e.tbs88.rbs7.mcs20.fm206.sl1.fmtx206.sltx7
        # there is also the possibility of getting schedules for retransmission:
        # sched.ue retx.rntib79f.tbs69.rbs6.mcs19.fm389.sl12.fmtx389.sltx18.hqpid9.hqround1
        SCHED_UE_STR = 'sched.ue'
        SCHED_CAUSE_STR = 'sched.cause'
        if (SCHED_UE_STR in line and (not 'cause' in line)):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            rnti_match = re.search(r'rnti([0-9a-fA-F]+)', line)
            tbs_match = re.search(r'tbs(\d+)', line)
            rbs_match = re.search(r'rbs(\d+)', line)
            mcs_match = re.search(r'mcs(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            fmtx_match = re.search(r'fmtx(\d+)', line)
            sltx_match = re.search(r'sltx(\d+)', line)
            is_retx = False
            if 'retx' in line:
                is_retx = True
                hqpid_match = re.search(r'hqpid(\d+)', line)
                hqround_match = re.search(r'hqround(\d+)', line)
            if timestamp_match and tbs_match and rbs_match and mcs_match and fm_match and sl_match and fmtx_match and sltx_match and rnti_match:
                rnti_value = rnti_match.group(1)
                timestamp = float(timestamp_match.group(1))
                tbs_value = int(tbs_match.group(1))
                rbs_value = int(rbs_match.group(1))
                mcs_value = int(mcs_match.group(1))
                fmtx_value = int(fmtx_match.group(1))
                sltx_value = int(sltx_match.group(1))
                sl_sched_value = int(sl_match.group(1))
                fm_sched_value = int(fm_match.group(1))
                hqround_value = None
                hqpid_value = None
                if is_retx:
                    if hqpid_match and hqround_match:
                        hqpid_value = int(hqpid_match.group(1))
                        hqround_value = int(hqround_match.group(1))
            else:
                logger.warning(f"[GNB] for {SCHED_UE_STR}, could not find properties in line {line_number}. Skipping the schedulling parts")
                continue

            if is_retx:
                sched_report = {
                    SCHED_UE_STR : {
                        'rnti' : rnti_value,
                        'frame':fm_sched_value,
                        'slot':sl_sched_value,
                        'frametx' : fmtx_value,
                        'slottx' : sltx_value,
                        'tbs': tbs_value,
                        'mcs': mcs_value,
                        'timestamp' : timestamp,
                        'rbs' : rbs_value,
                    },
                    SCHED_CAUSE_STR : {
                        'type': 4, # type_value = 4
                        'hqround':hqround_value, 
                        'hqpid':hqpid_value
                    }
                }
            else:
                sched_report = {
                    SCHED_UE_STR : {
                        'rnti' : rnti_value,
                        'frame':fm_sched_value,
                        'slot':sl_sched_value,
                        'frametx' : fmtx_value,
                        'slottx' : sltx_value,
                        'tbs': tbs_value,
                        'mcs': mcs_value,
                        'timestamp' : timestamp,
                        'rbs' : rbs_value,
                    },
                    SCHED_CAUSE_STR : {}
                }
            logger.debug(f"[GNB] Found '{SCHED_UE_STR}' in line {line_number}, {sched_report[SCHED_UE_STR]}")

            if not is_retx:
                # lets go back in lines
                prev_lines = previous_lines.reverse_items()

                # decode the cause of scheduling for this segment
                # sched.cause--sched.ue rntif58e.type1.buf142.sched63.fm206.sl1.fmtx206.sltx7
                # sched.cause--sched.ue rntif58e.type2.fm124.sl1.fmtx124.sltx7
                # sched.cause--sched.ue rntif58e.type3.fm148.sl2.fmtx148.sltx8.diff200
                # This will give us 3 types of causes that we discover later
                found_SCHED_CAUSE = False
                for jd,prev_ljne in enumerate(prev_lines):
                    if (SCHED_CAUSE_STR in prev_ljne) and (f'fmtx{fmtx_value}' in prev_ljne) and (f'sltx{sltx_value}' in prev_ljne) and (f'rnti{rnti_value}' in prev_ljne):
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
                            sched_report[SCHED_CAUSE_STR] = {
                                'type': type_value,
                                'frame':fm_cause_value,
                                'slot':sl_cause_value,
                                'buf': buf_value,
                                'sched': sched_value,
                                'timestamp' : timestamp,
                            }
                        elif type_value == 2:
                            sched_report[SCHED_CAUSE_STR] = {
                                'type': type_value,
                                'frame':fm_cause_value,
                                'slot':sl_cause_value,
                                'timestamp' : timestamp,
                            }
                        elif type_value == 3:
                            sched_report[SCHED_CAUSE_STR] = {
                                'type': type_value,
                                'frame':fm_cause_value,
                                'slot':sl_cause_value,
                                'diff':diff_value,
                                'timestamp' : timestamp,
                            }

                        logger.debug(f"[GNB] found '{SCHED_CAUSE_STR}', sltx{sltx_value}, fmtx{fmtx_value}, and rnti{rnti_value} in line {line_number-jd-1}, {sched_report[SCHED_CAUSE_STR]}")
                        found_SCHED_CAUSE = True
                        break

            if not found_SCHED_CAUSE and not is_retx:
                logger.warning(f"[GNB] Could not find '{SCHED_CAUSE_STR}', sltx{sltx_value}, fmtx{fmtx_value}, and rnti{rnti_value} in {MAX_DEPTH} lines before {line_number}, Skipping the schedulling parts of {sched_report[SCHED_UE_STR]}")

            sched_reports.append(flatten_dict(sched_report))
    
    logger.info(f"Extracted {len(sched_reports)} schedulling reports on GNB.")
    #Convert the list of dicts to a DataFrame
    df = pd.DataFrame(sched_reports)
    return df
    


def find_sched_maps(previous_lines : RingBuffer, lines):   

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    sched_maps = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # decode scheduling maps before and after ulsch scheduling
        # sched.map.pr fm56.sl2.fmtx56.sltx8.bsi106.bst0.i0m4294967295.i1m4294967295.i2m4294967295.i3m1023
        # sched.map.po fm56.sl2.fmtx56.sltx8.sb10.ss3.i0m4294967264.i1m4294967295.i2m4294967295.i3m1023

        # first we look for 'sched.map.po'
        # sched.map.po fm56.sl2.fmtx56.sltx8.sb10.ss3.i0m4294967264.i1m4294967295.i2m4294967295.i3m1023
        SCHED_MPO_STR = 'sched.map.po'
        if (SCHED_MPO_STR in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            sb_match = re.search(r'sb(\d+)', line)
            ss_match = re.search(r'ss(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            fmtx_match = re.search(r'fmtx(\d+)', line)
            sltx_match = re.search(r'sltx(\d+)', line)
            if timestamp_match and sb_match and ss_match and fm_match and sl_match and fmtx_match and sltx_match:
                timestamp = float(timestamp_match.group(1))
                sb_value = int(sb_match.group(1))
                ss_value = int(ss_match.group(1))
                fmtx_value = int(fmtx_match.group(1))
                sltx_value = int(sltx_match.group(1))
                sl_value = int(sl_match.group(1))
                fm_value = int(fm_match.group(1))
                i0m_value = int(re.search(r'i0m(\d+)', line).group(1))
                i1m_value = int(re.search(r'i1m(\d+)', line).group(1))
                i2m_value = int(re.search(r'i2m(\d+)', line).group(1))
                i3m_value = int(re.search(r'i3m(\d+)', line).group(1))
            else:
                logger.warning(f"[GNB] for {SCHED_MPO_STR}, could not find properties in line {line_number}. Skipping this schedulling map")
                continue

            sched_map = {
                SCHED_MPO_STR : {
                    'frame':fm_value,
                    'slot':sl_value,
                    'frametx' : fmtx_value,
                    'slottx' : sltx_value,
                    'sb': sb_value,
                    'ss': ss_value,
                    'i0m' : i0m_value,
                    'i1m' : i1m_value,
                    'i2m' : i2m_value,
                    'i3m' : i3m_value,
                }
            }
            logger.debug(f"[GNB] Found '{SCHED_MPO_STR}' in line {line_number}, {sched_map[SCHED_MPO_STR]}")


            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # decode the prior state resource blocks
            # sched.map.pr fm56.sl2.fmtx56.sltx8.bsi106.bst0.i0m4294967295.i1m4294967295.i2m4294967295.i3m1023
            found_SCHED_PRIOR = False
            SCHED_PRIOR_STR = 'sched.map.pr'
            for jd,prev_ljne in enumerate(prev_lines):
                if (SCHED_PRIOR_STR in prev_ljne) and (f'fmtx{fmtx_value}' in prev_ljne) and (f'sltx{sltx_value}' in prev_ljne) and (f'fm{fm_value}' in prev_ljne) and (f'sl{sl_value}' in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    bsi_match = re.search(r'bsi(\d+)', prev_ljne)
                    bst_match = re.search(r'bst(\d+)', prev_ljne)
                    if timestamp_match and bsi_match and bst_match:
                        timestamp = float(timestamp_match.group(1))
                        bsi_value = int(bsi_match.group(1))
                        bst_value = int(bst_match.group(1))
                        i0m_value = int(re.search(r'i0m(\d+)', prev_ljne).group(1))
                        i1m_value = int(re.search(r'i1m(\d+)', prev_ljne).group(1))
                        i2m_value = int(re.search(r'i2m(\d+)', prev_ljne).group(1))
                        i3m_value = int(re.search(r'i3m(\d+)', prev_ljne).group(1))
                    else:
                        logger.warning(f"[GNB] for {SCHED_PRIOR_STR}, could not find properties in line {line_number}. Skipping this schedulling map")
                        continue

                    sched_map[SCHED_PRIOR_STR] = {
                        'timestamp' : timestamp,
                        'bsi': bsi_value,
                        'bst': bst_value,
                        'i0m' : i0m_value,
                        'i1m' : i1m_value,
                        'i2m' : i2m_value,
                        'i3m' : i3m_value,
                    }
                    logger.debug(f"[GNB] Found '{SCHED_PRIOR_STR}' in line {line_number-jd-1}, {sched_map[SCHED_PRIOR_STR]}")
                    found_SCHED_PRIOR = True
                    break

            if not found_SCHED_PRIOR:
                logger.warning(f"[GNB] Could not find '{SCHED_PRIOR_STR} for {sched_map[SCHED_MPO_STR]} ")
                sched_map[SCHED_PRIOR_STR] = {}

            sched_maps.append(flatten_dict(sched_map))
    
    logger.info(f"Extracted {len(sched_maps)} schedulling maps on GNB.")
    #Convert the list of dicts to a DataFrame
    df = pd.DataFrame(sched_maps)
    return df
    


def find_mcs_reports(lines):

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    mcs_reports = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        
        # decode mcs assignment reports
        # mcs.ul rnti0545.mcs15.fm530.sl17.fmtx531.sltx3
        MCS_REP_STR = 'mcs.ul'
        if (MCS_REP_STR in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            rnti_match = re.search(r'rnti([0-9a-fA-F]+)', line)
            mcs_match = re.search(r'mcs(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            fmtx_match = re.search(r'fmtx(\d+)', line)
            sltx_match = re.search(r'sltx(\d+)', line)
            if timestamp_match and rnti_match and mcs_match and fm_match and sl_match and fmtx_match and sltx_match:
                timestamp = float(timestamp_match.group(1))
                rnti_value = rnti_match.group(1)
                mcs_value = int(mcs_match.group(1))
                fmtx_value = int(fmtx_match.group(1))
                sltx_value = int(sltx_match.group(1))
                sl_value = int(sl_match.group(1))
                fm_value = int(fm_match.group(1))
            else:
                logger.warning(f"[GNB] for {MCS_REP_STR}, could not find properties in line {line_number}. Skipping this UL mcs assignment")
                continue
            mcs_rep = {
                'timestamp' : timestamp,
                'frame':fm_value,
                'slot':sl_value,
                'frametx' : fmtx_value,
                'slottx' : sltx_value,
                'rnti': rnti_value,
                'mcs': mcs_value,
            }
            mcs_reports.append(flatten_dict(mcs_rep))
            logger.debug(f"[GNB] Found '{MCS_REP_STR}' in line {line_number}, {mcs_rep}")

    logger.info(f"Extracted {len(mcs_reports)} UL mcs assignments on GNB.")
    #Convert the list of dicts to a DataFrame
    df = pd.DataFrame(mcs_reports)
    return df