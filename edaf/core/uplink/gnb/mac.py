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

def find_mac_successful_attempts(previous_lines : RingBuffer, lines):

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    mac_attempts = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # we are looking for mac block exchenge attempts,
        # it can be a successful decoded:
        # phy.detectstart ::fm102.sl8.hqpid15.hqround0.Hbuf1527077280
        # phy.detectend suc1.fm102.sl8.hqpid15.hqround0.Hbuf1527077280.ptot559.pn284.pth50
        # phy.decodeend fm102.sl8.hqpid15.hqround0.Hbuf1527077280.rbb0.rbs5.tbs24

        # or it can be unsuccessful decode:
        # phy.detectstart ::fm918.sl8.hqpid9.hqround0.Hbuf1527077280
        # phy.detectend suc0.fm918.sl8.hqpid9.hqround0.Hbuf1527077280.ptot277.pn277.pth50

        # therefore we first look for 'phy.decodeend'
        # phy.decodeend fm102.sl8.hqpid15.hqround0.Hbuf1527077280.rbb0.rbs5.tbs24
        # if suc1, we look for mac.demuxed. otherwise we dont
        KW_MAC_DEC = 'phy.decodeend'
        if (KW_MAC_DEC in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            hqpid_match = re.search(r'hqpid(\d+)', line)
            hqround_match = re.search(r'hqround(\d+)', line)
            hbuf_match = re.search(r'Hbuf(\d+)', line)
            rbb_match = re.search(r'rbb(\d+)', line)
            rbs_match = re.search(r'rbs(\d+)', line)
            tbs_match = re.search(r'tbs(\d+)', line)
            mcs_match = re.search(r'mcs(\d+)', line)
            sb_match = re.search(r'sb(\d+)', line)
            ss_match = re.search(r'ss(\d+)', line)
            if timestamp_match and fm_match and sl_match and hqpid_match and hqround_match and hbuf_match and rbb_match and rbs_match and tbs_match and mcs_match and sb_match and ss_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                hqpid_value = int(hqpid_match.group(1))
                hqround_value = int(hqround_match.group(1))
                hbuf_value = int(hbuf_match.group(1))
                rbb_value = int(rbb_match.group(1))
                rbs_value = int(rbs_match.group(1))
                tbs_value = int(tbs_match.group(1))
                mcs_value = int(mcs_match.group(1))
                sb_value = int(sb_match.group(1))
                ss_value = int(ss_match.group(1))
            else:
                logger.warning(f"For {KW_MAC_DEC}, could not find properties in line {line_number}.")
                continue

            mac_dec_arr = {
                KW_MAC_DEC: {
                    'timestamp': timestamp,
                    'rbb': rbb_value,
                    'rbs': rbs_value,
                    'tbs': tbs_value,
                    'mcs': mcs_value,
                    'sb' : sb_value,
                    'ss' : ss_value
                }
            }
            logger.debug(f"Found '{KW_MAC_DEC}' in line {line_number}, {mac_dec_arr[KW_MAC_DEC]}")

            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # find 'phy.detectend' with suc1
            # phy.detectend suc1.fm102.sl8.hqpid15.hqround0.Hbuf1527077280.ptot559.pn284.pth50
            hbufstr = f'Hbuf{hbuf_value}'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            found_MAC_DETEND = False
            KW_MAC_DETEND = 'phy.detectend'
            for jd,prev_ljne in enumerate(prev_lines):
                if (KW_MAC_DETEND in prev_ljne) and (hbufstr in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    suc_match = re.search(r'suc(\d+)', prev_ljne)
                    ptot_match = re.search(r'ptot(\d+)', prev_ljne)
                    pn_match = re.search(r'pn(\d+)', prev_ljne)
                    pth_match = re.search(r'pth(\d+)', prev_ljne)
                    if timestamp_match and ptot_match and pn_match and pth_match and suc_match:
                        timestamp = float(timestamp_match.group(1))
                        suc_value = int(suc_match.group(1))
                        ptot_value = int(ptot_match.group(1))
                        pn_value = int(pn_match.group(1))
                        pth_value = int(pth_match.group(1))
                    else:
                        logger.warning(f"[GNB] For {KW_MAC_DETEND}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                        break
                    mac_dec_arr[KW_MAC_DETEND] = {
                        'timestamp' : timestamp,
                        'frame': fm_value,
                        'slot': sl_value,
                        'hqpid': hqpid_value,
                        'hqround': hqround_value,
                        'hbuf': hbuf_value,
                        'ptot' : ptot_value,
                        'pn' : pn_value,
                        'pth' : pth_value,
                        'suc' : suc_value,
                    }
                    logger.debug(f"[GNB] Found '{KW_MAC_DETEND}','{hbufstr}','{fmstr}','{slstr}' in line {line_number-jd-1}, {mac_dec_arr[KW_MAC_DETEND]}")
                    found_MAC_DETEND = True
                    break

            if not found_MAC_DETEND:
                logger.warning(f"[GNB] Could not find '{KW_MAC_DETEND}' before {line_number} for {KW_MAC_DEC}")
                continue


            # find 'phy.detectstart'
            hbufstr = f'Hbuf{hbuf_value}'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            found_MAC_DETSTART = False
            KW_MAC_DETSTART = 'phy.detectstart'
            for jd,prev_ljne in enumerate(prev_lines):
                if (KW_MAC_DETSTART in prev_ljne) and (hbufstr in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    if timestamp_match:
                        timestamp = float(timestamp_match.group(1))
                    else:
                        logger.warning(f"[GNB] For {KW_MAC_DETSTART}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                        break
                    mac_dec_arr[KW_MAC_DETSTART] = {
                        'timestamp' : timestamp,
                    }
                    logger.debug(f"[GNB] Found '{KW_MAC_DETSTART}' in line {line_number-jd-1}, {mac_dec_arr[KW_MAC_DETSTART]}")
                    found_MAC_DETSTART = True
                    break

            if not found_MAC_DETSTART:
                logger.warning(f"[GNB] Could not find '{KW_MAC_DETSTART}' before {line_number} for {KW_MAC_DEC}")
                mac_dec_arr[KW_MAC_DETSTART] = {
                    'timestamp' : None,
                }

            mac_attempts.append(flatten_dict(mac_dec_arr))

    logger.info(f"Extracted {len(mac_attempts)} successful mac attempts on GNB.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(mac_attempts)
    return df


def find_mac_failed_attempts(previous_lines : RingBuffer, lines):
    
    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    mac_attempts = []
    for line_number, line in enumerate(lines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        KW_MAC_DEC = 'phy.decodeend'

        # find 'phy.detectend' with 'suc0'
        # phy.detectend suc0.fm918.sl8.hqpid9.hqround0.Hbuf1527077280.ptot277.pn277.pth50
        KW_MAC_DETEND = 'phy.detectend'
        if (KW_MAC_DETEND in line) and ('suc0' in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            ptot_match = re.search(r'ptot(\d+)', line)
            pn_match = re.search(r'pn(\d+)', line)
            pth_match = re.search(r'pth(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            hqpid_match = re.search(r'hqpid(\d+)', line)
            hqround_match = re.search(r'hqround(\d+)', line)
            hbuf_match = re.search(r'Hbuf(\d+)', line)
            if timestamp_match and ptot_match and pn_match and pth_match and fm_match and sl_match and hqpid_match and hqround_match and hbuf_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                hqpid_value = int(hqpid_match.group(1))
                hqround_value = int(hqround_match.group(1))
                hbuf_value = int(hbuf_match.group(1))
                ptot_value = int(ptot_match.group(1))
                pn_value = int(pn_match.group(1))
                pth_value = int(pth_match.group(1))
            else:
                logger.warning(f"[GNB] For {KW_MAC_DETEND}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                continue
            mac_dec_arr = {
                KW_MAC_DEC : {
                    'timestamp': None,
                    'rbb': None,
                    'rbs': None,
                    'tbs': None,
                    'mcs': None,
                    'sb' : None,
                    'ss' : None
                },
                KW_MAC_DETEND : {
                    'timestamp' : timestamp,
                    'frame': fm_value,
                    'slot': sl_value,
                    'hqpid': hqpid_value,
                    'hqround': hqround_value,
                    'hbuf': hbuf_value,
                    'ptot' : ptot_value,
                    'pn' : pn_value,
                    'pth' : pth_value,
                    'suc' : 0,
                }
            }
            logger.debug(f"[GNB] Found '{KW_MAC_DETEND}' and suc0 in line {line_number}, {mac_dec_arr[KW_MAC_DETEND]}")


            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # find 'phy.detectstart'
            # phy.detectstart ::fm918.sl8.hqpid9.hqround0.Hbuf1527077280
            hbufstr = f'Hbuf{hbuf_value}'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            found_MAC_DETSTART = False
            KW_MAC_DETSTART = 'phy.detectstart'
            for jd,prev_ljne in enumerate(prev_lines):
                if (KW_MAC_DETSTART in prev_ljne) and (hbufstr in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    if timestamp_match:
                        timestamp = float(timestamp_match.group(1))
                    else:
                        logger.warning(f"[GNB] For {KW_MAC_DETSTART}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_MAC_DEC}'")
                        break
                    mac_dec_arr[KW_MAC_DETSTART] = {
                        'timestamp' : timestamp,
                    }
                    logger.debug(f"[GNB] Found '{KW_MAC_DETSTART}' in line {line_number-jd-1}, {mac_dec_arr[KW_MAC_DETSTART]}")
                    found_MAC_DETSTART = True
                    break

            if not found_MAC_DETSTART:
                logger.warning(f"[GNB] Could not find '{KW_MAC_DETSTART}' before {line_number} for {KW_MAC_DEC}")
                mac_dec_arr[KW_MAC_DETSTART] = {
                    'timestamp' : None,
                }
                continue

            mac_attempts.append(flatten_dict(mac_dec_arr))

    logger.info(f"Extracted {len(mac_attempts)} failed mac attempts on GNB.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(mac_attempts)
    return df

