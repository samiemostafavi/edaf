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

def find_mac_attempts(previous_lines : RingBuffer, lines):
    revlines = list(reversed(lines))

    #lines = sorted(unsortedlines, key=sort_key, reverse=False)
    mac_attempts = []
    for line_number, line in enumerate(revlines):
        line = line.replace('\n', '')
        previous_lines.append(line)

        # we are looking for mac transmission attempts
        #
        # for a sdu coming from RLC layer, typically the logs look like this:
        #
        # mac.sdu--mac.harq len57::hqpid1.fm989.sl3.M3buf1714447104.Hbuf548946528.rvi0.ndi1
        # mac.harq--phy.tx len57::rntiff4c.nb_rb5.nb_sym3.mod_or6.hqpid1.fm989.sl3.Hbuf548946528.rvi0.ndi102
        #
        # for an sdu which is being retransmitted, it looks like this:
        #
        # mac.sdu--mac.harq len24::hqpid1.fm989.sl18.M3buf1697661696.Hbuf548946528.rvi2.ndi0
        # mac.harq--phy.tx len57::rntiff4c.nb_rb5.nb_sym3.mod_or6.hqpid1.fm989.sl18.Hbuf548946528.rvi2.ndi103
        #
        # here ndi is a crucial parameter which is only correct in 'mac.sdu--mac.harq'.
        # ndi stands for new data indicator, it is set in ul dci, when it is 1 it means look for new data from RLC
        # otherwise, retransmit.

        # therefore we first look for 'phy.tx'
        # mac.harq--phy.tx len57::rntiff4c.nb_rb5.nb_sym3.mod_or6.hqpid1.fm989.sl18.Hbuf548946528.rvi2.ndi103
        # if suc1, we look for mac.demuxed. otherwise we dont
        KW_PHY_TX = 'phy.tx'
        if ('--'+KW_PHY_TX in line):
            timestamp_match = re.search(r'^(\d+\.\d+)', line)
            len_match = re.search(r'len(\d+)', line)
            rnti_match = re.search(r'rnti([0-9a-fA-F]+)', line)
            nb_rb_match = re.search(r'nb_rb(\d+)', line)
            nb_sym_match = re.search(r'nb_sym(\d+)', line)
            mod_or_match = re.search(r'mod_or(\d+)', line)
            hqpid_match = re.search(r'hqpid(\d+)', line)
            fm_match = re.search(r'fm(\d+)', line)
            sl_match = re.search(r'sl(\d+)', line)
            hbuf_match = re.search(r'Hbuf(\d+)', line)
            rvi_match = re.search(r'rvi(\d+)', line)
            if timestamp_match and fm_match and sl_match and hqpid_match and len_match and hbuf_match and rnti_match and nb_rb_match and nb_sym_match and mod_or_match and rvi_match:
                timestamp = float(timestamp_match.group(1))
                fm_value = int(fm_match.group(1))
                sl_value = int(sl_match.group(1))
                hqpid_value = int(hqpid_match.group(1))
                rvi_value = int(rvi_match.group(1))
                hbuf_value = int(hbuf_match.group(1))
                nb_rb_value = int(nb_rb_match.group(1))
                nb_sym_value = int(nb_sym_match.group(1))
                mod_or_value = int(mod_or_match.group(1))
                len_value = int(len_match.group(1))
                rnti_value = rnti_match.group(1)
            else:
                logger.warning(f"For {KW_PHY_TX}, could not find properties in line {line_number}.")
                continue

            mac_dec_arr = {
                'mac_id' : len(mac_attempts),
                KW_PHY_TX: {
                    'timestamp': timestamp,
                    'Hbuf': hbuf_value,
                    'rvi': rvi_value,
                    'fm': fm_value,
                    'sl': sl_value,
                    'nb_rb' : nb_rb_value,
                    'nb_sym' : nb_sym_value,
                    'mod_or' : mod_or_value,
                    'len' : len_value,
                    'rnti' : rnti_value,
                    'hqpid' : hqpid_value
                }
            }
            logger.debug(f"Found '{KW_PHY_TX}' in line {line_number}, {mac_dec_arr[KW_PHY_TX]}")

            # lets go back in lines
            prev_lines = previous_lines.reverse_items()

            # find 'mac.harq'
            # mac.sdu--mac.harq len24::hqpid1.fm989.sl18.M3buf1697661696.Hbuf548946528.rvi2.ndi0
            hbufstr = f'Hbuf{hbuf_value}'
            fmstr = f'fm{fm_value}'
            slstr = f'sl{sl_value}'
            found_MAC_HARQ = False
            KW_MAC_HARQ = 'mac.harq'
            for jd,prev_ljne in enumerate(prev_lines):
                if (('--'+KW_MAC_HARQ) in prev_ljne) and (hbufstr in prev_ljne) and (fmstr in prev_ljne) and (slstr in prev_ljne):
                    timestamp_match = re.search(r'^(\d+\.\d+)', prev_ljne)
                    len_match = re.search(r'len(\d+)', prev_ljne)
                    hqpid_match = re.search(r'hqpid(\d+)', prev_ljne)
                    m3buf_match = re.search(r'M3buf(\d+)', prev_ljne)
                    rvi_match = re.search(r'rvi(\d+)', prev_ljne)
                    ndi_match = re.search(r'ndi(\d+)', prev_ljne)
                    if timestamp_match and len_match and hqpid_match and rvi_match and ndi_match and m3buf_match:
                        timestamp = float(timestamp_match.group(1))
                        len_value = int(len_match.group(1))
                        hqpid_value = int(hqpid_match.group(1))
                        rvi_value = int(rvi_match.group(1))
                        ndi_value = int(ndi_match.group(1))
                        m3buf_value = int(m3buf_match.group(1))
                    else:
                        logger.warning(f"[UE] For {KW_MAC_HARQ}, could not find properties in line {line_number-jd-1}. Skipping this '{KW_PHY_TX}'")
                        continue
                    mac_dec_arr[KW_MAC_HARQ] = {
                        'timestamp' : timestamp,
                        'hqpid': hqpid_value,
                        'rvi': rvi_value,
                        'len': len_value,
                        'ndi' : ndi_value,
                        'M3buf' : m3buf_value
                    }
                    logger.debug(f"[UE] Found '{KW_MAC_HARQ}','{hbufstr}','{fmstr}','{slstr}' in line {line_number-jd-1}, {mac_dec_arr[KW_MAC_HARQ]}")
                    found_MAC_HARQ = True
                    break

            if not found_MAC_HARQ:
                logger.warning(f"[UE] Could not find '{KW_MAC_HARQ}','{hbufstr}','{fmstr}','{slstr}' before {line_number} for {KW_PHY_TX}")
                continue

            mac_attempts.append(flatten_dict(mac_dec_arr))

    logger.info(f"Extracted {len(mac_attempts)} mac attempts on UE.")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(mac_attempts)
    return df
